package com.hotpartition.transport.claim;

import com.hotpartition.admission.PeerCapacityAdmission;
import com.hotpartition.discovery.RegistryEntry;
import com.hotpartition.discovery.RegistryTopic;
import com.hotpartition.transport.spillover.SpilloverTopics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Peer-side half of transport Option C (work-stealing claim topic). Reuses the exact same
 * data path as transport Option B -- the spillover and completions topics, the same
 * header-based envelope, even the same owner-side {@code SpilloverBucketWorker} (the
 * owner's job, produce to a bucket's partition and wait for an ack, is identical no
 * matter how peers decide who owns which partition). The only thing Option C changes is
 * *how a bucket gets claimed*: instead of each peer being manually pinned to one
 * partition ({@code SpilloverPeerWorker}'s {@code assign()}), every claim-peer instance
 * for the same hot partition {@code subscribe()}s to the spillover topic under one shared
 * consumer group id -- so Kafka's own rebalance protocol decides partition ownership
 * among whichever peers are currently alive and have joined. That rebalance decision
 * *is* the claim: no hand-rolled distributed lock, no custom conflict-resolution
 * protocol, just the same group-membership mechanism Kafka already uses everywhere else.
 * It also gives this transport something neither A nor B has for free: if a claim-peer
 * process dies, Kafka automatically reassigns its partition(s) to a surviving group
 * member, and if there are more peers than buckets, the excess simply receives no
 * partitions and sits idle until needed -- genuine elastic, fault-tolerant work
 * assignment, not just load distribution.
 *
 * Because a single instance may end up owning zero, one, or several bucket partitions
 * depending on how the group rebalances, {@code bucketId} for acking purposes is read
 * per-record from {@code record.partition()} rather than fixed at construction time, the
 * way {@link com.hotpartition.transport.spillover.SpilloverPeerWorker} can fix it.
 *
 * <p><b>Admission control here is shedding, not a synchronous accept/reject -- same
 * asymmetry with transport Option A as {@link com.hotpartition.transport.spillover.SpilloverPeerWorker},
 * and even more pronounced here:</b> this transport's entire selling point is that Kafka's
 * own rebalance protocol hands this peer whichever partitions it hands it, with no hook
 * for this peer to refuse an assignment up front. So, as with Option B, this peer tracks
 * its own aggregate estimated incoming rate (summed across every partition the current
 * rebalance gave it, since one instance may hold several) against the *same*
 * {@link PeerCapacityAdmission} primitive, and on overload it (a) calls
 * {@code consumer.pause()} on all of its currently-assigned partitions -- stops pulling
 * more work, it cannot un-claim a partition on demand -- and (b) immediately publishes an
 * out-of-band overload entry to {@code __hotpartition_registry} rather than waiting for
 * the next heartbeat, exactly as Option B does. This is deliberately not claimed to be
 * equivalent to Option A's accept/reject handshake.
 */
public final class ClaimPeerWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ClaimPeerWorker.class);

    private static final long OVERLOAD_RECHECK_INTERVAL_MS = 500;
    private static final String AGGREGATE_CLAIM_ID = "claim-peer-aggregate";

    private final SpilloverTopics topics;
    private final KafkaConsumer<String, byte[]> spilloverConsumer;
    private final KafkaProducer<String, String> completionsProducer;
    private final long simulatedProcessingNanosPerRecord;
    private final AtomicBoolean running = new AtomicBoolean(true);

    // Shedding/backpressure support -- all nullable, off by default (back-compat).
    private final PeerCapacityAdmission admission;
    private final KafkaProducer<String, String> registryProducer;
    private final String consumerInstanceId;
    private volatile boolean overloaded = false;
    private volatile Set<TopicPartition> currentlyAssigned = Set.of();

    public ClaimPeerWorker(SpilloverTopics topics, KafkaConsumer<String, byte[]> spilloverConsumer,
                            KafkaProducer<String, String> completionsProducer,
                            long simulatedProcessingNanosPerRecord) {
        this(topics, spilloverConsumer, completionsProducer, simulatedProcessingNanosPerRecord, null, null, null);
    }

    /** See the shedding doc on {@link SpilloverPeerWorker}'s matching constructor -- same contract here. */
    public ClaimPeerWorker(SpilloverTopics topics, KafkaConsumer<String, byte[]> spilloverConsumer,
                            KafkaProducer<String, String> completionsProducer,
                            long simulatedProcessingNanosPerRecord,
                            PeerCapacityAdmission admission, KafkaProducer<String, String> registryProducer,
                            String consumerInstanceId) {
        this.topics = topics;
        this.spilloverConsumer = spilloverConsumer;
        this.completionsProducer = completionsProducer;
        this.simulatedProcessingNanosPerRecord = simulatedProcessingNanosPerRecord;
        this.admission = admission;
        this.registryProducer = registryProducer;
        this.consumerInstanceId = consumerInstanceId;
    }

    public boolean isOverloaded() {
        return overloaded;
    }

    public void stop() {
        running.set(false);
        spilloverConsumer.wakeup();
    }

    /** Blocks, consuming and acking whatever partitions the group rebalance assigns, until {@link #stop()}. */
    public void run() {
        spilloverConsumer.subscribe(List.of(topics.spilloverTopic()), new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                log.info("claim peer revoked buckets {}", partitions.stream().map(p -> p.partition()).toList());
                currentlyAssigned = Set.copyOf(currentlyAssigned.stream()
                        .filter(tp -> !partitions.contains(tp)).toList());
            }

            @Override
            public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                log.info("claim peer won buckets {}", partitions.stream().map(p -> p.partition()).toList());
                var merged = new java.util.HashSet<>(currentlyAssigned);
                merged.addAll(partitions);
                currentlyAssigned = Set.copyOf(merged);
            }
        });

        long windowStartMs = System.currentTimeMillis();
        int windowRecords = 0;

        while (running.get()) {
            try {
                ConsumerRecords<String, byte[]> records = spilloverConsumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, byte[]> record : records) {
                    int bucketId = record.partition();
                    long origOffset = headerLong(record, SpilloverTopics.HEADER_ORIG_OFFSET);
                    busyWork();
                    completionsProducer.send(new ProducerRecord<>(
                            topics.completionsTopic(), bucketId, null, Long.toString(origOffset)));
                }
                windowRecords += records.count();

                if (admission != null) {
                    long now = System.currentTimeMillis();
                    long elapsedMs = now - windowStartMs;
                    if (elapsedMs >= OVERLOAD_RECHECK_INTERVAL_MS) {
                        double estimatedLambda = windowRecords / (elapsedMs / 1000.0);
                        reevaluateAdmission(estimatedLambda);
                        windowStartMs = now;
                        windowRecords = 0;
                    }
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // expected on stop()
            } catch (RuntimeException e) {
                log.warn("claim peer worker poll failed", e);
            }
        }
    }

    private void reevaluateAdmission(double estimatedLambda) {
        admission.release(AGGREGATE_CLAIM_ID);
        PeerCapacityAdmission.AdmissionResult result = admission.tryAdmit(AGGREGATE_CLAIM_ID, estimatedLambda);
        boolean nowOverloaded = result == PeerCapacityAdmission.AdmissionResult.REJECT;
        Set<TopicPartition> assigned = currentlyAssigned;

        if (nowOverloaded && !overloaded) {
            if (!assigned.isEmpty()) {
                spilloverConsumer.pause(assigned);
            }
            log.warn("claim peer overloaded across {} assigned partitions: estimatedLambda={} > muWorkerCapacity={} "
                    + "-- pausing and publishing out-of-band overload signal",
                    assigned.size(), estimatedLambda, admission.muWorkerCapacity());
            publishOverloadSignal(assigned, estimatedLambda);
        } else if (!nowOverloaded && overloaded) {
            if (!assigned.isEmpty()) {
                spilloverConsumer.resume(assigned);
            }
            log.info("claim peer recovered: estimatedLambda={} <= muWorkerCapacity={} -- resuming",
                    estimatedLambda, admission.muWorkerCapacity());
        }
        overloaded = nowOverloaded;
    }

    private void publishOverloadSignal(Set<TopicPartition> assigned, double estimatedLambda) {
        if (registryProducer == null || consumerInstanceId == null) {
            return;
        }
        RegistryEntry overloadEntry = new RegistryEntry(
                consumerInstanceId, "claim-peer", assigned,
                estimatedLambda, admission.muWorkerCapacity(), admission.muWorkerCapacity(),
                System.currentTimeMillis(), "");
        try {
            registryProducer.send(new ProducerRecord<>(RegistryTopic.NAME, consumerInstanceId, overloadEntry.serialize()));
        } catch (RuntimeException e) {
            log.warn("claim peer failed to publish out-of-band overload signal", e);
        }
    }

    private void busyWork() {
        long end = System.nanoTime() + simulatedProcessingNanosPerRecord;
        while (System.nanoTime() < end) {
            // simulate per-record processing cost, matching the other transports' stubs
        }
    }

    private static long headerLong(ConsumerRecord<?, ?> record, String key) {
        var header = record.headers().lastHeader(key);
        if (header == null) {
            throw new IllegalStateException("missing header " + key + " on spillover record at offset " + record.offset());
        }
        byte[] b = header.value();
        long v = 0;
        for (byte value : b) {
            v = (v << 8) | (value & 0xFF);
        }
        return v;
    }

    @Override
    public void close() {
        stop();
        spilloverConsumer.close();
        completionsProducer.close();
    }

    /** Launches as a standalone process: args = bootstrapServers, namePrefix, processingNanosPerRecord. */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("usage: ClaimPeerWorker <bootstrapServers> <namePrefix> <processingNanosPerRecord>");
            System.exit(1);
        }
        String bootstrapServers = args[0];
        SpilloverTopics topics = new SpilloverTopics(args[1]);
        long processingNanos = Long.parseLong(args[2]);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        // shared group id across every claim-peer instance for this namePrefix -- this is
        // what makes partition assignment a "claim" rather than a pre-decided pin.
        consumerProps.put("group.id", "claim-peers-" + args[1]);
        // keep failure-detection latency low: a dead peer's partitions should get
        // reassigned quickly, not after the 45s classic-protocol default.
        consumerProps.put("session.timeout.ms", "6000");
        consumerProps.put("heartbeat.interval.ms", "2000");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        try (ClaimPeerWorker worker = new ClaimPeerWorker(topics,
                new KafkaConsumer<>(consumerProps), new KafkaProducer<>(producerProps), processingNanos)) {
            Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
            log.info("claim peer worker starting, group={}, bootstrap={}", "claim-peers-" + args[1], bootstrapServers);
            worker.run();
        }
    }
}
