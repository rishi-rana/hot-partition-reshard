package com.hotpartition.eval;

import com.hotpartition.reshard.OwningConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Skew-sensitivity evaluation: runs the same synthetic workload through three
 * configurations and reports drain throughput, per the design doc's Next
 * Steps. (a) baseline single-threaded consumer, (b) naive key-hash sharding
 * with no commit-watermark coordination (to isolate what the watermark
 * mechanism costs), (c) the full OwningConsumer mechanism.
 */
public final class BenchmarkRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private final String bootstrapServers;
    private final long simulatedProcessingNanosPerRecord;

    public BenchmarkRunner(String bootstrapServers, long simulatedProcessingNanosPerRecord) {
        this.bootstrapServers = bootstrapServers;
        this.simulatedProcessingNanosPerRecord = simulatedProcessingNanosPerRecord;
    }

    public record Result(String config, double skew, double hhi, int recordCount, long elapsedMs, double recordsPerSec) {
    }

    private void busyWork() {
        long end = System.nanoTime() + simulatedProcessingNanosPerRecord;
        while (System.nanoTime() < end) {
            // simulate per-record processing cost
        }
    }

    public Result runBaseline(String topic, TopicPartition tp, int recordCount) {
        Properties props = baseConsumerProps("baseline-" + topic);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            long start = System.currentTimeMillis();
            long processed = 0;
            while (processed < recordCount) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> r : records) {
                    busyWork();
                    processed++;
                }
                if (!records.isEmpty()) {
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(consumer.position(tp))));
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("baseline", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        }
    }

    public Result runNaiveShardingUnsafeCommit(String topic, TopicPartition tp, int recordCount, int numBuckets) {
        Properties props = baseConsumerProps("naive-" + topic);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));

            var workers = new com.hotpartition.reshard.InProcessBucketWorker[numBuckets];
            for (int i = 0; i < numBuckets; i++) {
                workers[i] = new com.hotpartition.reshard.InProcessBucketWorker(i);
                workers[i].start();
            }
            var router = new com.hotpartition.reshard.KeyBucketRouter(numBuckets, 0);
            java.util.concurrent.atomic.AtomicLong completed = new java.util.concurrent.atomic.AtomicLong();

            long start = System.currentTimeMillis();
            long dispatched = 0;
            while (dispatched < recordCount) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> r : records) {
                    int bucket = router.bucketFor(r.key());
                    workers[bucket].submit(r, (rec, done) -> {
                        busyWork();
                        done.run();
                    }, completed::incrementAndGet);
                    dispatched++;
                }
                if (!records.isEmpty()) {
                    // unsafe: commits the raw dispatched position regardless of whether buckets
                    // have actually finished processing it yet -- this is exactly the hazard the
                    // watermark mechanism exists to avoid, kept here only to isolate its cost.
                    consumer.commitSync(Map.of(tp, new OffsetAndMetadata(consumer.position(tp))));
                }
            }
            while (completed.get() < recordCount) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            for (var w : workers) {
                w.close();
            }
            return new Result("naive-sharding-unsafe-commit", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        }
    }

    public Result runFullMechanism(String topic, TopicPartition tp, int recordCount, int numBuckets) {
        Properties props = baseConsumerProps("full-" + topic);
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        try (OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, numBuckets, (rec, done) -> {
            busyWork();
            done.run();
        })) {
            owner.assignAndSeek();
            kafkaConsumer.seekToBeginning(List.of(tp));
            owner.forceReshardActive(true);

            long start = System.currentTimeMillis();
            long endOffset = kafkaConsumer.endOffsets(List.of(tp)).get(tp);
            int iter = 0;
            while (owner.lastCommittedOffset() + 1 < endOffset) {
                long t0 = System.currentTimeMillis();
                int n = owner.pollOnce(Duration.ofMillis(500));
                iter++;
                if (Boolean.getBoolean("debugFullMechanism")) {
                    log.info("DEBUG iter={} tookMs={} fetched={} committed={} position={} endOffset={}",
                            iter, System.currentTimeMillis() - t0, n, owner.lastCommittedOffset(),
                            kafkaConsumer.position(tp), endOffset);
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("full-mechanism", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        }
    }

    /**
     * Same drain as {@link #runFullMechanism}, but delegating via transport Option B
     * (spillover topic) to genuinely separate OS processes instead of in-process threads
     * -- the actual cross-process cost the design doc's Mechanism gap calls for measuring,
     * not the in-process dispatch cost {@link #runFullMechanism} measures.
     */
    public Result runFullMechanismSpillover(String topic, TopicPartition tp, int recordCount, int numBuckets,
                                             long processingNanosPerRecord) throws Exception {
        var topics = new com.hotpartition.transport.spillover.SpilloverTopics(topic);
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrapServers);
        try (org.apache.kafka.clients.admin.Admin admin = org.apache.kafka.clients.admin.Admin.create(adminProps)) {
            topics.ensureExists(admin, numBuckets, (short) 1);
        }

        List<Process> peers = new ArrayList<>();
        for (int b = 0; b < numBuckets; b++) {
            peers.add(new ProcessBuilder(
                    "java", "-cp", System.getProperty("java.class.path"),
                    "com.hotpartition.transport.spillover.SpilloverPeerWorker",
                    bootstrapServers, topic, String.valueOf(b), String.valueOf(processingNanosPerRecord))
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start());
        }
        // give the peer JVMs a moment to start and subscribe before the sweep begins.
        Thread.sleep(2000);

        Properties props = baseConsumerProps("spillover-" + topic);
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        try (OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, numBuckets, (rec, done) -> {
            throw new IllegalStateException("processor must not run on the owner side for the spillover transport");
        }, bucketId -> new com.hotpartition.transport.spillover.SpilloverBucketWorker(
                bucketId, tp, topics, spilloverProducer(), spilloverCompletionsConsumer(topic, bucketId)))) {
            owner.assignAndSeek();
            kafkaConsumer.seekToBeginning(List.of(tp));
            owner.forceReshardActive(true);

            long start = System.currentTimeMillis();
            long endOffset = kafkaConsumer.endOffsets(List.of(tp)).get(tp);
            while (owner.lastCommittedOffset() + 1 < endOffset) {
                owner.pollOnce(Duration.ofMillis(500));
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("full-mechanism-spillover", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        } finally {
            for (Process p : peers) {
                p.destroy();
            }
            for (Process p : peers) {
                p.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
                p.destroyForcibly();
            }
        }
    }

    /**
     * Same drain as {@link #runFullMechanism}, but delegating via transport Option A
     * (direct RPC) to genuinely separate OS processes. Despite having no broker hop at
     * all for the delegation path, this measured *slower* than Option B (spillover) --
     * see {@link #runFullMechanismBatchedRpc} for the batching follow-up this motivated.
     */
    public Result runFullMechanismRpc(String topic, TopicPartition tp, int recordCount, int numBuckets,
                                       long processingNanosPerRecord, int basePort) throws Exception {
        List<Process> peers = new ArrayList<>();
        for (int b = 0; b < numBuckets; b++) {
            int port = basePort + b;
            peers.add(new ProcessBuilder(
                    "java", "-cp", System.getProperty("java.class.path"),
                    "com.hotpartition.transport.rpc.RpcPeerWorker",
                    String.valueOf(port), String.valueOf(b), String.valueOf(processingNanosPerRecord))
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start());
        }
        // give the peer JVMs a moment to start listening before the sweep begins.
        Thread.sleep(2000);

        Properties props = baseConsumerProps("rpc-" + topic);
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        try (OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, numBuckets, (rec, done) -> {
            throw new IllegalStateException("processor must not run on the owner side for the RPC transport");
        }, bucketId -> new com.hotpartition.transport.rpc.RpcBucketWorker(bucketId, "localhost", basePort + bucketId))) {
            owner.assignAndSeek();
            kafkaConsumer.seekToBeginning(List.of(tp));
            owner.forceReshardActive(true);

            long start = System.currentTimeMillis();
            long endOffset = kafkaConsumer.endOffsets(List.of(tp)).get(tp);
            while (owner.lastCommittedOffset() + 1 < endOffset) {
                owner.pollOnce(Duration.ofMillis(500));
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("full-mechanism-rpc", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        } finally {
            for (Process p : peers) {
                p.destroy();
            }
            for (Process p : peers) {
                p.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
                p.destroyForcibly();
            }
        }
    }

    /**
     * Follow-up to {@link #runFullMechanismRpc}: tests whether giving direct RPC a
     * batching primitive (Kafka-style batch.size/linger.ms) closes the throughput gap
     * against the spillover transport, per the design doc's hypothesis that the gap is
     * explained by Kafka's own client-side batching rather than anything intrinsic to
     * avoiding the broker hop.
     */
    public Result runFullMechanismBatchedRpc(String topic, TopicPartition tp, int recordCount, int numBuckets,
                                              long processingNanosPerRecord, int basePort,
                                              int rpcBatchSize, long rpcLingerMs) throws Exception {
        List<Process> peers = new ArrayList<>();
        for (int b = 0; b < numBuckets; b++) {
            int port = basePort + b;
            peers.add(new ProcessBuilder(
                    "java", "-cp", System.getProperty("java.class.path"),
                    "com.hotpartition.transport.rpc.BatchedRpcPeerWorker",
                    String.valueOf(port), String.valueOf(b), String.valueOf(processingNanosPerRecord))
                    .redirectErrorStream(true)
                    .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                    .start());
        }
        Thread.sleep(2000);

        Properties props = baseConsumerProps("batched-rpc-" + topic);
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
        try (OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, numBuckets, (rec, done) -> {
            throw new IllegalStateException("processor must not run on the owner side for the batched RPC transport");
        }, bucketId -> new com.hotpartition.transport.rpc.BatchedRpcBucketWorker(
                bucketId, "localhost", basePort + bucketId, rpcBatchSize, rpcLingerMs))) {
            owner.assignAndSeek();
            kafkaConsumer.seekToBeginning(List.of(tp));
            owner.forceReshardActive(true);

            long start = System.currentTimeMillis();
            long endOffset = kafkaConsumer.endOffsets(List.of(tp)).get(tp);
            while (owner.lastCommittedOffset() + 1 < endOffset) {
                owner.pollOnce(Duration.ofMillis(500));
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("full-mechanism-batched-rpc", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        } finally {
            for (Process p : peers) {
                p.destroy();
            }
            for (Process p : peers) {
                p.waitFor(10, java.util.concurrent.TimeUnit.SECONDS);
                p.destroyForcibly();
            }
        }
    }

    private KafkaProducer<String, byte[]> spilloverProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> spilloverCompletionsConsumer(String topic, int bucketId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", "spillover-owner-" + topic + "-" + bucketId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private Properties baseConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    private static double rate(int count, long elapsedMs) {
        return elapsedMs == 0 ? 0 : count / (elapsedMs / 1000.0);
    }
}
