package com.hotpartition.eval;

import com.hotpartition.reshard.OwningConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Measures this mechanism's own reaction latency -- the one number the paper's claim
 * "virtual resharding reacts faster than physical resharding" needs and did not previously
 * have, regardless of whether a comparable published Pravega number exists. Unlike the
 * skew-sensitivity benchmarks elsewhere in this package, the detection gate here is NOT
 * forced active: {@link OwningConsumer#forceReshardActive} is never called, so the
 * measured latency reflects the real, non-forced path through the lambda/mu detection gate
 * (Section 4 of the design) and the key-concentration gate (Section 4, stage 3), exactly
 * as a production deployment would experience it.
 *
 * Methodology: a producer thread runs concurrently with the owner's poll loop (not a
 * pre-produced, already-sitting backlog) -- this matters, because lambda/mu detection is
 * fundamentally about rate-of-change over wall-clock time, and a backlog the owner just
 * happens to read through quickly would not exercise that at all. The producer runs a
 * quiet warm-up phase (a modest, steady rate, near-uniform keys) long enough for the
 * lambda/mu EMA baselines to actually converge, then switches without warning to a burst
 * phase (full producer speed, moderately skewed keys) that should trigger first "genuine
 * hot load" and then "diverse-but-skewed" within a handful of poll iterations. The
 * measured latency is wall-clock time from the moment the burst phase begins producing to
 * the moment {@link OwningConsumer#isReshardActive()} first reports true.
 */
public final class ReactionLatencyExperiment {

    private static final Logger log = LoggerFactory.getLogger(ReactionLatencyExperiment.class);

    public record Result(long reactionLatencyMs, long recordsProcessedAtDetection, boolean detected,
                          boolean falsePositiveDuringWarmup) {
    }

    public Result run(String bootstrapServers, String topic, int numBuckets,
                       int warmupBatches, int warmupBatchSize, long warmupIntervalMs,
                       int burstNumKeys, double burstSkew, int burstRecordCount,
                       long detectionTimeoutMs) throws Exception {
        SyntheticWorkloadProducer topicHelper = new SyntheticWorkloadProducer(bootstrapServers);
        topicHelper.ensureTopic(topic, (short) 1);
        TopicPartition tp = new TopicPartition(topic, 0);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("group.id", "reaction-latency-" + topic);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProps);

        OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, numBuckets, (rec, done) -> {
            done.run(); // negligible processing cost: isolates detection latency from processing cost
        });
        owner.assignAndSeek();
        kafkaConsumer.seekToBeginning(List.of(tp));

        AtomicBoolean detected = new AtomicBoolean(false);
        AtomicLong detectedAtMs = new AtomicLong(-1);
        AtomicLong recordsAtDetection = new AtomicLong(-1);
        AtomicBoolean ownerRunning = new AtomicBoolean(true);

        Thread ownerThread = new Thread(() -> {
            while (ownerRunning.get()) {
                owner.pollOnce(Duration.ofMillis(50));
                // Recorded unconditionally, even during warm-up: a detection that fires
                // before the burst phase begins is not a measurement error to hide, it is
                // itself a finding (the gate triggering on ordinary warm-up traffic), and
                // run() below checks timing against burstStartMs to tell the two apart
                // rather than silently reporting whichever fired first as "the" latency.
                if (owner.isReshardActive() && detected.compareAndSet(false, true)) {
                    detectedAtMs.set(System.currentTimeMillis());
                    recordsAtDetection.set(owner.lastCommittedOffset());
                }
            }
        }, "reaction-latency-owner");
        ownerThread.setDaemon(true);
        ownerThread.start();

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
        Random random = new Random(7);

        try {
            // warm-up: genuinely smooth, steady rate (one record per sub-interval, not a
            // burst-of-batchSize followed by a sleep) -- a bursty warm-up creates its own
            // lambda spikes that can trigger detection before the intended burst even
            // starts, which is exactly what an earlier version of this experiment did.
            long perRecordSleepMs = Math.max(1, warmupIntervalMs / warmupBatchSize);
            for (int b = 0; b < warmupBatches; b++) {
                for (int i = 0; i < warmupBatchSize; i++) {
                    String key = "warmup-key-" + random.nextInt(warmupBatchSize * 4);
                    producer.send(new ProducerRecord<>(topic, 0, key,
                            ("v" + i).getBytes(StandardCharsets.UTF_8)));
                    producer.flush();
                    Thread.sleep(perRecordSleepMs);
                }
            }

            if (detected.get()) {
                log.warn("reaction latency experiment: detection fired during warm-up, before the burst phase "
                        + "even began -- this run measures a false-positive trigger, not burst reaction latency");
                return new Result(-1, -1, false, true);
            }

            // burst: no warning, full producer speed, moderately skewed keys -- this is the
            // instant the mechanism should be trying to react to.
            long burstStartMs = System.currentTimeMillis();
            ZipfKeyGenerator keyGen = new ZipfKeyGenerator(burstNumKeys, burstSkew, 42L);
            for (int i = 0; i < burstRecordCount; i++) {
                producer.send(new ProducerRecord<>(topic, 0, keyGen.nextKey(),
                        ("v" + i).getBytes(StandardCharsets.UTF_8)));
            }
            producer.flush();

            long deadline = burstStartMs + detectionTimeoutMs;
            while (!detected.get() && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }

            ownerRunning.set(false);
            ownerThread.join(2000);

            if (!detected.get()) {
                log.warn("reaction latency experiment: detection did not fire within {} ms", detectionTimeoutMs);
                return new Result(-1, -1, false, false);
            }
            long latency = detectedAtMs.get() - burstStartMs;
            return new Result(latency, recordsAtDetection.get(), true, false);
        } finally {
            producer.close();
            owner.close();
        }
    }
}
