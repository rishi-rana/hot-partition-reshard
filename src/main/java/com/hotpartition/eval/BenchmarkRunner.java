package com.hotpartition.eval;

import com.hotpartition.reshard.OwningConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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
            while (owner.lastCommittedOffset() + 1 < endOffset) {
                owner.pollOnce(Duration.ofMillis(500));
            }
            long elapsed = System.currentTimeMillis() - start;
            return new Result("full-mechanism", 0, 0, recordCount, elapsed, rate(recordCount, elapsed));
        }
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
