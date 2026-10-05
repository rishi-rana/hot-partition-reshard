package com.hotpartition.eval;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;

/**
 * CLI entry point for the skew-sensitivity benchmark. Uses an existing Kafka
 * broker if {@code -Dbootstrap.servers=host:port} (or the BOOTSTRAP_SERVERS
 * env var) is set; otherwise spins up a disposable broker via Testcontainers.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        String external = System.getProperty("bootstrap.servers", System.getenv("BOOTSTRAP_SERVERS"));
        KafkaContainer container = null;
        String bootstrapServers;

        if (external != null && !external.isBlank()) {
            bootstrapServers = external;
            log.info("using external Kafka broker at {}", bootstrapServers);
        } else {
            log.info("no bootstrap.servers provided; starting a disposable broker via Testcontainers");
            container = new KafkaContainer(DockerImageName.parse("apache/kafka:4.3.1"));
            container.start();
            bootstrapServers = container.getBootstrapServers();
            log.info("Testcontainers Kafka broker started at {}", bootstrapServers);
        }

        try {
            runSweep(bootstrapServers);
        } finally {
            if (container != null) {
                container.stop();
            }
        }
    }

    private static void runSweep(String bootstrapServers) {
        int recordCount = Integer.getInteger("recordCount", 5000);
        int numKeys = Integer.getInteger("numKeys", 200);
        int numBuckets = Integer.getInteger("numBuckets", 4);
        long processingNanos = Long.getLong("processingNanosPerRecord", 50_000L);
        short replicationFactor = Short.parseShort(System.getProperty("replicationFactor", "1"));
        double[] skews = {0.0, 0.5, 1.0, 1.5, 2.5};

        SyntheticWorkloadProducer producer = new SyntheticWorkloadProducer(bootstrapServers);
        BenchmarkRunner runner = new BenchmarkRunner(bootstrapServers, processingNanos);

        List<BenchmarkRunner.Result> results = new ArrayList<>();
        for (double skew : skews) {
            String topic = "bench-skew-" + String.valueOf(skew).replace('.', '_');
            producer.ensureTopic(topic, replicationFactor);
            producer.produce(topic, recordCount, numKeys, skew, 42L, 64);
            double hhi = SyntheticWorkloadProducer.theoreticalHhi(numKeys, skew, 42L);
            TopicPartition tp = new TopicPartition(topic, 0);

            log.info("=== skew={} theoreticalHHI={} ===", skew, hhi);

            // Each run below uses its own consumer group, so all three configurations can
            // independently replay the same already-produced topic from the beginning.
            var baseline = runner.runBaseline(topic, tp, recordCount);
            results.add(withSkewHhi(baseline, skew, hhi));

            var naive = runner.runNaiveShardingUnsafeCommit(topic, tp, recordCount, numBuckets);
            results.add(withSkewHhi(naive, skew, hhi));

            var full = runner.runFullMechanism(topic, tp, recordCount, numBuckets);
            results.add(withSkewHhi(full, skew, hhi));
        }

        System.out.println("config,skew,hhi,recordCount,elapsedMs,recordsPerSec");
        for (var r : results) {
            System.out.printf("%s,%.2f,%.4f,%d,%d,%.1f%n",
                    r.config(), r.skew(), r.hhi(), r.recordCount(), r.elapsedMs(), r.recordsPerSec());
        }
    }

    private static BenchmarkRunner.Result withSkewHhi(BenchmarkRunner.Result r, double skew, double hhi) {
        return new BenchmarkRunner.Result(r.config(), skew, hhi, r.recordCount(), r.elapsedMs(), r.recordsPerSec());
    }
}
