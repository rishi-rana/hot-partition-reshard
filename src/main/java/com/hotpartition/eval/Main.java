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
            if (Boolean.getBoolean("measureReactionLatency")) {
                runReactionLatency(bootstrapServers);
            } else if (Boolean.getBoolean("repeatedTrials")) {
                runRepeatedTrials(bootstrapServers);
            } else {
                runSweep(bootstrapServers);
            }
        } finally {
            if (container != null) {
                container.stop();
            }
        }
    }

    private static void runReactionLatency(String bootstrapServers) throws Exception {
        int numBuckets = Integer.getInteger("numBuckets", 4);
        int warmupBatches = Integer.getInteger("warmupBatches", 20);
        int warmupBatchSize = Integer.getInteger("warmupBatchSize", 20);
        long warmupIntervalMs = Long.getLong("warmupIntervalMs", 150L);
        int burstNumKeys = Integer.getInteger("burstNumKeys", 200);
        double burstSkew = Double.parseDouble(System.getProperty("burstSkew", "1.0"));
        int burstRecordCount = Integer.getInteger("burstRecordCount", 3000);
        long detectionTimeoutMs = Long.getLong("detectionTimeoutMs", 30000L);
        String topic = "reaction-latency-" + System.nanoTime();

        ReactionLatencyExperiment experiment = new ReactionLatencyExperiment();
        var result = experiment.run(bootstrapServers, topic, numBuckets,
                warmupBatches, warmupBatchSize, warmupIntervalMs,
                burstNumKeys, burstSkew, burstRecordCount, detectionTimeoutMs);

        System.out.println("detected,reactionLatencyMs,recordsProcessedAtDetection,falsePositiveDuringWarmup");
        System.out.printf("%s,%d,%d,%s%n", result.detected(), result.reactionLatencyMs(),
                result.recordsProcessedAtDetection(), result.falsePositiveDuringWarmup());
    }

    private static void runSweep(String bootstrapServers) throws Exception {
        int recordCount = Integer.getInteger("recordCount", 5000);
        int numKeys = Integer.getInteger("numKeys", 200);
        int numBuckets = Integer.getInteger("numBuckets", 4);
        long processingNanos = Long.getLong("processingNanosPerRecord", 50_000L);
        short replicationFactor = Short.parseShort(System.getProperty("replicationFactor", "1"));
        boolean includeSpillover = Boolean.getBoolean("includeSpillover");
        boolean includeRpc = Boolean.getBoolean("includeRpc");
        int rpcBasePort = Integer.getInteger("rpcBasePort", 9500);
        boolean includeBatchedRpc = Boolean.getBoolean("includeBatchedRpc");
        int rpcBatchSize = Integer.getInteger("rpcBatchSize", 50);
        long rpcLingerMs = Long.getLong("rpcLingerMs", 5L);
        boolean includeClaim = Boolean.getBoolean("includeClaim");
        int claimNumPeers = Integer.getInteger("claimNumPeers", numBuckets);
        boolean includeRangeBarrier = Boolean.getBoolean("includeRangeBarrier");
        long dependencyTimeoutMs = Long.getLong("dependencyTimeoutMs", 5000L);
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

            if (includeSpillover) {
                var spillover = runner.runFullMechanismSpillover(topic, tp, recordCount, numBuckets, processingNanos);
                results.add(withSkewHhi(spillover, skew, hhi));
            }

            if (includeRpc) {
                var rpc = runner.runFullMechanismRpc(topic, tp, recordCount, numBuckets, processingNanos, rpcBasePort);
                results.add(withSkewHhi(rpc, skew, hhi));
            }

            if (includeBatchedRpc) {
                var batchedRpc = runner.runFullMechanismBatchedRpc(topic, tp, recordCount, numBuckets,
                        processingNanos, rpcBasePort + 100, rpcBatchSize, rpcLingerMs);
                results.add(withSkewHhi(batchedRpc, skew, hhi));
            }

            if (includeClaim) {
                var claim = runner.runFullMechanismClaim(topic, tp, recordCount, numBuckets, claimNumPeers,
                        processingNanos);
                results.add(withSkewHhi(claim, skew, hhi));
            }

            if (includeRangeBarrier) {
                var rangeBarrier = runner.runFullMechanismRangeBarrier(topic, tp, recordCount, numBuckets,
                        dependencyTimeoutMs);
                results.add(withSkewHhi(rangeBarrier, skew, hhi));
            }
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

    /**
     * Every benchmark number in the paper's Evaluation section was previously a single
     * run. This repeats the seven headline configurations {@code trials} times each, at
     * two representative skew points (near-uniform and the most-skewed point already
     * reported), and reports mean/stddev/min/max rather than one number -- the gap this
     * mode exists to close. Each run method already seeks to the beginning and builds
     * fresh state per call (verified before writing this), so repeated calls against the
     * same already-produced topic are valid independent trials; the topic is produced
     * once per skew point, not once per trial, to keep this affordable.
     */
    private static void runRepeatedTrials(String bootstrapServers) throws Exception {
        int trials = Integer.getInteger("trials", 3);
        int recordCount = Integer.getInteger("recordCount", 20000);
        int numKeys = Integer.getInteger("numKeys", 2000);
        int numBuckets = Integer.getInteger("numBuckets", 4);
        long processingNanos = Long.getLong("processingNanosPerRecord", 50_000L);
        short replicationFactor = Short.parseShort(System.getProperty("replicationFactor", "3"));
        int rpcBasePort = Integer.getInteger("rpcBasePort", 9500);
        int rpcBatchSize = Integer.getInteger("rpcBatchSize", 50);
        long rpcLingerMs = Long.getLong("rpcLingerMs", 5L);
        long dependencyTimeoutMs = Long.getLong("dependencyTimeoutMs", 5000L);
        double[] skews = {0.0, 2.5};

        SyntheticWorkloadProducer producer = new SyntheticWorkloadProducer(bootstrapServers);
        BenchmarkRunner runner = new BenchmarkRunner(bootstrapServers, processingNanos);

        System.out.println("config,skew,hhi,trials,meanRecordsPerSec,stddevRecordsPerSec,minRecordsPerSec,maxRecordsPerSec");

        for (double skew : skews) {
            String topic = "trials-skew-" + String.valueOf(skew).replace('.', '_') + "-" + System.nanoTime();
            producer.ensureTopic(topic, replicationFactor);
            producer.produce(topic, recordCount, numKeys, skew, 42L, 64);
            double hhi = SyntheticWorkloadProducer.theoreticalHhi(numKeys, skew, 42L);
            TopicPartition tp = new TopicPartition(topic, 0);
            log.info("=== repeated trials: skew={} theoreticalHHI={} ===", skew, hhi);

            reportTrials("baseline", skew, hhi, trials,
                    () -> runner.runBaseline(topic, tp, recordCount));
            reportTrials("naive-sharding-unsafe-commit", skew, hhi, trials,
                    () -> runner.runNaiveShardingUnsafeCommit(topic, tp, recordCount, numBuckets));
            reportTrials("full-mechanism", skew, hhi, trials,
                    () -> runner.runFullMechanism(topic, tp, recordCount, numBuckets));
            reportTrials("full-mechanism-spillover", skew, hhi, trials,
                    () -> call(() -> runner.runFullMechanismSpillover(topic, tp, recordCount, numBuckets, processingNanos)));
            reportTrials("full-mechanism-batched-rpc", skew, hhi, trials,
                    () -> call(() -> runner.runFullMechanismBatchedRpc(topic, tp, recordCount, numBuckets,
                            processingNanos, rpcBasePort + 100, rpcBatchSize, rpcLingerMs)));
            reportTrials("full-mechanism-claim", skew, hhi, trials,
                    () -> call(() -> runner.runFullMechanismClaim(topic, tp, recordCount, numBuckets, numBuckets,
                            processingNanos)));
            reportTrials("full-mechanism-range-barrier", skew, hhi, trials,
                    () -> runner.runFullMechanismRangeBarrier(topic, tp, recordCount, numBuckets, dependencyTimeoutMs));
        }
    }

    private interface ThrowingSupplier {
        BenchmarkRunner.Result get() throws Exception;
    }

    private static BenchmarkRunner.Result call(ThrowingSupplier s) {
        try {
            return s.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void reportTrials(String config, double skew, double hhi, int trials, ThrowingSupplier fn) {
        double[] samples = new double[trials];
        for (int t = 0; t < trials; t++) {
            BenchmarkRunner.Result r = call(fn);
            samples[t] = r.recordsPerSec();
            log.info("trial {}/{} for {} at skew={}: {} records/sec", t + 1, trials, config, skew, r.recordsPerSec());
        }
        double mean = 0;
        for (double s : samples) {
            mean += s;
        }
        mean /= trials;
        double variance = 0;
        for (double s : samples) {
            variance += (s - mean) * (s - mean);
        }
        double stddev = Math.sqrt(variance / trials);
        double min = samples[0];
        double max = samples[0];
        for (double s : samples) {
            min = Math.min(min, s);
            max = Math.max(max, s);
        }
        System.out.printf("%s,%.2f,%.4f,%d,%.1f,%.1f,%.1f,%.1f%n", config, skew, hhi, trials, mean, stddev, min, max);
    }
}
