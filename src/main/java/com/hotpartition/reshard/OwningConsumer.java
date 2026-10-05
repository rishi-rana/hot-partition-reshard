package com.hotpartition.reshard;

import com.hotpartition.concentration.KeyConcentrationGate;
import com.hotpartition.detection.DetectionGate;
import com.hotpartition.detection.ErrorRateTracker;
import com.hotpartition.detection.LambdaMuTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * The owning consumer: retains exclusive fetch/commit ownership of the Kafka
 * partition (no group-management bypass, per the design doc's Mechanism
 * section), and when the detection gate confirms genuine diverse-skew
 * hotness, routes records through {@link KeyBucketRouter} to
 * {@link BucketWorker}s instead of processing them inline. Offset commits are
 * gated by {@link WatermarkTracker#commitPoint()}.
 *
 * This prototype keeps the partition assignment manual (fixed single
 * partition, no consumer-group rebalancing) for determinism in the benchmark
 * harness; see README for the production-vs-prototype gap this implies.
 */
public final class OwningConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OwningConsumer.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final TopicPartition partition;
    private final BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor;

    private final ErrorRateTracker errorRateTracker = new ErrorRateTracker(0.2);
    private final LambdaMuTracker lambdaMuTracker = new LambdaMuTracker(0.3);
    private final DetectionGate detectionGate;
    private final KeyConcentrationGate<String> concentrationGate = new KeyConcentrationGate<>(20, 32, 42L);
    private final WatermarkTracker watermarkTracker = new WatermarkTracker();

    private int numBuckets;
    private KeyBucketRouter router;
    private final Map<Integer, BucketWorker> bucketWorkers = new HashMap<>();

    private boolean reshardActive = false;
    private boolean forcedDecision = false;
    private long lastCommittedOffset = -1;
    private long windowStartMs;
    private double measuredMuWorkerPerSec = 500.0;

    private long recordsProcessed = 0;

    /** Benchmark-only override: bypass the detection gate and force resharding on/off. Not part of the production design. */
    public void forceReshardActive(boolean active) {
        this.forcedDecision = true;
        this.reshardActive = active;
    }

    public OwningConsumer(KafkaConsumer<String, byte[]> consumer,
                           TopicPartition partition,
                           int numBuckets,
                           BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor) {
        this.consumer = consumer;
        this.partition = partition;
        this.numBuckets = numBuckets;
        this.router = new KeyBucketRouter(numBuckets, 0);
        this.processor = processor;
        this.windowStartMs = System.currentTimeMillis();

        Map<Integer, LambdaMuTracker> trackers = new HashMap<>();
        trackers.put(partition.partition(), lambdaMuTracker);
        this.detectionGate = new DetectionGate(errorRateTracker, partition.partition(), trackers,
                1.5, 0.6, 0.6);
    }

    public void assignAndSeek() {
        consumer.assign(java.util.List.of(partition));
    }

    private BucketWorker workerFor(int bucketId) {
        return bucketWorkers.computeIfAbsent(bucketId, id -> {
            BucketWorker w = new InProcessBucketWorker(id);
            w.start();
            return w;
        });
    }

    /** Runs one poll iteration. Returns the number of records processed. */
    public int pollOnce(Duration timeout) {
        ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
        long now = System.currentTimeMillis();

        long logEndOffset = consumer.endOffsets(java.util.List.of(partition)).get(partition);
        long position = consumer.position(partition);
        lambdaMuTracker.sample(now, logEndOffset, position);

        int count = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
            handleRecord(record);
            count++;
            recordsProcessed++;
        }

        maybeUpdateReshardDecision(now);
        maybeCommit();
        return count;
    }

    private void handleRecord(ConsumerRecord<String, byte[]> record) {
        errorRateTracker.recordProcessed();
        concentrationGate.record(record.key());

        if (!reshardActive) {
            try {
                processor.accept(record, () -> watermarkTracker.recordCompleted(0, record.offset()));
            } catch (RuntimeException e) {
                errorRateTracker.recordError();
            }
            watermarkTracker.recordDispatched(0, record.offset());
            return;
        }

        int bucket = router.bucketFor(record.key());
        watermarkTracker.recordDispatched(bucket, record.offset());
        workerFor(bucket).submit(record, processor, () -> watermarkTracker.recordCompleted(bucket, record.offset()));
    }

    private void maybeUpdateReshardDecision(long now) {
        if (forcedDecision) {
            return;
        }
        if (recordsProcessed == 0 || recordsProcessed % 50 != 0) {
            return;
        }
        DetectionGate.Verdict verdict = detectionGate.evaluate();
        if (verdict != DetectionGate.Verdict.GENUINE_HOT_LOAD) {
            reshardActive = false;
            return;
        }
        double windowSeconds = Math.max(1.0, (now - windowStartMs) / 1000.0);
        var result = concentrationGate.evaluate(windowSeconds, measuredMuWorkerPerSec);
        reshardActive = result.verdict() == com.hotpartition.concentration.KeyConcentrationGate.Verdict.DIVERSE_SKEWED;
        if (reshardActive) {
            log.info("partition {} resharding active: hhi={} dominantKey={} rate={}",
                    partition, result.hhi(), result.dominantKey(), result.dominantKeyEstimatedRate());
        }
    }

    private void maybeCommit() {
        long commitPoint;
        if (!reshardActive) {
            commitPoint = watermarkTracker.commitPoint();
        } else {
            commitPoint = watermarkTracker.commitPoint();
        }
        if (commitPoint > lastCommittedOffset) {
            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(commitPoint + 1)));
            lastCommittedOffset = commitPoint;
        }
    }

    public long lastCommittedOffset() {
        return lastCommittedOffset;
    }

    public boolean isReshardActive() {
        return reshardActive;
    }

    public WatermarkTracker watermarkTracker() {
        return watermarkTracker;
    }

    public void setMeasuredMuWorkerPerSec(double v) {
        this.measuredMuWorkerPerSec = v;
    }

    @Override
    public void close() {
        for (BucketWorker w : bucketWorkers.values()) {
            w.close();
        }
        consumer.close();
    }
}
