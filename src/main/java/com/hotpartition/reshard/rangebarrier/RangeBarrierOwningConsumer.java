package com.hotpartition.reshard.rangebarrier;

import com.hotpartition.reshard.WatermarkTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * The fallback mechanism's owner side: unlike {@link com.hotpartition.reshard.OwningConsumer}
 * (which can dispatch incrementally as records arrive, since key-bucket routing needs no
 * lookahead), this mechanism requires knowing every key's dependency chain across the
 * <em>entire</em> candidate span before any record in it is safe to dispatch -- so it
 * necessarily buffers the whole span into memory during one pre-scan pass, rather than
 * streaming through {@code pollOnce()} calls the way the key-bucket mechanism does. That
 * buffering is the pre-scan cost the design doc's Mechanism section flagged as this
 * design's main risk, made concrete rather than left as a qualitative claim.
 */
public final class RangeBarrierOwningConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RangeBarrierOwningConsumer.class);

    private final KafkaConsumer<String, byte[]> consumer;
    private final TopicPartition partition;
    private final int numRanges;
    private final BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor;
    private final long dependencyTimeoutMs;
    private final WatermarkTracker watermarkTracker = new WatermarkTracker();

    public RangeBarrierOwningConsumer(KafkaConsumer<String, byte[]> consumer, TopicPartition partition,
                                       int numRanges, BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                                       long dependencyTimeoutMs) {
        this.consumer = consumer;
        this.partition = partition;
        this.numRanges = numRanges;
        this.processor = processor;
        this.dependencyTimeoutMs = dependencyTimeoutMs;
    }

    public void assignAndSeek() {
        consumer.assign(List.of(partition));
        consumer.seekToBeginning(List.of(partition));
    }

    /**
     * Pre-scan + buffer the whole span, build the dependency index, dispatch every record
     * to its range, then block until the global commit point reaches the end and commit
     * once. Returns only when the whole span has been processed.
     */
    public void runToCompletion(long endOffsetExclusive) {
        List<ConsumerRecord<String, byte[]>> buffered = new ArrayList<>();
        KeyDependencyIndex.Builder indexBuilder = KeyDependencyIndex.builder();

        while (consumer.position(partition) < endOffsetExclusive) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, byte[]> record : records) {
                if (record.offset() >= endOffsetExclusive) {
                    continue;
                }
                buffered.add(record);
                indexBuilder.record(record.key(), record.offset());
            }
        }
        KeyDependencyIndex dependencyIndex = indexBuilder.build();
        if (buffered.isEmpty()) {
            return;
        }

        long startOffset = buffered.get(0).offset();
        RangePlan plan = new RangePlan(startOffset, endOffsetExclusive, numRanges);
        log.info("pre-scan complete: {} records buffered, {} distinct offsets with a tracked dependency, "
                + "{} ranges", buffered.size(), dependencyIndex.size(), numRanges);

        DependencyBarrier barrier = new DependencyBarrier();
        Map<Integer, RangeBarrierWorker> workers = new java.util.HashMap<>();
        for (RangePlan.Range r : plan.ranges()) {
            RangeBarrierWorker worker = new RangeBarrierWorker(r.rangeId(), dependencyIndex, barrier, dependencyTimeoutMs);
            worker.start();
            workers.put(r.rangeId(), worker);
        }

        for (ConsumerRecord<String, byte[]> record : buffered) {
            int rangeId = plan.rangeFor(record.offset());
            watermarkTracker.recordDispatched(rangeId, record.offset());
            workers.get(rangeId).submit(record, processor, () -> watermarkTracker.recordCompleted(rangeId, record.offset()));
        }

        long lastOffset = buffered.get(buffered.size() - 1).offset();
        while (watermarkTracker.commitPoint() < lastOffset) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        consumer.commitSync(Map.of(partition, new OffsetAndMetadata(watermarkTracker.commitPoint() + 1)));

        for (RangeBarrierWorker worker : workers.values()) {
            worker.close();
        }
    }

    public long lastCommittedOffset() {
        return watermarkTracker.commitPoint();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
