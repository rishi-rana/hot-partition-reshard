package com.hotpartition.reshard;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.function.BiConsumer;

/**
 * A worker responsible for one key-bucket's share of a hot partition's backlog.
 * In the full design these are application-level workers, possibly other
 * processes already assigned to other, non-hot partitions in the same
 * consumer group. This prototype only implements an in-process variant
 * ({@link InProcessBucketWorker}) &mdash; see the README for why that's an
 * acceptable simplification for validating the mechanism.
 */
public interface BucketWorker extends AutoCloseable {

    /**
     * Submit a record for processing. {@code processor} does the actual work;
     * {@code onComplete} must be invoked exactly once after it finishes,
     * reporting the record's offset back to the {@link WatermarkTracker}.
     */
    void submit(ConsumerRecord<String, byte[]> record,
                BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                Runnable onComplete);

    void start();

    @Override
    void close();
}
