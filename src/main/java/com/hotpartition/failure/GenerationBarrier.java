package com.hotpartition.failure;

import com.hotpartition.reshard.KeyBucketRouter;
import com.hotpartition.reshard.WatermarkTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Handles a bucket-count (N) change mid-flight via quiesce-and-switch, per
 * the design doc: never a silent rehash. The owner stops routing new records
 * under the old mapping, waits for every active bucket's low-watermark to
 * reach the boundary offset (drains in-flight work), then switches to the
 * new mapping. This guarantees no key is ever live under two different
 * bucket assignments at once.
 */
public final class GenerationBarrier {

    private static final Logger log = LoggerFactory.getLogger(GenerationBarrier.class);

    private final AtomicReference<KeyBucketRouter> currentRouter;

    public GenerationBarrier(KeyBucketRouter initial) {
        this.currentRouter = new AtomicReference<>(initial);
    }

    public KeyBucketRouter current() {
        return currentRouter.get();
    }

    /**
     * Blocks the calling thread (the owner's dispatch loop) until every bucket
     * active at the boundary has drained to {@code boundaryOffset}, then swaps
     * in a router for {@code newNumBuckets}. Callers must stop dispatching new
     * records before calling this and resume only after it returns.
     */
    public void changeBucketCount(int newNumBuckets,
                                   long boundaryOffset,
                                   Set<Integer> activeBucketsAtBoundary,
                                   WatermarkTracker tracker,
                                   long pollIntervalMs,
                                   long timeoutMs) throws InterruptedException {
        KeyBucketRouter old = currentRouter.get();
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!drained(activeBucketsAtBoundary, boundaryOffset, tracker)) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("generation barrier timed out draining to offset " + boundaryOffset);
            }
            Thread.sleep(pollIntervalMs);
        }
        KeyBucketRouter next = new KeyBucketRouter(newNumBuckets, old.generation() + 1);
        currentRouter.set(next);
        log.info("generation barrier: switched from {} buckets (gen {}) to {} buckets (gen {}) at boundary offset {}",
                old.numBuckets(), old.generation(), next.numBuckets(), next.generation(), boundaryOffset);
    }

    private boolean drained(Set<Integer> activeBuckets, long boundaryOffset, WatermarkTracker tracker) {
        for (int bucketId : activeBuckets) {
            if (tracker.watermarkFor(bucketId) < boundaryOffset) {
                return false;
            }
        }
        return true;
    }
}
