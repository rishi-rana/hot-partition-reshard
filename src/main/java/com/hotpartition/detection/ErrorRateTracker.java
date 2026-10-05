package com.hotpartition.detection;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 1 of the detection gate: cheapest check, run first. A spike here is
 * close to a direct diagnosis (deserialization exception, repeated retry on
 * one record) and short-circuits the rest of the gate &mdash; it means this is a
 * consumer-health problem, not partition hotness, so resharding must not fire.
 */
public final class ErrorRateTracker {

    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicLong retryCount = new AtomicLong();
    private final AtomicLong recordCount = new AtomicLong();
    private final double errorRateThreshold;

    public ErrorRateTracker(double errorRateThreshold) {
        this.errorRateThreshold = errorRateThreshold;
    }

    public void recordProcessed() {
        recordCount.incrementAndGet();
    }

    public void recordError() {
        errorCount.incrementAndGet();
    }

    public void recordRetry() {
        retryCount.incrementAndGet();
    }

    /** True when the error/retry rate over the window is elevated enough to explain lag on its own. */
    public boolean isElevated() {
        long records = recordCount.get();
        if (records == 0) {
            return false;
        }
        double rate = (double) (errorCount.get() + retryCount.get()) / records;
        return rate >= errorRateThreshold;
    }

    public void reset() {
        errorCount.set(0);
        retryCount.set(0);
        recordCount.set(0);
    }
}
