package com.hotpartition.reshard;

import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-bucket low-watermark tracking and the global commit point:
 * {@code commit_point = min(per-bucket low-watermark)} across every bucket
 * with dispatched-but-not-yet-fully-processed work.
 *
 * A bucket only ever receives a subset of the partition's offsets (the rest
 * go to other buckets), so a per-bucket watermark alone can't safely answer
 * "what's the highest offset we can tell Kafka is done" &mdash; a bucket with
 * no outstanding work right now imposes no constraint on its own (more work
 * may be dispatched to it later, always at a higher offset than anything
 * dispatched globally so far, since the owner reads the partition in strictly
 * increasing offset order). The global commit point is therefore tracked
 * separately from per-bucket watermarks: it is the highest offset the owner
 * has dispatched to any bucket, minus everything still outstanding anywhere.
 */
public final class WatermarkTracker {

    private final Map<Integer, TreeSet<Long>> outstandingByBucket = new ConcurrentHashMap<>();
    private final Map<Integer, Long> highestDispatchedByBucket = new ConcurrentHashMap<>();
    private final TreeSet<Long> globalOutstanding = new TreeSet<>();
    private long globalHighestDispatched = -1;

    public synchronized void recordDispatched(int bucketId, long offset) {
        outstandingByBucket.computeIfAbsent(bucketId, k -> new TreeSet<>()).add(offset);
        highestDispatchedByBucket.merge(bucketId, offset, Math::max);
        globalOutstanding.add(offset);
        globalHighestDispatched = Math.max(globalHighestDispatched, offset);
    }

    public synchronized void recordCompleted(int bucketId, long offset) {
        TreeSet<Long> outstanding = outstandingByBucket.get(bucketId);
        if (outstanding != null) {
            outstanding.remove(offset);
        }
        globalOutstanding.remove(offset);
    }

    /**
     * This bucket's own highest offset such that every offset ever dispatched
     * to it at or below it has completed; -1 if nothing ever dispatched to it.
     * A bucket with no outstanding work contributes no constraint of its own
     * here (returns its highest dispatched offset, which only matters if
     * nothing else has gone further) &mdash; used for drain checks (see
     * {@link com.hotpartition.failure.GenerationBarrier}), not for the global
     * commit point (see {@link #commitPoint()}).
     */
    public synchronized long watermarkFor(int bucketId) {
        Long highestDispatched = highestDispatchedByBucket.get(bucketId);
        if (highestDispatched == null) {
            return -1;
        }
        TreeSet<Long> outstanding = outstandingByBucket.get(bucketId);
        if (outstanding == null || outstanding.isEmpty()) {
            return highestDispatched;
        }
        return outstanding.first() - 1;
    }

    /**
     * The highest partition offset such that every offset dispatched to ANY
     * bucket at or below it has completed; -1 if nothing dispatched yet.
     */
    public synchronized long commitPoint() {
        if (globalHighestDispatched < 0) {
            return -1;
        }
        return globalOutstanding.isEmpty() ? globalHighestDispatched : globalOutstanding.first() - 1;
    }

    /** Snapshot of all per-bucket watermarks, for persisting to the control topic. */
    public synchronized Map<Integer, Long> snapshot() {
        Map<Integer, Long> out = new java.util.HashMap<>();
        for (int bucketId : highestDispatchedByBucket.keySet()) {
            out.put(bucketId, watermarkFor(bucketId));
        }
        return out;
    }

    /** Restore state recovered from the control topic on owner failover: treats each bucket's recovered watermark as already fully completed. */
    public synchronized void restore(Map<Integer, Long> snapshot) {
        for (Map.Entry<Integer, Long> e : snapshot.entrySet()) {
            highestDispatchedByBucket.merge(e.getKey(), e.getValue(), Math::max);
            outstandingByBucket.computeIfAbsent(e.getKey(), k -> new TreeSet<>());
            globalHighestDispatched = Math.max(globalHighestDispatched, e.getValue());
        }
    }

    public synchronized void clear() {
        outstandingByBucket.clear();
        highestDispatchedByBucket.clear();
        globalOutstanding.clear();
        globalHighestDispatched = -1;
    }
}
