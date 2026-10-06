package com.hotpartition.reshard.rangebarrier;

import java.util.HashMap;
import java.util.Map;

/**
 * The pre-scan this design's Mechanism section calls out as its known cost: a single pass
 * over a candidate span's records (in increasing offset order), recording for every
 * offset the nearest <em>earlier</em> offset carrying the same key, or -1 if that offset
 * is the first occurrence of its key in the span. This is exactly the dependency a worker
 * must wait on before it's safe to process a given offset, per the offset-range
 * fallback's correctness rule: a key's records must still be processed in produced order
 * even though different offset ranges are now handled by different, concurrent workers.
 *
 * Pure lookup table once built -- O(1) per offset at dispatch time -- but building it
 * requires having already read every record in the span, which is precisely the
 * pre-scan cost the design doc warns will dominate under exactly the skewed-traffic
 * conditions this whole mechanism exists to help with (more repeated keys -&gt; more
 * offsets with a real dependency -&gt; more cross-range blocking).
 */
public final class KeyDependencyIndex {

    private final Map<Long, Long> requiredPriorOffset = new HashMap<>();

    public static final class Builder {
        private final Map<String, Long> lastSeenOffsetForKey = new HashMap<>();
        private final KeyDependencyIndex index = new KeyDependencyIndex();

        /** Records one more (key, offset) pair. Must be called in increasing offset order. */
        public Builder record(String key, long offset) {
            Long prior = lastSeenOffsetForKey.get(key);
            index.requiredPriorOffset.put(offset, prior == null ? -1L : prior);
            lastSeenOffsetForKey.put(key, offset);
            return this;
        }

        public KeyDependencyIndex build() {
            return index;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * -1 means no dependency (first occurrence of this offset's key in the scanned span,
     * or the offset wasn't in the span at all); otherwise the offset a worker must see
     * {@link DependencyBarrier#markCompleted} for before processing this one.
     */
    public long requiredPriorOffset(long offset) {
        return requiredPriorOffset.getOrDefault(offset, -1L);
    }

    public int size() {
        return requiredPriorOffset.size();
    }
}
