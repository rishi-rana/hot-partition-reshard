package com.hotpartition.reshard.rangebarrier;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a span of offsets [startOffset, endOffsetExclusive) into {@code numRanges}
 * contiguous sub-ranges, one per worker -- the offset-range analog of
 * {@link com.hotpartition.reshard.KeyBucketRouter}'s key-hash buckets. Unlike key
 * buckets, range membership is static and purely positional: it doesn't need to be
 * recomputed as new records arrive, and routing a given offset to its range is an O(1)
 * arithmetic lookup, not a hash.
 */
public final class RangePlan {

    public record Range(int rangeId, long startOffset, long endOffsetExclusive) {
        public boolean contains(long offset) {
            return offset >= startOffset && offset < endOffsetExclusive;
        }
    }

    private final List<Range> ranges;
    private final long startOffset;
    private final long span;
    private final int numRanges;

    public RangePlan(long startOffset, long endOffsetExclusive, int numRanges) {
        if (endOffsetExclusive <= startOffset) {
            throw new IllegalArgumentException("endOffsetExclusive must be > startOffset");
        }
        if (numRanges <= 0) {
            throw new IllegalArgumentException("numRanges must be positive");
        }
        this.startOffset = startOffset;
        this.span = endOffsetExclusive - startOffset;
        this.numRanges = numRanges;

        List<Range> built = new ArrayList<>(numRanges);
        long perRange = Math.max(1, span / numRanges);
        long cursor = startOffset;
        for (int i = 0; i < numRanges; i++) {
            long end = (i == numRanges - 1) ? endOffsetExclusive : Math.min(endOffsetExclusive, cursor + perRange);
            if (cursor >= endOffsetExclusive) {
                end = cursor; // empty trailing range if numRanges > span
            }
            built.add(new Range(i, cursor, end));
            cursor = end;
        }
        this.ranges = List.copyOf(built);
    }

    public List<Range> ranges() {
        return ranges;
    }

    /** Which range id a given offset belongs to. */
    public int rangeFor(long offset) {
        for (Range r : ranges) {
            if (r.contains(offset)) {
                return r.rangeId();
            }
        }
        // offset at or beyond the last range's end belongs to the last non-empty range,
        // matching Kafka's own "last partition absorbs the remainder" convention.
        return ranges.get(ranges.size() - 1).rangeId();
    }
}
