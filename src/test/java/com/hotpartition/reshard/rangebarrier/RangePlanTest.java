package com.hotpartition.reshard.rangebarrier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RangePlanTest {

    @Test
    void splitsEvenSpanIntoEqualContiguousRanges() {
        RangePlan plan = new RangePlan(0, 100, 4);

        assertEquals(4, plan.ranges().size());
        assertEquals(0, plan.rangeFor(0));
        assertEquals(0, plan.rangeFor(24));
        assertEquals(1, plan.rangeFor(25));
        assertEquals(3, plan.rangeFor(99));
    }

    @Test
    void lastRangeAbsorbsAnyRemainder() {
        RangePlan plan = new RangePlan(0, 10, 3); // 10/3 = 3 per range, remainder to last

        long total = 0;
        for (RangePlan.Range r : plan.ranges()) {
            total += (r.endOffsetExclusive() - r.startOffset());
        }
        assertEquals(10, total);
        assertEquals(2, plan.rangeFor(9)); // last offset belongs to the last range
    }

    @Test
    void everyOffsetInSpanMapsToExactlyOneRange() {
        RangePlan plan = new RangePlan(1000, 1050, 5);
        for (long offset = 1000; offset < 1050; offset++) {
            int rangeId = plan.rangeFor(offset);
            assertTrue(plan.ranges().get(rangeId).contains(offset));
        }
    }

    @Test
    void rejectsEmptyOrInvalidSpan() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RangePlan(10, 10, 2));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> new RangePlan(0, 10, 0));
    }
}
