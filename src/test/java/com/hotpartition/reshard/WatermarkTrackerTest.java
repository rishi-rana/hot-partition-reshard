package com.hotpartition.reshard;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WatermarkTrackerTest {

    @Test
    void commitPointIsMinAcrossActiveBuckets() {
        // Realistic dispatch order: the owner reads the partition in strictly
        // increasing offset order and routes each offset to exactly one bucket,
        // so offset 10 goes to bucket 0 and the next offset, 11, goes to bucket 1.
        WatermarkTracker tracker = new WatermarkTracker();
        tracker.recordDispatched(0, 10);
        tracker.recordDispatched(1, 11);

        tracker.recordCompleted(0, 10);
        // bucket 1 hasn't completed its offset yet -> commit point can't pass offset 9
        assertEquals(10, tracker.commitPoint());

        tracker.recordCompleted(1, 11);
        assertEquals(11, tracker.commitPoint());
    }

    @Test
    void outOfOrderCompletionWithinBucketStillAdvancesCorrectly() {
        WatermarkTracker tracker = new WatermarkTracker();
        // offset 4 went to a different bucket; this bucket only ever sees 1,2,3,5.
        tracker.recordDispatched(0, 1);
        tracker.recordDispatched(0, 2);
        tracker.recordDispatched(0, 3);
        tracker.recordDispatched(0, 5);
        tracker.recordCompleted(0, 3);
        tracker.recordCompleted(0, 1);
        tracker.recordCompleted(0, 2);
        // 1,2,3 are done; 5 is still outstanding -> this bucket vouches for
        // everything up to (but not including) its smallest outstanding offset.
        assertEquals(4, tracker.watermarkFor(0));
    }

    @Test
    void bucketWithNoDispatchedWorkNeverBlocksCommit() {
        WatermarkTracker tracker = new WatermarkTracker();
        tracker.recordDispatched(0, 10);
        tracker.recordCompleted(0, 10);
        // bucket 1 was never dispatched any work; it must not block the commit point.
        assertEquals(10, tracker.commitPoint());
    }

    @Test
    void emptyTrackerHasNoCommitPoint() {
        WatermarkTracker tracker = new WatermarkTracker();
        assertEquals(-1, tracker.commitPoint());
    }
}
