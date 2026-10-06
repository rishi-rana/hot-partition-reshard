package com.hotpartition.reshard.rangebarrier;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The correctness property this whole fallback mechanism exists to provide: a key whose
 * records straddle multiple concurrently-running offset ranges must still be processed in
 * produced order, even though the ranges themselves run concurrently and -- deliberately,
 * in this test -- at different speeds that would expose a reordering bug if the
 * dependency barrier weren't actually gating anything. No Kafka broker needed: this
 * drives {@link RangeBarrierWorker} directly, the same way
 * {@link RangeBarrierOwningConsumer} does internally, just without the Kafka plumbing
 * around it.
 */
class RangeBarrierOrderingTest {

    @Test
    void hotKeyStraddlingAllFourRangesCompletesInOrderDespiteSkewedRangeSpeeds() throws Exception {
        int numRanges = 4;
        int offsetsPerRange = 5;
        int totalOffsets = numRanges * offsetsPerRange; // 20
        String hotKey = "hot-key";

        // hot-key appears once per range, at the LAST offset of each range -- the worst
        // case for exposing reordering, since each range's worker would otherwise be free
        // to finish its hot-key record right as it starts, with no reason to wait for an
        // earlier range unless the barrier actually forces it to.
        Map<Long, String> keysByOffset = new HashMap<>();
        for (long offset = 0; offset < totalOffsets; offset++) {
            boolean isHotKeyOffset = (offset % offsetsPerRange) == offsetsPerRange - 1;
            keysByOffset.put(offset, isHotKeyOffset ? hotKey : "other-key-" + offset);
        }

        KeyDependencyIndex.Builder indexBuilder = KeyDependencyIndex.builder();
        for (long offset = 0; offset < totalOffsets; offset++) {
            indexBuilder.record(keysByOffset.get(offset), offset);
        }
        KeyDependencyIndex dependencyIndex = indexBuilder.build();

        RangePlan plan = new RangePlan(0, totalOffsets, numRanges);
        DependencyBarrier barrier = new DependencyBarrier();

        List<Long> hotKeyCompletionOrder = new CopyOnWriteArrayList<>();
        CountDownLatch allDone = new CountDownLatch(totalOffsets);

        Map<Integer, RangeBarrierWorker> workers = new HashMap<>();
        for (RangePlan.Range r : plan.ranges()) {
            RangeBarrierWorker worker = new RangeBarrierWorker(r.rangeId(), dependencyIndex, barrier, 5000);
            worker.start();
            workers.put(r.rangeId(), worker);
        }

        // deliberately inverted speed: the LAST range (which must wait longest on the
        // dependency chain) is also the FASTEST once unblocked, and the FIRST range
        // (which gates everyone else) is the SLOWEST -- maximizing the chance that a
        // broken barrier would let a later range's hot-key completion overtake an
        // earlier one.
        List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
        for (long offset = 0; offset < totalOffsets; offset++) {
            String key = keysByOffset.get(offset);
            records.add(new ConsumerRecord<>("range-barrier-test", 0, offset, key,
                    ("v" + offset).getBytes(StandardCharsets.UTF_8)));
        }

        for (ConsumerRecord<String, byte[]> record : records) {
            int rangeId = plan.rangeFor(record.offset());
            long simulatedCostMs = Math.max(1, 20 - (rangeId * 5)); // range 0 slowest, range 3 fastest
            workers.get(rangeId).submit(record, (rec, done) -> {
                try {
                    Thread.sleep(simulatedCostMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (hotKey.equals(rec.key())) {
                    hotKeyCompletionOrder.add(rec.offset());
                }
                done.run();
            }, allDone::countDown);
        }

        assertTrue(allDone.await(10, TimeUnit.SECONDS), "all 20 records should complete within the timeout");
        for (RangeBarrierWorker w : workers.values()) {
            w.close();
        }

        List<Long> expectedHotKeyOrder = List.of(4L, 9L, 14L, 19L);
        assertEquals(expectedHotKeyOrder, hotKeyCompletionOrder,
                "hot-key's four occurrences, one per range, must complete in produced order "
                        + "even though later ranges process faster than earlier ones");
    }
}
