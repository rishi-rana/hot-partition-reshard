package com.hotpartition.transport.rpc;

import com.hotpartition.reshard.BucketWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Owner-side fallback-on-rejection behavior for transport Option A: two "owners" racing
 * for the same bucket on the same peer -- the exact scenario {@code PeerCapacityAdmission}
 * exists to protect against -- where the first should be admitted and delegate to the RPC
 * peer, and the second should be rejected and fall back to in-process processing instead
 * of failing the bucket. Entirely in-process (the peer's HTTP server embedded in this JVM,
 * not a spawned process), deterministic: both claim calls are synchronous.
 */
class ClaimingRpcBucketWorkerFactoryTest {

    @Test
    void secondOwnerFallsBackInProcessWhenFirstOwnerAlreadyClaimedPeerCapacity() throws Exception {
        int bucketId = 0;
        int port = 18080 + (int) (System.nanoTime() % 1000);
        double muWorkerCapacity = 100.0;

        try (RpcPeerWorker peer = new RpcPeerWorker(bucketId, port, 1_000_000, muWorkerCapacity, 4)) {
            peer.start();

            AtomicBoolean fallbackProcessorRan = new AtomicBoolean(false);

            // owner 1: estimated lambda 70 -- fits under capacity 100.
            ClaimingRpcBucketWorkerFactory factory1 = new ClaimingRpcBucketWorkerFactory(
                    "owner-1", "localhost", port, () -> 70.0,
                    (rec, done) -> { throw new AssertionError("owner-1's bucket should be RPC-delegated, not in-process"); });

            // owner 2: estimated lambda 70 again -- 70 + 70 = 140 > 100, must be rejected.
            ClaimingRpcBucketWorkerFactory factory2 = new ClaimingRpcBucketWorkerFactory(
                    "owner-2", "localhost", port, () -> 70.0,
                    (rec, done) -> { fallbackProcessorRan.set(true); done.run(); });

            BucketWorker worker1 = factory1.apply(bucketId);
            BucketWorker worker2 = factory2.apply(bucketId);
            worker1.start();
            worker2.start();

            try {
                assertEquals(0, factory1.fallbackCount(), "owner 1 should have been accepted, not fallen back");
                assertEquals(1, factory2.fallbackCount(), "owner 2 should have fallen back exactly once");
                assertEquals(70.0, peer.admission().currentlyAdmittedLoad(),
                        "only owner 1's claim should be counted against the peer's admitted load");

                CountDownLatch owner1Done = new CountDownLatch(1);
                ConsumerRecord<String, byte[]> rec1 = new ConsumerRecord<>("src", 0, 0, "k1",
                        "v1".getBytes(StandardCharsets.UTF_8));
                worker1.submit(rec1, (r, d) -> { throw new AssertionError("unused for RPC path"); }, owner1Done::countDown);
                assertTrue(owner1Done.await(10, TimeUnit.SECONDS), "owner 1's record should complete via the RPC peer");

                CountDownLatch owner2Done = new CountDownLatch(1);
                ConsumerRecord<String, byte[]> rec2 = new ConsumerRecord<>("src", 0, 0, "k2",
                        "v2".getBytes(StandardCharsets.UTF_8));
                worker2.submit(rec2, (r, d) -> { throw new AssertionError("unused; worker2 substitutes its own fallback processor"); },
                        owner2Done::countDown);
                assertTrue(owner2Done.await(10, TimeUnit.SECONDS), "owner 2's record should complete via in-process fallback");
                assertTrue(fallbackProcessorRan.get(), "fallback processor must have actually executed for owner 2's record");
            } finally {
                worker1.close();
                worker2.close();
            }

            assertEquals(0.0, peer.admission().currentlyAdmittedLoad(),
                    "owner 1's claim must be released once its worker is closed");
        }
    }

    @Test
    void bothOwnersAdmittedWhenCombinedLoadFitsUnderCapacity() throws Exception {
        int bucketId = 1;
        int port = 19080 + (int) (System.nanoTime() % 1000);
        double muWorkerCapacity = 200.0;

        try (RpcPeerWorker peer = new RpcPeerWorker(bucketId, port, 1_000_000, muWorkerCapacity, 4)) {
            peer.start();

            AtomicInteger neverCalled = new AtomicInteger();
            ClaimingRpcBucketWorkerFactory factory1 = new ClaimingRpcBucketWorkerFactory(
                    "owner-1", "localhost", port, () -> 70.0, (rec, done) -> neverCalled.incrementAndGet());
            ClaimingRpcBucketWorkerFactory factory2 = new ClaimingRpcBucketWorkerFactory(
                    "owner-2", "localhost", port, () -> 70.0, (rec, done) -> neverCalled.incrementAndGet());

            BucketWorker worker1 = factory1.apply(bucketId);
            BucketWorker worker2 = factory2.apply(bucketId);
            try {
                assertEquals(0, factory1.fallbackCount());
                assertEquals(0, factory2.fallbackCount());
                assertEquals(140.0, peer.admission().currentlyAdmittedLoad());
                assertFalse(neverCalled.get() > 0, "neither bucket should have fallen back to in-process");
            } finally {
                worker1.close();
                worker2.close();
            }
        }
    }
}
