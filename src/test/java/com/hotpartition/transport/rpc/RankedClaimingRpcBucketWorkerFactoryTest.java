package com.hotpartition.transport.rpc;

import com.hotpartition.reshard.BucketWorker;
import com.hotpartition.transport.rpc.RankedClaimingRpcBucketWorkerFactory.PeerCandidate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Owner-side ranked-candidate-list retry behavior: unlike {@code ClaimingRpcBucketWorkerFactory}
 * (single hardcoded peer, immediate fallback on rejection), this factory should walk a
 * ranked list of candidates, trying each in order until one ACCEPTs, and fall back to
 * in-process only if every candidate REJECTs. Entirely in-process (each peer's HTTP server
 * embedded in this JVM), deterministic: every claim call is synchronous.
 */
class RankedClaimingRpcBucketWorkerFactoryTest {

    @Test
    void firstCandidateAcceptedUsesItDirectly() throws Exception {
        int bucketId = 0;
        int port1 = 20080 + (int) (System.nanoTime() % 1000);
        int port2 = port1 + 1;
        double muWorkerCapacity = 100.0;

        try (RpcPeerWorker peer1 = new RpcPeerWorker(bucketId, port1, 1_000_000, muWorkerCapacity, 4);
             RpcPeerWorker peer2 = new RpcPeerWorker(bucketId, port2, 1_000_000, muWorkerCapacity, 4)) {
            peer1.start();
            peer2.start();

            RankedClaimingRpcBucketWorkerFactory factory = new RankedClaimingRpcBucketWorkerFactory(
                    "owner-1", List.of(new PeerCandidate("localhost", port1), new PeerCandidate("localhost", port2)),
                    () -> 50.0, (rec, done) -> { throw new AssertionError("should never fall back here"); });

            BucketWorker worker = factory.apply(bucketId);
            worker.start();
            try {
                assertEquals(0, factory.fallbackCount());
                assertEquals(port1, factory.acceptedPeerFor(bucketId).orElseThrow().port(),
                        "first candidate should be the one that accepted");
                assertEquals(50.0, peer1.admission().currentlyAdmittedLoad());
                assertEquals(0.0, peer2.admission().currentlyAdmittedLoad(),
                        "second candidate should never have been tried");

                CountDownLatch recordDone = new CountDownLatch(1);
                ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("src", 0, 0, "k1",
                        "v1".getBytes(StandardCharsets.UTF_8));
                worker.submit(rec, (r, d) -> { throw new AssertionError("unused for RPC path"); }, recordDone::countDown);
                assertTrue(recordDone.await(10, TimeUnit.SECONDS), "record should complete via peer1");
            } finally {
                worker.close();
            }
            assertEquals(0.0, peer1.admission().currentlyAdmittedLoad(), "claim must be released on close");
        }
    }

    @Test
    void firstCandidateRejectsSecondAccepts() throws Exception {
        int bucketId = 1;
        int port1 = 21080 + (int) (System.nanoTime() % 1000);
        int port2 = port1 + 1;
        double muWorkerCapacity = 50.0;

        try (RpcPeerWorker peer1 = new RpcPeerWorker(bucketId, port1, 1_000_000, muWorkerCapacity, 4);
             RpcPeerWorker peer2 = new RpcPeerWorker(bucketId, port2, 1_000_000, muWorkerCapacity, 4)) {
            peer1.start();
            peer2.start();

            // Fill peer1's entire capacity with an unrelated claim first, so our factory's
            // first candidate is guaranteed to reject.
            peer1.admission().tryAdmit("someone-elses-claim", 50.0);

            RankedClaimingRpcBucketWorkerFactory factory = new RankedClaimingRpcBucketWorkerFactory(
                    "owner-1", List.of(new PeerCandidate("localhost", port1), new PeerCandidate("localhost", port2)),
                    () -> 30.0, (rec, done) -> { throw new AssertionError("should never fall back here"); });

            BucketWorker worker = factory.apply(bucketId);
            worker.start();
            try {
                assertEquals(0, factory.fallbackCount(), "should have succeeded on the second candidate, not fallen back");
                assertEquals(port2, factory.acceptedPeerFor(bucketId).orElseThrow().port(),
                        "second candidate should be the one that accepted after the first rejected");
                assertEquals(50.0, peer1.admission().currentlyAdmittedLoad(),
                        "peer1's load should be unchanged by our rejected attempt");
                assertEquals(30.0, peer2.admission().currentlyAdmittedLoad());

                CountDownLatch recordDone = new CountDownLatch(1);
                ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("src", 0, 0, "k1",
                        "v1".getBytes(StandardCharsets.UTF_8));
                worker.submit(rec, (r, d) -> { throw new AssertionError("unused for RPC path"); }, recordDone::countDown);
                assertTrue(recordDone.await(10, TimeUnit.SECONDS), "record should complete via peer2");
            } finally {
                worker.close();
            }
            assertEquals(50.0, peer1.admission().currentlyAdmittedLoad(),
                    "peer1's unrelated claim should still be held -- we never touched it");
            assertEquals(0.0, peer2.admission().currentlyAdmittedLoad(), "our claim on peer2 must be released on close");
        }
    }

    @Test
    void allCandidatesRejectFallsBackToInProcess() throws Exception {
        int bucketId = 2;
        int port1 = 22080 + (int) (System.nanoTime() % 1000);
        int port2 = port1 + 1;
        double muWorkerCapacity = 50.0;

        try (RpcPeerWorker peer1 = new RpcPeerWorker(bucketId, port1, 1_000_000, muWorkerCapacity, 4);
             RpcPeerWorker peer2 = new RpcPeerWorker(bucketId, port2, 1_000_000, muWorkerCapacity, 4)) {
            peer1.start();
            peer2.start();

            // Fill both peers' entire capacity so every candidate rejects.
            peer1.admission().tryAdmit("filler-1", 50.0);
            peer2.admission().tryAdmit("filler-2", 50.0);

            AtomicBoolean fallbackRan = new AtomicBoolean(false);
            AtomicInteger fallbackInvocations = new AtomicInteger();
            RankedClaimingRpcBucketWorkerFactory factory = new RankedClaimingRpcBucketWorkerFactory(
                    "owner-1", List.of(new PeerCandidate("localhost", port1), new PeerCandidate("localhost", port2)),
                    () -> 30.0, (rec, done) -> {
                        fallbackRan.set(true);
                        fallbackInvocations.incrementAndGet();
                        done.run();
                    });

            BucketWorker worker = factory.apply(bucketId);
            worker.start();
            try {
                assertEquals(1, factory.fallbackCount(), "every candidate rejected -- must fall back exactly once");
                assertFalse(factory.acceptedPeerFor(bucketId).isPresent(), "no peer should have accepted");
                assertEquals(50.0, peer1.admission().currentlyAdmittedLoad(), "peer1 untouched by our rejected attempt");
                assertEquals(50.0, peer2.admission().currentlyAdmittedLoad(), "peer2 untouched by our rejected attempt");

                CountDownLatch recordDone = new CountDownLatch(1);
                ConsumerRecord<String, byte[]> rec = new ConsumerRecord<>("src", 0, 0, "k1",
                        "v1".getBytes(StandardCharsets.UTF_8));
                worker.submit(rec, (r, d) -> { throw new AssertionError("unused; fallback substitutes its own processor"); },
                        recordDone::countDown);
                assertTrue(recordDone.await(10, TimeUnit.SECONDS), "record should complete via in-process fallback");
                assertTrue(fallbackRan.get(), "fallback processor must have actually executed");
                assertEquals(1, fallbackInvocations.get());
            } finally {
                worker.close();
            }
        }
    }
}
