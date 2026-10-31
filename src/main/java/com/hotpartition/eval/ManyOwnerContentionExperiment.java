package com.hotpartition.eval;

import com.hotpartition.reshard.BucketWorker;
import com.hotpartition.transport.rpc.ClaimingRpcBucketWorkerFactory;
import com.hotpartition.transport.rpc.RankedClaimingRpcBucketWorkerFactory;
import com.hotpartition.transport.rpc.RankedClaimingRpcBucketWorkerFactory.PeerCandidate;
import com.hotpartition.transport.rpc.RpcPeerWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Closes the gap {@link com.hotpartition.eval.AdmissionControlRaceExperiment} left open:
 * that experiment proved {@link com.hotpartition.admission.PeerCapacityAdmission}
 * correctly serializes two competing claims on <em>one</em> peer. It says nothing about
 * what happens with many simultaneously-hot owners choosing among many peers, which is
 * exactly the scenario {@code PeerRegistry#findIdlePeer} (out of scope here -- this stays
 * pure-JVM and points owners directly at hardcoded candidate lists, same convention as
 * the prior experiment) is vulnerable to: if every owner's independent "most idle peer"
 * pick converges on the same answer off a shared/stale view, a factory that only ever
 * tries ONE candidate (today's shipped {@link ClaimingRpcBucketWorkerFactory}) gives every
 * owner exactly one shot at that one peer and sends the rest straight to fallback, even
 * while other, equally-idle peers sit completely unused ("herding"). This experiment
 * measures that directly, and whether the aggregate capacity bound (sum of admitted load
 * across the whole peer pool staying within sum of peer capacities) holds once many
 * independent, locally-correct admission decisions are happening at once, not just one.
 *
 * <p>Two conditions, same 4-peer pool and 12 owners, differing only in how each owner's
 * owner-side factory is wired:
 * <ul>
 *   <li>{@link #runSingleCandidateCondition} ("A", no retry) -- every owner gets a
 *       {@link ClaimingRpcBucketWorkerFactory} pointed at the SAME one peer (simulating
 *       every owner's independent "most idle" pick converging on the same answer). No
 *       retry against a second peer on rejection.</li>
 *   <li>{@link #runRankedCandidateCondition} ("B", with retry) -- every owner gets a
 *       {@link RankedClaimingRpcBucketWorkerFactory} with the full ordered list of all 4
 *       peers (same order for every owner -- the point under test is retry-on-rejection,
 *       not randomized tie-breaking).</li>
 * </ul>
 *
 * <p><b>Determinism.</b> Each owner's claim ({@code factory.apply(bucketId)}, a
 * synchronous HTTP round trip) is issued sequentially in owner order 0..11, not
 * concurrently -- matching {@code AdmissionControlRaceExperiment}'s own
 * {@code runWithAdmissionControl} convention of calling {@code apply()} synchronously
 * before starting any worker. This makes which owners land where a deterministic function
 * of claim order rather than a race, so the predicted outcome (exactly 2 owners admitted
 * in condition A, exactly 8 in condition B) is actually checkable rather than depending on
 * how the JVM happened to schedule 12 threads. Record submission/processing, once claims
 * are settled, runs fully concurrently across all 12 owners.
 */
public final class ManyOwnerContentionExperiment {

    private static final Logger log = LoggerFactory.getLogger(ManyOwnerContentionExperiment.class);

    /**
     * Pairs a started {@link RpcPeerWorker} with the port this experiment itself chose for
     * it. {@code RpcPeerWorker} (shipped/committed in the prior admission-control-race
     * task) exposes no port getter of its own -- deliberately not adding one there, since
     * that class is out of scope for this task -- so this experiment tracks the port
     * itself instead.
     */
    private record Peer(RpcPeerWorker worker, int port) {
    }

    public record PeerOutcome(
            String host,
            int port,
            double muWorkerCapacity,
            long admittedClaimCount,
            double admittedLoad,
            boolean admittedLoadWithinCapacity
    ) {
    }

    public record ConditionResult(
            String conditionName,
            List<PeerOutcome> peerOutcomes,
            long ownersServedViaRpc,
            long ownersFellBackToInProcess,
            double aggregateAdmittedLoad,
            long totalRecordsExpected,
            long totalRecordsCompleted,
            boolean allRecordsCompleted,
            long elapsedMs
    ) {
    }

    /**
     * Condition A: every owner's candidate list is the single peer at {@code basePort}
     * (the first of the {@code numPeers} peers started) -- the other {@code numPeers - 1}
     * peers are started (so their idleness is itself an observed, not assumed, result) but
     * never targeted by any owner.
     */
    public ConditionResult runSingleCandidateCondition(int basePort, int numPeers, int numOwners,
                                                        int recordsPerOwner, long processingNanosPerRecord,
                                                        double muWorkerCapacity, double perOwnerOfferedLambda,
                                                        long awaitTimeoutSeconds, long joinTimeoutMs) throws Exception {
        List<Peer> peers = startPeers(basePort, numPeers, processingNanosPerRecord, muWorkerCapacity);
        try {
            AtomicLong fallbackExecutions = new AtomicLong();
            var realProcessor = fallbackProcessor(processingNanosPerRecord, fallbackExecutions);

            List<ClaimingRpcBucketWorkerFactory> factories = new ArrayList<>(numOwners);
            List<BucketWorker> workers = new ArrayList<>(numOwners);
            // Every owner's "most idle peer" pick converges on peers.get(0) -- the herding
            // scenario this condition exists to reproduce.
            Peer targetPeer = peers.get(0);
            for (int i = 0; i < numOwners; i++) {
                ClaimingRpcBucketWorkerFactory factory = new ClaimingRpcBucketWorkerFactory(
                        "owner-" + i, "localhost", targetPeer.port(), () -> perOwnerOfferedLambda, realProcessor);
                BucketWorker worker = factory.apply(0); // synchronous claim, in owner order
                factories.add(factory);
                workers.add(worker);
            }

            List<PeerOutcome> peerOutcomes = snapshotPeerOutcomes(peers);
            long ownersServedViaRpc = factories.stream().filter(f -> f.fallbackCount() == 0).count();
            long ownersFellBack = factories.stream().filter(f -> f.fallbackCount() == 1).count();

            return runRecordsAndCollect("A-single-candidate-no-retry", peerOutcomes, ownersServedViaRpc, ownersFellBack,
                    workers, numOwners, recordsPerOwner, perOwnerOfferedLambda, awaitTimeoutSeconds, joinTimeoutMs);
        } finally {
            stopPeers(peers);
        }
    }

    /**
     * Condition B: every owner gets the full ordered list of all {@code numPeers} peers
     * (same order for every owner), and retries down the list on rejection before falling
     * back to in-process.
     */
    public ConditionResult runRankedCandidateCondition(int basePort, int numPeers, int numOwners,
                                                         int recordsPerOwner, long processingNanosPerRecord,
                                                         double muWorkerCapacity, double perOwnerOfferedLambda,
                                                         long awaitTimeoutSeconds, long joinTimeoutMs) throws Exception {
        List<Peer> peers = startPeers(basePort, numPeers, processingNanosPerRecord, muWorkerCapacity);
        try {
            AtomicLong fallbackExecutions = new AtomicLong();
            var realProcessor = fallbackProcessor(processingNanosPerRecord, fallbackExecutions);

            List<PeerCandidate> candidatesInRankOrder = new ArrayList<>(numPeers);
            for (Peer peer : peers) {
                candidatesInRankOrder.add(new PeerCandidate("localhost", peer.port()));
            }

            List<RankedClaimingRpcBucketWorkerFactory> factories = new ArrayList<>(numOwners);
            List<BucketWorker> workers = new ArrayList<>(numOwners);
            for (int i = 0; i < numOwners; i++) {
                RankedClaimingRpcBucketWorkerFactory factory = new RankedClaimingRpcBucketWorkerFactory(
                        "owner-" + i, candidatesInRankOrder, () -> perOwnerOfferedLambda, realProcessor);
                BucketWorker worker = factory.apply(0); // synchronous claim, in owner order
                factories.add(factory);
                workers.add(worker);
            }

            List<PeerOutcome> peerOutcomes = snapshotPeerOutcomes(peers);
            long ownersServedViaRpc = factories.stream().filter(f -> f.fallbackCount() == 0).count();
            long ownersFellBack = factories.stream().filter(f -> f.fallbackCount() == 1).count();

            return runRecordsAndCollect("B-ranked-candidates-with-retry", peerOutcomes, ownersServedViaRpc, ownersFellBack,
                    workers, numOwners, recordsPerOwner, perOwnerOfferedLambda, awaitTimeoutSeconds, joinTimeoutMs);
        } finally {
            stopPeers(peers);
        }
    }

    private List<Peer> startPeers(int basePort, int numPeers, long processingNanosPerRecord,
                                   double muWorkerCapacity) throws Exception {
        List<Peer> peers = new ArrayList<>(numPeers);
        for (int p = 0; p < numPeers; p++) {
            // bucketId 0 for every peer: admission is keyed by claimId/lambda, not bucketId,
            // so every peer serving the same bucketId is harmless and matches
            // AdmissionControlRaceExperiment's own convention.
            int port = basePort + p;
            RpcPeerWorker worker = new RpcPeerWorker(0, port, processingNanosPerRecord, muWorkerCapacity, 1);
            worker.start();
            peers.add(new Peer(worker, port));
        }
        return peers;
    }

    private void stopPeers(List<Peer> peers) {
        for (Peer peer : peers) {
            try {
                peer.worker().close();
            } catch (RuntimeException e) {
                log.warn("failed to stop peer on port {}", peer.port(), e);
            }
        }
    }

    private java.util.function.BiConsumer<ConsumerRecord<String, byte[]>, Runnable> fallbackProcessor(
            long processingNanosPerRecord, AtomicLong fallbackExecutions) {
        return (rec, onDone) -> {
            long end = System.nanoTime() + processingNanosPerRecord;
            while (System.nanoTime() < end) {
                // mirrors RpcPeerWorker's own busyWork, so the fallback path's per-record
                // cost is comparable to what a peer would have charged for the same record.
            }
            fallbackExecutions.incrementAndGet();
            onDone.run();
        };
    }

    /**
     * Snapshot taken right after the claim phase (before any record submission or
     * worker close/release), so it reflects exactly what admission decided -- admitted
     * load does not change again until a worker closes, since each owner claims exactly
     * once in this experiment.
     */
    private List<PeerOutcome> snapshotPeerOutcomes(List<Peer> peers) {
        List<PeerOutcome> outcomes = new ArrayList<>(peers.size());
        for (Peer peer : peers) {
            double load = peer.worker().admission().currentlyAdmittedLoad();
            double capacity = peer.worker().admission().muWorkerCapacity();
            boolean withinCapacity = load <= capacity + 1e-9;
            if (!withinCapacity) {
                // Core correctness property under test -- fail loudly rather than just
                // recording a false boolean if it's ever violated.
                throw new IllegalStateException("peer " + peer.port() + " admitted load " + load
                        + " exceeds its own capacity " + capacity);
            }
            outcomes.add(new PeerOutcome("localhost", peer.port(), capacity,
                    peer.worker().admission().admittedClaimCount(), load, withinCapacity));
        }
        return outcomes;
    }

    private ConditionResult runRecordsAndCollect(String conditionName, List<PeerOutcome> peerOutcomes,
                                                  long ownersServedViaRpc, long ownersFellBack,
                                                  List<BucketWorker> workers, int numOwners, int recordsPerOwner,
                                                  double perOwnerOfferedLambda, long awaitTimeoutSeconds,
                                                  long joinTimeoutMs) throws Exception {
        long totalExpected = (long) numOwners * recordsPerOwner;
        AtomicLong totalCompleted = new AtomicLong();
        CountDownLatch done = new CountDownLatch((int) totalExpected);

        for (BucketWorker worker : workers) {
            worker.start();
        }

        long start = System.currentTimeMillis();
        List<Thread> offerThreads = new ArrayList<>(numOwners);
        for (int i = 0; i < numOwners; i++) {
            Thread t = offerThread("owner-" + i, workers.get(i), recordsPerOwner, perOwnerOfferedLambda,
                    totalCompleted, done);
            offerThreads.add(t);
        }
        for (Thread t : offerThreads) {
            t.start();
        }
        boolean completed = done.await(awaitTimeoutSeconds, TimeUnit.SECONDS);
        for (Thread t : offerThreads) {
            t.join(joinTimeoutMs);
        }
        if (!completed) {
            log.warn("{}: not all records completed within timeout (completed={}/{})",
                    conditionName, totalCompleted.get(), totalExpected);
        }

        for (BucketWorker worker : workers) {
            worker.close();
        }
        long elapsed = System.currentTimeMillis() - start;

        double aggregateAdmittedLoad = peerOutcomes.stream().mapToDouble(PeerOutcome::admittedLoad).sum();

        return new ConditionResult(conditionName, peerOutcomes, ownersServedViaRpc, ownersFellBack,
                aggregateAdmittedLoad, totalExpected, totalCompleted.get(),
                totalCompleted.get() == totalExpected, elapsed);
    }

    private Thread offerThread(String name, BucketWorker worker, int recordCount, double offeredLambda,
                                AtomicLong totalCompletedOut, CountDownLatch done) {
        long sleepMs = offeredLambda <= 0 ? 0 : Math.max(0, (long) (1000.0 / offeredLambda));
        return new Thread(() -> {
            for (int i = 0; i < recordCount; i++) {
                ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(name + "-source", 0, i,
                        name + "-key-" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
                worker.submit(record, (rec, d) -> { throw new AssertionError("unused in this experiment"); }, () -> {
                    totalCompletedOut.incrementAndGet();
                    done.countDown();
                });
                if (sleepMs > 0) {
                    try {
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }, name);
    }
}
