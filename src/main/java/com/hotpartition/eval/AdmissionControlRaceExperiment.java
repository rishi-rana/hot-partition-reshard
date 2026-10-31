package com.hotpartition.eval;

import com.hotpartition.reshard.BucketWorker;
import com.hotpartition.transport.rpc.ClaimingRpcBucketWorkerFactory;
import com.hotpartition.transport.rpc.RpcBucketWorker;
import com.hotpartition.transport.rpc.RpcPeerWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demonstrates the cross-owner oversubscription gap described in
 * {@link com.hotpartition.admission.PeerCapacityAdmission}'s javadoc, and that the
 * admission-control primitive actually closes it, against transport Option A (direct
 * RPC) -- the one transport with a real synchronous channel, per the design doc's
 * Mechanism gap. Pure-JVM: this transport needs no Kafka broker at all for its own
 * delegation path (confirmed by {@code RpcEndToEndSmokeTest}'s own comment to that
 * effect), so this experiment constructs two "owners" (O1, O2) directly, pointed at a
 * shared small peer pool, with no registry/Kafka plumbing standing in for peer discovery
 * -- the race this reproduces is "two owners picked the same peer," which is simulated by
 * simply pointing both at it, the same end state {@code findIdlePeer} returning a stale
 * answer twice would produce.
 *
 * <p>The peer is deliberately configured with a single HTTP-server worker thread, so
 * {@code muWorkerCapacity} maps onto a real, physically observable bottleneck (one
 * busy-waiting thread) rather than just a bookkeeping number nothing enforces.
 *
 * <p>Two runs:
 * <ul>
 *   <li>{@link #runWithoutAdmissionControl} -- both owners dispatch directly via
 *       {@link RpcBucketWorker}, no {@code /claim} call, exactly the pre-fix code path.
 *       Both owners offer load the peer's one worker thread cannot sustain combined;
 *       the signal is per-record completion latency climbing as internal queues back up
 *       (a valid signal per the task spec when raw throughput alone doesn't show it,
 *       since the peer's own serialization already caps "throughput" at its physical
 *       ceiling regardless of how much is offered -- the thing that actually degrades is
 *       how long each caller waits).</li>
 *   <li>{@link #runWithAdmissionControl} -- both owners go through
 *       {@link ClaimingRpcBucketWorkerFactory}; the first to claim is accepted, the second
 *       is rejected and falls back to in-process processing instead of oversubscribing the
 *       peer.</li>
 * </ul>
 */
public final class AdmissionControlRaceExperiment {

    private static final Logger log = LoggerFactory.getLogger(AdmissionControlRaceExperiment.class);

    public record LatencyStats(double avgMs, double maxMs, double p95Ms, int count) {
    }

    public record WithoutAdmissionResult(
            double muWorkerCapacity,
            double owner1OfferedLambda,
            double owner2OfferedLambda,
            long totalElapsedMs,
            double achievedCombinedThroughputPerSec,
            LatencyStats owner1Latency,
            LatencyStats owner2Latency
    ) {
    }

    public record WithAdmissionResult(
            double muWorkerCapacity,
            double owner1OfferedLambda,
            double owner2OfferedLambda,
            double admittedLoadAfterBothClaim,
            boolean admittedLoadStayedUnderCapacity,
            long rejectedClaimCount,
            long fallbackExecutionCount,
            long totalElapsedMs,
            LatencyStats owner1Latency,
            LatencyStats owner2Latency
    ) {
    }

    /**
     * @param port                    base port for the embedded peer's HTTP server.
     * @param recordsPerOwner         how many records each of the two owners sends.
     * @param processingNanosPerRecord simulated per-record busy-work cost -- shared by the
     *                                peer's RPC path and the owner's in-process fallback
     *                                path, so the two are apples-to-apples per record.
     * @param muWorkerCapacity        the peer's configured sustainable rate (records/sec).
     * @param perOwnerOfferedLambda   each owner's offered rate, used both as the
     *                                claim's estimatedLambda and (loosely) the rate each
     *                                owner's submission loop targets.
     * @param awaitTimeoutSeconds     how long to wait for all records to complete before
     *                                giving up and reporting a partial result -- must scale
     *                                with {@code recordsPerOwner}; the original hardcoded
     *                                120s was sized for a 60-record toy run, not the
     *                                1,000,000-record scale this is also used at.
     * @param joinTimeoutMs           how long to wait for each offer thread to join after
     *                                the completion wait returns/times out.
     */
    public WithoutAdmissionResult runWithoutAdmissionControl(int port, int recordsPerOwner,
                                                              long processingNanosPerRecord,
                                                              double muWorkerCapacity,
                                                              double perOwnerOfferedLambda,
                                                              long awaitTimeoutSeconds,
                                                              long joinTimeoutMs) throws Exception {
        int bucketId = 0;
        // muWorkerCapacity is passed through for logging/comparison only here -- this run
        // does NOT enforce it; direct RpcBucketWorker dispatch, exactly the pre-fix path.
        try (RpcPeerWorker peer = new RpcPeerWorker(bucketId, port, processingNanosPerRecord, muWorkerCapacity, 1)) {
            peer.start();

            List<Long> owner1LatenciesMs = Collections.synchronizedList(new ArrayList<>());
            List<Long> owner2LatenciesMs = Collections.synchronizedList(new ArrayList<>());

            long start = System.currentTimeMillis();
            try (RpcBucketWorker worker1 = new RpcBucketWorker(bucketId, "localhost", port);
                 RpcBucketWorker worker2 = new RpcBucketWorker(bucketId, "localhost", port)) {
                worker1.start();
                worker2.start();

                CountDownLatch done = new CountDownLatch(recordsPerOwner * 2);
                Thread t1 = offerThread("owner-1-no-admission", worker1, recordsPerOwner, perOwnerOfferedLambda,
                        owner1LatenciesMs, done);
                Thread t2 = offerThread("owner-2-no-admission", worker2, recordsPerOwner, perOwnerOfferedLambda,
                        owner2LatenciesMs, done);
                t1.start();
                t2.start();
                boolean completed = done.await(awaitTimeoutSeconds, TimeUnit.SECONDS);
                t1.join(joinTimeoutMs);
                t2.join(joinTimeoutMs);
                if (!completed) {
                    log.warn("runWithoutAdmissionControl: not all records completed within timeout "
                            + "(owner1 completed={}, owner2 completed={}) -- itself part of the degradation story",
                            owner1LatenciesMs.size(), owner2LatenciesMs.size());
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            int totalCompleted = owner1LatenciesMs.size() + owner2LatenciesMs.size();
            double achievedThroughput = elapsed == 0 ? 0 : totalCompleted / (elapsed / 1000.0);

            return new WithoutAdmissionResult(muWorkerCapacity, perOwnerOfferedLambda, perOwnerOfferedLambda,
                    elapsed, achievedThroughput,
                    stats(owner1LatenciesMs), stats(owner2LatenciesMs));
        }
    }

    public WithAdmissionResult runWithAdmissionControl(int port, int recordsPerOwner,
                                                        long processingNanosPerRecord,
                                                        double muWorkerCapacity,
                                                        double perOwnerOfferedLambda,
                                                        long awaitTimeoutSeconds,
                                                        long joinTimeoutMs) throws Exception {
        int bucketId = 0;
        try (RpcPeerWorker peer = new RpcPeerWorker(bucketId, port, processingNanosPerRecord, muWorkerCapacity, 1)) {
            peer.start();

            AtomicLong fallbackExecutions = new AtomicLong();
            // The owner's "real" processor: used only by whichever owner's claim is
            // rejected (the RPC path itself never invokes it -- the peer does equivalent
            // busy-work -- so this is specifically the fallback-in-process logic).
            var realProcessor = (java.util.function.BiConsumer<ConsumerRecord<String, byte[]>, Runnable>) (rec, onDone) -> {
                long end = System.nanoTime() + processingNanosPerRecord;
                while (System.nanoTime() < end) {
                    // mirrors RpcPeerWorker's own busyWork, so the fallback path's per-record
                    // cost is comparable to what the peer would have charged.
                }
                fallbackExecutions.incrementAndGet();
                onDone.run();
            };

            ClaimingRpcBucketWorkerFactory factory1 = new ClaimingRpcBucketWorkerFactory(
                    "owner-1", "localhost", port, () -> perOwnerOfferedLambda, realProcessor);
            ClaimingRpcBucketWorkerFactory factory2 = new ClaimingRpcBucketWorkerFactory(
                    "owner-2", "localhost", port, () -> perOwnerOfferedLambda, realProcessor);

            List<Long> owner1LatenciesMs = Collections.synchronizedList(new ArrayList<>());
            List<Long> owner2LatenciesMs = Collections.synchronizedList(new ArrayList<>());

            long start = System.currentTimeMillis();
            BucketWorker worker1 = factory1.apply(bucketId); // claims synchronously here
            BucketWorker worker2 = factory2.apply(bucketId); // claims synchronously here -- sees owner1 already admitted

            double admittedLoadAfterBothClaim = peer.admission().currentlyAdmittedLoad();
            boolean stayedUnderCapacity = admittedLoadAfterBothClaim <= muWorkerCapacity + 1e-9;

            worker1.start();
            worker2.start();
            try {
                CountDownLatch done = new CountDownLatch(recordsPerOwner * 2);
                Thread t1 = offerThread("owner-1-with-admission", worker1, recordsPerOwner, perOwnerOfferedLambda,
                        owner1LatenciesMs, done);
                Thread t2 = offerThread("owner-2-with-admission", worker2, recordsPerOwner, perOwnerOfferedLambda,
                        owner2LatenciesMs, done);
                t1.start();
                t2.start();
                boolean completed = done.await(awaitTimeoutSeconds, TimeUnit.SECONDS);
                t1.join(joinTimeoutMs);
                t2.join(joinTimeoutMs);
                if (!completed) {
                    log.warn("runWithAdmissionControl: not all records completed within timeout");
                }
            } finally {
                worker1.close();
                worker2.close();
            }
            long elapsed = System.currentTimeMillis() - start;

            long rejected = factory1.fallbackCount() + factory2.fallbackCount();

            return new WithAdmissionResult(muWorkerCapacity, perOwnerOfferedLambda, perOwnerOfferedLambda,
                    admittedLoadAfterBothClaim, stayedUnderCapacity, rejected, fallbackExecutions.get(),
                    elapsed, stats(owner1LatenciesMs), stats(owner2LatenciesMs));
        }
    }

    private Thread offerThread(String name, BucketWorker worker, int recordCount, double offeredLambda,
                                List<Long> latenciesMsOut, CountDownLatch done) {
        long sleepMs = offeredLambda <= 0 ? 0 : Math.max(0, (long) (1000.0 / offeredLambda));
        return new Thread(() -> {
            for (int i = 0; i < recordCount; i++) {
                long sendAt = System.currentTimeMillis();
                ConsumerRecord<String, byte[]> record = new ConsumerRecord<>(name + "-source", 0, i,
                        name + "-key-" + i, ("v" + i).getBytes(StandardCharsets.UTF_8));
                worker.submit(record, (rec, d) -> { throw new AssertionError("unused in this experiment"); }, () -> {
                    latenciesMsOut.add(System.currentTimeMillis() - sendAt);
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

    private static LatencyStats stats(List<Long> latenciesMs) {
        List<Long> snapshot;
        synchronized (latenciesMs) {
            snapshot = new ArrayList<>(latenciesMs);
        }
        if (snapshot.isEmpty()) {
            return new LatencyStats(0, 0, 0, 0);
        }
        Collections.sort(snapshot);
        double sum = 0;
        for (long v : snapshot) {
            sum += v;
        }
        double avg = sum / snapshot.size();
        double max = snapshot.get(snapshot.size() - 1);
        int p95Index = Math.max(0, (int) Math.ceil(snapshot.size() * 0.95) - 1);
        double p95 = snapshot.get(p95Index);
        return new LatencyStats(avg, max, p95, snapshot.size());
    }
}
