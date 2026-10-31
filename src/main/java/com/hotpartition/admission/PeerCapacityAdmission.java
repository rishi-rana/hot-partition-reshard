package com.hotpartition.admission;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Transport-agnostic admission-control primitive for one peer process: tracks the sum of
 * estimated λ across every bucket-claim this peer has currently accepted <em>from any
 * owner</em>, and atomically decides whether a new claim fits under this peer's own
 * configured/measured sustainable rate (μ_worker).
 *
 * This exists to close a real gap: {@link com.hotpartition.discovery.PeerRegistry#findIdlePeer}
 * picks a peer off a stale, self-reported, periodically-published headroom number. If two
 * different hot-partition owners query the registry in the same window, nothing stops both
 * from picking the same idle peer before that peer's next heartbeat reflects either
 * commitment. This class is the thing that *does* stop it, for whichever transport can wire
 * it in synchronously (see {@code RpcPeerWorker}) -- a single running total per peer
 * process, guarded so that "check capacity" and "commit the claim" happen as one atomic
 * step, not a check-then-set race between two threads handling two concurrent owners'
 * requests.
 *
 * <p>Not thread-contended in the "many claims per microsecond" sense this prototype needs
 * to worry about -- the lock is held only across simple arithmetic -- so a single
 * {@code synchronized} block is both correct and sufficiently fast, in preference to a
 * lock-free {@link ConcurrentHashMap}-plus-{@link java.util.concurrent.atomic.AtomicLong}
 * scheme whose check-and-increment would itself race across two threads calling
 * {@link #tryAdmit} at the same time (the exact bug class this class exists to avoid
 * reintroducing one level down).
 */
public final class PeerCapacityAdmission {

    /** Outcome of a {@link #tryAdmit} call. */
    public enum AdmissionResult {
        ACCEPT,
        REJECT
    }

    private final double muWorkerCapacity;
    private final Object lock = new Object();

    /** claimId -> the estimatedLambda it was admitted with. Mutated only under {@link #lock}. */
    private final Map<String, Double> admittedClaims = new ConcurrentHashMap<>();
    private double currentlyAdmittedLoad = 0.0;

    /**
     * @param muWorkerCapacity this peer's own measured/configured sustainable processing
     *                         rate (records/sec, or whatever unit {@code estimatedLambda}
     *                         is expressed in -- the two must agree). Must be positive.
     */
    public PeerCapacityAdmission(double muWorkerCapacity) {
        if (muWorkerCapacity <= 0) {
            throw new IllegalArgumentException("muWorkerCapacity must be positive, got " + muWorkerCapacity);
        }
        this.muWorkerCapacity = muWorkerCapacity;
    }

    /**
     * Atomically checks whether {@code currentlyAdmittedLoad + estimatedLambda} fits under
     * {@code muWorkerCapacity}; if so, records the claim and returns {@link AdmissionResult#ACCEPT},
     * otherwise returns {@link AdmissionResult#REJECT} and the running total is left
     * unchanged. Re-admitting the same {@code claimId} (e.g. a retried request) is treated
     * as idempotent: it does not double-count, and does not re-run the capacity check
     * against the (possibly different) {@code estimatedLambda} passed the second time --
     * callers that need to change a claim's size must {@link #release} then re-{@code
     * tryAdmit} under a fresh capacity check.
     */
    public AdmissionResult tryAdmit(String claimId, double estimatedLambda) {
        if (estimatedLambda < 0) {
            throw new IllegalArgumentException("estimatedLambda must be non-negative, got " + estimatedLambda);
        }
        synchronized (lock) {
            if (admittedClaims.containsKey(claimId)) {
                return AdmissionResult.ACCEPT;
            }
            if (currentlyAdmittedLoad + estimatedLambda > muWorkerCapacity) {
                return AdmissionResult.REJECT;
            }
            admittedClaims.put(claimId, estimatedLambda);
            currentlyAdmittedLoad += estimatedLambda;
            return AdmissionResult.ACCEPT;
        }
    }

    /**
     * Removes {@code claimId}'s contribution from the running total (bucket completed,
     * reassigned, or owner died). A no-op if {@code claimId} was never admitted or was
     * already released -- callers do not need to track admission state themselves to call
     * this safely.
     */
    public void release(String claimId) {
        synchronized (lock) {
            Double lambda = admittedClaims.remove(claimId);
            if (lambda != null) {
                currentlyAdmittedLoad -= lambda;
            }
        }
    }

    /** The current running total of admitted estimated λ across all live claims. */
    public double currentlyAdmittedLoad() {
        synchronized (lock) {
            return currentlyAdmittedLoad;
        }
    }

    public double muWorkerCapacity() {
        return muWorkerCapacity;
    }

    /** Number of currently-admitted (not yet released) claims. */
    public int admittedClaimCount() {
        return admittedClaims.size();
    }
}
