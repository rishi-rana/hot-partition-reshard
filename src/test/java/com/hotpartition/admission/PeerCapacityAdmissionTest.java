package com.hotpartition.admission;

import org.junit.jupiter.api.Test;

import static com.hotpartition.admission.PeerCapacityAdmission.AdmissionResult.ACCEPT;
import static com.hotpartition.admission.PeerCapacityAdmission.AdmissionResult.REJECT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Deterministic, non-timing-based checks of the admission primitive's three required
 * behaviors: accept within capacity, reject the claim that would push total admitted load
 * over capacity (while the first claim remains admitted), and accept again once the first
 * claim is released.
 */
class PeerCapacityAdmissionTest {

    @Test
    void singleClaimWithinCapacityIsAccepted() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);

        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 60.0));
        assertEquals(60.0, admission.currentlyAdmittedLoad());
        assertEquals(1, admission.admittedClaimCount());
    }

    @Test
    void secondClaimThatWouldExceedCapacityIsRejectedWhileFirstRemainsAdmitted() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);

        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 60.0));
        assertEquals(REJECT, admission.tryAdmit("claim-2", 50.0)); // 60 + 50 = 110 > 100

        // the first claim's admission must not have been disturbed by the second's rejection.
        assertEquals(60.0, admission.currentlyAdmittedLoad());
        assertEquals(1, admission.admittedClaimCount());
    }

    @Test
    void afterReleaseAPreviouslyRejectedSizedClaimIsNowAccepted() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);

        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 60.0));
        assertEquals(REJECT, admission.tryAdmit("claim-2", 50.0));

        admission.release("claim-1");
        assertEquals(0.0, admission.currentlyAdmittedLoad());
        assertEquals(0, admission.admittedClaimCount());

        // the same claim-2-sized request (50.0), retried under a fresh claimId since
        // claim-2's earlier rejection was never recorded as admitted, now fits.
        assertEquals(ACCEPT, admission.tryAdmit("claim-2-retry", 50.0));
        assertEquals(50.0, admission.currentlyAdmittedLoad());
    }

    @Test
    void exactlyAtCapacityIsAccepted() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);
        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 100.0));
        assertEquals(REJECT, admission.tryAdmit("claim-2", 0.0001));
    }

    @Test
    void reAdmittingSameClaimIdIsIdempotentAndDoesNotDoubleCount() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);
        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 60.0));
        assertEquals(ACCEPT, admission.tryAdmit("claim-1", 60.0)); // retried request, same id
        assertEquals(60.0, admission.currentlyAdmittedLoad());
        assertEquals(1, admission.admittedClaimCount());
    }

    @Test
    void releasingUnknownClaimIsANoOp() {
        PeerCapacityAdmission admission = new PeerCapacityAdmission(100.0);
        admission.release("never-admitted");
        assertEquals(0.0, admission.currentlyAdmittedLoad());
    }
}
