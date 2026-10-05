package com.hotpartition.detection;

import java.util.Map;

/**
 * The first two stages of the three-stage detection gate (stage 3, key
 * concentration, lives in {@code com.hotpartition.concentration.KeyConcentrationGate}
 * and only runs if this gate returns GENUINE_HOT_LOAD).
 *
 * Stage 1: error/retry counters &mdash; cheapest, run first.
 * Stage 2: lambda/mu decomposition with cross-partition corroboration.
 */
public final class DetectionGate {

    public enum Verdict {
        NORMAL,
        DEGRADED_CONSUMER,
        GENUINE_HOT_LOAD
    }

    private final ErrorRateTracker errorTracker;
    private final int thisPartition;
    private final Map<Integer, LambdaMuTracker> perPartitionTrackers;
    private final double elevatedFactor;
    private final double depressedFactor;
    private final double crossPartitionDepressedFraction;

    public DetectionGate(ErrorRateTracker errorTracker,
                          int thisPartition,
                          Map<Integer, LambdaMuTracker> perPartitionTrackers,
                          double elevatedFactor,
                          double depressedFactor,
                          double crossPartitionDepressedFraction) {
        this.errorTracker = errorTracker;
        this.thisPartition = thisPartition;
        this.perPartitionTrackers = perPartitionTrackers;
        this.elevatedFactor = elevatedFactor;
        this.depressedFactor = depressedFactor;
        this.crossPartitionDepressedFraction = crossPartitionDepressedFraction;
    }

    public Verdict evaluate() {
        if (errorTracker.isElevated()) {
            return Verdict.DEGRADED_CONSUMER;
        }

        LambdaMuTracker self = perPartitionTrackers.get(thisPartition);
        if (self == null || !self.hasBaseline()) {
            return Verdict.NORMAL;
        }

        boolean lambdaElevated = self.lambdaRate() >= elevatedFactor * self.lambdaBaseline();
        if (!lambdaElevated) {
            return Verdict.NORMAL;
        }

        boolean muDepressed = self.muRate() <= depressedFactor * self.muBaseline();
        if (!muDepressed) {
            // lambda is up and mu is tracking it proportionally: this partition is draining
            // more work because more work arrived, not because it got slower.
            return Verdict.GENUINE_HOT_LOAD;
        }

        // mu depressed while lambda elevated: could be hotness outrunning capacity, or
        // degradation. Corroborate with siblings on the same process.
        long total = 0;
        long depressed = 0;
        for (LambdaMuTracker t : perPartitionTrackers.values()) {
            if (!t.hasBaseline()) {
                continue;
            }
            total++;
            if (t.muRate() <= depressedFactor * t.muBaseline()) {
                depressed++;
            }
        }
        double depressedFraction = total == 0 ? 0.0 : (double) depressed / total;
        if (depressedFraction >= crossPartitionDepressedFraction) {
            // most/all partitions on this process show depressed mu simultaneously:
            // process-wide problem (GC, CPU starvation, shared downstream dependency).
            return Verdict.DEGRADED_CONSUMER;
        }

        // only this partition is depressed while lambda is elevated: content-specific
        // degradation (poison pill, expensive key) rather than pure volume.
        return Verdict.DEGRADED_CONSUMER;
    }
}
