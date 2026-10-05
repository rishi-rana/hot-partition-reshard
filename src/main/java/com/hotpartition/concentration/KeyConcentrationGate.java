package com.hotpartition.concentration;

import java.util.Optional;

/**
 * Stage 3 of the detection gate: decides, for a hot partition already confirmed
 * genuinely load-driven (see {@code com.hotpartition.detection}), whether it is
 * diverse-but-skewed (resharding helps) or single-dominant-key (resharding
 * cannot help, per the capacity-derived bound: a key is a structural bottleneck
 * once its own rate meets or exceeds a single worker's sustainable throughput).
 */
public final class KeyConcentrationGate<K> {

    public enum Verdict {
        DIVERSE_SKEWED,
        SINGLE_DOMINANT_KEY,
        INSUFFICIENT_DATA
    }

    public record Result<K>(Verdict verdict, double hhi, Optional<K> dominantKey,
                             double dominantKeyEstimatedRate, double muWorkerBaseline) {
    }

    private final MisraGriesSketch<K> topK;
    private final AmsSketch ams;

    public KeyConcentrationGate(int topKCapacity, int amsEstimators, long amsSeed) {
        this.topK = new MisraGriesSketch<>(topKCapacity);
        this.ams = new AmsSketch(amsEstimators, amsSeed);
    }

    public void record(K key) {
        topK.add(key);
        ams.add(key);
    }

    /**
     * @param windowSeconds        wall-clock duration the sketches have been accumulating over
     * @param muWorkerRecordsPerSec measured baseline sustainable throughput of a single worker
     */
    public Result<K> evaluate(double windowSeconds, double muWorkerRecordsPerSec) {
        if (ams.totalCount() == 0 || windowSeconds <= 0) {
            return new Result<>(Verdict.INSUFFICIENT_DATA, 0.0, Optional.empty(), 0.0, muWorkerRecordsPerSec);
        }
        double hhi = ams.estimateHhi();
        Optional<MisraGriesSketch.Candidate<K>> top = topK.topCandidate();
        if (top.isEmpty()) {
            return new Result<>(Verdict.INSUFFICIENT_DATA, hhi, Optional.empty(), 0.0, muWorkerRecordsPerSec);
        }
        double lambdaKey = top.get().estimatedCount() / windowSeconds;
        Verdict verdict = lambdaKey >= muWorkerRecordsPerSec
                ? Verdict.SINGLE_DOMINANT_KEY
                : Verdict.DIVERSE_SKEWED;
        return new Result<>(verdict, hhi, Optional.of(top.get().key()), lambdaKey, muWorkerRecordsPerSec);
    }

    public void reset() {
        topK.reset();
        ams.reset();
    }
}
