package com.hotpartition.concentration;

import java.util.Arrays;

/**
 * AMS (Alon-Matias-Szegedy) second-frequency-moment sketch. Estimates
 * F2 = sum(count_i^2) over the key distribution seen so far, from which the
 * Herfindahl-Hirschman concentration index HHI = F2 / n^2 is derived without
 * ever enumerating individual keys.
 *
 * Each of {@code numEstimators} independent counters accumulates a random
 * +1/-1 sign per key occurrence; each counter squared is an unbiased
 * estimator of F2, and we take the median across counters for robustness.
 */
public final class AmsSketch {

    private final int numEstimators;
    private final long[] seeds;
    private final long[] z;
    private long n = 0;

    public AmsSketch(int numEstimators, long randomSeed) {
        if (numEstimators < 1) {
            throw new IllegalArgumentException("numEstimators must be >= 1");
        }
        this.numEstimators = numEstimators;
        this.seeds = new long[numEstimators];
        this.z = new long[numEstimators];
        java.util.Random rnd = new java.util.Random(randomSeed);
        for (int i = 0; i < numEstimators; i++) {
            seeds[i] = rnd.nextLong();
        }
    }

    public synchronized void add(Object key) {
        n++;
        int keyHash = key.hashCode();
        for (int i = 0; i < numEstimators; i++) {
            long h = mix(keyHash, seeds[i]);
            int sign = (h & 1L) == 0 ? 1 : -1;
            z[i] += sign;
        }
    }

    /** Estimated second frequency moment F2 = sum(count_i^2). */
    public synchronized double estimateF2() {
        double[] estimates = new double[numEstimators];
        for (int i = 0; i < numEstimators; i++) {
            estimates[i] = (double) z[i] * (double) z[i];
        }
        Arrays.sort(estimates);
        return estimates[estimates.length / 2];
    }

    /** Herfindahl-Hirschman concentration index in [1/distinctKeys, 1]; near 1 means one key dominates. */
    public synchronized double estimateHhi() {
        if (n == 0) {
            return 0.0;
        }
        double f2 = estimateF2();
        return f2 / ((double) n * (double) n);
    }

    public synchronized long totalCount() {
        return n;
    }

    public synchronized void reset() {
        Arrays.fill(z, 0L);
        n = 0;
    }

    private static long mix(int keyHash, long seed) {
        long h = keyHash ^ seed;
        h ^= (h >>> 33);
        h *= 0xff51afd7ed558ccdL;
        h ^= (h >>> 33);
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= (h >>> 33);
        return h;
    }
}
