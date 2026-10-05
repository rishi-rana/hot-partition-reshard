package com.hotpartition.eval;

import java.util.Random;

/**
 * Generates keys from a Zipf-like distribution so the skew-sensitivity
 * evaluation can sweep from uniform (skew=0, every key equally likely,
 * "diverse" case) to single-dominant-key (large skew, one key dominates
 * almost all mass).
 */
public final class ZipfKeyGenerator {

    private final int numKeys;
    private final double[] cumulative;
    private final Random random;

    public ZipfKeyGenerator(int numKeys, double skew, long seed) {
        this.numKeys = numKeys;
        this.random = new Random(seed);
        double[] weights = new double[numKeys];
        double total = 0;
        for (int i = 1; i <= numKeys; i++) {
            weights[i - 1] = 1.0 / Math.pow(i, skew);
            total += weights[i - 1];
        }
        this.cumulative = new double[numKeys];
        double running = 0;
        for (int i = 0; i < numKeys; i++) {
            running += weights[i] / total;
            cumulative[i] = running;
        }
    }

    public String nextKey() {
        double u = random.nextDouble();
        int lo = 0, hi = numKeys - 1;
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            if (cumulative[mid] < u) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return "key-" + lo;
    }

    /** Theoretical HHI for this distribution: sum of (share_i)^2 over all keys. */
    public double theoreticalHhi() {
        double hhi = 0;
        double prev = 0;
        for (double c : cumulative) {
            double share = c - prev;
            hhi += share * share;
            prev = c;
        }
        return hhi;
    }
}
