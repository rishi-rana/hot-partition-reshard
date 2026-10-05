package com.hotpartition.detection;

/**
 * Tracks lambda_t (broker-side arrival rate, from log-end-offset growth) and
 * mu_t (consumer-side service rate, from committed/current position growth)
 * independently, each against its own exponentially-weighted baseline. Lag
 * alone cannot distinguish a genuine load spike from a consumer-side
 * slowdown; this decomposition is what makes that distinction possible.
 */
public final class LambdaMuTracker {

    private final double emaAlpha;

    private Long lastTimestampMs;
    private long lastLogEndOffset;
    private long lastPosition;

    private double lambdaRate = 0.0;
    private double muRate = 0.0;
    private double lambdaBaseline = -1.0;
    private double muBaseline = -1.0;

    public LambdaMuTracker(double emaAlpha) {
        if (emaAlpha <= 0 || emaAlpha > 1) {
            throw new IllegalArgumentException("emaAlpha must be in (0, 1]");
        }
        this.emaAlpha = emaAlpha;
    }

    public synchronized void sample(long timestampMs, long logEndOffset, long position) {
        if (lastTimestampMs != null) {
            double dtSec = (timestampMs - lastTimestampMs) / 1000.0;
            if (dtSec > 0) {
                lambdaRate = Math.max(0.0, (logEndOffset - lastLogEndOffset) / dtSec);
                muRate = Math.max(0.0, (position - lastPosition) / dtSec);
                lambdaBaseline = lambdaBaseline < 0 ? lambdaRate : ema(lambdaBaseline, lambdaRate);
                muBaseline = muBaseline < 0 ? muRate : ema(muBaseline, muRate);
            }
        }
        lastTimestampMs = timestampMs;
        lastLogEndOffset = logEndOffset;
        lastPosition = position;
    }

    private double ema(double prevBaseline, double sample) {
        return emaAlpha * sample + (1 - emaAlpha) * prevBaseline;
    }

    public synchronized double lambdaRate() {
        return lambdaRate;
    }

    public synchronized double muRate() {
        return muRate;
    }

    public synchronized double lambdaBaseline() {
        return lambdaBaseline;
    }

    public synchronized double muBaseline() {
        return muBaseline;
    }

    public synchronized long lag() {
        return lastLogEndOffset - lastPosition;
    }

    public synchronized boolean hasBaseline() {
        return lambdaBaseline >= 0 && muBaseline >= 0;
    }
}
