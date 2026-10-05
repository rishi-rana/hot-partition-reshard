package com.hotpartition.failure;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Worker-to-owner heartbeat and dead-bucket detection, per the design doc's
 * resolved failure/recovery protocol: each bucket worker reports
 * (bucketId, leaseToken, currentLowWatermark) every {@code heartbeatIntervalMs}
 * (default 2s); a bucket with no heartbeat for {@code leaseTimeoutMs}
 * (default 3x the interval, ~6s) is declared dead and its lease revoked.
 */
public final class HeartbeatLease {

    public static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 2000;
    public static final long DEFAULT_LEASE_TIMEOUT_MS = 3 * DEFAULT_HEARTBEAT_INTERVAL_MS;

    public record Heartbeat(int bucketId, String leaseToken, long lowWatermark, long atMillis) {
    }

    private final long heartbeatIntervalMs;
    private final long leaseTimeoutMs;
    private final Map<Integer, Heartbeat> lastHeartbeat = new ConcurrentHashMap<>();

    public HeartbeatLease(long heartbeatIntervalMs, long leaseTimeoutMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.leaseTimeoutMs = leaseTimeoutMs;
    }

    public HeartbeatLease() {
        this(DEFAULT_HEARTBEAT_INTERVAL_MS, DEFAULT_LEASE_TIMEOUT_MS);
    }

    public void heartbeat(int bucketId, String leaseToken, long lowWatermark) {
        lastHeartbeat.put(bucketId, new Heartbeat(bucketId, leaseToken, lowWatermark, System.currentTimeMillis()));
    }

    /** Buckets in {@code activeBuckets} with no heartbeat within the lease timeout. */
    public List<Integer> detectDeadBuckets(Set<Integer> activeBuckets) {
        long now = System.currentTimeMillis();
        List<Integer> dead = new ArrayList<>();
        for (int bucketId : activeBuckets) {
            Heartbeat hb = lastHeartbeat.get(bucketId);
            if (hb == null || now - hb.atMillis() > leaseTimeoutMs) {
                dead.add(bucketId);
            }
        }
        return dead;
    }

    public long lastKnownWatermark(int bucketId) {
        Heartbeat hb = lastHeartbeat.get(bucketId);
        return hb == null ? -1 : hb.lowWatermark();
    }

    public void revoke(int bucketId) {
        lastHeartbeat.remove(bucketId);
    }

    public long heartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public long leaseTimeoutMs() {
        return leaseTimeoutMs;
    }
}
