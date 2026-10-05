package com.hotpartition.discovery;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory view of the {@code __hotpartition_registry} control topic: every consumer
 * instance's latest self-reported state, keyed by consumerInstanceId (compaction keeps
 * only the latest per key, so this map's eventual state mirrors the topic's). Pure logic,
 * no Kafka dependency -- {@link RegistryReader} is what actually feeds it from the topic.
 *
 * "Idle" is approximated here as low headroomRatio-complement: a peer whose own mu_t is
 * comfortably below its own mu_baseline has slack to lend out. This is a simplification
 * flagged in the design doc as worth revisiting -- mu_t tracking its own baseline really
 * only means "keeping up with its own load," not "has spare CPU," but it's the cheapest
 * signal available without adding a separate resource-utilization probe.
 */
public final class PeerRegistry {

    public static final long DEFAULT_TTL_MS = 3 * com.hotpartition.failure.HeartbeatLease.DEFAULT_HEARTBEAT_INTERVAL_MS;
    public static final double DEFAULT_IDLE_HEADROOM_THRESHOLD = 0.4;

    private final Map<String, RegistryEntry> entries = new ConcurrentHashMap<>();

    public void update(RegistryEntry entry) {
        entries.put(entry.consumerInstanceId(), entry);
    }

    public void remove(String consumerInstanceId) {
        entries.remove(consumerInstanceId);
    }

    /** All entries not older than {@code ttlMillis} as of {@code nowMillis}. */
    public List<RegistryEntry> listLivePeers(long nowMillis, long ttlMillis) {
        List<RegistryEntry> live = new ArrayList<>();
        for (RegistryEntry e : entries.values()) {
            if (nowMillis - e.atMillis() <= ttlMillis) {
                live.add(e);
            }
        }
        return live;
    }

    /**
     * The live peer (excluding {@code excludeInstanceId}) with the most headroom, provided
     * it clears {@code minHeadroomRatio}. Empty if no live peer qualifies.
     */
    public Optional<RegistryEntry> findIdlePeer(String excludeInstanceId, long nowMillis, long ttlMillis,
                                                 double minHeadroomRatio) {
        return listLivePeers(nowMillis, ttlMillis).stream()
                .filter(e -> !e.consumerInstanceId().equals(excludeInstanceId))
                .filter(e -> e.headroomRatio() >= minHeadroomRatio)
                .max(Comparator.comparingDouble(RegistryEntry::headroomRatio));
    }

    public int liveCount(long nowMillis, long ttlMillis) {
        return listLivePeers(nowMillis, ttlMillis).size();
    }
}
