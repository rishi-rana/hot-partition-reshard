package com.hotpartition.discovery;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PeerRegistryTest {

    private static RegistryEntry entry(String id, double muRate, double muBaseline, long atMillis) {
        return new RegistryEntry(id, "g1", Set.of(), 0.0, muRate, muBaseline, atMillis, "");
    }

    @Test
    void stalePeersAreExcludedFromLiveList() {
        PeerRegistry registry = new PeerRegistry();
        registry.update(entry("fresh", 0, 100, 10_000));
        registry.update(entry("stale", 0, 100, 0));

        var live = registry.listLivePeers(10_000, 5_000);

        assertEquals(1, live.size());
        assertEquals("fresh", live.get(0).consumerInstanceId());
    }

    @Test
    void findIdlePeerExcludesSelf() {
        PeerRegistry registry = new PeerRegistry();
        registry.update(entry("self", 0, 100, 10_000));

        Optional<RegistryEntry> found = registry.findIdlePeer("self", 10_000, 5_000, 0.1);

        assertTrue(found.isEmpty());
    }

    @Test
    void findIdlePeerRequiresMinimumHeadroom() {
        PeerRegistry registry = new PeerRegistry();
        // headroom = 1 - 90/100 = 0.1, below the 0.4 threshold
        registry.update(entry("busy", 90, 100, 10_000));

        Optional<RegistryEntry> found = registry.findIdlePeer("self", 10_000, 5_000, 0.4);

        assertTrue(found.isEmpty());
    }

    @Test
    void findIdlePeerPicksHighestHeadroomAmongCandidates() {
        PeerRegistry registry = new PeerRegistry();
        registry.update(entry("somewhat-idle", 60, 100, 10_000)); // headroom 0.4
        registry.update(entry("very-idle", 10, 100, 10_000));     // headroom 0.9

        Optional<RegistryEntry> found = registry.findIdlePeer("self", 10_000, 5_000, 0.4);

        assertTrue(found.isPresent());
        assertEquals("very-idle", found.get().consumerInstanceId());
    }

    @Test
    void updateOverwritesPriorEntryForSameInstance() {
        PeerRegistry registry = new PeerRegistry();
        registry.update(entry("c1", 0, 100, 1_000));
        registry.update(entry("c1", 50, 100, 2_000));

        var live = registry.listLivePeers(2_000, 5_000);

        assertEquals(1, live.size());
        assertEquals(50.0, live.get(0).muRate());
    }

    @Test
    void removeDropsEntryEvenIfFresh() {
        PeerRegistry registry = new PeerRegistry();
        registry.update(entry("c1", 0, 100, 10_000));
        registry.remove("c1");

        assertFalse(registry.listLivePeers(10_000, 5_000).stream()
                .anyMatch(e -> e.consumerInstanceId().equals("c1")));
    }
}
