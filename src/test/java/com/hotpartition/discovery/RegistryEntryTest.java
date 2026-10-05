package com.hotpartition.discovery;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RegistryEntryTest {

    @Test
    void serializeThenParseRoundTrips() {
        RegistryEntry original = new RegistryEntry(
                "consumer-7", "my-group",
                Set.of(new TopicPartition("orders", 3), new TopicPartition("orders", 4)),
                1234.5, 900.0, 1000.0,
                1_700_000_000_000L, "10.0.0.5:7001");

        RegistryEntry roundTripped = RegistryEntry.parse(original.serialize());

        assertEquals(original.consumerInstanceId(), roundTripped.consumerInstanceId());
        assertEquals(original.groupId(), roundTripped.groupId());
        assertEquals(original.assignedPartitions(), roundTripped.assignedPartitions());
        assertEquals(original.lambdaRate(), roundTripped.lambdaRate());
        assertEquals(original.muRate(), roundTripped.muRate());
        assertEquals(original.muBaseline(), roundTripped.muBaseline());
        assertEquals(original.atMillis(), roundTripped.atMillis());
        assertEquals(original.reachability(), roundTripped.reachability());
    }

    @Test
    void emptyAssignedPartitionsRoundTrips() {
        RegistryEntry original = new RegistryEntry("c1", "g1", Set.of(), 0.0, 0.0, 0.0, 1L, "");
        RegistryEntry roundTripped = RegistryEntry.parse(original.serialize());
        assertEquals(Set.of(), roundTripped.assignedPartitions());
    }

    @Test
    void headroomRatioIsZeroWhenSaturated() {
        RegistryEntry saturated = new RegistryEntry("c1", "g1", Set.of(), 100.0, 100.0, 100.0, 1L, "");
        assertEquals(0.0, saturated.headroomRatio(), 1e-9);
    }

    @Test
    void headroomRatioIsOneWhenFullyIdle() {
        RegistryEntry idle = new RegistryEntry("c1", "g1", Set.of(), 0.0, 0.0, 100.0, 1L, "");
        assertEquals(1.0, idle.headroomRatio(), 1e-9);
    }

    @Test
    void headroomRatioIsZeroWhenNoBaselineYet() {
        RegistryEntry noBaseline = new RegistryEntry("c1", "g1", Set.of(), 0.0, 0.0, 0.0, 1L, "");
        assertEquals(0.0, noBaseline.headroomRatio(), 1e-9);
    }
}
