package com.hotpartition.reshard.rangebarrier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyDependencyIndexTest {

    @Test
    void firstOccurrenceOfAKeyHasNoDependency() {
        KeyDependencyIndex index = KeyDependencyIndex.builder()
                .record("k1", 10L)
                .build();

        assertEquals(-1L, index.requiredPriorOffset(10L));
    }

    @Test
    void secondOccurrenceDependsOnFirst() {
        KeyDependencyIndex index = KeyDependencyIndex.builder()
                .record("k1", 10L)
                .record("k1", 25L)
                .build();

        assertEquals(10L, index.requiredPriorOffset(25L));
    }

    @Test
    void chainOfThreeOccurrencesEachDependsOnTheImmediatelyPrecedingOne() {
        KeyDependencyIndex index = KeyDependencyIndex.builder()
                .record("k1", 10L)
                .record("k1", 25L)
                .record("k1", 40L)
                .build();

        assertEquals(-1L, index.requiredPriorOffset(10L));
        assertEquals(10L, index.requiredPriorOffset(25L));
        assertEquals(25L, index.requiredPriorOffset(40L));
    }

    @Test
    void differentKeysAreIndependent() {
        KeyDependencyIndex index = KeyDependencyIndex.builder()
                .record("k1", 10L)
                .record("k2", 11L)
                .record("k1", 12L)
                .build();

        assertEquals(-1L, index.requiredPriorOffset(11L)); // k2's first occurrence
        assertEquals(10L, index.requiredPriorOffset(12L)); // k1's second occurrence
    }

    @Test
    void unscannedOffsetHasNoDependency() {
        KeyDependencyIndex index = KeyDependencyIndex.builder().build();
        assertEquals(-1L, index.requiredPriorOffset(999L));
    }
}
