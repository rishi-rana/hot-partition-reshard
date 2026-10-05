package com.hotpartition.concentration;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MisraGriesSketchTest {

    @Test
    void identifiesDominantKeyUnderHeavySkew() {
        MisraGriesSketch<String> sketch = new MisraGriesSketch<>(10);
        for (int i = 0; i < 900; i++) {
            sketch.add("hot");
        }
        Random rnd = new Random(1);
        for (int i = 0; i < 100; i++) {
            sketch.add("cold-" + rnd.nextInt(1000));
        }
        var top = sketch.topCandidate().orElseThrow();
        assertEquals("hot", top.key());
        assertTrue(top.estimatedShare() > 0.8, "expected dominant key share > 0.8, got " + top.estimatedShare());
    }

    @Test
    void underUniformTrafficNoKeyDominates() {
        MisraGriesSketch<String> sketch = new MisraGriesSketch<>(20);
        for (int i = 0; i < 10_000; i++) {
            sketch.add("key-" + (i % 500));
        }
        var top = sketch.topCandidate().orElseThrow();
        // Space-Saving overestimates under eviction when distinct keys >> capacity (by
        // design, to guarantee catching true heavy hitters) -- the point of this test is
        // that no candidate looks anywhere near dominant, not that the estimate is tight.
        assertTrue(top.estimatedShare() < 0.15, "expected no dominant key, got share " + top.estimatedShare());
    }

    @Test
    void emptySketchHasNoCandidate() {
        MisraGriesSketch<String> sketch = new MisraGriesSketch<>(5);
        assertTrue(sketch.topCandidate().isEmpty());
    }
}
