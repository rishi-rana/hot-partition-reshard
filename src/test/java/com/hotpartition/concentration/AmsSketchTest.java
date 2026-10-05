package com.hotpartition.concentration;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class AmsSketchTest {

    @Test
    void hhiIsHighUnderSingleDominantKey() {
        AmsSketch sketch = new AmsSketch(64, 7);
        for (int i = 0; i < 950; i++) {
            sketch.add("hot");
        }
        for (int i = 0; i < 50; i++) {
            sketch.add("cold-" + i);
        }
        double hhi = sketch.estimateHhi();
        assertTrue(hhi > 0.7, "expected high HHI under skew, got " + hhi);
    }

    @Test
    void hhiIsLowUnderUniformTraffic() {
        AmsSketch sketch = new AmsSketch(64, 7);
        int numKeys = 500;
        for (int i = 0; i < 20_000; i++) {
            sketch.add("key-" + (i % numKeys));
        }
        double hhi = sketch.estimateHhi();
        double expected = 1.0 / numKeys;
        assertTrue(hhi < expected * 5, "expected HHI near " + expected + ", got " + hhi);
    }
}
