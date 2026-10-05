package com.hotpartition.concentration;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Space-Saving (Misra-Gries family) heavy-hitter sketch. Maintains a fixed-size
 * table of candidate keys with approximate counts. Any key whose true frequency
 * exceeds 1/(k+1) of the stream is guaranteed to appear in the table, with
 * estimated count bounded by: trueCount <= estimatedCount <= trueCount + error.
 */
public final class MisraGriesSketch<K> {

    private record Entry<K>(K key, long count, long error) {
        Entry<K> incremented() {
            return new Entry<>(key, count + 1, error);
        }
    }

    private final int capacity;
    private final Map<K, Entry<K>> table;
    private long totalCount = 0;

    public MisraGriesSketch(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be >= 1");
        }
        this.capacity = capacity;
        this.table = new LinkedHashMap<>(capacity * 2);
    }

    public synchronized void add(K key) {
        totalCount++;
        Entry<K> existing = table.get(key);
        if (existing != null) {
            table.put(key, existing.incremented());
            return;
        }
        if (table.size() < capacity) {
            table.put(key, new Entry<>(key, 1, 0));
            return;
        }
        Entry<K> min = table.values().stream()
                .min(Comparator.comparingLong(Entry::count))
                .orElseThrow();
        table.remove(min.key());
        table.put(key, new Entry<>(key, min.count() + 1, min.count()));
    }

    /** The current top candidate by estimated count, if any records have been seen. */
    public synchronized Optional<Candidate<K>> topCandidate() {
        return table.values().stream()
                .max(Comparator.comparingLong(Entry::count))
                .map(e -> new Candidate<>(e.key(), e.count(), e.error(), totalCount));
    }

    public synchronized long totalCount() {
        return totalCount;
    }

    public synchronized void reset() {
        table.clear();
        totalCount = 0;
    }

    public record Candidate<K>(K key, long estimatedCount, long maxError, long windowTotal) {
        /** Fraction of the observed window this candidate's estimated count represents. */
        public double estimatedShare() {
            return windowTotal == 0 ? 0.0 : (double) estimatedCount / windowTotal;
        }
    }
}
