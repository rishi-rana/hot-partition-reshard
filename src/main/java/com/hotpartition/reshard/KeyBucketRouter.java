package com.hotpartition.reshard;

/**
 * Hashes a record's key into one of N fixed buckets for the lifetime of a
 * "generation" (see {@link com.hotpartition.failure.GenerationBarrier} for how
 * N changes mid-flight). Every record for a given key always lands in the same
 * bucket for a given generation, so per-key order is preserved across the
 * virtual resharding boundary for free, the same way partition-by-key already
 * works at the broker level.
 */
public final class KeyBucketRouter {

    private final int numBuckets;
    private final int generation;

    public KeyBucketRouter(int numBuckets, int generation) {
        if (numBuckets < 1) {
            throw new IllegalArgumentException("numBuckets must be >= 1");
        }
        this.numBuckets = numBuckets;
        this.generation = generation;
    }

    public int bucketFor(Object key) {
        if (key == null) {
            return 0;
        }
        int h = stableHash(key);
        return Math.floorMod(h, numBuckets);
    }

    public int numBuckets() {
        return numBuckets;
    }

    public int generation() {
        return generation;
    }

    private static int stableHash(Object key) {
        byte[] bytes = key instanceof byte[] b ? b : key.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // Murmur2-style mix, same family Kafka's own default partitioner uses, so the
        // bucket assignment is as stable/well-distributed as Kafka's own key partitioning.
        int seed = 0x9747b28c;
        int m = 0x5bd1e995;
        int r = 24;
        int h = seed ^ bytes.length;
        int length4 = bytes.length / 4 * 4;
        for (int i = 0; i < length4; i += 4) {
            int k = (bytes[i] & 0xff) | ((bytes[i + 1] & 0xff) << 8)
                    | ((bytes[i + 2] & 0xff) << 16) | ((bytes[i + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        int remaining = bytes.length - length4;
        if (remaining == 3) {
            h ^= (bytes[length4 + 2] & 0xff) << 16;
            h ^= (bytes[length4 + 1] & 0xff) << 8;
            h ^= (bytes[length4] & 0xff);
            h *= m;
        } else if (remaining == 2) {
            h ^= (bytes[length4 + 1] & 0xff) << 8;
            h ^= (bytes[length4] & 0xff);
            h *= m;
        } else if (remaining == 1) {
            h ^= (bytes[length4] & 0xff);
            h *= m;
        }
        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h & 0x7fffffff;
    }
}
