package com.hotpartition.reshard;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyBucketRouterTest {

    @Test
    void sameKeyAlwaysRoutesToSameBucket() {
        KeyBucketRouter router = new KeyBucketRouter(8, 0);
        int first = router.bucketFor("user-42");
        for (int i = 0; i < 100; i++) {
            assertEquals(first, router.bucketFor("user-42"),
                    "per-key order relies on every record for a key landing in the same bucket");
        }
    }

    @Test
    void distributesKeysAcrossAllBuckets() {
        KeyBucketRouter router = new KeyBucketRouter(4, 0);
        boolean[] seen = new boolean[4];
        for (int i = 0; i < 1000; i++) {
            seen[router.bucketFor("key-" + i)] = true;
        }
        for (boolean b : seen) {
            assertTrue(b, "expected every bucket to receive at least one key out of 1000");
        }
    }

    @Test
    void bucketIndexAlwaysInRange() {
        KeyBucketRouter router = new KeyBucketRouter(3, 0);
        for (int i = 0; i < 500; i++) {
            int b = router.bucketFor("k" + i);
            assertTrue(b >= 0 && b < 3);
        }
    }
}
