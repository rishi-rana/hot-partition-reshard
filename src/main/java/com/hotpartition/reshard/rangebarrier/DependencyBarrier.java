package com.hotpartition.reshard.rangebarrier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runtime half of the offset-range fallback's correctness rule: a worker about to process
 * offset O, where {@link KeyDependencyIndex#requiredPriorOffset} says O depends on some
 * earlier offset P (same key, not yet confirmed done), must block until P is marked
 * complete -- regardless of which range/worker is responsible for P. This is what lets
 * different offset ranges run concurrently while still preserving per-key order: workers
 * for unrelated keys never wait on each other, but a key straddling two ranges forces
 * exactly the serialization it needs, no more.
 *
 * Implemented with a {@link CompletableFuture} per offset rather than polling: a waiter
 * registers interest (or finds the future already completed, if the dependency finished
 * before it was even asked about) and blocks on it directly, no busy-wait. A long-running
 * hot period would need to evict completed futures below the global low-watermark to
 * bound memory; not done here since a prototype benchmark run is bounded and short-lived,
 * but flagged as a real gap for anything long-running.
 */
public final class DependencyBarrier {

    private final ConcurrentHashMap<Long, CompletableFuture<Void>> futures = new ConcurrentHashMap<>();

    /** Blocks until {@code requiredPriorOffset} has been marked complete. -1 means no wait. */
    public void awaitReady(long requiredPriorOffset, long timeoutMs) throws InterruptedException, TimeoutException {
        if (requiredPriorOffset < 0) {
            return;
        }
        CompletableFuture<Void> future = futures.computeIfAbsent(requiredPriorOffset, k -> new CompletableFuture<>());
        try {
            future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.ExecutionException e) {
            throw new IllegalStateException("dependency future should never complete exceptionally", e);
        }
    }

    public void markCompleted(long offset) {
        futures.computeIfAbsent(offset, k -> new CompletableFuture<>()).complete(null);
    }

    /** Number of distinct offsets ever awaited or completed. Exposed for tests/observability. */
    public int trackedCount() {
        return futures.size();
    }
}
