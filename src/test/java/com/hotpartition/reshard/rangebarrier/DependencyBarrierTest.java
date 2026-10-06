package com.hotpartition.reshard.rangebarrier;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DependencyBarrierTest {

    @Test
    void negativeRequiredPriorOffsetReturnsImmediately() throws Exception {
        DependencyBarrier barrier = new DependencyBarrier();
        barrier.awaitReady(-1, 100); // must not block or throw
    }

    @Test
    void awaitReadyReturnsImmediatelyIfAlreadyCompleted() throws Exception {
        DependencyBarrier barrier = new DependencyBarrier();
        barrier.markCompleted(5L);
        barrier.awaitReady(5L, 100); // dependency already satisfied before being asked about
    }

    @Test
    void awaitReadyBlocksUntilMarkCompletedIsCalledLater() throws Exception {
        DependencyBarrier barrier = new DependencyBarrier();
        AtomicBoolean unblocked = new AtomicBoolean(false);
        CountDownLatch waiterStarted = new CountDownLatch(1);

        Thread waiter = new Thread(() -> {
            try {
                waiterStarted.countDown();
                barrier.awaitReady(5L, 5000);
                unblocked.set(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        waiter.start();
        waiterStarted.await();

        Thread.sleep(100); // give the waiter a chance to actually block
        assertTrue(!unblocked.get(), "should still be blocked before markCompleted");

        barrier.markCompleted(5L);
        waiter.join(2000);

        assertTrue(unblocked.get(), "should unblock once the dependency is marked completed");
    }

    @Test
    void awaitReadyTimesOutIfDependencyNeverCompletes() {
        DependencyBarrier barrier = new DependencyBarrier();
        assertThrows(TimeoutException.class, () -> barrier.awaitReady(5L, 50));
    }

    @Test
    void trackedCountReflectsBothAwaitedAndCompletedOffsets() throws Exception {
        DependencyBarrier barrier = new DependencyBarrier();
        barrier.markCompleted(1L);
        try {
            barrier.awaitReady(2L, 10);
        } catch (TimeoutException ignored) {
        }
        assertTrue(barrier.trackedCount() == 2);
    }
}
