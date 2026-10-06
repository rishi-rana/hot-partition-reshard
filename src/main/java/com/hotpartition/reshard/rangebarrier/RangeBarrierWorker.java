package com.hotpartition.reshard.rangebarrier;

import com.hotpartition.reshard.BucketWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * One offset range's worker for the fallback mechanism: in-process, single-threaded FIFO
 * like {@link com.hotpartition.reshard.InProcessBucketWorker}, but before processing each
 * record it consults the shared {@link KeyDependencyIndex} and, if that offset depends on
 * an earlier not-yet-confirmed occurrence of the same key, blocks on the shared
 * {@link DependencyBarrier} until that dependency clears -- the mechanism that lets
 * different ranges run concurrently while keys straddling range boundaries still get
 * processed in produced order. Different ranges never block each other for unrelated
 * keys; a shared key forces exactly the ordering it needs, nothing more.
 */
public final class RangeBarrierWorker implements BucketWorker {

    private static final Logger log = LoggerFactory.getLogger(RangeBarrierWorker.class);

    private record Task(ConsumerRecord<String, byte[]> record,
                         BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                         Runnable onComplete) {
    }

    private final int rangeId;
    private final KeyDependencyIndex dependencyIndex;
    private final DependencyBarrier barrier;
    private final long dependencyTimeoutMs;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private volatile boolean running = false;

    public RangeBarrierWorker(int rangeId, KeyDependencyIndex dependencyIndex, DependencyBarrier barrier,
                               long dependencyTimeoutMs) {
        this.rangeId = rangeId;
        this.dependencyIndex = dependencyIndex;
        this.barrier = barrier;
        this.dependencyTimeoutMs = dependencyTimeoutMs;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "range-barrier-worker-" + rangeId);
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() {
        running = true;
        executor.submit(this::runLoop);
    }

    private void runLoop() {
        while (running || !queue.isEmpty()) {
            try {
                Task task = queue.poll(200, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                long offset = task.record().offset();
                long requiredPrior = dependencyIndex.requiredPriorOffset(offset);
                try {
                    barrier.awaitReady(requiredPrior, dependencyTimeoutMs);
                    task.processor().accept(task.record(), () -> {
                        barrier.markCompleted(offset);
                        task.onComplete().run();
                    });
                } catch (TimeoutException e) {
                    log.warn("range {} timed out waiting on dependency offset {} for its offset {}; "
                            + "processing anyway to avoid a permanent stall", rangeId, requiredPrior, offset);
                    barrier.markCompleted(offset);
                    task.onComplete().run();
                } catch (RuntimeException e) {
                    log.warn("range {} failed to process offset {}: {}", rangeId, offset, e.toString());
                    barrier.markCompleted(offset);
                    task.onComplete().run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void submit(ConsumerRecord<String, byte[]> record,
                        BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                        Runnable onComplete) {
        queue.add(new Task(record, processor, onComplete));
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
