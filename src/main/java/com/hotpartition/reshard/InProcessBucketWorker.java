package com.hotpartition.reshard;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

/**
 * Single-threaded, FIFO, in-process implementation of {@link BucketWorker}.
 * Records for this bucket are processed strictly in the order they were
 * submitted (the order the owner dispatched them in, which is partition
 * offset order), so completion within a bucket is naturally contiguous.
 */
public final class InProcessBucketWorker implements BucketWorker {

    private static final Logger log = LoggerFactory.getLogger(InProcessBucketWorker.class);

    private record Task(ConsumerRecord<String, byte[]> record,
                         BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                         Runnable onComplete) {
    }

    private final int bucketId;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private volatile boolean running = false;

    public InProcessBucketWorker(int bucketId) {
        this.bucketId = bucketId;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "bucket-worker-" + bucketId);
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
                Task task = queue.poll(200, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                try {
                    task.processor().accept(task.record(), task.onComplete());
                } catch (RuntimeException e) {
                    log.warn("bucket {} failed to process offset {}: {}", bucketId,
                            task.record().offset(), e.toString());
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

    public int queueDepth() {
        return queue.size();
    }

    @Override
    public void close() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
