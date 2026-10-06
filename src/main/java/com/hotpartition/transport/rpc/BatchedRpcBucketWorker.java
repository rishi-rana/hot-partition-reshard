package com.hotpartition.transport.rpc;

import com.hotpartition.reshard.BucketWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Batched follow-up to {@link RpcBucketWorker}, built to test whether the plain-RPC
 * transport's measured throughput deficit (50-68% of in-process, vs. the spillover
 * transport's 84-90%, per the design doc's Mechanism section) is explained by its lack of
 * a batching primitive -- Kafka's client batches hundreds of records per network round
 * trip on both the delegation and ack path, which is what let the spillover transport
 * amortize its own per-call overhead. This class gives direct RPC the same amortization:
 * instead of one HTTP request per record, it accumulates up to {@code maxBatchSize}
 * records (or whatever arrived within {@code lingerMs} of the first one, Kafka's own
 * batch.size/linger.ms naming deliberately reused here) into a single request, sends one
 * HTTP call for the whole batch, and resolves every record's {@code onComplete} in
 * submission order once the single response arrives -- still serialized per bucket
 * (one in-flight batch at a time), so per-key order is preserved the same way
 * {@link RpcBucketWorker} guarantees it.
 */
public final class BatchedRpcBucketWorker implements BucketWorker {

    private static final Logger log = LoggerFactory.getLogger(BatchedRpcBucketWorker.class);

    private record Task(ConsumerRecord<String, byte[]> record, Runnable onComplete) {
    }

    private final int bucketId;
    private final URI peerUri;
    private final HttpClient httpClient;
    private final int maxBatchSize;
    private final long lingerMs;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private volatile boolean running = false;

    public BatchedRpcBucketWorker(int bucketId, String peerHost, int peerPort, int maxBatchSize, long lingerMs) {
        this.bucketId = bucketId;
        this.peerUri = URI.create("http://" + peerHost + ":" + peerPort + "/bucket/" + bucketId + "/batch");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.maxBatchSize = maxBatchSize;
        this.lingerMs = lingerMs;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "batched-rpc-bucket-worker-" + bucketId);
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
                Task first = queue.poll(200, TimeUnit.MILLISECONDS);
                if (first == null) {
                    continue;
                }
                List<Task> batch = new ArrayList<>(maxBatchSize);
                batch.add(first);
                while (batch.size() < maxBatchSize) {
                    Task more = queue.poll(lingerMs, TimeUnit.MILLISECONDS);
                    if (more == null) {
                        break;
                    }
                    batch.add(more);
                }
                sendBatchSync(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void sendBatchSync(List<Task> batch) {
        List<RpcBatchEnvelope.Item> items = new ArrayList<>(batch.size());
        for (Task t : batch) {
            ConsumerRecord<String, byte[]> r = t.record();
            items.add(new RpcBatchEnvelope.Item(r.key(), r.offset(), r.value()));
        }
        byte[] body = RpcBatchEnvelope.encode(items);
        HttpRequest request = HttpRequest.newBuilder(peerUri)
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                log.warn("bucket {} batched RPC returned status {} for batch of {}",
                        bucketId, response.statusCode(), batch.size());
            }
        } catch (java.io.IOException | InterruptedException e) {
            log.warn("bucket {} batched RPC failed for batch of {}", bucketId, batch.size(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            for (Task t : batch) {
                t.onComplete().run();
            }
        }
    }

    @Override
    public void submit(ConsumerRecord<String, byte[]> record,
                        BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                        Runnable onComplete) {
        queue.add(new Task(record, onComplete));
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
        httpClient.close();
    }
}
