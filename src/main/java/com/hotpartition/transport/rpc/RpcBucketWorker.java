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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;

/**
 * Owner-side half of transport Option A: instead of a Kafka round trip (Option B) or an
 * in-process queue, submit() makes a direct HTTP call to the peer process responsible for
 * this bucket and invokes {@code onComplete} when the response arrives. Lowest latency of
 * the three transports (no broker hop at all for the delegation path itself), at the cost
 * this design doc flags explicitly: a second liveness/transport model alongside Kafka's
 * own, and a networking/auth surface this prototype deliberately keeps minimal (plain
 * HTTP, no TLS/auth) since that hardening is out of scope for validating the mechanism.
 *
 * <p><b>Ordering.</b> Requests for this bucket are dispatched strictly one at a time, in
 * submission order (matching {@link com.hotpartition.reshard.InProcessBucketWorker}'s
 * single-threaded FIFO and the spillover transport's naturally-ordered single-partition
 * consumer). This was <em>not</em> the first version of this class: an earlier draft fired
 * requests concurrently via {@code sendAsync} for lower latency, and a same-key-order test
 * (see {@code RpcEndToEndSmokeTest}) exposed that completions -- and therefore per-key
 * order -- are not guaranteed under concurrent dispatch, since the peer's own HTTP server
 * thread pool can finish requests out of arrival order. Serializing per bucket is the fix;
 * it trades away intra-bucket request pipelining to restore the ordering guarantee every
 * other transport in this design provides. Different buckets remain fully concurrent with
 * each other (each runs its own single-request-at-a-time queue).
 *
 * <p>Like {@link com.hotpartition.transport.spillover.SpilloverBucketWorker}, the
 * {@code processor} argument to {@link #submit} is never invoked here -- the peer at the
 * other end of the HTTP call is assumed to run equivalent processing logic itself (see
 * {@link RpcPeerWorker}), for the same reason: a Java closure cannot cross a process
 * boundary, direct RPC or otherwise.
 */
public final class RpcBucketWorker implements BucketWorker {

    private static final Logger log = LoggerFactory.getLogger(RpcBucketWorker.class);

    private record Task(ConsumerRecord<String, byte[]> record, Runnable onComplete) {
    }

    private final int bucketId;
    private final URI peerUri;
    private final HttpClient httpClient;
    private final BlockingQueue<Task> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor;
    private volatile boolean running = false;

    public RpcBucketWorker(int bucketId, String peerHost, int peerPort) {
        this.bucketId = bucketId;
        this.peerUri = URI.create("http://" + peerHost + ":" + peerPort + "/bucket/" + bucketId);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "rpc-bucket-worker-" + bucketId);
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
                sendSync(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private void sendSync(Task task) {
        ConsumerRecord<String, byte[]> record = task.record();
        byte[] body = RpcEnvelope.encode(record.key(), record.offset(), record.value());
        HttpRequest request = HttpRequest.newBuilder(peerUri)
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                log.warn("bucket {} RPC returned status {} for offset {}",
                        bucketId, response.statusCode(), record.offset());
            }
        } catch (java.io.IOException | InterruptedException e) {
            log.warn("bucket {} RPC failed for offset {}", bucketId, record.offset(), e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            task.onComplete().run();
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
            if (!executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        httpClient.close();
    }
}
