package com.hotpartition.transport.rpc;

import com.hotpartition.reshard.BucketWorker;
import com.hotpartition.reshard.InProcessBucketWorker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;

/**
 * Owner-side half of the admission-control handshake for transport Option A. Plugs in at
 * exactly the point {@link com.hotpartition.reshard.OwningConsumer#workerFor(int)} already
 * calls out to its injected {@code workerFactory} to obtain a (not-yet-started)
 * {@link BucketWorker} for a bucket -- no change to {@code OwningConsumer} itself was
 * needed; that extension point already exists and is generic enough to host this.
 *
 * <p>Sequence per bucket, the first time it's needed:
 * <ol>
 *   <li>synchronously {@code POST /claim/{bucketId}?lambda=...&claimId=...} to the chosen
 *       peer and block for the response (this is "wait for ACCEPT or REJECT before
 *       dispatching the bucket's first record," not fire-and-forget);</li>
 *   <li>on 200 (ACCEPT): return an {@link RpcBucketWorker} wrapped so that {@code close()}
 *       also calls {@code POST /release/{bucketId}?claimId=...}, freeing the peer's
 *       capacity when this bucket is done (bucket completion in this prototype is "owner
 *       closes," since there's no separate reassignment protocol -- see the report for
 *       what that leaves out of scope);</li>
 *   <li>on 409 (REJECT), or on any I/O failure reaching the peer at all (a dead/unreachable
 *       peer is treated the same as "can't get capacity there," not as "assume success"):
 *       fall back to {@link InProcessBucketWorker} -- partial resharding, processed by the
 *       owner itself instead of failing the bucket outright -- and increment the fallback
 *       counter so callers (tests, the experiment) can assert this path actually ran.</li>
 * </ol>
 */
public final class ClaimingRpcBucketWorkerFactory implements IntFunction<BucketWorker> {

    private static final Logger log = LoggerFactory.getLogger(ClaimingRpcBucketWorkerFactory.class);

    private final String ownerId;
    private final String peerHost;
    private final int peerPort;
    private final DoubleSupplier estimatedLambdaForBucket;
    private final HttpClient httpClient;
    private final AtomicLong fallbackCount = new AtomicLong();
    private final BiConsumer<ConsumerRecord<String, byte[]>, Runnable> inProcessProcessor;

    /**
     * @param ownerId                   identifies this owner in claim ids, so two
     *                                   concurrently-hot owners racing for the same peer
     *                                   produce distinct claim ids even for the same
     *                                   bucket number.
     * @param estimatedLambdaForBucket  this owner's own estimate of the arrival rate it
     *                                   intends to delegate for a bucket (e.g. its own
     *                                   measured per-bucket λ_t) -- the number the peer's
     *                                   admission check is evaluated against.
     * @param inProcessProcessor        real processing logic to run for a bucket that
     *                                   falls back to in-process (the RPC path itself
     *                                   never invokes this -- the peer does equivalent
     *                                   work -- so this must be a genuine, non-throwing
     *                                   processor, not the "must not run on owner side"
     *                                   stub the benchmark harness uses for the
     *                                   non-admission-controlled RPC path).
     */
    public ClaimingRpcBucketWorkerFactory(String ownerId, String peerHost, int peerPort,
                                           DoubleSupplier estimatedLambdaForBucket,
                                           BiConsumer<ConsumerRecord<String, byte[]>, Runnable> inProcessProcessor) {
        this.ownerId = ownerId;
        this.peerHost = peerHost;
        this.peerPort = peerPort;
        this.estimatedLambdaForBucket = estimatedLambdaForBucket;
        this.inProcessProcessor = inProcessProcessor;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public long fallbackCount() {
        return fallbackCount.get();
    }

    @Override
    public BucketWorker apply(int bucketId) {
        String claimId = ownerId + "-bucket-" + bucketId + "-" + System.nanoTime();
        double lambda = estimatedLambdaForBucket.getAsDouble();

        boolean accepted = tryClaim(bucketId, claimId, lambda);
        if (accepted) {
            log.info("owner {} bucket {} claim {} ACCEPTED (lambda={}) -- delegating to RPC peer {}:{}",
                    ownerId, bucketId, claimId, lambda, peerHost, peerPort);
            RpcBucketWorker rpcWorker = new RpcBucketWorker(bucketId, peerHost, peerPort);
            return new ReleasingRpcBucketWorker(bucketId, claimId, rpcWorker);
        }

        log.warn("owner {} bucket {} claim {} REJECTED or peer unreachable (lambda={}) -- "
                + "falling back to in-process (partial resharding) instead of failing the bucket",
                ownerId, bucketId, claimId, lambda);
        fallbackCount.incrementAndGet();
        return new FallbackInProcessBucketWorker(bucketId, inProcessProcessor);
    }

    private boolean tryClaim(int bucketId, String claimId, double lambda) {
        URI claimUri = URI.create("http://" + peerHost + ":" + peerPort + "/claim/" + bucketId
                + "?lambda=" + lambda + "&claimId=" + claimId);
        HttpRequest request = HttpRequest.newBuilder(claimUri)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            return response.statusCode() == 200;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("owner {} bucket {} claim {} failed to reach peer {}:{}",
                    ownerId, bucketId, claimId, peerHost, peerPort, e);
            return false;
        }
    }

    /**
     * Wraps an {@link RpcBucketWorker} so that closing it also releases the claim on the
     * peer -- the owner-side half of freeing capacity on bucket completion/close.
     */
    private final class ReleasingRpcBucketWorker implements BucketWorker {
        private final int bucketId;
        private final String claimId;
        private final RpcBucketWorker delegate;

        ReleasingRpcBucketWorker(int bucketId, String claimId, RpcBucketWorker delegate) {
            this.bucketId = bucketId;
            this.claimId = claimId;
            this.delegate = delegate;
        }

        @Override
        public void submit(ConsumerRecord<String, byte[]> record,
                            BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor,
                            Runnable onComplete) {
            delegate.submit(record, processor, onComplete);
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void close() {
            try {
                delegate.close();
            } finally {
                release(bucketId, claimId);
            }
        }
    }

    private void release(int bucketId, String claimId) {
        URI releaseUri = URI.create("http://" + peerHost + ":" + peerPort + "/release/" + bucketId
                + "?claimId=" + claimId);
        HttpRequest request = HttpRequest.newBuilder(releaseUri)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            log.info("owner {} bucket {} claim {} released", ownerId, bucketId, claimId);
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("owner {} bucket {} claim {} failed to release (peer {}:{} unreachable) -- "
                    + "capacity there will stay pinned until the peer's own TTL/heartbeat logic "
                    + "catches up, a known limitation of a best-effort release call",
                    ownerId, bucketId, claimId, peerHost, peerPort, e);
        }
    }

    /**
     * The fallback-on-rejection worker: a plain {@link InProcessBucketWorker} underneath,
     * just using the real (non-throwing) processor so rejected buckets are actually
     * processed rather than silently dropped.
     */
    private final class FallbackInProcessBucketWorker implements BucketWorker {
        private final InProcessBucketWorker delegate;
        private final BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor;

        FallbackInProcessBucketWorker(int bucketId, BiConsumer<ConsumerRecord<String, byte[]>, Runnable> processor) {
            this.delegate = new InProcessBucketWorker(bucketId);
            this.processor = processor;
        }

        @Override
        public void submit(ConsumerRecord<String, byte[]> record,
                            BiConsumer<ConsumerRecord<String, byte[]>, Runnable> ignoredProcessor,
                            Runnable onComplete) {
            // the owner's injected processor for the RPC path is typically a "must not run
            // here" stub (processing happens on the peer); the fallback path genuinely
            // needs to do the work itself, hence substituting the real processor given to
            // this factory instead of propagating whatever was passed through.
            delegate.submit(record, processor, onComplete);
        }

        @Override
        public void start() {
            delegate.start();
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
