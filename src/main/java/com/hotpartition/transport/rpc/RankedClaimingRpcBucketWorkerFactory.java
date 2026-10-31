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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.DoubleSupplier;
import java.util.function.IntFunction;

/**
 * Owner-side half of the admission-control handshake, "with retry" variant: instead of
 * {@link ClaimingRpcBucketWorkerFactory}'s single hardcoded peer, this takes an ordered
 * list of candidate peers for a bucket and walks it in order, trying {@code /claim}
 * against each until one ACCEPTs, falling back to in-process only if every candidate in
 * the list REJECTs or is unreachable.
 *
 * <p>This exists to close the herding gap {@link ClaimingRpcBucketWorkerFactory} cannot:
 * if every owner's "most idle peer" pick independently lands on the same answer (because
 * they're all reading the same stale/shared view), a single-candidate factory gives every
 * owner exactly one shot at that one peer and sends everyone else straight to fallback,
 * even when other, equally-idle peers sit completely unused. Trying a ranked list lets an
 * owner whose first choice is already full retry against its second, third, etc. choice
 * before giving up -- spreading admitted load across the peer pool instead of piling it
 * onto whichever peer happened to rank first for everyone.
 *
 * <p>Deliberately a separate class rather than a modification of
 * {@link ClaimingRpcBucketWorkerFactory}: that class is already shipped and measured (as
 * the single-candidate/no-retry baseline) in the paper; this is the new "with retry"
 * condition compared against it, not a replacement.
 *
 * <p>Same release-on-close pattern as {@link ClaimingRpcBucketWorkerFactory}: whichever
 * peer ultimately accepted the claim gets a {@code /release} call when the returned
 * {@link BucketWorker} is closed. Per-peer HTTP claim/release mechanics are factored out
 * into {@link #tryClaim} and {@link #release} here rather than duplicated from the
 * sibling class, since the two classes' claim/release wire format is identical but their
 * selection logic (one peer vs. an ordered list) is not -- sharing a single POJO between
 * two independently-measured, intentionally-unmodified classes seemed like more risk than
 * the ~40 lines saved.
 */
public final class RankedClaimingRpcBucketWorkerFactory implements IntFunction<BucketWorker> {

    private static final Logger log = LoggerFactory.getLogger(RankedClaimingRpcBucketWorkerFactory.class);

    /** One candidate peer in ranked order. */
    public record PeerCandidate(String host, int port) {
    }

    private final String ownerId;
    private final List<PeerCandidate> candidatesInRankOrder;
    private final DoubleSupplier estimatedLambdaForBucket;
    private final HttpClient httpClient;
    private final AtomicLong fallbackCount = new AtomicLong();
    private final BiConsumer<ConsumerRecord<String, byte[]>, Runnable> inProcessProcessor;

    /** bucketId -> the peer candidate whose claim was ultimately accepted for it, if any. */
    private final Map<Integer, PeerCandidate> acceptedPeerForBucket = new ConcurrentHashMap<>();

    /**
     * @param ownerId                   identifies this owner in claim ids, same role as in
     *                                   {@link ClaimingRpcBucketWorkerFactory}.
     * @param candidatesInRankOrder     the ordered list of peers to try for every bucket
     *                                   this factory is asked for -- must be non-empty.
     * @param estimatedLambdaForBucket  this owner's own estimate of the arrival rate it
     *                                   intends to delegate for a bucket.
     * @param inProcessProcessor        real processing logic to run for a bucket that
     *                                   falls back to in-process after exhausting every
     *                                   candidate.
     */
    public RankedClaimingRpcBucketWorkerFactory(String ownerId, List<PeerCandidate> candidatesInRankOrder,
                                                 DoubleSupplier estimatedLambdaForBucket,
                                                 BiConsumer<ConsumerRecord<String, byte[]>, Runnable> inProcessProcessor) {
        if (candidatesInRankOrder.isEmpty()) {
            throw new IllegalArgumentException("candidatesInRankOrder must not be empty");
        }
        this.ownerId = ownerId;
        this.candidatesInRankOrder = List.copyOf(candidatesInRankOrder);
        this.estimatedLambdaForBucket = estimatedLambdaForBucket;
        this.inProcessProcessor = inProcessProcessor;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public long fallbackCount() {
        return fallbackCount.get();
    }

    /** The peer whose claim was ultimately accepted for {@code bucketId}, or empty if it fell back. */
    public java.util.Optional<PeerCandidate> acceptedPeerFor(int bucketId) {
        return java.util.Optional.ofNullable(acceptedPeerForBucket.get(bucketId));
    }

    @Override
    public BucketWorker apply(int bucketId) {
        double lambda = estimatedLambdaForBucket.getAsDouble();

        for (PeerCandidate candidate : candidatesInRankOrder) {
            String claimId = ownerId + "-bucket-" + bucketId + "-" + System.nanoTime();
            boolean accepted = tryClaim(candidate, bucketId, claimId, lambda);
            if (accepted) {
                log.info("owner {} bucket {} claim {} ACCEPTED (lambda={}) -- delegating to RPC peer {}:{}",
                        ownerId, bucketId, claimId, lambda, candidate.host(), candidate.port());
                acceptedPeerForBucket.put(bucketId, candidate);
                RpcBucketWorker rpcWorker = new RpcBucketWorker(bucketId, candidate.host(), candidate.port());
                return new ReleasingRpcBucketWorker(bucketId, claimId, candidate, rpcWorker);
            }
            log.info("owner {} bucket {} claim {} REJECTED or peer unreachable (lambda={}) at candidate {}:{} -- "
                    + "trying next candidate, if any",
                    ownerId, bucketId, claimId, lambda, candidate.host(), candidate.port());
        }

        log.warn("owner {} bucket {} exhausted all {} candidates (lambda={}) -- "
                + "falling back to in-process (partial resharding) instead of failing the bucket",
                ownerId, bucketId, candidatesInRankOrder.size(), lambda);
        fallbackCount.incrementAndGet();
        return new FallbackInProcessBucketWorker(bucketId, inProcessProcessor);
    }

    private boolean tryClaim(PeerCandidate candidate, int bucketId, String claimId, double lambda) {
        URI claimUri = URI.create("http://" + candidate.host() + ":" + candidate.port() + "/claim/" + bucketId
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
                    ownerId, bucketId, claimId, candidate.host(), candidate.port(), e);
            return false;
        }
    }

    /**
     * Wraps an {@link RpcBucketWorker} so that closing it also releases the claim on
     * whichever peer ultimately accepted it.
     */
    private final class ReleasingRpcBucketWorker implements BucketWorker {
        private final int bucketId;
        private final String claimId;
        private final PeerCandidate acceptedPeer;
        private final RpcBucketWorker delegate;

        ReleasingRpcBucketWorker(int bucketId, String claimId, PeerCandidate acceptedPeer, RpcBucketWorker delegate) {
            this.bucketId = bucketId;
            this.claimId = claimId;
            this.acceptedPeer = acceptedPeer;
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
                release(bucketId, claimId, acceptedPeer);
            }
        }
    }

    private void release(int bucketId, String claimId, PeerCandidate peer) {
        URI releaseUri = URI.create("http://" + peer.host() + ":" + peer.port() + "/release/" + bucketId
                + "?claimId=" + claimId);
        HttpRequest request = HttpRequest.newBuilder(releaseUri)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        try {
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            log.info("owner {} bucket {} claim {} released on {}:{}", ownerId, bucketId, claimId,
                    peer.host(), peer.port());
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("owner {} bucket {} claim {} failed to release (peer {}:{} unreachable) -- "
                    + "capacity there will stay pinned until the peer's own TTL/heartbeat logic "
                    + "catches up, a known limitation of a best-effort release call",
                    ownerId, bucketId, claimId, peer.host(), peer.port(), e);
        }
    }

    /**
     * The fallback-after-exhausting-every-candidate worker: a plain
     * {@link InProcessBucketWorker} underneath, using the real (non-throwing) processor so
     * fully-rejected buckets are actually processed rather than silently dropped.
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
