package com.hotpartition.transport.rpc;

import com.hotpartition.admission.PeerCapacityAdmission;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Peer-side half of transport Option A: an HTTP server exposing one endpoint per bucket
 * ({@code POST /bucket/{bucketId}}), running the same simulated-processing stub as the
 * other transports (for apples-to-apples comparison) and replying 200 once done -- the
 * response itself is the completion ack, no separate acknowledgement channel needed,
 * unlike transport Option B's completions topic.
 *
 * <p><b>Admission control.</b> This is the one transport with a real synchronous channel,
 * so it is the one transport that gets a genuine accept/reject handshake for the
 * cross-owner oversubscription gap described in {@link PeerCapacityAdmission}: each
 * instance owns exactly one {@link PeerCapacityAdmission}, sized to its own
 * {@code muWorkerCapacity}, and exposes it over two extra endpoints:
 * <ul>
 *   <li>{@code POST /claim/{bucketId}?lambda=<estimatedLambda>&claimId=<id>} -- 200 if the
 *       claim fits under this peer's remaining capacity (and is now counted against it),
 *       409 if not.</li>
 *   <li>{@code POST /release/{bucketId}?claimId=<id>} -- always 200; frees the claim's
 *       contribution back out of the running total.</li>
 * </ul>
 * Owners are expected to call {@code /claim} before dispatching any work to
 * {@code /bucket/{bucketId}} and {@code /release} once that bucket's delegation ends (see
 * {@code ClaimingRpcBucketWorkerFactory} on the owner side). The {@code /bucket} endpoint
 * itself does not re-check admission -- by the time a record arrives there the owner
 * already holds an accepted claim; it is the claim handshake, not every record, that is
 * admission-controlled, for the same reason Kafka's own quotas are enforced at the
 * connection/client level rather than per record.
 *
 * Runnable embedded (tests/benchmarks) via {@link #start()}/{@link #stop()}, or via
 * {@link #main} as a genuinely separate OS process.
 */
public final class RpcPeerWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RpcPeerWorker.class);

    private final int bucketId;
    private final HttpServer server;
    private final long simulatedProcessingNanosPerRecord;
    private final PeerCapacityAdmission admission;
    private final java.util.concurrent.ExecutorService executorService;

    /**
     * Back-compat constructor: no admission control (equivalent to an effectively
     * unlimited {@code muWorkerCapacity}) -- preserves the pre-admission-control behavior
     * for existing callers/tests that don't exercise this gap.
     */
    public RpcPeerWorker(int bucketId, int port, long simulatedProcessingNanosPerRecord) throws IOException {
        this(bucketId, port, simulatedProcessingNanosPerRecord, Double.MAX_VALUE, 4);
    }

    /**
     * @param muWorkerCapacity this peer's own measured/configured sustainable processing
     *                         rate, in the same units callers will pass as {@code lambda}
     *                         on {@code /claim} requests.
     * @param workerThreads    size of the HTTP server's executor thread pool -- the
     *                         prototype's stand-in for how many records this peer can
     *                         actually process concurrently; the admission-control
     *                         experiment pins this to 1 so {@code muWorkerCapacity} maps
     *                         onto a real, observable physical bottleneck rather than just
     *                         a business-side bookkeeping number.
     */
    public RpcPeerWorker(int bucketId, int port, long simulatedProcessingNanosPerRecord,
                          double muWorkerCapacity, int workerThreads) throws IOException {
        this.bucketId = bucketId;
        this.simulatedProcessingNanosPerRecord = simulatedProcessingNanosPerRecord;
        this.admission = new PeerCapacityAdmission(muWorkerCapacity);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/bucket/" + bucketId, exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] body = exchange.getRequestBody().readAllBytes();
                RpcEnvelope.Decoded decoded = RpcEnvelope.decode(body);
                busyWork();
                exchange.sendResponseHeaders(200, -1);
            } catch (RuntimeException e) {
                log.warn("bucket {} failed to handle request", bucketId, e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/claim/" + bucketId, exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
                String claimId = query.get("claimId");
                double lambda = Double.parseDouble(query.get("lambda"));
                PeerCapacityAdmission.AdmissionResult result = admission.tryAdmit(claimId, lambda);
                if (result == PeerCapacityAdmission.AdmissionResult.ACCEPT) {
                    log.info("bucket {} claim {} ACCEPTED lambda={} totalAdmitted={}/{}",
                            bucketId, claimId, lambda, admission.currentlyAdmittedLoad(), admission.muWorkerCapacity());
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    log.info("bucket {} claim {} REJECTED lambda={} totalAdmitted={}/{}",
                            bucketId, claimId, lambda, admission.currentlyAdmittedLoad(), admission.muWorkerCapacity());
                    exchange.sendResponseHeaders(409, -1);
                }
            } catch (RuntimeException e) {
                log.warn("bucket {} failed to handle claim request", bucketId, e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        server.createContext("/release/" + bucketId, exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
                String claimId = query.get("claimId");
                admission.release(claimId);
                log.info("bucket {} claim {} RELEASED totalAdmitted={}/{}",
                        bucketId, claimId, admission.currentlyAdmittedLoad(), admission.muWorkerCapacity());
                exchange.sendResponseHeaders(200, -1);
            } catch (RuntimeException e) {
                log.warn("bucket {} failed to handle release request", bucketId, e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        this.executorService = Executors.newFixedThreadPool(workerThreads);
        server.setExecutor(executorService);
    }

    public void start() {
        server.start();
        log.info("RPC peer worker for bucket {} listening on port {}", bucketId, server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
        // HttpServer.stop() does NOT shut down an executor supplied via setExecutor() --
        // that's documented JDK behavior, not an oversight here. Without this, the fixed
        // thread pool's non-daemon worker threads stay alive forever, and the JVM (and
        // hence `./gradlew run`) never exits even after every experiment has logically
        // finished and printed its results.
        executorService.shutdownNow();
    }

    /** Exposed for tests/experiments that want to assert on admitted load directly. */
    public PeerCapacityAdmission admission() {
        return admission;
    }

    private void busyWork() {
        long end = System.nanoTime() + simulatedProcessingNanosPerRecord;
        while (System.nanoTime() < end) {
            // simulate per-record processing cost, matching the other transports' stubs
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> result = new java.util.HashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return result;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                continue;
            }
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.put(key, value);
        }
        return result;
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * Launches as a standalone process: args = port, bucketId, processingNanosPerRecord,
     * optionally muWorkerCapacity and workerThreads (both default to "unlimited"/4 if
     * omitted, matching the pre-admission-control behavior).
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 3 && args.length != 5) {
            System.err.println("usage: RpcPeerWorker <port> <bucketId> <processingNanosPerRecord> "
                    + "[<muWorkerCapacity> <workerThreads>]");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        int bucketId = Integer.parseInt(args[1]);
        long processingNanos = Long.parseLong(args[2]);
        double muWorkerCapacity = args.length == 5 ? Double.parseDouble(args[3]) : Double.MAX_VALUE;
        int workerThreads = args.length == 5 ? Integer.parseInt(args[4]) : 4;

        RpcPeerWorker worker = new RpcPeerWorker(bucketId, port, processingNanos, muWorkerCapacity, workerThreads);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
        worker.start();
        // block forever; the shutdown hook handles a clean stop on SIGTERM.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
