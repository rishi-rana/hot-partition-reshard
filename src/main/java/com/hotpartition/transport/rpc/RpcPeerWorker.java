package com.hotpartition.transport.rpc;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Peer-side half of transport Option A: an HTTP server exposing one endpoint per bucket
 * ({@code POST /bucket/{bucketId}}), running the same simulated-processing stub as the
 * other transports (for apples-to-apples comparison) and replying 200 once done -- the
 * response itself is the completion ack, no separate acknowledgement channel needed,
 * unlike transport Option B's completions topic.
 *
 * Runnable embedded (tests/benchmarks) via {@link #start()}/{@link #stop()}, or via
 * {@link #main} as a genuinely separate OS process.
 */
public final class RpcPeerWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RpcPeerWorker.class);

    private final int bucketId;
    private final HttpServer server;
    private final long simulatedProcessingNanosPerRecord;

    public RpcPeerWorker(int bucketId, int port, long simulatedProcessingNanosPerRecord) throws IOException {
        this.bucketId = bucketId;
        this.simulatedProcessingNanosPerRecord = simulatedProcessingNanosPerRecord;
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
        server.setExecutor(Executors.newFixedThreadPool(4));
    }

    public void start() {
        server.start();
        log.info("RPC peer worker for bucket {} listening on port {}", bucketId, server.getAddress().getPort());
    }

    public void stop() {
        server.stop(0);
    }

    private void busyWork() {
        long end = System.nanoTime() + simulatedProcessingNanosPerRecord;
        while (System.nanoTime() < end) {
            // simulate per-record processing cost, matching the other transports' stubs
        }
    }

    @Override
    public void close() {
        stop();
    }

    /** Launches as a standalone process: args = port, bucketId, processingNanosPerRecord. */
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("usage: RpcPeerWorker <port> <bucketId> <processingNanosPerRecord>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        int bucketId = Integer.parseInt(args[1]);
        long processingNanos = Long.parseLong(args[2]);

        RpcPeerWorker worker = new RpcPeerWorker(bucketId, port, processingNanos);
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
