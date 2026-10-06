package com.hotpartition.transport.rpc;

import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

/**
 * Peer-side half of the batched RPC follow-up: one endpoint per bucket
 * ({@code POST /bucket/{bucketId}/batch}), decodes a {@link RpcBatchEnvelope}, runs the
 * same simulated-processing stub as every other transport's peer once per record in the
 * batch, and replies 200 once the whole batch is done -- a single response acks the
 * entire batch, same "response is the ack" model as {@link RpcPeerWorker}, just batched.
 */
public final class BatchedRpcPeerWorker implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(BatchedRpcPeerWorker.class);

    private final int bucketId;
    private final HttpServer server;
    private final long simulatedProcessingNanosPerRecord;

    public BatchedRpcPeerWorker(int bucketId, int port, long simulatedProcessingNanosPerRecord) throws IOException {
        this.bucketId = bucketId;
        this.simulatedProcessingNanosPerRecord = simulatedProcessingNanosPerRecord;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/bucket/" + bucketId + "/batch", exchange -> {
            try {
                if (!"POST".equals(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
                byte[] body = exchange.getRequestBody().readAllBytes();
                var items = RpcBatchEnvelope.decode(body);
                for (var item : items) {
                    busyWork();
                }
                exchange.sendResponseHeaders(200, -1);
            } catch (RuntimeException e) {
                log.warn("bucket {} failed to handle batch request", bucketId, e);
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        });
        server.setExecutor(Executors.newFixedThreadPool(4));
    }

    public void start() {
        server.start();
        log.info("batched RPC peer worker for bucket {} listening on port {}", bucketId, server.getAddress().getPort());
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
            System.err.println("usage: BatchedRpcPeerWorker <port> <bucketId> <processingNanosPerRecord>");
            System.exit(1);
        }
        int port = Integer.parseInt(args[0]);
        int bucketId = Integer.parseInt(args[1]);
        long processingNanos = Long.parseLong(args[2]);

        BatchedRpcPeerWorker worker = new BatchedRpcPeerWorker(bucketId, port, processingNanos);
        Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
        worker.start();
        try {
            Thread.currentThread().join();
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
