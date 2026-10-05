package com.hotpartition.transport.rpc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates transport Option A against a genuinely separate OS process for the peer, same
 * standard as {@code SpilloverEndToEndSmokeTest} for Option B: spawns
 * {@link RpcPeerWorker#main} as a child process via ProcessBuilder, drives it through
 * {@link RpcBucketWorker} on the owner side, and checks every record completes and
 * per-key order is preserved end to end. No Kafka broker involved at all for this
 * transport's own delegation path -- unlike the spillover transport, this test needs
 * none of the usual @EnabledIfEnvironmentVariable(BOOTSTRAP_SERVERS) gating.
 */
class RpcEndToEndSmokeTest {

    @Test
    void peerAsSeparateProcessCompletesAllRecordsInOrder() throws Exception {
        int bucketId = 0;
        int recordCount = 20;
        int port = 9999;

        Process peerProcess = new ProcessBuilder(
                "java", "-cp", System.getProperty("java.class.path"),
                "com.hotpartition.transport.rpc.RpcPeerWorker",
                String.valueOf(port), String.valueOf(bucketId), "10000")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .start();

        try {
            // give the peer's HTTP server a moment to bind before the first request.
            Thread.sleep(1500);

            try (RpcBucketWorker ownerWorker = new RpcBucketWorker(bucketId, "localhost", port)) {
                ownerWorker.start();

                List<Long> completedInOrder = new CopyOnWriteArrayList<>();
                CountDownLatch latch = new CountDownLatch(recordCount);

                for (int i = 0; i < recordCount; i++) {
                    ConsumerRecord<String, byte[]> fakeRecord = new ConsumerRecord<>(
                            "rpc-smoke-source", 0, i,
                            "same-key", ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    ownerWorker.submit(fakeRecord, (rec, done) -> {
                        throw new AssertionError("processor must not run on the owner side for this transport");
                    }, () -> {
                        completedInOrder.add(fakeRecord.offset());
                        latch.countDown();
                    });
                }

                boolean allCompleted = latch.await(60, TimeUnit.SECONDS);
                assertTrue(allCompleted, "expected all " + recordCount + " records to complete, got "
                        + (recordCount - latch.getCount()));
                assertEquals(recordCount, completedInOrder.size());
                for (int i = 0; i < recordCount; i++) {
                    assertEquals((long) i, completedInOrder.get(i),
                            "same-key records must complete in submission order even across the process boundary "
                                    + "(RpcBucketWorker serializes per-bucket precisely to guarantee this)");
                }
            }
        } finally {
            peerProcess.destroy();
            peerProcess.waitFor(10, TimeUnit.SECONDS);
            peerProcess.destroyForcibly();
        }
    }
}
