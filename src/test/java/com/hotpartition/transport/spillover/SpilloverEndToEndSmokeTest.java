package com.hotpartition.transport.spillover;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates transport Option B against a genuinely separate OS process for the peer --
 * not just another thread in this JVM -- since that distinction is the entire point of
 * this transport (see the design doc's Mechanism gap). Spawns
 * {@link SpilloverPeerWorker#main} as a child process via ProcessBuilder, feeds it
 * records through {@link SpilloverBucketWorker} on the owner side, and checks every
 * record completes and per-key order is preserved end to end.
 */
class SpilloverEndToEndSmokeTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "BOOTSTRAP_SERVERS", matches = ".+")
    void peerAsSeparateProcessCompletesAllRecordsInOrder() throws Exception {
        String bootstrap = System.getenv("BOOTSTRAP_SERVERS");
        int bucketId = 0;
        int recordCount = 20;
        TopicPartition originalPartition = new TopicPartition("spillover-smoke-source", 0);
        String namePrefix = "spillover-smoke-" + System.nanoTime();
        SpilloverTopics topics = new SpilloverTopics(namePrefix);

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            topics.ensureExists(admin, 1, (short) 1);
        }

        Process peerProcess = new ProcessBuilder(
                "java", "-cp", System.getProperty("java.class.path"),
                "com.hotpartition.transport.spillover.SpilloverPeerWorker",
                bootstrap, namePrefix, String.valueOf(bucketId), "10000")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .start();

        try {
            SpilloverBucketWorker ownerWorker = new SpilloverBucketWorker(
                    bucketId, originalPartition, topics, newByteProducer(bootstrap), newStringConsumer(bootstrap));
            ownerWorker.start();

            List<Long> completedInOrder = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(recordCount);

            try {
                for (int i = 0; i < recordCount; i++) {
                    ConsumerRecord<String, byte[]> fakeRecord = new ConsumerRecord<>(
                            originalPartition.topic(), originalPartition.partition(), i,
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
                            "same-key records must complete in submission order even across the process boundary");
                }
            } finally {
                ownerWorker.close();
            }
        } finally {
            peerProcess.destroy();
            peerProcess.waitFor(10, TimeUnit.SECONDS);
            peerProcess.destroyForcibly();
        }
    }

    private static KafkaProducer<String, byte[]> newByteProducer(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> newStringConsumer(String bootstrap) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "spillover-owner-smoke-" + System.nanoTime());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }
}
