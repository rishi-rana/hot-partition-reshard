package com.hotpartition.transport.claim;

import com.hotpartition.transport.spillover.SpilloverBucketWorker;
import com.hotpartition.transport.spillover.SpilloverTopics;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the property that actually distinguishes transport Option C from B: Kafka's
 * own consumer-group rebalance deciding bucket ownership, including automatic
 * reassignment after a peer process dies -- not just the happy-path data path, which is
 * already identical to (and proven by) the spillover transport's own tests since this
 * reuses {@link SpilloverBucketWorker} unchanged on the owner side.
 *
 * Two buckets, started with ONE peer process owning both (proving a single claim-peer
 * can hold multiple partitions), then that peer is killed and a second is started: the
 * survivor must pick up the orphaned bucket via rebalance and complete records
 * submitted to it after the kill, with no owner-side changes at all.
 */
class ClaimEndToEndSmokeTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "BOOTSTRAP_SERVERS", matches = ".+")
    void survivingPeerClaimsOrphanedBucketAfterPeerDeath() throws Exception {
        String bootstrap = System.getenv("BOOTSTRAP_SERVERS");
        int numBuckets = 2;
        String namePrefix = "claim-smoke-" + System.nanoTime();
        SpilloverTopics topics = new SpilloverTopics(namePrefix);
        TopicPartition originalPartition = new TopicPartition("claim-smoke-source", 0);

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            topics.ensureExists(admin, numBuckets, (short) 1);
        }

        Process peer1 = startClaimPeer(bootstrap, namePrefix);
        try {
            // give peer1 time to join the group and claim both partitions alone.
            Thread.sleep(4000);

            var completed = new ConcurrentHashMap<Integer, CountDownLatch>();
            completed.put(0, new CountDownLatch(5));
            completed.put(1, new CountDownLatch(5));

            SpilloverBucketWorker worker0 = newOwnerWorker(0, originalPartition, topics, bootstrap, namePrefix);
            SpilloverBucketWorker worker1 = newOwnerWorker(1, originalPartition, topics, bootstrap, namePrefix);
            worker0.start();
            worker1.start();

            try {
                submitFakeRecords(worker0, originalPartition, 0, 5, completed.get(0));
                submitFakeRecords(worker1, originalPartition, 100, 5, completed.get(1));

                assertTrue(completed.get(0).await(30, TimeUnit.SECONDS), "bucket 0 should complete while peer1 alone owns it");
                assertTrue(completed.get(1).await(30, TimeUnit.SECONDS), "bucket 1 should complete while peer1 alone owns it");

                // kill peer1 and start a replacement -- its partitions become orphaned
                // until the group rebalances them onto the survivor.
                peer1.destroy();
                peer1.waitFor(10, TimeUnit.SECONDS);
                peer1.destroyForcibly();

                Process peer2 = startClaimPeer(bootstrap, namePrefix);
                try {
                    Thread.sleep(12000); // allow the rebalance to complete

                    var afterFailover = new CountDownLatch(5);
                    submitFakeRecords(worker0, originalPartition, 200, 5, afterFailover);

                    assertTrue(afterFailover.await(45, TimeUnit.SECONDS),
                            "surviving/replacement peer should claim bucket 0 after the original owner died "
                                    + "and complete records submitted to it post-failover");
                } finally {
                    peer2.destroy();
                    peer2.waitFor(10, TimeUnit.SECONDS);
                    peer2.destroyForcibly();
                }
            } finally {
                worker0.close();
                worker1.close();
            }
        } finally {
            peer1.destroy();
            peer1.waitFor(5, TimeUnit.SECONDS);
            peer1.destroyForcibly();
        }
    }

    private static void submitFakeRecords(SpilloverBucketWorker worker, TopicPartition originalPartition,
                                            long startOffset, int count, CountDownLatch latch) {
        for (int i = 0; i < count; i++) {
            long offset = startOffset + i;
            ConsumerRecord<String, byte[]> fakeRecord = new ConsumerRecord<>(
                    originalPartition.topic(), originalPartition.partition(), offset,
                    "key-" + offset, ("value-" + offset).getBytes(StandardCharsets.UTF_8));
            worker.submit(fakeRecord, (rec, done) -> {
                throw new AssertionError("processor must not run on the owner side for this transport");
            }, latch::countDown);
        }
    }

    private static Process startClaimPeer(String bootstrap, String namePrefix) throws Exception {
        return new ProcessBuilder(
                "java", "-cp", System.getProperty("java.class.path"),
                "com.hotpartition.transport.claim.ClaimPeerWorker",
                bootstrap, namePrefix, "10000")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                .start();
    }

    private static SpilloverBucketWorker newOwnerWorker(int bucketId, TopicPartition originalPartition,
                                                          SpilloverTopics topics, String bootstrap, String namePrefix) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("group.id", "claim-owner-" + namePrefix + "-" + bucketId);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());

        return new SpilloverBucketWorker(bucketId, originalPartition, topics,
                new KafkaProducer<>(producerProps), new KafkaConsumer<>(consumerProps));
    }
}
