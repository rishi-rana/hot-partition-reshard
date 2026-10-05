package com.hotpartition.reshard;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Smoke test against a real Testcontainers-managed Kafka broker: produces
 * records for a handful of known keys, forces resharding on, and verifies
 * (a) per-key order is preserved across bucket workers and (b) the commit
 * watermark never advances past what has actually been processed.
 */
@Testcontainers
class OwningConsumerSmokeTest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.0"));

    @Test
    void perKeyOrderAndCommitSafetyHoldUnderResharding() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        String topic = "smoke-" + System.nanoTime();

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }

        String[] keys = {"alice", "bob", "carol"};
        int perKeyRecords = 50;
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrap);
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", ByteArraySerializer.class.getName());
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < perKeyRecords; i++) {
                for (String key : keys) {
                    byte[] value = String.valueOf(i).getBytes();
                    producer.send(new ProducerRecord<>(topic, key, value));
                }
            }
            producer.flush();
        }

        var seenPerKey = new ConcurrentHashMap<String, CopyOnWriteArrayList<Integer>>();
        for (String key : keys) {
            seenPerKey.put(key, new CopyOnWriteArrayList<>());
        }
        // tracks, for every completed offset, the committed watermark AT THE TIME it
        // completed -- if the watermark ever exceeds an offset before that offset's
        // own completion was recorded, commit safety has been violated.
        var maxOffsetSeenSoFar = new java.util.concurrent.atomic.AtomicLong(-1);
        var commitSafetyViolated = new java.util.concurrent.atomic.AtomicBoolean(false);

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrap);
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        consumerProps.put("group.id", "smoke-test");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(consumerProps);
        TopicPartition tp = new TopicPartition(topic, 0);

        OwningConsumer[] ownerHolder = new OwningConsumer[1];
        try (OwningConsumer owner = new OwningConsumer(kafkaConsumer, tp, 3, (record, onComplete) -> {
            int value = Integer.parseInt(new String(record.value()));
            seenPerKey.get(record.key()).add(value);
            long committedBefore = ownerHolder[0] == null ? -1 : ownerHolder[0].watermarkTracker().commitPoint();
            if (committedBefore > record.offset()) {
                commitSafetyViolated.set(true);
            }
            maxOffsetSeenSoFar.set(Math.max(maxOffsetSeenSoFar.get(), record.offset()));
            onComplete.run();
        })) {
            ownerHolder[0] = owner;
            owner.assignAndSeek();
            kafkaConsumer.seekToBeginning(List.of(tp));
            owner.forceReshardActive(true);

            long endOffset = kafkaConsumer.endOffsets(List.of(tp)).get(tp);
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            while (owner.lastCommittedOffset() + 1 < endOffset && System.currentTimeMillis() < deadline) {
                owner.pollOnce(Duration.ofMillis(500));
            }

            assertEquals(endOffset - 1, owner.lastCommittedOffset(),
                    "owner should have committed through the full backlog within the deadline");
        }

        for (String key : keys) {
            List<Integer> seen = seenPerKey.get(key);
            assertEquals(perKeyRecords, seen.size(), "expected all records for key " + key);
            for (int i = 0; i < seen.size(); i++) {
                assertEquals(i, seen.get(i), "per-key order violated for key " + key + " at position " + i);
            }
        }

        assertTrue(!commitSafetyViolated.get(), "commit watermark must never advance past an offset before it completed");
    }
}
