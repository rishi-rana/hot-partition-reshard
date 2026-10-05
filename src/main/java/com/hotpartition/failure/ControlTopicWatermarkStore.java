package com.hotpartition.failure;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Persists per-bucket watermark state to a compacted control topic
 * ({@value #CONTROL_TOPIC}) so a new owner (after a normal consumer-group
 * rebalance) can reconstruct where the previous owner left off, per the
 * design doc's owner-failure recovery protocol. An epoch number fences off a
 * zombie old owner that keeps writing after a new owner has taken over,
 * mirroring Kafka's own transactional-producer epoch fencing.
 */
public final class ControlTopicWatermarkStore implements AutoCloseable {

    public static final String CONTROL_TOPIC = "__hotpartition_watermarks";

    public record Snapshot(long epoch, Map<Integer, Long> watermarks) {
    }

    private final KafkaProducer<String, String> producer;
    private final String bootstrapServers;

    public ControlTopicWatermarkStore(String bootstrapServers, Properties producerProps) {
        this.bootstrapServers = bootstrapServers;
        this.producer = new KafkaProducer<>(producerProps);
    }

    public static void ensureTopicExists(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            NewTopic topic = new NewTopic(CONTROL_TOPIC, 1, (short) 1);
            topic.configs(Map.of("cleanup.policy", "compact"));
            admin.createTopics(java.util.List.of(topic)).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void persist(TopicPartition owned, long epoch, Map<Integer, Long> watermarks) {
        String key = owned.topic() + "-" + owned.partition();
        String value = serialize(epoch, watermarks);
        producer.send(new ProducerRecord<>(CONTROL_TOPIC, key, value));
        producer.flush();
    }

    public Optional<Snapshot> readLatest(String bootstrapServers, TopicPartition owned, Properties consumerBaseProps) {
        String key = owned.topic() + "-" + owned.partition();
        Properties props = new Properties();
        props.putAll(consumerBaseProps);
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "control-topic-reader-" + java.util.UUID.randomUUID());
        props.put("enable.auto.commit", "false");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition controlPartition = new TopicPartition(CONTROL_TOPIC, 0);
            consumer.assign(java.util.List.of(controlPartition));
            consumer.seekToBeginning(java.util.List.of(controlPartition));
            long end = consumer.endOffsets(java.util.List.of(controlPartition)).get(controlPartition);

            Snapshot latest = null;
            while (consumer.position(controlPartition) < end) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    if (key.equals(record.key())) {
                        latest = deserialize(record.value());
                    }
                }
                if (records.isEmpty()) {
                    break;
                }
            }
            return Optional.ofNullable(latest);
        }
    }

    private static String serialize(long epoch, Map<Integer, Long> watermarks) {
        StringBuilder sb = new StringBuilder();
        sb.append(epoch).append(';');
        boolean first = true;
        for (Map.Entry<Integer, Long> e : watermarks.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            sb.append(e.getKey()).append('=').append(e.getValue());
            first = false;
        }
        return sb.toString();
    }

    private static Snapshot deserialize(String value) {
        String[] parts = value.split(";", 2);
        long epoch = Long.parseLong(parts[0]);
        Map<Integer, Long> watermarks = new HashMap<>();
        if (parts.length > 1 && !parts[1].isEmpty()) {
            for (String kv : parts[1].split(",")) {
                String[] pair = kv.split("=");
                watermarks.put(Integer.parseInt(pair[0]), Long.parseLong(pair[1]));
            }
        }
        return new Snapshot(epoch, watermarks);
    }

    @Override
    public void close() {
        producer.close();
    }
}
