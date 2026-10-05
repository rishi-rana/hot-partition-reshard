package com.hotpartition.eval;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/** Produces a synthetic keyed workload to a single-partition topic with a controllable key-concentration distribution. */
public final class SyntheticWorkloadProducer {

    private final String bootstrapServers;

    public SyntheticWorkloadProducer(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void ensureTopic(String topic) {
        ensureTopic(topic, (short) 1);
    }

    /**
     * Deletes the topic first if it already exists, then creates it fresh. Benchmark runs
     * reuse fixed topic names (e.g. "bench-skew-0_0") across separate invocations of this
     * harness; without deleting first, each new run would silently append to -- rather
     * than replace -- whatever backlog earlier runs left behind, so both recordCount and
     * endOffsets() would reflect the accumulated total, not this run's own data. (This was
     * a real bug, not a hypothetical: it's exactly what made an early comparison run look
     * like a ~90x throughput regression that didn't actually exist.)
     */
    public void ensureTopic(String topic, short replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            try {
                admin.deleteTopics(java.util.List.of(topic)).all().get();
                // deletion is async on the broker; give it a moment before recreating under
                // the same name, or creation can race a still-in-progress delete.
                Thread.sleep(1000);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                    throw new RuntimeException(e);
                }
            }
            admin.createTopics(java.util.List.of(new NewTopic(topic, 1, replicationFactor))).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public void produce(String topic, int recordCount, int numKeys, double skew, long seed, int valueSizeBytes) {
        ZipfKeyGenerator keyGen = new ZipfKeyGenerator(numKeys, skew, seed);
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put("acks", "1");
        props.put("linger.ms", "5");
        byte[] value = new byte[valueSizeBytes];
        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < recordCount; i++) {
                String key = keyGen.nextKey();
                producer.send(new ProducerRecord<>(topic, key, value));
            }
            producer.flush();
        }
    }

    public static double theoreticalHhi(int numKeys, double skew, long seed) {
        return new ZipfKeyGenerator(numKeys, skew, seed).theoreticalHhi();
    }
}
