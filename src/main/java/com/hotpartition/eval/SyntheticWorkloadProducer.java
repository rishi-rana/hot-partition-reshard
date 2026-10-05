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
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (Admin admin = Admin.create(props)) {
            admin.createTopics(java.util.List.of(new NewTopic(topic, 1, (short) 1))).all().get();
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
