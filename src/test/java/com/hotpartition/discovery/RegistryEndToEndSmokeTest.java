package com.hotpartition.discovery;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the real registry topic against an actual broker (set BOOTSTRAP_SERVERS to
 * run) rather than the pure in-memory PeerRegistry tests: two simulated consumer
 * instances publish, and each should discover the other as a live peer.
 */
class RegistryEndToEndSmokeTest {

    @Test
    @EnabledIfEnvironmentVariable(named = "BOOTSTRAP_SERVERS", matches = ".+")
    void twoInstancesDiscoverEachOtherViaRealBroker() throws Exception {
        String bootstrap = System.getenv("BOOTSTRAP_SERVERS");

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", bootstrap);
        try (Admin admin = Admin.create(adminProps)) {
            RegistryTopic.ensureExists(admin, 1, (short) 1);
        }

        PeerRegistry sharedView = new PeerRegistry();
        RegistryReader reader = newReader(bootstrap, sharedView);
        reader.start();

        RegistryPublisher pubA = newPublisher(bootstrap, "instance-A", 90.0, 100.0);
        RegistryPublisher pubB = newPublisher(bootstrap, "instance-B", 10.0, 100.0);
        pubA.start();
        pubB.start();

        try {
            long deadline = System.currentTimeMillis() + 20_000;
            boolean sawBoth = false;
            while (System.currentTimeMillis() < deadline) {
                long now = System.currentTimeMillis();
                if (sharedView.liveCount(now, 10_000) >= 2) {
                    sawBoth = true;
                    break;
                }
                TimeUnit.MILLISECONDS.sleep(300);
            }
            assertTrue(sawBoth, "expected both instances to appear in the shared registry view");

            long now = System.currentTimeMillis();
            var idleFromA = sharedView.findIdlePeer("instance-A", now, 10_000, 0.3);
            assertTrue(idleFromA.isPresent(), "instance-A should see instance-B as idle (low mu/baseline ratio)");
        } finally {
            pubA.close();
            pubB.close();
            reader.close();
        }
    }

    private static RegistryReader newReader(String bootstrap, PeerRegistry registry) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "registry-reader-smoke-" + System.nanoTime());
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        return new RegistryReader(new KafkaConsumer<>(props), registry);
    }

    private static RegistryPublisher newPublisher(String bootstrap, String instanceId, double muRate, double muBaseline) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        return new RegistryPublisher(producer, () -> new RegistryEntry(
                instanceId, "smoke-group",
                Set.of(new TopicPartition("some-topic", 0)),
                0.0, muRate, muBaseline,
                System.currentTimeMillis(), ""
        ), 1000);
    }
}
