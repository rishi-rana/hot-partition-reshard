package com.hotpartition.discovery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tails {@code __hotpartition_registry} (a normal subscribe, relying on the topic's own
 * compaction to keep the full set of live-ish keys bounded) and feeds every record into a
 * {@link PeerRegistry} on a dedicated background thread. One reader instance is shared by
 * whichever transport(s) need peer discovery -- the registry itself is transport-agnostic.
 */
public final class RegistryReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RegistryReader.class);

    private final KafkaConsumer<String, String> consumer;
    private final PeerRegistry registry;
    private final Thread pollThread;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RegistryReader(KafkaConsumer<String, String> consumer, PeerRegistry registry) {
        this.consumer = consumer;
        this.registry = registry;
        this.pollThread = new Thread(this::runLoop, "hotpartition-registry-reader");
        this.pollThread.setDaemon(true);
    }

    public void start() {
        running.set(true);
        consumer.subscribe(List.of(RegistryTopic.NAME));
        pollThread.start();
    }

    private void runLoop() {
        while (running.get()) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() == null) {
                        registry.remove(record.key());
                        continue;
                    }
                    registry.update(RegistryEntry.parse(record.value()));
                }
            } catch (org.apache.kafka.common.errors.WakeupException e) {
                // expected on close()
            } catch (RuntimeException e) {
                log.warn("registry reader poll failed", e);
            }
        }
    }

    public PeerRegistry registry() {
        return registry;
    }

    @Override
    public void close() {
        running.set(false);
        consumer.wakeup();
        try {
            pollThread.join(Duration.ofSeconds(5).toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        consumer.close();
    }
}
