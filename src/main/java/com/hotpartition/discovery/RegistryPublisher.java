package com.hotpartition.discovery;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Self-publishes this consumer instance's current state to {@code __hotpartition_registry}
 * on the same heartbeat cadence as the bucket-worker lease (default 2s, per the design
 * doc's resolved failure/recovery protocol) -- a consumer must run this even when its own
 * partitions are not hot, since lending capacity to a peer requires peers to know it's
 * available.
 */
public final class RegistryPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RegistryPublisher.class);

    private final KafkaProducer<String, String> producer;
    private final Supplier<RegistryEntry> entrySupplier;
    private final long intervalMs;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "hotpartition-registry-publisher");
        t.setDaemon(true);
        return t;
    });

    public RegistryPublisher(KafkaProducer<String, String> producer, Supplier<RegistryEntry> entrySupplier,
                              long intervalMs) {
        this.producer = producer;
        this.entrySupplier = entrySupplier;
        this.intervalMs = intervalMs;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::publishOnce, 0, intervalMs, TimeUnit.MILLISECONDS);
    }

    void publishOnce() {
        try {
            RegistryEntry entry = entrySupplier.get();
            producer.send(new ProducerRecord<>(RegistryTopic.NAME, entry.consumerInstanceId(), entry.serialize()));
        } catch (RuntimeException e) {
            log.warn("failed to publish registry entry", e);
        }
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        producer.close();
    }
}
