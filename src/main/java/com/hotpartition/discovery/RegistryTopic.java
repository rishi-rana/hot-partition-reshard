package com.hotpartition.discovery;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/** The shared control topic all three communication transports discover peers through. */
public final class RegistryTopic {

    public static final String NAME = "__hotpartition_registry";

    private RegistryTopic() {
    }

    /** Creates the topic as compacted (keyed by consumerInstanceId) if it doesn't already exist. */
    public static void ensureExists(Admin admin, int partitions, short replicationFactor) {
        NewTopic topic = new NewTopic(NAME, partitions, replicationFactor)
                .configs(Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
        try {
            admin.createTopics(List.of(topic)).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
