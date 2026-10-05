package com.hotpartition.transport.spillover;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Transport Option B from the design doc's Mechanism gap: delegated work is routed back
 * through Kafka itself rather than a new RPC surface. Bucket i's records go to partition
 * i of {@code SPILLOVER_TOPIC}; a peer processing bucket i's completion acks go to
 * partition i of {@code COMPLETIONS_TOPIC} -- same partition-per-bucket convention on both
 * topics, so routing is just "assign to partition = bucketId," no separate key-based
 * lookup needed.
 */
public final class SpilloverTopics {

    public static final String SPILLOVER_TOPIC = "__hotpartition_spillover";
    public static final String COMPLETIONS_TOPIC = "__hotpartition_spillover_completions";

    public static final String HEADER_ORIG_TOPIC = "orig-topic";
    public static final String HEADER_ORIG_PARTITION = "orig-partition";
    public static final String HEADER_ORIG_OFFSET = "orig-offset";

    private SpilloverTopics() {
    }

    /** Creates both topics with {@code numBuckets} partitions each, if they don't already exist. */
    public static void ensureExists(Admin admin, int numBuckets, short replicationFactor) {
        List<NewTopic> topics = List.of(
                new NewTopic(SPILLOVER_TOPIC, numBuckets, replicationFactor),
                new NewTopic(COMPLETIONS_TOPIC, numBuckets, replicationFactor));
        try {
            admin.createTopics(topics).all().get();
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
