package com.hotpartition.transport.spillover;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Transport Option B from the design doc's Mechanism gap: delegated work is routed back
 * through Kafka itself rather than a new RPC surface. Bucket i's records go to partition
 * i of the spillover topic; a peer processing bucket i's completion acks go to partition
 * i of the completions topic -- same partition-per-bucket convention on both, so routing
 * is just "assign to partition = bucketId," no separate key-based lookup needed.
 *
 * Namespaced per source partition (via {@code namePrefix}) rather than one fixed global
 * pair of topics: a single shared pair would let two different hot partitions' bucket 0
 * collide on the same spillover partition if resharding were ever active on more than one
 * partition at a time, and would let leftover unconsumed records from an earlier run leak
 * into a later one reusing the same bucket ids (exactly what happens across a skew sweep's
 * sequential iterations in the benchmark harness). The production design would need a
 * real namespacing scheme (e.g. keyed by topic-partition); this is the benchmark-harness
 * version of that same requirement, not a shortcut unique to testing.
 */
public final class SpilloverTopics {

    public static final String HEADER_ORIG_TOPIC = "orig-topic";
    public static final String HEADER_ORIG_PARTITION = "orig-partition";
    public static final String HEADER_ORIG_OFFSET = "orig-offset";

    private final String spilloverTopic;
    private final String completionsTopic;

    public SpilloverTopics(String namePrefix) {
        this.spilloverTopic = namePrefix + "-spillover";
        this.completionsTopic = namePrefix + "-spillover-completions";
    }

    public String spilloverTopic() {
        return spilloverTopic;
    }

    public String completionsTopic() {
        return completionsTopic;
    }

    /**
     * Deletes both topics first if they already exist, then creates them fresh with
     * {@code numBuckets} partitions each. Required for the same reason
     * {@code SyntheticWorkloadProducer.ensureTopic} deletes first: a benchmark namePrefix
     * (e.g. "bench-skew-0_0") is reused across separate invocations of this harness, and
     * without deleting first, a later run would silently replay an earlier run's leftover
     * spillover/completion records alongside its own.
     */
    public void ensureExists(Admin admin, int numBuckets, short replicationFactor) {
        List<String> names = List.of(spilloverTopic, completionsTopic);
        try {
            try {
                admin.deleteTopics(names).all().get();
                Thread.sleep(1000);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)) {
                    throw new RuntimeException(e);
                }
            }
            List<NewTopic> topics = List.of(
                    new NewTopic(spilloverTopic, numBuckets, replicationFactor),
                    new NewTopic(completionsTopic, numBuckets, replicationFactor));
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
