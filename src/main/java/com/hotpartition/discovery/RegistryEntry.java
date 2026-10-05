package com.hotpartition.discovery;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * One consumer instance's self-published state in the {@code __hotpartition_registry}
 * control topic: who it is, what it owns, and how loaded it currently is (reusing the
 * Detection Pipeline's own lambda/mu machinery, self-reported rather than observed by
 * someone else). {@code reachability} is a transport-specific address (e.g. "host:port"
 * for the direct-RPC transport, or empty for transports that don't need one, such as the
 * spillover-topic or work-stealing-claim-topic transports) -- left as a free-form string
 * here since the three communication transports haven't been built yet.
 */
public record RegistryEntry(
        String consumerInstanceId,
        String groupId,
        Set<TopicPartition> assignedPartitions,
        double lambdaRate,
        double muRate,
        double muBaseline,
        long atMillis,
        String reachability
) {

    private static final String FIELD_SEP = "|";
    private static final String TP_SEP = ",";
    private static final String TP_PART_SEP = ":";

    /** Headroom ratio: how far muRate sits below its own baseline. 0 = fully saturated, 1 = idle. */
    public double headroomRatio() {
        if (muBaseline <= 0) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, 1.0 - (muRate / muBaseline)));
    }

    public String serialize() {
        String tps = assignedPartitions.stream()
                .map(tp -> tp.topic() + TP_PART_SEP + tp.partition())
                .collect(Collectors.joining(TP_SEP));
        return String.join(FIELD_SEP,
                consumerInstanceId,
                groupId,
                tps,
                Double.toString(lambdaRate),
                Double.toString(muRate),
                Double.toString(muBaseline),
                Long.toString(atMillis),
                reachability == null ? "" : reachability);
    }

    public static RegistryEntry parse(String s) {
        String[] f = s.split("\\" + FIELD_SEP, -1);
        if (f.length != 8) {
            throw new IllegalArgumentException("malformed registry entry: " + s);
        }
        List<TopicPartition> tps = new ArrayList<>();
        if (!f[2].isEmpty()) {
            for (String part : f[2].split(TP_SEP)) {
                String[] tp = part.split(TP_PART_SEP);
                tps.add(new TopicPartition(tp[0], Integer.parseInt(tp[1])));
            }
        }
        return new RegistryEntry(
                f[0], f[1], Set.copyOf(tps),
                Double.parseDouble(f[3]), Double.parseDouble(f[4]), Double.parseDouble(f[5]),
                Long.parseLong(f[6]), f[7]);
    }
}
