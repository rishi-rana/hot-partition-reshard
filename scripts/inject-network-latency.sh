#!/usr/bin/env bash
# Injects realistic inter-host network latency/jitter onto each broker
# container's network interface, approximating a genuine multi-host
# deployment without actually needing separate physical machines (see the
# paper's Evaluation Methodology and Limitations sections for why this
# matters: without this, all cross-container traffic stays on one physical
# NIC with no propagation delay or jitter at all, which understates the
# cost of every cross-process transport).
#
# Implemented via a netshoot sidecar sharing each broker's network
# namespace (--network container:<name>) rather than installing iproute2
# into the apache/kafka image itself -- this needs no changes to the
# broker containers and is trivially reversible (see teardown-network-latency.sh).
#
# Default: 1ms +/- 0.3ms (normal distribution), representative of
# same-datacenter / same-availability-zone round-trip latency between
# genuinely separate hosts -- not a cross-region WAN scenario, since a
# single Kafka cluster is not normally deployed cross-region.
set -euo pipefail

DELAY="${1:-1ms}"
JITTER="${2:-0.3ms}"
BROKERS=(kafka-1 kafka-2 kafka-3 kafka-4)

for b in "${BROKERS[@]}"; do
    echo "=== injecting ${DELAY} +/- ${JITTER} on ${b} ==="
    docker run --rm --cap-add NET_ADMIN --network "container:${b}" nicolaka/netshoot \
        tc qdisc add dev eth0 root netem delay "${DELAY}" "${JITTER}" distribution normal
done

echo "done. Verify with: docker run --rm --network container:kafka-1 nicolaka/netshoot tc qdisc show dev eth0"
