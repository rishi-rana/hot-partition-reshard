#!/usr/bin/env bash
# Reverses inject-network-latency.sh, removing the netem qdisc from each
# broker's network interface.
set -euo pipefail

BROKERS=(kafka-1 kafka-2 kafka-3 kafka-4)

for b in "${BROKERS[@]}"; do
    echo "=== removing netem from ${b} ==="
    docker run --rm --cap-add NET_ADMIN --network "container:${b}" nicolaka/netshoot \
        tc qdisc del dev eth0 root netem || echo "(nothing to remove on ${b})"
done
