# Virtual Resharding of Hot Kafka Partitions — Prototype

A Java/Gradle prototype implementing and measuring the design described in
the companion paper, *Virtual Resharding of Hot Kafka Partitions*. This
README is a reproducibility guide: every table and headline number in the
paper has a corresponding command below.

## What's here

- **Detection pipeline** (`com.hotpartition.detection`, `com.hotpartition.concentration`):
  the three-stage gate (error counters, λ/μ decomposition, key-concentration
  sketches) that distinguishes genuine load-driven hotness from
  consumer-side degradation before anything reshards.
- **Primary mechanism** (`com.hotpartition.reshard`): key-bucket virtual
  resharding with a per-bucket commit-watermark scheme (`OwningConsumer`,
  `KeyBucketRouter`, `WatermarkTracker`).
- **Three cross-process delegation transports** (`com.hotpartition.transport.*`):
  direct RPC with Kafka-style batching (`rpc`), a Kafka-native spillover
  topic (`spillover`), and a work-stealing claim topic built on Kafka's own
  consumer-group rebalance (`claim`).
- **Cross-process peer discovery** (`com.hotpartition.discovery`): the
  `__hotpartition_registry` control topic.
- **Failure/recovery protocol** (`com.hotpartition.failure`): heartbeat
  lease, control-topic watermark store with epoch fencing, generation
  barrier for bucket-count changes.
- **Fallback mechanism** (`com.hotpartition.reshard.rangebarrier`):
  offset-range delegation with a per-key dependency barrier, as an
  alternative to key-bucket routing.
- **Evaluation harness** (`com.hotpartition.eval`): the skew-sensitivity
  benchmark, the reaction-latency experiment, and the repeated-trials driver.

## Prerequisites

- Java 21, Docker Desktop (or equivalent) running.
- No cloud account or credentials needed — the "multi-host" evaluation
  (see below) is approximated locally via injected network latency rather
  than genuinely separate machines; see **Network latency injection**.

## 1. Build and run the unit test suite

```
./gradlew build
```

48 tests, 47 pass without a broker running. The one expected failure is
`OwningConsumerSmokeTest`, which needs a real Docker-reachable Testcontainers
broker and fails in some sandboxed environments on a docker-java API-version
negotiation quirk unrelated to this project — see **Known environment
quirks** below for the workaround, or just use the standing local cluster
(step 2) and run it against that instead:

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" \
  ./gradlew test --tests "com.hotpartition.reshard.OwningConsumerSmokeTest"
```

## 2. Stand up the 4-broker cluster

```
docker compose -f docker-compose.kafka-cluster.yml up -d
```

Real KRaft mode (no ZooKeeper), 4 brokers each acting as both broker and
controller-quorum voter, topics created at replication factor 3. Verify:

```
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 describe --status
```

## 3. Network latency injection (approximating multi-host without a cloud account)

By default, all four brokers run on one physical machine; Docker's bridge
network between them has essentially zero propagation delay and no jitter,
understating the real cost every cross-process transport would pay between
genuinely separate hosts. `scripts/inject-network-latency.sh` closes this
gap without needing actual separate machines: it attaches a `netem` qdisc
to each broker's network interface via a `nicolaka/netshoot` sidecar
sharing that broker's network namespace (`--network container:<name>`),
so no changes to the broker image itself are needed and it's trivially
reversible.

```
./scripts/inject-network-latency.sh            # default: 1ms +/- 0.3ms, same-AZ realistic
./scripts/inject-network-latency.sh 2ms 0.5ms  # custom delay/jitter
./scripts/teardown-network-latency.sh          # remove it
```

Verify it's active:

```
docker run --rm --network container:kafka-1 nicolaka/netshoot tc qdisc show dev eth0
```

All benchmark results in the paper reported as run "under injected network
latency" used this script with its defaults before running step 4/5 below.
This does not cover the direct-RPC transport's owner-to-peer hop
specifically, since that transport communicates over host loopback rather
than through the dockerized brokers — a documented, narrower limitation
(see the paper's Limitations section).

## 4. Reproduce the skew-sensitivity benchmark (single run)

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" ./gradlew run \
  -DincludeSpillover=true -DincludeBatchedRpc=true -DincludeClaim=true -DincludeRangeBarrier=true \
  -DrecordCount=20000 -DnumKeys=2000 -DnumBuckets=4 -DreplicationFactor=3
```

Prints a CSV: `config,skew,hhi,recordCount,elapsedMs,recordsPerSec` for
baseline, naive-sharding-unsafe-commit, full-mechanism, and each of
full-mechanism-spillover / full-mechanism-batched-rpc / full-mechanism-claim
/ full-mechanism-range-barrier, swept across nine skew points from
near-uniform to the single-dominant-key regime (HHI≈0.0005 to ≈0.99).

For the in-process cost model at the original 1,000,000-record scale
(Section 10.1 of the paper, including the single-dominant-key limit
measurement in 10.1.1), drop the cross-process flags and raise the record
count:

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" ./gradlew run \
  -DrecordCount=1000000 -DnumKeys=2000 -DnumBuckets=8 -DreplicationFactor=3
```

(This one takes a while — baseline alone needs ~50 seconds per skew point
from its fixed per-record simulated cost, times nine skew points.)

## 5. Reproduce the repeated-trial variance table (Section 10.5)

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" ./gradlew run \
  -DrepeatedTrials=true -Dtrials=3
```

Runs all seven headline configurations 3 times each across the same
nine-point skew sweep, printing
`config,skew,hhi,trials,meanRecordsPerSec,stddevRecordsPerSec,minRecordsPerSec,maxRecordsPerSec`.
This is the longest-running single command in this repo (dozens of runs,
several of which spawn and tear down real OS processes for the
cross-process transports) — expect it to take on the order of an hour.

## 6. Reproduce the reaction-latency experiment (Section 10.4)

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" ./gradlew run \
  -DmeasureReactionLatency=true
```

Runs the real (non-forced) detection path once and prints
`detected,reactionLatencyMs,recordsProcessedAtDetection,falsePositiveDuringWarmup`.
Run it several times for a range rather than trusting a single run; the
paper reports 6 runs.

## Known environment quirks

- **Testcontainers/docker-java API-version negotiation.** If `./gradlew run`
  without `-Dbootstrap.servers`/`BOOTSTRAP_SERVERS` (which spins up a
  disposable Testcontainers broker instead of using the standing cluster)
  fails with a Docker API-version error, add
  `-Dapi.version=<your daemon's actual API version>` as a JVM system
  property (check `docker version` for the server's `API version`). This
  project's own evaluation runs always point at the standing 4-broker
  cluster via `BOOTSTRAP_SERVERS` instead, which avoids this path entirely.
- **`nicolaka/netshoot`** is pulled from Docker Hub on first use of the
  latency-injection scripts; no local build step needed.

## What's simplified relative to a production system

- Partition ownership in the benchmark harness is manual (`consumer.assign()`),
  not normal consumer-group membership, for benchmark determinism. The
  owner-failure recovery protocol (`ControlTopicWatermarkStore`, epoch
  fencing) is implemented and unit-tested in isolation but isn't wired into
  an actual rebalance listener in the benchmark path.
- `OwningConsumer.forceReshardActive()` is a benchmark-only hook bypassing
  the detection gate to measure steady-state throughput directly; the real,
  non-forced path is specifically exercised by the reaction-latency
  experiment (step 6).
- The direct-RPC transport's owner-to-peer hop runs over host loopback even
  with network latency injection active (see step 3) — a known, documented
  gap, not an oversight.
- All experiments run on a single physical machine; network latency
  injection (step 3) approximates but does not replace genuinely separate
  hosts — real inter-host jitter, packet loss under contention, and
  cross-host clock skew are not represented.

## Package layout

| Package | Contents |
|---|---|
| `com.hotpartition.detection` | `ErrorRateTracker`, `LambdaMuTracker`, `DetectionGate` |
| `com.hotpartition.concentration` | `MisraGriesSketch`, `AmsSketch`, `KeyConcentrationGate` |
| `com.hotpartition.reshard` | `KeyBucketRouter`, `BucketWorker`/`InProcessBucketWorker`, `WatermarkTracker`, `OwningConsumer` |
| `com.hotpartition.reshard.rangebarrier` | `KeyDependencyIndex`, `DependencyBarrier`, `RangePlan`, `RangeBarrierWorker`, `RangeBarrierOwningConsumer` |
| `com.hotpartition.discovery` | `RegistryEntry`, `PeerRegistry`, `RegistryTopic`, `RegistryPublisher`, `RegistryReader` |
| `com.hotpartition.transport.rpc` | `RpcEnvelope`/`RpcBatchEnvelope`, `RpcBucketWorker`/`BatchedRpcBucketWorker`, `RpcPeerWorker`/`BatchedRpcPeerWorker` |
| `com.hotpartition.transport.spillover` | `SpilloverTopics`, `SpilloverBucketWorker`, `SpilloverPeerWorker` |
| `com.hotpartition.transport.claim` | `ClaimPeerWorker` (reuses `SpilloverBucketWorker` on the owner side) |
| `com.hotpartition.failure` | `HeartbeatLease`, `ControlTopicWatermarkStore`, `GenerationBarrier` |
| `com.hotpartition.eval` | `ZipfKeyGenerator`, `SyntheticWorkloadProducer`, `BenchmarkRunner`, `ReactionLatencyExperiment`, `Main` |
