# Hot-Partition Virtual Resharding — Prototype

A Java/Gradle prototype of the design in
[Hot-Partition P2P Resharding: Design & Related Work](https://claude.ai/artifact/BhXBBvCWZRiVkSUUiVMW2W).
Implements the three-stage detection gate, the key-concentration sketches, the
key-bucket fan-out mechanism with commit-watermark tracking, and the
failure/recovery protocol (heartbeat lease, control-topic watermark store,
generation barrier for bucket-count changes).

## Build and test

```
./gradlew build          # compile + run unit tests
./gradlew test --tests "com.hotpartition.concentration.*" --tests "com.hotpartition.reshard.WatermarkTrackerTest" --tests "com.hotpartition.reshard.KeyBucketRouterTest"
```

These unit tests need no Kafka broker and are the highest-value tests in this
repo — they caught two real off-by-indexing bugs in `WatermarkTracker` during
development (per-bucket offsets aren't contiguous integers, and a drained
bucket must not permanently cap the global commit point at its own last
dispatched offset). See the git history for the before/after.

## Running the benchmark

```
./gradlew run
# or against a real broker instead of a disposable Testcontainers one:
./gradlew run -Dbootstrap.servers=localhost:9092
```

Sweeps key-concentration (skew) from uniform to single-dominant-key and
prints a CSV comparing: baseline single-threaded consumer, naive key-hash
sharding with an **unsafe** commit (fans out to buckets but commits the raw
consumer position regardless of whether buckets have actually finished —
kept deliberately unsafe to isolate what the watermark mechanism *costs*,
not to recommend it), and the full `OwningConsumer` mechanism.

## What's simplified vs. the full design doc

This is a prototype for validating the mechanism and measuring overhead, not
a production system. The most important gap:

- **Bucket workers are in-process threads (`InProcessBucketWorker`), not
  separate processes/machines.** The design doc's real motivation is
  borrowing spare capacity from *other consumers already assigned to other,
  non-hot partitions in the same group* — that requires cross-process
  dispatch (RPC or a local queue reachable from another JVM), which this
  prototype does not implement. The detection gate, concentration sketches,
  key-bucket routing, and commit-watermark logic are all process-topology-
  agnostic, so swapping in a cross-process `BucketWorker` implementation
  later shouldn't require touching anything else.
- **Partition ownership is manual (`consumer.assign()`), not a normal
  consumer-group membership.** This was done for benchmark determinism; the
  owner-failure recovery protocol (`ControlTopicWatermarkStore`, epoch
  fencing) is implemented and unit-testable in isolation, but isn't wired
  into an actual rebalance listener here.
- **`OwningConsumer.forceReshardActive()`** is a benchmark-only hook that
  bypasses the detection gate so the benchmark can measure the mechanism's
  steady-state throughput directly, rather than waiting on the gate's own
  (separately-tested) decision logic.
- **`GenerationBarrier`** (bucket-count changes mid-flight) is implemented
  and unit-testable but not exercised by the benchmark or the integration
  smoke test — there's no scenario in either that changes the key-
  concentration picture enough mid-run to trigger it.

## Integration test / known environment limitation

`src/test/java/com/hotpartition/reshard/OwningConsumerSmokeTest.java` spins
up a real Kafka broker via Testcontainers, forces resharding on, and asserts
(a) per-key order is preserved across bucket workers and (b) the commit
watermark never advances past an offset before that offset actually
completed. **This did not run successfully in the sandboxed environment this
prototype was built in**: Testcontainers' Docker client failed to negotiate
with the local Docker Desktop socket (`unix:///Users/<user>/.docker/run/docker.sock`,
daemon API 1.55) — one attempt got past detection and failed on a stale
hardcoded API version (1.32, below the daemon's minimum of 1.40), and
subsequent attempts regressed to "no valid Docker environment" even with
`DOCKER_HOST` and `~/.testcontainers.properties` overrides tried. Plain
`docker ps` and a raw `curl` against the socket both worked fine from the
shell throughout, so this looks like a JVM/sandbox-specific socket access or
environment-detection quirk in this run, not a real Docker outage. The test
code itself compiles and should run normally on a developer machine with
Docker Desktop reachable from the JVM — re-run `./gradlew test --tests
"com.hotpartition.reshard.OwningConsumerSmokeTest"` there to confirm before
relying on it.

## Package layout

- `com.hotpartition.detection` — stages 1–2 of the detection gate
  (`ErrorRateTracker`, `LambdaMuTracker`, `DetectionGate`).
- `com.hotpartition.concentration` — stage 3 (`MisraGriesSketch`,
  `AmsSketch`, `KeyConcentrationGate`).
- `com.hotpartition.reshard` — the core mechanism (`KeyBucketRouter`,
  `BucketWorker`/`InProcessBucketWorker`, `WatermarkTracker`,
  `OwningConsumer`).
- `com.hotpartition.failure` — the failure/recovery protocol
  (`HeartbeatLease`, `ControlTopicWatermarkStore`, `GenerationBarrier`).
- `com.hotpartition.eval` — the skew-sensitivity evaluation harness
  (`ZipfKeyGenerator`, `SyntheticWorkloadProducer`, `BenchmarkRunner`, `Main`).
