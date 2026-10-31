# Running tests in this repo

Always run the test suite (and any `./gradlew run` experiment/benchmark) with
`BOOTSTRAP_SERVERS` pointing at the local standing Kafka cluster, e.g.:

```
BOOTSTRAP_SERVERS="localhost:9092,localhost:9094,localhost:9095,localhost:9096" ./gradlew test
```

Without it, the live-Kafka end-to-end tests
(`RegistryEndToEndSmokeTest`, `SpilloverEndToEndSmokeTest`,
`ClaimEndToEndSmokeTest`) are skipped (gated by
`@EnabledIfEnvironmentVariable(BOOTSTRAP_SERVERS)`), which silently hides
real coverage of the cross-process transport paths.

Before running, confirm the local cluster is actually up (`docker ps` should
show `kafka-1`..`kafka-4`); start it per the README if not.

Note: `OwningConsumerSmokeTest` does NOT read `BOOTSTRAP_SERVERS` — it uses
its own JUnit `@Testcontainers`-managed broker unconditionally, so setting
this env var has no effect on that one test. Its current failure in this
sandbox is a known, root-caused Testcontainers/docker-java API-version
negotiation issue (missing `api.version` system-property forward in
`build.gradle.kts`'s `tasks.test` block), unrelated to application code.
