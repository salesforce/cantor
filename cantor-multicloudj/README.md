# cantor-multicloudj

Cantor on top of MultiCloudJ — cloud-agnostic object storage for AWS S3, GCP Cloud Storage, Alibaba OSS.

## Maven Coordinates

```xml
<dependency>
    <groupId>com.salesforce.cantor</groupId>
    <artifactId>cantor-multicloudj</artifactId>
    <version>0.5.25-SNAPSHOT</version>
</dependency>
```

## Provider Runtime

You must add exactly one provider runtime to your classpath:

| Cloud Provider | Artifact |
|---|---|
| AWS S3 | `com.salesforce.multicloudj:blob-aws:0.4.0` |
| GCP Cloud Storage | `com.salesforce.multicloudj:blob-gcp:0.4.0` |
| Alibaba OSS | `com.salesforce.multicloudj:blob-ali:0.4.0` |

For test/dev use only: `com.salesforce.multicloudj:blob-inmemory:0.4.0` provides an in-memory backend that requires no cloud credentials.

## Java Version Requirement

**Java 11+ required at runtime.** MultiCloudJ requires Java 11. The rest of Cantor targets Java 8; this module overrides source/target to 11 in its own POM. Consumers must run on JDK 11 or later.

## Quick Start (AWS)

```java
BucketClient bc = BucketClient.builder("aws")
        .withRegion("us-west-2")
        .withBucket("my-bucket")
        .build();
try (CantorOnMulticloudj cantor = new CantorOnMulticloudj(bc)) {
    cantor.objects().create("orders");
    cantor.objects().store("orders", "k1", "hello".getBytes(UTF_8));
}
```

## Advanced Configuration

For credentials override, retry config, endpoint override, parallel upload settings, or tracing policy, use `BucketClient.builder(...)` directly and pass the resulting `BucketClient` to `new CantorOnMulticloudj(bucketClient)`. This module does NOT re-expose every builder option — refer to the [MultiCloudJ documentation](https://github.com/salesforce/multicloudj) for the full builder API.

## Sets

`Sets` are explicitly **unsupported** (matches `cantor-s3`). Calling `cantor.sets()` throws `UnsupportedOperationException`.

## Azure

Azure is **not supported**. MultiCloudJ 0.4.0 ships AWS, GCP, and Alibaba adapters only. No Azure adapter exists upstream as of June 2026.

## Known Limitations / Trade-offs

> **Performance-sensitive consumers must read this section before adopting `cantor-multicloudj`.**

1. **No S3 Select equivalent — `EventsOnMulticloudj` uses client-side filtering.**
   `EventsOnS3` pushes metadata/dimension predicates to S3 via S3 Select; the server returns only matching rows, and only the matching bytes traverse the network. MultiCloudJ 0.4.0 exposes no equivalent server-side query API. `EventsOnMulticloudj` therefore:
   1. lists matching time-prefix blobs,
   2. downloads each `.json` blob **in full**,
   3. parses every line with Gson,
   4. applies metadata/dimension predicates in-process.

   This is correct but slower than `EventsOnS3` and incurs higher network egress proportional to the unfiltered event volume. For a 1 GB-per-namespace-per-day workload the difference is negligible; for 100 GB+ workloads with selective predicates it can be significant — acceptable for v1; performance-sensitive consumers should profile before adopting. **Future work:** evaluate columnar/Parquet event storage, an external secondary index (e.g., manifest blobs per namespace/time-window with materialized metadata), or push for a `query`/`scan` API in MultiCloudJ.

2. **Java 11+ runtime.** MultiCloudJ requires Java 11. The rest of Cantor targets Java 8. `cantor-multicloudj` overrides source/target to 11 in its own pom; consumers must run on Java 11+.

3. **`Sets` unsupported.** `cantor.sets()` throws `UnsupportedOperationException` — same as `cantor-s3`. Out of scope for this module; would require a separate spec.

4. **Azure not supported.** MultiCloudJ 0.4.0 ships AWS, GCP, and Alibaba adapters only. No Azure adapter exists upstream as of June 2026.

5. **Provider runtime is opt-in.** `blob-aws` / `blob-gcp` / `blob-ali` are declared `<optional>true</optional>`. Consumers must add the runtime they need to their own POM. The facade's convenience constructor surfaces a clear `IOException` if the runtime is missing.

6. **Delete batch size capped at 100.** AWS supports 1000, GCP 100, Alibaba 1000. We use the floor for portability. Large drops/expires are paginated.

7. **In-memory provider feature coverage.** `blob-inmemory` may not implement every method identically to real providers (notably range downloads). Tests are written defensively: where the in-memory provider diverges from a real provider, the test exercises the production fallback path and a Javadoc records the limitation.

8. **`trim()` namespace collisions.** Namespace sanitisation uses `String.hashCode()` (32-bit). Collisions are theoretically findable at scale; inherited from `cantor-s3`'s design and kept for layout parity.

9. **Aggregate javadoc / Java mixed-mode.** The parent POM aggregates javadoc with `source=${source.version}` (=1.8). `cantor-multicloudj`'s sources are Java 11. The module's per-module javadoc jar uses source=11 (correct); if running `mvn site` aggregates javadoc fails on Java-11 sources, the parent's aggregate javadoc execution must either bump to `source=11` (requires JDK 11 to build the site) or exclude this module. Out of scope for this spec; flagged for the maintainer who runs `mvn site` / `mvn release`.

10. **Concurrent writes are serialised per-namespace.** Buffer writes are guarded by a `ReentrantLock` per namespace. Stores to different namespaces run in parallel; stores to the same namespace serialise. This matches `EventsOnS3` semantics.
