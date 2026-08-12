# Millpond — Kafka to DuckLake

A standalone Python app that consumes from a Kafka topic and writes to a [DuckLake](https://github.com/duckdb/ducklake) table. Single thread, single loop, no Kafka Connect. One deployment writes to exactly one table.

**Contents:**
[Naming](#naming) | [Why](#why) | [Architecture](#architecture) | [Destination](#destination) | [Record Handling](#record-handling) | [Adaptive Backpressure](#adaptive-backpressure) | [Performance](#performance) | [Resource Footprint](#resource-footprint) | [Setup](#setup) | [Development](#development) | [Configuration](#configuration) | [Releases](#releases) | [Deployment](#deployment) | [Partitioning](#partitioning) | [Object Sizing](#object-sizing) | [Error Handling](#error-handling-and-retries) | [Multiple Pipelines](#multiple-pipelines) | [AWS Credential Isolation](#aws-credential-isolation) | [Operational Notes](#operational-notes) | [tools](tools/README.md)

## Naming

<img src="imgs/500px-Hagley_mill_race.jpeg" alt="A mill pond" width="300" align="right">

> **millpond** (noun): a pond created by damming a stream to produce a head of water for operating a mill.
> — [Merriam-Webster](https://www.merriam-webster.com/dictionary/millpond)

Millpond accumulates a stream of Kafka records until a threshold is reached, then releases them into a downstream lake. Like a [mill pond](https://en.wikipedia.org/wiki/Mill_pond) feeding a lake.

## Why

Kafka Connect imposes ~1100 lines of lock management, scheduled executors, and rebalance handling to work around its lack of backpressure and explicit offset control. Millpond replaces all of that with:

```
loop:
  consume() → JSON → Arrow → accumulate
  when buffer full or time elapsed:
    write to lake → commit offsets
```

Single thread, single loop. Kafka is the buffer. Offset commit is explicit (after successful write only). No data loss window.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       └─ Single loop: consume → convert → [filter] → accumulate → [sort] → flush → commit
```

- One topic and one table per deployment
- Static partition assignment via pod ordinal — no consumer groups
- If a pod dies, its partitions stop being consumed until K8s restarts it
- Optional filter and sort stages — see [Record Handling](#record-handling) below

## Destination

Millpond writes to DuckLake. A single deployment writes to exactly one table — there is no per-batch routing.

|  | DuckLake |
|---|---|
| Catalog | Postgres (via DuckDB ducklake extension) |
| Storage | S3 / S3-compatible |
| Reader ecosystem | DuckDB-native; growing third-party support |
| Partitioning | Caller-supplied via `DUCKLAKE_PARTITION_BY`; arbitrary DDL expression |
| Schema evolution | DuckDB DDL (`ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN SET DATA TYPE` with widening enforcement) |
| Maintenance tooling | Bundled (`tools/ducklake_maintenance.py` CronJob, `tools/ducklake_metrics.py` daemon) |
| `_inserted_at` column | Added at INSERT via DuckDB `NOW()` (per-row, microsecond drift possible within a flush) |
| Multi-pod concurrent writes | Native; idempotent DDL handles races |

The sink (`millpond/ducklake.py`) exposes three methods to `main.py`: `write(batch)`, `reset_caches()`, `close()`.

## Record Handling

Two optional stages sit between Kafka conversion and the sink. Both are disabled when their env vars are unset.

### Allowlist filter

Drops records whose value in a configured field is not in a configured allowlist. Applied immediately after JSON→Arrow conversion, before records enter the pending buffer.

```
MILLPOND_FILTER_KEEP_FIELD_NAME=team_id
MILLPOND_FILTER_VALUES=2,4,1956,69
```

Values auto-detect: tokens that all parse as integers become an int allowlist; otherwise the whole list is treated as strings.

Two skip reasons are tracked on `millpond_records_skipped_total`:

- `filter_field_missing` — column absent from this batch's schema, null for that row, or column type is not filterable (only integer and string columns are supported; bool, float, timestamp, struct, list, etc. are rejected explicitly to avoid silent surprising matches under PyArrow's `safe=True` cast semantics).
- `filter_excluded` — column present and non-null but value not in the allowlist. Expected steady-state drop reason.

`MILLPOND_FILTER_DROP_FIELD_NAME` is reserved at the config layer (mutex with keep) and currently rejected at startup. It will become a denylist filter in a future release without env-var churn.

### Dynamic allowlist source

The allowlist can be sourced at runtime from an HTTP endpoint instead of being fixed at startup: `MILLPOND_INCLUDE_VALUES_URL` names a URL returning a JSON array of scalars (ints or strings, matching the static list's type), polled on a background thread (default every 60s, ±10% jitter). Millpond knows nothing about the endpoint's meaning — the URL and an optional auth header (`MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME` + `_AUTH_TOKEN`) are plain config.

How the static list and the polled set interact:

| `MILLPOND_FILTER_VALUES` (static) | `MILLPOND_INCLUDE_VALUES_URL` | `MILLPOND_INCLUDE_VALUES_MODE` | Effective allowlist | Endpoint's role |
|---|---|---|---|---|
| set | unset | unset | the static list | none — today's behavior, unchanged |
| set | set | `shadow` (default) | the static list | observability only: polled each interval, exports diff-vs-static gauges and staleness; its values are never applied |
| set | set | `authoritative` | the polled set **∪ the static list** | live: the endpoint governs everything it serves; the static list is a permanent manual floor ("pins") — values the endpoint has never heard of (legacy/grandfathered) stay included and can only be removed by a config deploy. Startup **blocks** until the first successful poll (no proceed-on-stale-bootstrap) |
| unset | unset | unset | no filter — all records kept | — |
| unset | set | any | **startup error** | the URL requires an active keep-filter, which requires static values |
| any | unset | set (or auth vars set) | **startup error** | MODE/auth without a URL means a dynamic source was intended; refusing beats silently running static-only |

In `authoritative` mode the polled set changes under safety rules shaped by a consequence asymmetry — an erroneous addition writes surplus rows, an erroneous removal silently drops records with no recovery:

- **Additions** apply on the first successful poll that shows them.
- **Removals** require `MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS` (default 5) *consecutive successful* polls with the value absent. Failed polls freeze the countdown; a reappearing value resets it. Statically-pinned values are exempt regardless of endpoint state — a pin stays served even if the endpoint once served it and later dropped it; removing a pin is a config deploy.
- **Poll failures** keep the last-known-good set indefinitely; staleness is observable via `millpond_include_values_last_success_timestamp_seconds`.
- **Refused polls** (counted on `millpond_include_values_refused_total{reason}`) keep the set and advance nothing: empty arrays (`empty` — never removal evidence when endpoint-managed values are held; a pins-only set accepts an empty endpoint as a legitimate steady state), removals of more than half of the *endpoint-managed slice* at once (`bulk_removal` — measured against current minus endpoint-invisible pins; the refused poll's **additions still apply**, additions being the safe direction), and int↔str type changes (`type_flip` — a type-flipped set would fail the filter's cast against the column and drop whole batches).

Rollout is designed to be shadow-first: run `shadow`, watch `millpond_include_values_shadow_only_static` / `_shadow_only_remote`, and flip to `authoritative` once `shadow_only_static` equals the intentional pin *count* (`millpond_include_values_pinned_only` — the pins are logged at startup so membership is checkable) and `shadow_only_remote` matches the expected dynamic expansion. The shadow prober carries the same pins as authoritative mode, so its size/pending-removal gauges predict exactly the set the flip would serve. `millpond_include_values_pinned` / `_pinned_only` survive the flip (the shadow gauges don't), keeping pin/endpoint divergence observable in authoritative mode.

**Prune the static list to the intentional pins before flipping.** Every static value is a permanent pin: a static list that fully mirrors the endpoint at flip time leaves the endpoint with nothing it can ever remove — the damping machinery goes dead with `pending_removals` reading a healthy-looking 0. `pinned_only` at its expected count (vs `pinned` ≈ the whole set) is the tell. `millpond_include_values_mode` reports which mode each replica actually runs, so a fleet-level flip gate can't pass vacuously on a replica that never got the URL.

### Pre-write sort

Sorts the consolidated batch by one or more columns ascending, right before `sink.write()`. The sink sees pre-sorted data, which improves Parquet compression (especially for low-cardinality keys like `team_id`) and downstream reader predicate pushdown.

```
MILLPOND_SORT_BY=team_id,timestamp
```

Sort order is left-to-right (`team_id` primary, `timestamp` secondary). Direction is ascending only today; if you need descending, file an issue. PyArrow's sort is stable, so equal-key rows preserve their consume order.

If any sort field is missing from a batch's schema, the sort is skipped (records still flow through, just unsorted), `millpond_sort_skipped_total{reason="field_missing"}` increments by the record count, and a warning logs once per distinct missing-fields pattern (per pod lifetime — prevents log floods under sustained misconfiguration).

Per-flush cost is ~50–200 ms on a 256 MB / 30k-row batch. Peak memory roughly doubles during the sort because `pa.Table.take()` rewrites a fresh copy of every column; budget accordingly relative to the pod's memory limit.

### Column type coercion

JSON carries no type schema, so millpond infers a column's type from its values. When the values don't carry enough type information the inference diverges from the destination DuckLake column, and DuckLake's widening-only schema evolution then rejects the narrowing `ALTER` every flush (the insert stalls under DuckLake). `MILLPOND_TYPED_COLUMNS` pins named columns to a target type *before* the write, so the batch type matches the destination — the insert is a typed append with no DDL, and freshly-created tables get the right type from the start.

Format is comma-separated `column:type` pairs; supported types are `timestamptz`, `bigint`, `double`, `boolean`, `varchar`. For re-pointing a consumer at the duckling backfill's `posthog.events` (its `events` table has 8 `TIMESTAMPTZ` columns and `project_id BIGINT`):

```
MILLPOND_TYPED_COLUMNS=timestamp:timestamptz,created_at:timestamptz,person_created_at:timestamptz,group0_created_at:timestamptz,group1_created_at:timestamptz,group2_created_at:timestamptz,group3_created_at:timestamptz,group4_created_at:timestamptz,project_id:bigint
```

Why those columns: date-times arrive as strings, so inference types them `VARCHAR` against a `TIMESTAMPTZ` column; and `project_id` is the one numeric column the producer serializes as explicit JSON `null` (no `skip_serializing_if`), so an all-null batch infers `VARCHAR` against `BIGINT`. Pinning both makes the re-point fully clean. (`person_mode`/`historical_migration` need no pin — they serialize as a string and an omitted-or-bool respectively, matching the table.)

The timestamp wire format is space-separated, UTC implied, with 0, 3, or 6 fractional digits depending on column/producer (e.g. `2024-01-01 12:00:00.123`); all parse. Each cleanly-coerced column increments `millpond_columns_coerced_total{target_type=...}`. Coercion is **non-fatal and type-consistent**: a present, configured column is always emitted as the target type — values that can't be cast (a producer format/type drift) are **nulled** (only the unconvertible ones; good values in the batch are kept), never left as the source type. That keeps buffered batches concat-compatible at flush and bumps `millpond_errors_total{type="column_coercion"}` so the drift is loud via metrics, without crashing the pod or risking offset commits past unwritten records. Columns absent from a batch or already the target type are left untouched, so the same map is safe across heterogeneous batches.

### VARIANT dual-write (JSON properties → shredded VARIANT)

JSON property blobs (e.g. PostHog `properties`) land as VARCHAR today. `MILLPOND_VARIANT_COLUMNS` dual-writes listed source columns into companion VARIANT columns so DuckDB can auto-shred common sub-fields into typed Parquet columns, without dropping the original string:

```
MILLPOND_VARIANT_COLUMNS=properties,person_properties
```

For each listed source present in a batch, millpond:

1. Keeps the original column as-is (VARCHAR JSON text)
2. `ADD COLUMN IF NOT EXISTS {name}_variant VARIANT` on the DuckLake table
3. Projects `try_cast(try_cast(col AS JSON) AS VARIANT) AS {name}_variant` on INSERT

Malformed JSON nulls only the VARIANT companion (the string column still lands). DuckDB shreds VARIANT on Parquet write automatically — no millpond-side shredding config. Existing tables get the companion column via schema evolution on the first dual-write flush; historical rows keep a NULL companion until rewritten.

Degrades without crash-looping when dual-write cannot run cleanly:

- Payload fields named `{name}_variant` (any casing — DuckDB identifiers are case-insensitive) are stripped non-fatally (`variant_companion_columns_dropped_total`; records still land minus the field) so a poison key cannot evolve a VARCHAR companion or bind-conflict the INSERT. Writers also strip any payload field whose *live* table column is VARIANT, so a pod whose `MILLPOND_VARIANT_COLUMNS` is unset or stale (mixed fleet) cannot corrupt a companion via the implicit VARCHAR→VARIANT cast. A batch left with zero columns by the strip is skipped whole (`records_skipped_total{reason="variant_companion_collision"}` — those records *are* lost and excluded from `records_written_total`) instead of crash-looping the partition.
- If `{name}_variant` already exists as a non-VARIANT type, or ADD COLUMN fails, that source is omitted from the VARIANT projection (string column still writes); `errors_total{type="schema"}` is bumped. DuckLake cannot `ALTER VARCHAR → VARIANT`.
- If the dual-write INSERT itself fails, the flush is retried string-only and the companion is NULL for that batch (`variant_write_fallback_total`, `errors_total{type="variant_write"}`); if the retry fails too the error propagates, since the projection wasn't the cause. This exists because some values pass `try_cast(… AS VARIANT)` but break DuckDB's *shredded Parquet write* — a JSON integer above `INT64_MAX` arrives as UINT64 and overflows the shredded INT64 column, which crash-looped every prod NRT consumer on 2026-08-12. Values are not rewritten: the string column is authoritative, and a nonzero counter means some rows lack VARIANT data.

This is an opt-in migration step: readers can move from `json_extract(properties, …)` to `properties_variant."$browser"` (etc.) once the companion is populated, then a later cutover can drop the string column if desired. Dual-write (new column) is the supported path for that reason.

**Production caveats (canary first):**

- **Key cardinality / shredding.** DuckDB auto-shreds VARIANT from the structure it sees at Parquet write time. PostHog-scale `properties` have a long tail of custom keys; a flush can produce very wide Parquet schemas (hundreds–thousands of shredded leaf fields). Prefer canarying on a filtered consumer or lower-cardinality table before enabling fleet-wide on `events`.
- **Memory.** Dual-write keeps the VARCHAR column and materializes VARIANT at INSERT — peak flush memory is higher than string-only. Leave headroom vs `FLUSH_SIZE` and the pod limit when turning this on for large property blobs.
- **Test coverage.** Unit/integration dual-write tests exercise the SQL cast and companion DDL against plain DuckDB; they do not exercise DuckLake catalog DDL, Parquet shredding, or data-inlining edge cases. Validate shredding and file shape on a real DuckLake canary before relying on query performance.

## Adaptive Backpressure

The consume batch size automatically scales based on how full the pending buffer is relative to the flush threshold. When the buffer is empty, millpond consumes at full speed. As the buffer approaches the flush size, the batch size drops proportionally, smoothing throughput during catchup and traffic spikes. OOM prevention comes from bounding librdkafka's internal fetch buffer via `queued.max.messages.kbytes` (16MB per partition).

```
fullness = pending_bytes / flush_size
batch_size = max(10, int(CONSUME_BATCH_SIZE * (1.0 - fullness)))
```

Metrics: `millpond_buffer_fullness` and `millpond_consume_batch_size_current`.

## Performance

The hot path is all C/C++: librdkafka → orjson → PyArrow → DuckDB (zero-copy Arrow scan). Python is glue.

## Resource Footprint

| | Kafka Connect worker | Millpond pod |
|-|---------------------|-----------|
| Memory request | 4-8Gi (JVM heap) | 256Mi |
| Memory limit | 8-16Gi | 512Mi |
| Steady-state | ~4GB (JVM + framework + GC headroom) | ~250-300MB |

No JVM, no framework, no GC heap overhead. ~16x less memory per pod. The entire runtime is C/C++ libraries with a Python glue layer.

## Setup

Requires [Flox](https://flox.dev):

```bash
flox activate
just sync
just run
```

## Development

```bash
just fmt               # format code
just lint              # lint code
just test              # run unit tests
just test-integration  # run integration tests (in-memory DuckDB — fast, no docker stack)
just test-e2e          # run E2E tests (docker-compose, builds stack automatically)
just ci                # format check + lint + unit tests
just up                # start docker-compose stack (DuckLake — plaintext Kafka)
just up-ssl            # start docker-compose stack (DuckLake — SSL Kafka, closer to prod)
just down              # stop docker-compose stack
just down-ssl          # stop SSL docker-compose stack
```

### SSL Kafka Testing

The `just up-ssl` recipe generates self-signed certs and runs Kafka with SSL listeners, matching the production MSK configuration. This exercises the `KAFKA_CONSUMER_*` env var override path that isn't tested with plaintext Kafka.

Requires Docker (uses `keytool` from the Kafka container image for cert generation).

### DuckLake Maintenance and state metrics

The `tools/` directory ships two DuckLake-only operational binaries inside the same image as the writer:

- **`tools/ducklake_maintenance.py`** — CLI for snapshot expiry, file cleanup, orphan recovery, tiered compaction, fsck. Runs as a K8s CronJob.
- **`tools/ducklake_metrics.py`** — Long-running Prometheus-exposition daemon for catalog-side lake-state metrics. Runs as a single-replica Deployment.

Subcommand and YAML schema reference, full env-var contract, and the `just` recipe inventory live in [`tools/README.md`](tools/README.md). Both binaries reuse the writer's `DUCKLAKE_RDS_*` / `DUCKDB_S3_*` / `DUCKLAKE_DATA_PATH` env vars.

## Configuration

All configuration via environment variables.

### Core (always required)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka broker addresses |
| `KAFKA_TOPIC` | yes | | Topic to consume |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas (must match `spec.replicas`) |
| `MILLPOND_DESTINATION` | no | `ducklake` | Destination — `ducklake` is the only accepted value; anything else raises at startup. Case-insensitive; empty/whitespace falls back to `ducklake`. |
| `FLUSH_SIZE` | no | `104857600` | Flush after this many bytes of accumulated Arrow data (default 100MB) |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `GROUP_ID` | no | `millpond-{topic}-{ducklake_table}` | Kafka group.id — used for offset storage in `__consumer_offsets` only, no consumer group semantics. Changing this loses committed offsets and triggers full replay. |
| `CONSUME_BATCH_SIZE` | no | `1000` | Max messages per `consume()` call — amortizes Python↔C boundary cost |
| `FETCH_MIN_BYTES` | no | `1048576` | Broker accumulates at least this many bytes before responding (1MB) |
| `FETCH_MAX_WAIT_MS` | no | `500` | Max broker wait when `fetch.min.bytes` not yet satisfied |
| `STATS_INTERVAL_MS` | no | `5000` | librdkafka internal stats emission interval (0 to disable) |
| `LOG_LEVEL` | no | `INFO` | Python log level (DEBUG, INFO, WARNING, ERROR) |

### DuckLake

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DUCKLAKE_TABLE` | yes | | Target DuckLake table name |
| `DUCKLAKE_DATA_PATH` | yes | | S3 path for DuckLake data files |
| `DUCKLAKE_CONNECTION` | yes | | DuckDB connection string |
| `DUCKLAKE_RDS_HOST` | yes | | Postgres host for DuckLake metadata |
| `DUCKLAKE_RDS_PORT` | no | `5432` | Postgres port |
| `DUCKLAKE_RDS_DATABASE` | no | `ducklake` | Postgres database name |
| `DUCKLAKE_RDS_USERNAME` | no | `ducklake` | Postgres username |
| `DUCKLAKE_RDS_PASSWORD` | yes | | Postgres password |
| `DUCKLAKE_PARTITION_BY` | no | | Hive-style partition expression (e.g. `year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)`). Applied via `ALTER TABLE SET PARTITIONED BY` on first write. |
| `DUCKDB_S3_ACCESS_KEY_ID` | yes | | Static S3 access key for DuckDB |
| `DUCKDB_S3_SECRET_ACCESS_KEY` | yes | | Static S3 secret for DuckDB |
| `DUCKDB_S3_REGION` | no | | S3 region |
| `DUCKDB_S3_ENDPOINT` | no | | S3 endpoint override (MinIO, etc.) |
| `DUCKDB_S3_USE_SSL` | no | | `true` / `false` |
| `DUCKDB_S3_URL_STYLE` | no | | `vhost` / `path` |

### Optional record handling

See [Record Handling](#record-handling) for context. All four variables below are optional; unset means the corresponding stage is disabled.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MILLPOND_FILTER_KEEP_FIELD_NAME` | no | | Column name to check against the allowlist. Must be set with `MILLPOND_FILTER_VALUES`. Validated as a safe identifier. |
| `MILLPOND_FILTER_DROP_FIELD_NAME` | no | | Reserved for a future denylist filter; setting it today raises at startup. Mutually exclusive with `MILLPOND_FILTER_KEEP_FIELD_NAME`. |
| `MILLPOND_FILTER_VALUES` | no | | Comma-separated allowed values. Auto-detected as int if every token parses as an integer, string otherwise. Required when either filter field name is set. |
| `MILLPOND_INCLUDE_VALUES_URL` | no | | HTTP endpoint returning a JSON array of allowlist values (see [Dynamic allowlist source](#dynamic-allowlist-source)). Requires the keep-filter to be configured. |
| `MILLPOND_INCLUDE_VALUES_MODE` | no | `shadow` | `shadow` (static authoritative, endpoint observed for diff metrics) or `authoritative` (polled set live). Only valid with the URL set. |
| `MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S` | no | `60` | Poll cadence, jittered ±10%. |
| `MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS` | no | `5` | Consecutive successful polls a value must be absent before removal. `1` disables damping (warned at startup). |
| `MILLPOND_INCLUDE_VALUES_REQUEST_TIMEOUT_S` | no | `10` | Per-request HTTP timeout. |
| `MILLPOND_INCLUDE_VALUES_STARTUP_TIMEOUT_S` | no | `60` | Authoritative mode: how long startup blocks for the first successful poll before failing the pod. |
| `MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME` | no | | Header name sent with each poll (e.g. an internal-secret header). Must be set together with the token. Redirects are refused so the header can't leak cross-host. |
| `MILLPOND_INCLUDE_VALUES_AUTH_TOKEN` | no | | Header value. Must be set together with the header name. |
| `MILLPOND_SORT_BY` | no | | Comma-separated column names; the batch is sorted ascending by these in tuple order before each write. Missing fields cause the sort to be skipped (records still flow). |
| `MILLPOND_TYPED_COLUMNS` | no | | Comma-separated `column:type` pairs pinning columns to a target type before write (types: `timestamptz`, `bigint`, `double`, `boolean`, `varchar`). Needed when writing into a table whose columns are already typed and JSON inference would diverge (date-times → `VARCHAR` vs `TIMESTAMPTZ`; all-null `project_id` → `VARCHAR` vs `BIGINT`). Column names validated as safe identifiers; types validated against the allowlist. |
| `MILLPOND_VARIANT_COLUMNS` | no | | Comma-separated source column names to dual-write as DuckLake `VARIANT` companions (`properties` → `properties_variant`). Original string columns are kept. Malformed JSON nulls only the VARIANT side. Column names validated as safe identifiers; names ending in `_variant` are rejected (list the source, not the derived column). |

## Releases

Every merge to `main` automatically:
1. Bumps the patch version (`v0.0.1` → `v0.0.2`)
2. Builds and pushes a Docker image to `ghcr.io/posthog/millpond:<tag>`
3. Creates a GitHub release with changelog

Images: `ghcr.io/posthog/millpond:v0.0.X` or `ghcr.io/posthog/millpond:latest`

## Deployment

```bash
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/pdb.yaml
kubectl apply -f k8s/statefulset.yaml
```

Partition count is discovered at startup via `admin.list_topics(topic=cfg.topic, timeout=30)` (an `AdminClient`, not the consumer instance). Each pod computes its partition assignment from its ordinal:

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

### Updating

Rolling updates are a poor fit — pods with different `REPLICA_COUNT` values cause double-assignment or gaps. Since Kafka is the durable buffer:

1. **Canary**: Deploy one pod with the new version, verify metrics
2. **Graceful shutdown**: Scale to 0 (pods flush and commit)
3. **Full redeploy**: Update image/config, scale back up from committed offsets

Downtime = drain time + startup time (~2-3 min). Kafka buffers trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically.

## Partitioning

Set `DUCKLAKE_PARTITION_BY` to enable Hive-style partitioning on S3. Files are written into `key=value/` directories (e.g. `year=2026/month=3/day=23/hour=21/*.parquet`), enabling S3 prefix filtering, bulk lifecycle rules, and partition discovery by external tools.

```bash
DUCKLAKE_PARTITION_BY="year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)"
```

Partition on `_inserted_at` (always a real TIMESTAMP), not source `timestamp` fields (typically VARCHAR). Applied via `ALTER TABLE SET PARTITIONED BY` on first write — idempotent, safe for multiple pods and restarts. If added to an existing unpartitioned table, new files get HSP layout while old files remain flat; DuckLake queries both transparently via metadata.

## Object Sizing

S3 throughput scales with object size — small objects (<1MB) waste per-request overhead, while larger objects (128MB+) maximize GET/PUT throughput. Millpond flushes are triggered by whichever comes first: `FLUSH_SIZE` (Arrow bytes in memory) or `FLUSH_INTERVAL_MS` (wall clock). The resulting Parquet file is typically **3-4x smaller** than the Arrow representation due to columnar encoding and compression.

At steady state with moderate volume, most flushes are **time-triggered** — the interval expires before the size ceiling is hit. Object size is therefore driven by: `(msgs/s per pod) × (bytes/msg as Parquet) × (flush interval)`.

### Sizing by volume

Assuming ~366 bytes/row in Parquet (7-column event schema), 512 partitions, 8 replicas (64 partitions/pod):

| Per-partition msg/s | Total msg/s | Per-pod msg/s | Parquet/file @60s | Parquet/file @90s | Memory/pod @90s |
|---|---|---|---|---|---|
| 500 | 256K | 32K | ~11MB | ~17MB | 512Mi |
| 1K | 512K | 64K | ~23MB | ~34MB | 512Mi |
| 2K | 1M | 128K | ~45MB | ~68MB | 512Mi |
| 4K | 2M | 256K | ~90MB | ~135MB | 640Mi |
| 9.5K (peak) | 4.9M | 608K | ~213MB | ~320MB | 1Gi |

### Recommended settings for ~128MB target objects

For a pipeline averaging 4K msg/s per partition with 512 partitions and 8 replicas:

```yaml
FLUSH_SIZE: "1073741824"       # 1GB Arrow ceiling (safety valve for burst/catchup)
FLUSH_INTERVAL_MS: "90000"     # 90s — produces ~135MB Parquet at mean volume
```

Memory limit: 640Mi (90s × 256K msg/s × ~1KB Arrow/msg ≈ ~230MB Arrow + DuckDB + librdkafka overhead).

At peak (9.5K/partition), the size trigger fires at ~35s producing ~320MB objects — acceptable, and the pod stays within 1Gi.

### When to add a merge job

If your volume is low enough that time-triggered flushes produce <10MB objects, run periodic compaction. The `ducklake_maintenance.py compact` subcommand implements a tiered strategy: small files merge frequently into medium files, medium files merge less often into large files. Tier ranges, `target_file_size` save/restore semantics, and the `--threads` / `--memory-limit` knobs are documented in [`tools/README.md`](tools/README.md). This is an out-of-band maintenance operation, not part of the hot path.

See the [sizing calculator](https://posthog.github.io/millpond/sizing-calculator.html) for interactive estimates.

## Error Handling and Retries

The flush path has two failure points, each with its own retry policy:

| Operation | Attempts | Backoff between failures | On exhaustion |
|-----------|----------|--------------------------|---------------|
| Lake write | 3 | 1s, 2s (last attempt raises immediately) | Re-raise → pod crashes, K8s restarts, replays from last committed offset |
| Offset commit | 3 | 0.5s, 1s (last attempt raises immediately) | Re-raise → pod crashes, replays from last committed offset (duplicates bounded by one flush batch) |

Both use `errors_total{type="write_retry"}` and `errors_total{type="offset_commit"}` counters so transient vs persistent failures are distinguishable in dashboards.

The write-retry loop catches `Exception` broadly to cover the backend's failure modes — `duckdb.Error` for DuckLake; `OSError` for S3; `KafkaException` for broker disconnects. Each retry invokes `sink.reset_caches()` to drop cached table/schema state so the next attempt re-checks the catalog (covers the case where another pod evolved the schema or recreated the table between attempts).

**Why crash after exhausting retries?** A persistent write failure means S3 or the catalog is down — continuing would just accumulate pending data in memory until OOM. A persistent commit failure means the Kafka coordinator is unreachable — the write already succeeded, but without committed offsets the next restart will replay the batch (at-least-once duplicates). In both cases, crashing lets K8s apply its restart backoff, and Kafka holds the data safely until the dependency recovers.

## Multiple Pipelines

Each topic→table mapping is a separate StatefulSet. The application doesn't change — just the env vars. Template with Helm:

```yaml
# values.yaml
pipelines:
  events:
    topic: clickhouse_events_json
    table: events
    partitions: 512
    replicas: 8
  sessions:
    topic: clickhouse_sessions_json
    table: sessions
    partitions: 64
    replicas: 4
  logs:
    topic: app_logs
    table: logs
    partitions: 128
    replicas: 8
```

One `range` over `pipelines` in the StatefulSet template produces N independent StatefulSets. Adding a pipeline is adding a block to `values.yaml` and running `helm upgrade`.

## AWS Credential Isolation

Millpond uses two separate AWS credential paths that must not interfere with each other:

| Component | Auth | Credential source |
|---|---|---|
| Kafka (MSK) | SASL/OAUTHBEARER | IRSA (standard AWS credential chain) |
| S3 (lake data files) | Static IAM keys | `DUCKDB_S3_*` |

The S3 path does not use the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars — those take precedence in the credential chain and would shadow the IRSA role used for Kafka authentication. Do not rename the `DUCKDB_S3_*` env vars to the standard AWS names.

DuckDB's [aws extension does not support IRSA](https://github.com/duckdb/duckdb-aws/issues/31) — it cannot perform the `AssumeRoleWithWebIdentity` token exchange that IRSA requires, hence the static keys.

## Operational Notes

### Periodic MSK IAM auth errors

When using MSK IAM authentication (SASL/OAUTHBEARER), you will see periodic bursts of `connection reset by peer` and `SASL OAUTHBEARER mechanism handshake failed` errors in the logs every ~48 minutes. These are **expected and harmless**.

librdkafka does not re-authenticate on existing connections when the OAUTHBEARER token refreshes ([KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255%3A+OAuth+Authentication+via+SASL%2FOAUTHBEARER)). Instead, the MSK broker closes the connection when the old token expires (~15 min lifetime), and librdkafka reconnects with the refreshed token. The ~48 minute interval corresponds to the IRSA projected token refresh (80% of the default 1-hour TTL).

The errors come from librdkafka's internal logger (the `%3|...|FAIL|` lines) and bypass Python's log formatting. They auto-resolve within seconds with no data loss.

Related issues:
- [confluent-kafka-python #1485](https://github.com/confluentinc/confluent-kafka-python/issues/1485) — oauth token not refreshing on existing connections
- [aws-msk-iam-auth #143](https://github.com/aws/aws-msk-iam-auth/issues/143) — re-authentication fails with OAUTHBEARER
- [aws-msk-iam-auth #176](https://github.com/aws/aws-msk-iam-auth/issues/176) — second re-authentication fails with default credentials

## Note
This project should absolutely be called TableFowl, but that would be an [SEO](https://www.confluent.io/product/tableflow/) and linguistic palaver.

---

Photo: Public Domain, [Wikimedia Commons](https://commons.wikimedia.org/w/index.php?curid=695982)
