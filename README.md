# Millpond — Kafka to DuckLake or Iceberg

A standalone Python app that consumes from a Kafka topic and writes to a lake table. Single thread, single loop, no Kafka Connect. One deployment writes to exactly one destination — either [DuckLake](https://github.com/duckdb/ducklake) or [Apache Iceberg](https://iceberg.apache.org/), selected via `MILLPOND_DESTINATION`.

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
       └─ Single loop: consume → convert → accumulate → flush → commit
```

- One topic and one table per deployment
- Static partition assignment via pod ordinal — no consumer groups
- If a pod dies, its partitions stop being consumed until K8s restarts it

## Destinations

Millpond writes to one of two lake formats, selected at startup by `MILLPOND_DESTINATION` (default `ducklake`). A single deployment writes to exactly one — there is no per-batch routing. Switching destinations requires redeploying with different env vars; the at-rest data is not portable between the two without a separate migration.

|  | DuckLake | Iceberg |
|---|---|---|
| Catalog | Postgres (via DuckDB ducklake extension) | REST catalog (e.g. Polaris, Tabular, AWS Glue REST adapter) |
| Storage | S3 / S3-compatible | S3 / S3-compatible |
| Reader ecosystem | DuckDB-native; growing third-party support | Broad (Spark, Trino, Athena, Snowflake, DuckDB ≥1.5) |
| Partitioning | Caller-supplied via `DUCKLAKE_PARTITION_BY`; arbitrary DDL expression | Hardcoded identity transforms on derived `year`/`month`/`day`/`hour` int32 columns; Hive-style layout |
| Schema evolution | DuckDB DDL (`ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN SET DATA TYPE` with widening enforcement) | PyIceberg `update_schema()` transaction (single commit per flush) |
| Maintenance tooling | Bundled (`tools/ducklake_maintenance.py` CronJob, `tools/ducklake_metrics.py` daemon) | Not bundled — use your catalog's native compaction/expiry |
| `_inserted_at` column | Added at INSERT via DuckDB `NOW()` (per-row, microsecond drift possible within a flush) | Added at write time in Python (single timestamp shared by every row in a flush) |
| Multi-pod concurrent writes | Native; idempotent DDL handles races | Native; PyIceberg optimistic concurrency + retry loop handles races |

The selection is a thin Protocol-based abstraction (`millpond/sink.py`) — `main.py` only sees `Sink.write(batch)`, `reset_caches()`, `close()`. Both implementations are in their own module (`ducklake.py`, `iceberg.py`).

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
just test              # run unit tests (includes both backends' suites + cross-backend equivalence)
just test-integration  # run integration tests (local DuckDB + MinIO/iceberg-rest via testcontainers)
just test-e2e          # run E2E tests (docker-compose, builds stack automatically)
just ci                # format check + lint + unit tests
just up                # start docker-compose stack (DuckLake — plaintext Kafka)
just up-ssl            # start docker-compose stack (DuckLake — SSL Kafka, closer to prod)
just down              # stop docker-compose stack
just down-ssl          # stop SSL docker-compose stack
```

The `just up` / `just up-ssl` dev stacks are DuckLake-only. For Iceberg local dev, the integration test fixture in `tests/integration/compose.yaml` brings up MinIO + a tabulario/iceberg-rest catalog; that stack is what the iceberg integration tests use and what to point at for ad-hoc Iceberg work.

### SSL Kafka Testing

The `just up-ssl` recipe generates self-signed certs and runs Kafka with SSL listeners, matching the production MSK configuration. This exercises the `KAFKA_CONSUMER_*` env var override path that isn't tested with plaintext Kafka.

Requires Docker (uses `keytool` from the Kafka container image for cert generation).

### DuckLake Maintenance

`tools/ducklake_maintenance.py` is a self-contained Python script for DuckLake maintenance operations (snapshot expiry, file cleanup, orphan deletion, checkpoint, tiered compaction, deletion-queue dedup, catalog-side orphan recovery). It is baked into the Docker image at `/app/tools/ducklake_maintenance.py` and designed to run as a K8s CronJob reusing the same image and credentials as the main application.

```bash
python /app/tools/ducklake_maintenance.py maintain --days 7           # expire snapshots + cleanup files
python /app/tools/ducklake_maintenance.py maintain --days 7 --dry-run # preview only
python /app/tools/ducklake_maintenance.py expire --days 3             # expire snapshots only
python /app/tools/ducklake_maintenance.py cleanup --days 1            # cleanup scheduled files only
python /app/tools/ducklake_maintenance.py cleanup-all                 # cleanup all scheduled files regardless of age
python /app/tools/ducklake_maintenance.py dedup-deletions             # drop duplicate rows in the pending-deletion queue
python /app/tools/ducklake_maintenance.py find-orphans                # list catalog rows whose S3 key no longer exists
python /app/tools/ducklake_maintenance.py heal-orphans                # delete those catalog rows (gated B1/B3 safety checks)
python /app/tools/ducklake_maintenance.py cleanup-all-safe            # dedup + heal-orphans + cleanup-all in a loop until clean
python /app/tools/ducklake_maintenance.py fsck                        # cleanup-all-safe + ducklake_delete_orphaned_files
python /app/tools/ducklake_maintenance.py checkpoint                  # integrated merge + expire + cleanup
python /app/tools/ducklake_maintenance.py orphans                     # delete S3-side orphaned files (catalog has no row)
python /app/tools/ducklake_maintenance.py compact --tier 1            # tiered compaction (see "When to add a merge job")
```

The script logs `cleanup throughput: files_processed=N elapsed_s=T rate_obj_s=R queue_depth_after=A` after every `cleanup` / `cleanup-all` (skipped on `--dry-run`), so you can confirm steady-state throughput without enabling debug logging. `files_processed` is the actual count of files the call returned, not a queue-depth delta, so the number is accurate even when other writers enqueue deletions during the run. Pass `--debug` to opt back into DuckDB's HTTP and Postgres-extension query logging — both are off by default because they add per-call overhead that compounds across tens of thousands of S3 deletes.

If `PUSHGATEWAY_URL` is set, the script pushes `maintenance_start_time` (on start) and `maintenance_duration_seconds` (on completion) to a Prometheus Pushgateway, enabling Grafana annotation queries for maintenance windows.

#### Catalog-side orphan recovery

If a `cleanup-all` run is interrupted (DuckLake bug: an S3 NoSuchKey on DELETE rolls back the whole transaction, but the S3 deletes already-completed are permanent), the catalog ends up with rows in `ducklake_files_scheduled_for_deletion` that point at S3 keys that no longer exist. Every subsequent `cleanup-all` will crash on those orphans until they're cleaned up. The catalog-recovery subcommands handle this without manual SQL surgery:

| Subcommand | Action |
|---|---|
| `find-orphans` | List orphan rows on stdout (read-only). |
| `heal-orphans` | Delete the orphan rows. Two safety gates: B1 proves `ducklake_data_file` is non-empty AND no orphan path is still live; B3 aborts if any positional-delete vector references an orphan id. `--dry-run` runs the gates but skips the DELETE. |
| `cleanup-all-safe` | Loop dedup-deletions + heal-orphans + cleanup-all under one advisory lock until cleanup-all exits clean. Caps at `--max-iterations` (default 10). |
| `fsck` | `cleanup-all-safe` followed by `ducklake_delete_orphaned_files` (S3-side orphan sweep). The end-to-end "lake catalog is healthy" recipe. |

Mutual exclusion comes from `pg_try_advisory_lock(hashtext('millpond-ducklake-maintenance')::bigint)` taken on the `pg` ATTACH; concurrent maintenance invocations bail with a clear error rather than racing each other's DELETEs.

`tools/ducklake_maintenance.sql` is loaded at every session start (both by `ducklake_maintenance.py` and by the `just shell` recipe) and defines small DuckDB macros for ad-hoc inspection — `SELECT count_pending_dups()` for queue dup count, `SELECT * FROM find_catalog_orphans('s3://bucket/lake/data')` for the orphan list. The header documents the conventions (no `LEFT ANTI JOIN`, no duckdb-side `ctid`, advisory-lock key) that any new recipe must follow.

`tools/justfile` wraps the script and is also baked into the image at `/justfile` for interactive use:

```bash
just --list                  # see available recipes
just maintain-dry-run 3      # preview: expire >3 day snapshots + cleanup
just maintain 3              # execute it
just dedup-deletions-dry-run # preview duplicate rows in the pending-deletion queue
just dedup-deletions         # drop them
just find-orphans            # list catalog-side orphan rows
just heal-orphans-dry-run    # preview heal-orphans (gates only, no DELETE)
just heal-orphans            # delete catalog-side orphan rows
just cleanup-all-safe        # dedup + heal + cleanup-all in a loop
just fsck-dry-run            # preview fsck end-to-end
just fsck                    # bring catalog to known-good state
just shell                   # interactive DuckDB shell with lake + pg ATTACHed and macros loaded
just drop events             # drop a table (data files remain until cleanup)
just orphans-dry-run         # preview S3-side orphaned files
```

All commands use the pod's existing env vars (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`, `DUCKLAKE_DATA_PATH`).

### DuckLake state metrics

`tools/ducklake_metrics.py` is a small long-running daemon that runs catalog-side queries against the DuckLake on a schedule and exposes results as Prometheus gauges over HTTP. Same Docker image as `ducklake_maintenance.py`; intended to run as a single-replica Deployment so a Prometheus scraper can watch lake shape, compaction backlog, snapshot age, partition skew, and the pending-deletion queue without S3 round trips.

```bash
just ducklake-metrics                       # built-ins only, listens on :9100
just ducklake-metrics-with-config queries.yaml   # extend built-ins from user YAML
just ducklake-metrics-list                  # print resolved query list and exit (no connection needed)
```

Endpoints: `/metrics` (Prometheus exposition), `/-/healthy` (k8s liveness), `/-/ready` (k8s readiness). The daemon reconnects to the catalog with exponential backoff (1s → 60s cap) on connect failure, and forces a reconnect after 10 consecutive query failures across all queries; transient SQL errors in a single query log + increment `ducklake_metrics_query_errors_total` without killing the process.

Built-in queries:

| Metric prefix | Labels | Values | Source |
|---|---|---|---|
| `ducklake_pending_deletes` | — | `total`, `unique_paths`, `dup_rows` | `ducklake_files_scheduled_for_deletion` |
| `ducklake_files_per_band` | `band` (`lt1mib` / `1to5mib` / `5to10mib` / `10to32mib` / `32to64mib` / `64to128mib` / `gt128mib`) | `count`, `bytes` | `ducklake_data_file` |
| `ducklake_compaction_candidates` | `tier` (`tier1` / `tier2` / `tier3` / `large` / `total`) | `count` | `ducklake_data_file` |
| `ducklake_snapshots` | — | `count`, `oldest_seconds_ago`, `newest_seconds_ago` | `ducklake_snapshot` |
| `ducklake_files_per_partition_top20` | `partition` | `count` | `ducklake_data_file` ⨝ `ducklake_file_partition_value` |
| `ducklake_catalog` | `suffix` | `format_version` | `ducklake_metadata` (key=`version`); numeric `major.minor` lands in the value, any trailing tag (e.g. `-dev1`, `-rc7`) lands in the `suffix` label so dev/pre-release builds stay distinguishable. Empty `suffix=""` for clean releases |

Plus self-metrics: `ducklake_metrics_up`, `ducklake_metrics_query_duration_seconds{query}`, `ducklake_metrics_query_last_success_timestamp{query}`, `ducklake_metrics_query_errors_total{query}`.

User YAML schema (extends or overrides built-ins by name):

```yaml
queries:
  - name: events_files_per_table
    help: Live data file count by table (custom example)
    interval_mins: 5            # positive integer; minimum 1
    labels: [table_name]
    values: [count]
    sql: |
      SELECT t.table_name, COUNT(*) AS count
      FROM __ducklake_metadata_lake.ducklake_data_file df
      JOIN __ducklake_metadata_lake.ducklake_table t USING (table_id)
      WHERE df.end_snapshot IS NULL
      GROUP BY t.table_name
```

Built-ins are intentionally lake-wide (no `table_name` label); per-table breakdowns belong in user YAML when needed.

Configuration env vars (in addition to the standard `DUCKLAKE_*` / `DUCKDB_*` set used by `ducklake_maintenance.py`):

| Variable | Default | Description |
|---|---|---|
| `DUCKLAKE_METRICS_PORT` | `9100` | HTTP listen port |
| `DUCKLAKE_METRICS_CONFIG` | unset | Path to user-supplied queries YAML |
| `DUCKLAKE_METRICS_DISABLE` | unset | Comma-separated query names to skip from built-ins |

## Configuration

All configuration via environment variables.

### Shared (always required)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka broker addresses |
| `KAFKA_TOPIC` | yes | | Topic to consume |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas (must match `spec.replicas`) |
| `MILLPOND_DESTINATION` | no | `ducklake` | Destination format: `ducklake` or `iceberg`. Case-insensitive; empty/whitespace falls back to `ducklake`. |
| `FLUSH_SIZE` | no | `104857600` | Flush after this many bytes of accumulated Arrow data (default 100MB) |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `GROUP_ID` | no | `millpond-{topic}-{table}` | Kafka group.id — used for offset storage in `__consumer_offsets` only, no consumer group semantics. Changing this loses committed offsets and triggers full replay. For Iceberg the default is `millpond-{topic}-{iceberg_table}` (the namespace prefix only shows up in metrics/client.id, not group.id). |
| `CONSUME_BATCH_SIZE` | no | `1000` | Max messages per `consume()` call — amortizes Python↔C boundary cost |
| `FETCH_MIN_BYTES` | no | `1048576` | Broker accumulates at least this many bytes before responding (1MB) |
| `FETCH_MAX_WAIT_MS` | no | `500` | Max broker wait when `fetch.min.bytes` not yet satisfied |
| `STATS_INTERVAL_MS` | no | `5000` | librdkafka internal stats emission interval (0 to disable) |
| `LOG_LEVEL` | no | `INFO` | Python log level (DEBUG, INFO, WARNING, ERROR) |

### DuckLake (required when `MILLPOND_DESTINATION=ducklake`)

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

### Iceberg (required when `MILLPOND_DESTINATION=iceberg`)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `ICEBERG_CATALOG_URI` | yes | | REST catalog endpoint (e.g. `https://catalog.example.com`) |
| `ICEBERG_WAREHOUSE` | yes | | Warehouse identifier, typically the S3 root (`s3://warehouse/`) |
| `ICEBERG_NAMESPACE` | yes | | Catalog namespace (validated as a safe identifier) |
| `ICEBERG_TABLE` | yes | | Target table name within the namespace |
| `ICEBERG_TABLE_LOCATION` | no | | Explicit `s3://...` table location; if unset, catalog picks |
| `ICEBERG_CATALOG_TOKEN` | no | | Bearer / OAuth token for the REST catalog |
| `MILLPOND_S3_ACCESS_KEY_ID` | yes | | Static S3 access key for PyIceberg's PyArrow S3 filesystem |
| `MILLPOND_S3_SECRET_ACCESS_KEY` | yes | | Static S3 secret |
| `MILLPOND_S3_REGION` | yes | | S3 region |
| `MILLPOND_S3_ENDPOINT` | no | | S3 endpoint override (MinIO, etc.) |

`MILLPOND_S3_*` is a separate env var family from `DUCKDB_S3_*` deliberately — they target different client libraries, and a deployment switch from DuckLake to Iceberg should be a clean swap of env vars rather than re-using the DuckDB-specific names.

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

Partition count is discovered at startup via `consumer.list_topics()`. Each pod computes its partition assignment from its ordinal:

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

Partitioning is per-destination — DuckLake takes a caller-supplied expression, Iceberg is hardcoded.

### DuckLake

Set `DUCKLAKE_PARTITION_BY` to enable Hive-style partitioning on S3. Files are written into `key=value/` directories (e.g. `year=2026/month=3/day=23/hour=21/*.parquet`), enabling S3 prefix filtering, bulk lifecycle rules, and partition discovery by external tools.

```bash
DUCKLAKE_PARTITION_BY="year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)"
```

Partition on `_inserted_at` (always a real TIMESTAMP), not source `timestamp` fields (typically VARCHAR). Applied via `ALTER TABLE SET PARTITIONED BY` on first write — idempotent, safe for multiple pods and restarts. If added to an existing unpartitioned table, new files get HSP layout while old files remain flat; DuckLake queries both transparently via metadata.

### Iceberg

The partition spec is hardcoded: identity transforms on four int32 columns (`year`, `month`, `day`, `hour`) derived from `_inserted_at` at write time. This produces the same Hive-style layout as DuckLake — `year=2026/month=3/day=23/hour=21/*.parquet` — for the same S3-prefix-filter and lifecycle reasons. There is no env var; every Iceberg deployment gets the same spec.

Trade-off: Iceberg doesn't know the four columns are derived from `_inserted_at`, so reader queries need to filter on the partition columns explicitly to get pruning. A future spec evolution can layer hidden partitioning on top without rewriting data if reader ergonomics start to matter; not needed today.

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

If your volume is low enough that time-triggered flushes produce <10MB objects, run periodic compaction. The `compact` subcommand implements a tiered strategy: small files merge frequently into medium files, medium files merge less often into large files. Each tier saves and restores the catalog's `target_file_size` so running one tier doesn't permanently change file sizing for inserts or other compactions.

```bash
just compact-to-tier-1-dry-run        # preview: files <1 MiB → ~5 MiB
just compact-to-tier-1                # execute (catalog-wide)
just compact-to-tier-2 events         # tier 1->2, scoped to one table
just compact-probe events 4           # diagnostic: merge up to 4 adjacent files in 'events'
```

Tier ranges (verified semantics: `min_file_size` inclusive, `max_file_size` exclusive):

| Recipe | Input range | Target |
|---|---|---|
| `compact-to-tier-1` | `[0, 1 MiB)` | ~5 MiB |
| `compact-to-tier-2` | `[1 MiB, 10 MiB)` | ~32 MiB |
| `compact-to-tier-3` | `[10 MiB, 64 MiB)` | ~128 MiB |

The `compact` subcommand bounds DuckDB resource use during the merge — `--threads` (default 2) and `--memory-limit` (default 4GB) — because `ducklake_merge_adjacent_files` isn't fully streaming today and over-uses memory relative to input size. The defaults are conservative; raise them on lakes that fit comfortably in pod memory.

This is an out-of-band maintenance operation, not part of the hot path.

See the [sizing calculator](https://posthog.github.io/millpond/sizing-calculator.html) for interactive estimates.

## Error Handling and Retries

The flush path has two failure points, each with its own retry policy:

| Operation | Attempts | Backoff between failures | On exhaustion |
|-----------|----------|--------------------------|---------------|
| Lake write | 3 | 1s, 2s (last attempt raises immediately) | Re-raise → pod crashes, K8s restarts, replays from last committed offset |
| Offset commit | 3 | 0.5s, 1s (last attempt raises immediately) | Re-raise → pod crashes, replays from last committed offset (duplicates bounded by one flush batch) |

Both use `errors_total{type="write_retry"}` and `errors_total{type="offset_commit"}` counters so transient vs persistent failures are distinguishable in dashboards.

The write-retry loop catches `Exception` broadly to cover both backends' failure modes — `duckdb.Error` for DuckLake; `pyiceberg.exceptions.CommitFailedException`, `CommitStateUnknownException`, `ServerError`, `ServiceUnavailableError` for Iceberg REST catalog 5xx; `OSError` for S3; `KafkaException` for broker disconnects. Each retry invokes `sink.reset_caches()` to drop cached table/schema state so the next attempt re-checks the catalog (covers the case where another pod evolved the schema or recreated the table between attempts).

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
| S3 (lake data files) | Static IAM keys | `DUCKDB_S3_*` (DuckLake) or `MILLPOND_S3_*` (Iceberg) |

Neither backend uses the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars — those take precedence in the credential chain and would shadow the IRSA role used for Kafka authentication. DuckDB-specific names for DuckLake, Millpond-specific names for Iceberg. The two families are deliberately separate so a deployment switch between destinations is a clean env-var swap rather than a re-use.

DuckDB's [aws extension does not support IRSA](https://github.com/duckdb/duckdb-aws/issues/31) — it cannot perform the `AssumeRoleWithWebIdentity` token exchange that IRSA requires. PyIceberg's S3 access is similarly handled via static credentials passed through catalog properties (`s3.access-key-id` etc.) to keep the IRSA token out of the S3 client's credential resolution. Same isolation pattern, different transport.

## Operational Notes

### Periodic MSK IAM auth errors

When using MSK IAM authentication (SASL/OAUTHBEARER), you will see periodic bursts of `connection reset by peer` and `SASL OAUTHBEARER mechanism handshake failed` errors in the logs every ~48 minutes. These are **expected and harmless**.

librdkafka does not re-authenticate on existing connections when the OAUTHBEARER token refreshes ([KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255%3A+OAuth+Authentication+via+SASL%2FOAUTHBEARER)). Instead, the MSK broker closes the connection when the old token expires (~15 min lifetime), and librdkafka reconnects with the refreshed token. The ~48 minute interval corresponds to the IRSA projected token refresh (80% of the default 1-hour TTL).

The errors come from librdkafka's internal logger (the `%3|...|FAIL|` lines) and bypass Python's log formatting. They auto-resolve within seconds with no data loss.

Related issues:
- [confluent-kafka-python #1485](https://github.com/confluentinc/confluent-kafka-python/issues/1485) — oauth token not refreshing on existing connections
- [aws-msk-iam-auth #143](https://github.com/aws/aws-msk-iam-auth/issues/143) — re-authentication fails with OAUTHBEARER
- [aws-msk-iam-auth #176](https://github.com/aws/aws-msk-iam-auth/issues/176) — second re-authentication fails with default credentials

## Next steps

### Iceberg multi-writer commit contention

The Iceberg sink can't currently sustain two pods committing to the same table at typical flush cadence. PyIceberg's REST commit attaches a branch-snapshot requirement (`expected id != actual id`); when a second writer commits between when we loaded the table and when we send the commit, the catalog rejects with `CommitFailedException: branch main has changed`. `_write_with_retry` in `main.py` invalidates caches and retries up to 3 times with exponential backoff (1s, 2s, 4s), but under sustained dual-writer load with `FLUSH_INTERVAL_MS=5000`, the retries collide with the *next* round of commits and exhaust the budget — the pod exits.

This is why `docker-compose.iceberg.yaml` runs a single millpond pod while `docker-compose.yaml` (DuckLake) runs two. The DuckLake path serialises writes through Postgres-backed catalog locks; Iceberg's optimistic concurrency control needs more retry headroom and jittered backoff to avoid retry-storms, or the writers need partition-aware table layouts so they aren't contending on the same snapshot.

Surfaced by the e2e suite when first wired up — see `tests/e2e/test_e2e_iceberg.py` and the comment in `docker-compose.iceberg.yaml`.

## Note
This project should absolutely be called TableFowl, but that would be an [SEO](https://www.confluent.io/product/tableflow/) and linguistic palaver.

---

Photo: Public Domain, [Wikimedia Commons](https://commons.wikimedia.org/w/index.php?curid=695982)
