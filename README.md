# Millpond — Kafka to DuckLake

A standalone Python app that consumes from a Kafka topic and writes to a DuckLake table. Single thread, single loop, no Kafka Connect.

## Naming

<img src="imgs/500px-Hagley_mill_race.jpeg" alt="A mill pond" width="300" align="right">

> **millpond** (noun): a pond created by damming a stream to produce a head of water for operating a mill.
> — [Merriam-Webster](https://www.merriam-webster.com/dictionary/millpond)

Millpond accumulates a stream of Kafka records until a threshold is reached, then releases them into the [DuckLake](https://github.com/duckdb/ducklake). Like a [mill pond](https://en.wikipedia.org/wiki/Mill_pond) feeding a lake.

## Why

Kafka Connect imposes ~1100 lines of lock management, scheduled executors, and rebalance handling to work around its lack of backpressure and explicit offset control. Millpond replaces all of that with:

```
loop:
  consume() → JSON → Arrow → accumulate
  when buffer full or time elapsed:
    write to DuckLake → commit offsets
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

## Record Filtering

Millpond can optionally filter records by a field value, keeping only records where a specified column matches a given string. Set `FILTER_FIELD` and `FILTER_VALUE` (both required together).

Filtered records are tracked via the `millpond_records_skipped_total{reason="filter"}` metric.

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
just test-integration  # run integration tests (local DuckDB)
just test-e2e          # run E2E tests (docker-compose, builds stack automatically)
just ci                # format check + lint + unit tests
just up                # start docker-compose stack (plaintext Kafka)
just up-ssl            # start docker-compose stack with SSL Kafka (closer to prod)
just down              # stop docker-compose stack
just down-ssl          # stop SSL docker-compose stack
```

### SSL Kafka Testing

The `just up-ssl` recipe generates self-signed certs and runs Kafka with SSL listeners, matching the production MSK configuration. This exercises the `KAFKA_CONSUMER_*` env var override path that isn't tested with plaintext Kafka.

Requires Docker (uses `keytool` from the Kafka container image for cert generation).

### DuckLake Maintenance

`tools/maintenance.py` is a self-contained Python script for DuckLake maintenance operations (snapshot expiry, file cleanup, orphan deletion, checkpoint). It is baked into the Docker image at `/app/tools/maintenance.py` and designed to run as a K8s CronJob reusing the same image and credentials as the main application.

```bash
python /app/tools/maintenance.py maintain --days 7          # expire snapshots + cleanup files
python /app/tools/maintenance.py maintain --days 7 --dry-run # preview only
python /app/tools/maintenance.py expire --days 3            # expire snapshots only
python /app/tools/maintenance.py cleanup --days 1           # cleanup scheduled files only
python /app/tools/maintenance.py checkpoint                 # integrated merge + expire + cleanup
python /app/tools/maintenance.py orphans                    # delete orphaned S3 files
```

If `PUSHGATEWAY_URL` is set, the script pushes `maintenance_start_time` (on start) and `maintenance_duration_seconds` (on completion) to a Prometheus Pushgateway, enabling Grafana annotation queries for maintenance windows.

`tools/justfile` wraps the script and is also baked into the image at `/justfile` for interactive use:

```bash
just --list              # see available recipes
just maintain-dry-run 3  # preview: expire >3 day snapshots + cleanup
just maintain 3          # execute it
just shell               # interactive DuckDB shell connected to DuckLake
just drop events         # drop a table (data files remain until cleanup)
just orphans-dry-run     # preview orphaned S3 files
```

All commands use the pod's existing env vars (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`, `DUCKLAKE_DATA_PATH`).

## Configuration

All configuration via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka broker addresses |
| `KAFKA_TOPIC` | yes | | Topic to consume |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas (must match `spec.replicas`) |
| `DUCKLAKE_TABLE` | yes | | Target DuckLake table name |
| `DUCKLAKE_DATA_PATH` | yes | | S3 path for DuckLake data files |
| `DUCKLAKE_CONNECTION` | yes | | DuckDB connection string |
| `DUCKLAKE_RDS_HOST` | yes | | Postgres host for DuckLake metadata |
| `DUCKLAKE_RDS_PORT` | no | `5432` | Postgres port |
| `DUCKLAKE_RDS_DATABASE` | no | `ducklake` | Postgres database name |
| `DUCKLAKE_RDS_USERNAME` | no | `ducklake` | Postgres username |
| `DUCKLAKE_RDS_PASSWORD` | yes | | Postgres password |
| `DUCKLAKE_PARTITION_BY` | no | | Hive-style partition expression (e.g. `year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)`). Applied via `ALTER TABLE SET PARTITIONED BY` on first write. |
| `FLUSH_SIZE` | no | `104857600` | Flush after this many bytes of accumulated Arrow data (default 100MB) |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `GROUP_ID` | no | `millpond-{topic}-{table}` | Kafka group.id — used for offset storage in `__consumer_offsets` only, no consumer group semantics. Changing this loses committed offsets and triggers full replay. |
| `CONSUME_BATCH_SIZE` | no | `1000` | Max messages per `consume()` call — amortizes Python↔C boundary cost |
| `FETCH_MIN_BYTES` | no | `1048576` | Broker accumulates at least this many bytes before responding (1MB) |
| `FETCH_MAX_WAIT_MS` | no | `500` | Max broker wait when `fetch.min.bytes` not yet satisfied |
| `STATS_INTERVAL_MS` | no | `5000` | librdkafka internal stats emission interval (0 to disable) |
| `LOG_LEVEL` | no | `INFO` | Python log level (DEBUG, INFO, WARNING, ERROR) |
| `FILTER_FIELD` | no | | Column name to filter on. Must be set with `FILTER_VALUE`. |
| `FILTER_VALUE` | no | | Value to match in `FILTER_FIELD`. Only records where `FILTER_FIELD == FILTER_VALUE` (string comparison) are written. All others are discarded after parsing. |

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

If your volume is low enough that time-triggered flushes produce <10MB objects, consider running `ducklake_merge_adjacent_files()` periodically to compact small files:

```sql
CALL ducklake_merge_adjacent_files('lake', 'events');
```

This is an out-of-band maintenance operation, not part of the hot path.

See the [sizing calculator](https://posthog.github.io/millpond/sizing-calculator.html) for interactive estimates.

## Error Handling and Retries

The flush path has two failure points, each with its own retry policy:

| Operation | Attempts | Backoff between failures | On exhaustion |
|-----------|----------|--------------------------|---------------|
| DuckLake write | 3 | 1s, 2s (last attempt raises immediately) | Re-raise → pod crashes, K8s restarts, replays from last committed offset |
| Offset commit | 3 | 0.5s, 1s (last attempt raises immediately) | Re-raise → pod crashes, replays from last committed offset (duplicates bounded by one flush batch) |

Both use `errors_total{type="write_retry"}` and `errors_total{type="offset_commit"}` counters so transient vs persistent failures are distinguishable in dashboards.

**Why crash after exhausting retries?** A persistent write failure means S3 or Postgres is down — continuing would just accumulate pending data in memory until OOM. A persistent commit failure means the Kafka coordinator is unreachable — the write already succeeded, but without committed offsets the next restart will replay the batch (at-least-once duplicates). In both cases, crashing lets K8s apply its restart backoff, and Kafka holds the data safely until the dependency recovers.

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

## Note
This project should absolutely be called TableFowl, but that would be an [SEO](https://www.confluent.io/product/tableflow/) and linguistic palaver.

---

Photo: Public Domain, [Wikimedia Commons](https://commons.wikimedia.org/w/index.php?curid=695982)
