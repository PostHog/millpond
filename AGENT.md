# Millpond — Dead Simple Kafka to DuckLake

## What This Is

A standalone Python app that replaces Kafka Connect for writing Kafka topic data to DuckLake. Single thread, single loop, no framework.

Replaces: [PostHog/ducklake-kafka-connect](https://github.com/PostHog/ducklake-kafka-connect) (~1100 lines of lock management, scheduled executors, two-lock protocols imposed by the Kafka Connect framework).

## Why Not Kafka Connect

Kafka is already a queue with buffering, backpressure, and offset management built in. The entire Kafka Connect connector exists to re-implement worse versions of these things because the framework won't let you use them directly.

Connect owns the consumer. The connector is a plugin that implements `SinkTask.put()`. Connect calls `put()` with records; if it returns, Connect considers them handled. This creates:

- **No backpressure**: Can't say "not ready." Blocking in `put()` triggers consumer eviction + rebalance.
- **No explicit offset control**: Connect commits after `put()` returns, not after successful write. Data loss window.
- **No consumer configuration**: Can't set `ConsumerRebalanceListener`, can't control poll timing, can't use `pause()`/`resume()`.
- **Rebalance hell**: `close(revoked)` / `open(assigned)` while scheduler thread may still be flushing from old assignment.

- **Metrics hell**: Connect defines and only supports its own `KafkaMetric` for individual connectors. Exposed via JMX, not Prometheus. Getting useful latency percentiles requires a JMX Exporter sidecar, custom relabeling rules, and significant mangling to produce anything actionable.

The result: the connector reimplements a blocking queue with ~1100 lines of ceremony.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       └─ Single loop: consume() → JSON→Arrow → accumulate → flush → commit
```

```python
while not shutdown:
    records = consumer.consume(num_messages=N, timeout=remaining_until_flush)
    if records:
        batch = convert_to_arrow(records)
        pending.append(batch)

    if should_flush(pending):
        write_to_ducklake(pending)
        consumer.commit(offsets, asynchronous=False)
        pending.clear()
```

- **No consumer groups.** Each pod computes its partitions from its StatefulSet ordinal and total partition count, uses `consumer.assign()`.
- **No threads, no queues.** Kafka is the queue. Backpressure is implicit: while flushing to DuckLake, the consumer simply doesn't call `consume()`. Kafka holds the data.
- **Offset commit** is explicit: only after successful DuckLake write.
- **If a pod dies**, its partitions stop being consumed until K8s restarts it. No rebalance.


## Key Design Decisions

### Language: Python

The hot path is all C/C++ (librdkafka, orjson, PyArrow, DuckDB). Python is glue — it touches each record once to pass a parsed dict into a list. Performance bottleneck is S3 write latency, not Python.

If Python ever shows up in profiles, the entire app ports to C with the same structure and similar line count — all libraries have C APIs.

#### Kafka Consumer Tuning

Two critical tuning knobs for confluent-kafka-python:

1. **Use `consume(num_messages=N)` batch API, not `poll()`**. Amortizes the Python↔C boundary crossing cost per call. The `consume()` [docstring](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.consume) explicitly notes it is more performant than calling `poll()` in a loop. Community benchmarks report significant throughput gains (see confluent-kafka-python issues [#291](https://github.com/confluentinc/confluent-kafka-python/issues/291), [#612](https://github.com/confluentinc/confluent-kafka-python/issues/612)).

2. **Set `fetch.min.bytes` to 1MB+** (default is 1 byte). This is the single biggest throughput lever — reduces fetch request count dramatically by letting the broker accumulate data before responding. Trade latency for throughput. Pair with `fetch.max.wait.ms=500`. See [Kafka consumer config docs](https://kafka.apache.org/documentation/#consumerconfigs_fetch.min.bytes) and [Confluent throughput optimization guide](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html).

### Static Partition Assignment

Pod ordinal from StatefulSet hostname (e.g. `millpond-events-3` → ordinal `3`).

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

**Partition count**: discovered at startup via `consumer.list_topics(topic)`. No env var — eliminates desync risk if partitions are added server-side.

**Replica count**: set via `REPLICA_COUNT` env var (matches `spec.replicas` in the StatefulSet). This is operator-controlled and can't be discovered reliably from inside the pod.

Alternative considered: both as env vars (`KAFKA_PARTITION_COUNT`, `REPLICA_COUNT`). Simpler but creates a desync risk for partition count, which can change server-side without the env var being updated. Replica count doesn't have this problem — it's always set by the operator via kubectl.

Scaling requires updating both `spec.replicas` and the `REPLICA_COUNT` env var.

**`auto.offset.reset=earliest`**: required. With `assign()`, if a partition has no committed offset (new partition, or `GROUP_ID` changed), the default `latest` silently drops all existing data. `earliest` replays from the beginning — safe for at-least-once.

**`group.id`**: defaults to `millpond-{topic}-{table}`. Used only for offset storage in `__consumer_offsets` (no consumer group semantics). Changing `group.id` loses all committed offsets and triggers a full replay from `earliest`.

### Flush Triggers

Both time-based and size-based:
- **Size**: accumulated Arrow bytes in pending buffer ≥ `FLUSH_SIZE` (bytes, not records)
- **Time**: elapsed time since last flush ≥ `FLUSH_INTERVAL_MS`

`consume(timeout=remaining_until_flush)` handles both: it returns early with data (check size), or times out (check time). Single thread, no coordination needed. Synchronous commit (`asynchronous=False`) after each successful write — required for at-least-once correctness.

### Arrow Conversion

Ported from ducklake-kafka-connect's `SinkRecordToArrowConverter`:

1. Parse JSON via `orjson` (Rust, ~1GB/s)
2. `pa.Table.from_pylist()` builds the columnar batch — PyArrow infers the superset schema across all dicts
3. v1 types: all numbers → DOUBLE, all strings → VARCHAR, nested objects → JSON. No timestamp detection, no type promotion (see Deferred Complexity)

**Important**: `orjson` parses JSON integers as Python `int`, so `pa.Table.from_pylist()` infers INT64 for integer-only columns. A column that's INT64 in batch N and DOUBLE in batch N+1 (because one value was `1.5`) causes type wobble. v1 must cast all numeric columns to DOUBLE after `from_pylist()` to avoid this.

**Caveat**: `pa.Table.from_pylist()` infers the schema from the first record's keys only. `arrow_converter.py` works around this by pre-scanning all records to build the full key union and passing an explicit schema to `from_pylist()`. The pre-scan is effectively free (pointer iteration over dict keys).

### DuckLake Initialization

At startup, `ducklake.py` must:

```python
conn = duckdb.connect(config.ducklake_connection)
conn.execute("LOAD httpfs")       # must load before ducklake — race condition with S3 access
conn.execute("LOAD ducklake")
conn.execute(f"ATTACH 'ducklake:{config.ducklake_metadata_url}' AS lake (DATA_PATH '{config.ducklake_data_path}')")
```

Extensions are pre-installed in the Docker image at build time (no runtime network dependency). `httpfs` must be loaded before `ducklake` to avoid a race condition where ducklake tries to access S3 before httpfs is available.

### DuckLake Write

```python
conn.register('arrow_batch', table)
conn.execute("INSERT INTO lake.main.{table} SELECT *, NOW() AS _inserted_at FROM arrow_batch")
conn.unregister('arrow_batch')
```

Zero-copy Arrow scan. Table auto-created and evolved (ADD COLUMN, ALTER COLUMN SET DATA TYPE) to match Arrow schema.

### Hive-Style Partitioning

If `DUCKLAKE_PARTITION_BY` is set, `_ensure_table()` runs `ALTER TABLE SET PARTITIONED BY (...)` after table creation. DuckLake writes files into Hive-style directories (`year=2026/month=3/day=23/hour=21/*.parquet`).

- Partitioning is applied once on first write (idempotent — DuckLake ignores if already partitioned)
- Typical expression: `year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)`
- `_inserted_at` is always a TIMESTAMP (set at write time via `NOW()`), so temporal functions work reliably
- Source `timestamp` fields are VARCHAR (not TIMESTAMP), so partition on `_inserted_at` not `timestamp`
- DuckLake handles partition routing automatically on INSERT — no changes to write path

### Table Schema Evolution

The ducklake-kafka-connect connector has two custom layers for schema evolution:

1. **`ArrowSchemaMerge`** — Unifies Arrow schemas within a single batch (records in the same flush with different shapes). Field union, numeric/timestamp type promotion, recursive struct/list/map merging.
2. **`DucklakeTableManager`** — Compares the unified Arrow schema against the DuckLake table and issues DDL (`ADD COLUMN`, `ALTER COLUMN SET DATA TYPE`). Caches known columns to avoid repeated `PRAGMA table_info` round-trips.

**DuckLake handles all the DDL natively.** The extension supports `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN SET DATA TYPE` with widening-only enforcement (TINYINT→SMALLINT→INTEGER→BIGINT, FLOAT→DOUBLE, TIMESTAMP→TIMESTAMPTZ). Invalid promotions are rejected by the extension itself.

**Millpond simplifies this.** The connector's `ArrowSchemaMerge` exists because Kafka Connect can deliver heterogeneous records in the same `put()` call. In Millpond, `pa.Table.from_pylist()` handles intra-batch schema unification implicitly — PyArrow infers the superset schema across all dicts in the list.

**Batch consolidation is also free.** The connector has a 170-line `BatchConsolidator.java` that groups contiguous batches by schema compatibility and does vector-by-vector in-place append — because Java Arrow has no `concat_tables()` equivalent. In Python, `pa.concat_tables(pending)` does schema unification, type promotion, and concatenation in one call. Hundreds of lines of manual memory management and vector arithmetic replaced by a single function call.

Millpond schema evolution approach:

1. `pa.Table.from_pylist()` infers superset schema across all records in the batch
2. Before write, compare `table.schema` against cached DuckLake table schema
3. New field → `ALTER TABLE ADD COLUMN IF NOT EXISTS`
4. Wider type → `ALTER TABLE ALTER COLUMN SET DATA TYPE` (DuckLake enforces widening-only)
5. Incompatible change → DuckLake rejects it, log + metric + skip
6. `_inserted_at TIMESTAMP` added automatically, set to `NOW()` on write

Source files for reference:
- `DucklakeTableManager.java` — DDL detection and execution
- `ArrowSchemaMerge.java` — intra-batch schema unification
- DuckLake extension `ducklake_table_entry.cpp` lines 698-770 — native type promotion rules

## Reference: PostHog Events Schema

The primary use case is consuming from the `clickhouse_events_json` topic. The schema is defined by the [PostHog ingestion pipeline](https://posthog.com/docs/how-posthog-works/ingestion-pipeline) and documented in the [data model](https://posthog.com/docs/how-posthog-works/data-model).

| Field | Type | Description |
|-------|------|-------------|
| `uuid` | String | UUIDv4/v7 event identifier |
| `event` | String | Event name (`page_view`, `$autocapture`, custom) |
| `properties` | String | **JSON-encoded string** — not a nested object |
| `timestamp` | String (ISO 8601) | When the event was captured |
| `team_id` | Int64 | PostHog project identifier |
| `distinct_id` | String | User identifier |
| `created_at` | String (ISO 8601) | Server receipt time |
| `elements_chain` | String | DOM hierarchy for autocapture events |
| `elements_hash` | String | Hash of elements chain |

Key observations:
- **`properties` is a JSON string within the JSON record**, not a nested object. From Millpond's perspective it's a VARCHAR column — no struct/map type inference needed.
- **Top-level schema is extremely stable.** All extensibility lives inside `properties`. New top-level fields are rare, validating the "schema rarely changes" assumption.
- **`_timestamp` and `_offset`** appear in ClickHouse's [Kafka table engine](https://posthog.com/docs/how-posthog-works/clickhouse) but are Kafka metadata not present in the message payload. Millpond adds `_inserted_at` instead.

Source: [PostHog architecture](https://posthog.com/docs/how-posthog-works), [plugin-server](https://github.com/PostHog/plugin-server) (event producer).

## Metrics

Prometheus via `prometheus_client`, HTTP on port 8000.

| Metric | Type | Description |
|--------|------|-------------|
| `millpond_records_consumed_total` | Counter | Records polled (by partition) |
| `millpond_records_written_total` | Counter | Records written to DuckLake |
| `millpond_batches_flushed_total` | Counter | Flush cycles completed |
| `millpond_records_skipped_total` | Counter | Records skipped (by reason: json_parse, schema) |
| `millpond_errors_total` | Counter | Errors by type (kafka/duckdb/arrow/json) |
| `millpond_arrow_conversion_seconds` | Histogram | Time to convert JSON to Arrow table |
| `millpond_flush_duration_seconds` | Histogram | Time per DuckLake write |
| `millpond_flush_size_bytes` | Histogram | Arrow bytes per flush |
| `millpond_flush_size_records` | Histogram | Records per flush |
| `millpond_pending_bytes` | Gauge | Current pending Arrow bytes awaiting flush |
| `millpond_consumer_lag` | Gauge | Highwater - committed (by partition) |
| `millpond_last_committed_offset` | Gauge | Last committed offset (by partition) |
| `millpond_schema_columns_added_total` | Counter | Columns added via schema evolution |
| `millpond_schema_columns_widened_total` | Counter | Columns widened via schema evolution |
| `millpond_rdkafka_replyq` | Gauge | Ops waiting for broker response (librdkafka) |
| `millpond_rdkafka_msg_cnt` | Gauge | Messages in internal librdkafka queues |
| `millpond_rdkafka_msg_size` | Gauge | Bytes in internal librdkafka queues |
| `millpond_rdkafka_broker_rtt_avg_seconds` | Gauge | Broker round-trip time average (by broker) |
| `millpond_rdkafka_broker_rtt_p99_seconds` | Gauge | Broker round-trip time p99 (by broker) |

## Known Risks and Mitigations (Architect Review)

### Critical

| Risk | Mitigation |
|------|------------|
| Partition count desync | Partition count discovered via `consumer.list_topics()` at startup — no env var. `REPLICA_COUNT` env var must match `spec.replicas`; desync causes uneven assignment but not data loss (some partitions double-assigned, some unassigned). |
| Concurrent DDL from multiple pods (two pods both ALTER TABLE simultaneously) | `ADD COLUMN IF NOT EXISTS` is idempotent — multiple pods racing is harmless. `ALTER COLUMN SET DATA TYPE` widening to the same target is also idempotent. Postgres advisory locks (`pg_advisory_lock(hashtext('millpond-schema-' || table))`) available if contention materializes, but unlikely. Cannot designate a single schema-owner pod because schema discovery is distributed (new fields can appear in any partition). In practice, schema changes are rare — the primary use case (events) uses a stable schema that relies on maps/dictionaries for extensibility rather than adding columns. |
| Liveness probe only checks prometheus HTTP, not app health | Add `/healthz` endpoint that checks last-poll and last-flush recency. Pod is unhealthy if either exceeds a threshold. |

### High

| Risk | Mitigation |
|------|------------|
| Duplicate writes on crash (INSERT succeeds, commitSync doesn't) | At-least-once is the design point. Duplicates bounded by flush interval. Downstream consumers must tolerate duplicates. |
| Shutdown sequencing must be explicit | SIGTERM sets shutdown flag → exit loop → flush pending → commit → close consumer. `terminationGracePeriodSeconds: 120` covers S3 latency. |
| Offset tracking must be max-per-partition across accumulated batches | Track `dict[TopicPartition, offset]`, update with max on each batch append. |

### Medium

| Risk | Mitigation |
|------|------------|
| Poison records (malformed JSON) | `orjson.loads()` failure skips record, increments `millpond_errors_total{type=json}`, logs. Does not kill batch or pod. |
| Memory limits (pending batches + DuckDB + PyArrow + librdkafka) | Pending size bounded by `FLUSH_SIZE`. Steady-state ~250-300MB (Python ~30MB, librdkafka ~50-100MB, pending Arrow ~100-128MB, DuckDB ~20-30MB). 512Mi limit with 256Mi request. |

### What We Lose

| Feature | Impact |
|---------|--------|
| Consumer-level fan-out (N consumers for M partitions where N > M) | Not needed. A single Python consumer handles 500K+ msg/sec; per-partition peak is ~9.5K. Fan-out would require consumer groups, which we've eliminated. |
| Auto-healing (consumer group reassigns partitions from failed consumers) | A failed pod's partitions stop being consumed until K8s restarts it. If a partition can't consistently be read, that's a Kafka-side problem, not a consumer architecture problem. |
| DLQ (dead letter queue) | See below. |

#### Why We Don't Implement a DLQ

Kafka Connect provides DLQ [for free](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues/), but in practice DLQs are an anti-pattern for pipelines with ordering and at-least-once guarantees.

**DLQs break ordering.** Rerouting a message to a DLQ and reprocessing it later means it arrives out of order relative to newer events. For idempotent data (logs) that's tolerable. For anything with ordering semantics, [it's catastrophic](https://www.kai-waehner.de/blog/2022/05/30/error-handling-via-dead-letter-queue-in-apache-kafka/). You can't re-insert a message into its original partition order.

**DLQs become silent graveyards.** They're [enabled after the first outage, then nobody monitors them](https://www.confluent.io/learn/kafka-dead-letter-queue/). The topic exists but nobody watches it grow. Without a recovery workflow, they just accumulate unresolved messages.

**DLQ floods are almost always one root cause.** [Debugging 25,000 failed DLQ messages](https://skey.uk/post/kafka-dead-letter-queue-troubleshooting-guide/) typically reveals 1-2 underlying issues amplified by batch processing and retries. The DLQ is a noisy symptom log, not a resolution mechanism.

**DLQs defer decisions, they don't help make them.** You still need to investigate, fix, and replay. At which point you could have just fixed the root cause and replayed from Kafka offsets directly — which is exactly what `assign()` + explicit offset commit gives you for free.

Even [Confluent's own docs](https://www.confluent.io/learn/kafka-dead-letter-queue/) admit the feature is "currently limited in scope," and Confluent's Kai Waehner [explicitly calls out](https://www.kai-waehner.de/blog/2022/05/30/error-handling-via-dead-letter-queue-in-apache-kafka/) using DLQ for backpressure as an anti-pattern.

**Millpond approach:** Poison records get logged, metricked (`millpond_records_skipped_total`), and skipped. If the skip rate spikes, fix the root cause and replay from committed offsets.

### Deferred Complexity (not for v1)

| Feature | Status |
|---------|--------|
| Type promotion (int8→int16→int32→int64→float) | v1: all numbers are DOUBLE, all strings are VARCHAR, nested objects are JSON. Add promotion later if storage costs justify it. |
| Timestamp detection heuristic | v1: store as VARCHAR. Let query engine cast. The ISO8601 regex will misfire on non-timestamp strings that happen to match the pattern. Add opt-in timestamp columns later. When adding this, port the ID field heuristic from ducklake-kafka-connect's `SinkRecordToArrowConverter`: fields ending in `_uuid`, `uuid`, `_id`, `id`, `_key`, `key` must be forced to VARCHAR to prevent UUID strings like `"2024-02-28T23:59:59Z"` from being mis-inferred as timestamps. |

## DuckDB Logging

Unlike the JVM client (which supports custom log storage callbacks — see `duckdb-jvm`'s `NativeLogRouter.kt`), the Python client only supports `memory`, `stdout`, and `file` log storage. No way to route DuckDB internal logs into Python's `logging` module.

For now, DuckDB logging is left at defaults. If needed, enable with `CALL enable_logging(storage='stdout')` and DuckDB will write to stderr in CSV format alongside Python's structured logs.

## Deployment Strategy

Rolling updates are a poor fit for static partition assignment — during the roll, pods run with different `REPLICA_COUNT` values, causing temporary double-assignment (duplicate writes) or gaps. Since Kafka is the durable buffer, a simpler strategy works:

1. **Canary**: Deploy one pod with the new version. Verify it consumes and flushes correctly (check metrics, lag, error rate).
2. **Graceful shutdown**: Scale the StatefulSet to 0. All pods flush pending writes, commit offsets, and exit. Partitions stop being consumed — Kafka holds the data.
3. **Full redeploy**: Update the image/config, scale back up. Each pod picks up from committed offsets. Zero data loss.

Downtime = time to drain + time to start new pods. With `terminationGracePeriodSeconds: 120` and typical S3 flush latency, expect ~2-3 minutes of no consumption. Kafka buffers this trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically. If someone scales without Helm, partitions will be unevenly or doubly assigned until corrected.

## HTTP Server

Both Prometheus metrics (`/metrics`) and health checks (`/healthz`) run on port 8000. `prometheus_client.start_http_server()` does not support custom routes, so Millpond uses a custom `http.server.HTTPServer` that serves both:

- `GET /metrics` → Prometheus exposition format (via `prometheus_client.generate_latest()`)
- `GET /healthz` → 200 if last poll and last flush are within thresholds, 503 otherwise

## Toolchain

- **Flox**: System dependencies (Python 3.12+, uv)
- **uv**: Python package management. `pyproject.toml` is source of truth, `uv.lock` is committed.

## Dependencies

| Package | Why |
|---------|-----|
| `confluent-kafka>=2.6` | librdkafka Python wrapper |
| `duckdb>=1.2` | DuckDB Python client |
| `pyarrow>=18.0` | Arrow tables, zero-copy DuckDB integration |
| `orjson>=3.10` | Fast JSON parsing (Rust) |
| `prometheus-client>=0.21` | Metrics exposition |

## Project Structure

```
millpond/
├── pyproject.toml            # Dependencies and metadata (uv source of truth)
├── uv.lock                   # Resolved dependency lock (committed)
├── .flox/                    # Flox environment
├── Dockerfile
├── docker-compose.yaml       # Full dev stack (Kafka, Postgres, MinIO, Grafana)
├── k8s/
│   ├── statefulset.yaml
│   ├── service.yaml          # Headless service for StatefulSet
│   └── pdb.yaml              # PodDisruptionBudget
├── millpond/
│   ├── __init__.py
│   ├── main.py               # Entry point, main loop, signal handling
│   ├── config.py             # Env var → dataclass
│   ├── arrow_converter.py    # JSON → PyArrow Table (orjson + from_pylist + DOUBLE cast)
│   ├── ducklake.py           # DuckDB/DuckLake connection, table mgmt, writes
│   ├── metrics.py            # Prometheus metric definitions
│   └── server.py             # HTTP server for /metrics and /healthz
├── tests/
│   ├── unit/                 # Fast, no external deps
│   ├── integration/          # Local DuckDB write path + schema evolution
│   └── e2e/                  # Full docker-compose stack via testcontainers
└── test/                     # Dev fixtures (producer.py, ducklake-init.sql)
```
