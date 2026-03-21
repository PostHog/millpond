# DSK2D — Dead Simple Kafka to DuckLake

## What This Is

A standalone Python app that replaces Kafka Connect for writing Kafka topic data to DuckLake. Two threads, one blocking queue, no framework.

Replaces: [PostHog/ducklake-kafka-connect](https://github.com/PostHog/ducklake-kafka-connect) (~1100 lines of lock management, scheduled executors, two-lock protocols imposed by the Kafka Connect framework).

## Why Not Kafka Connect

Kafka is already a queue with buffering, backpressure, and offset management built in. The entire Kafka Connect connector exists to re-implement worse versions of these things because the framework won't let you use them directly.

Connect owns the consumer. The connector is a plugin that implements `SinkTask.put()`. Connect calls `put()` with records; if it returns, Connect considers them handled. This creates:

- **No backpressure**: Can't say "not ready." Blocking in `put()` triggers consumer eviction + rebalance.
- **No explicit offset control**: Connect commits after `put()` returns, not after successful write. Data loss window.
- **No consumer configuration**: Can't set `ConsumerRebalanceListener`, can't control poll timing, can't use `pause()`/`resume()`.
- **Rebalance hell**: `close(revoked)` / `open(assigned)` while scheduler thread may still be flushing from old assignment.

The result: the connector reimplements a blocking queue with ~1100 lines of ceremony.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       ├─ Consumer Thread: poll() → JSON→Arrow → queue.put()
       ├─ Writer Thread:   queue.get(timeout) → accumulate → DuckLake write → commit offsets
       └─ ByteBoundedQueue (capacity bounded by BUFFER_MAX_BYTES)
```

**Note**: Python's `queue.Queue` only supports item-count capacity. `ByteBoundedQueue` is a custom ~20-line wrapper using `threading.Semaphore` (initialized to `BUFFER_MAX_BYTES`), `collections.deque`, and `threading.Lock`. Acquire `batch.nbytes` on put, release on take. Guard: if a single batch exceeds `BUFFER_MAX_BYTES`, log a warning and allow it through to avoid deadlock.

- **No consumer groups.** Each pod computes its partitions from its StatefulSet ordinal and total partition count, uses `consumer.assign()`.
- **Backpressure** is implicit: queue fills → consumer blocks → poll() stops → Kafka waits.
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

Pod ordinal from StatefulSet hostname (e.g. `dsk2d-events-3` → ordinal `3`).

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

**Partition count**: discovered at startup via `consumer.list_topics(topic)`. No env var — eliminates desync risk if partitions are added server-side.

**Replica count**: set via `REPLICA_COUNT` env var (matches `spec.replicas` in the StatefulSet). This is operator-controlled and can't be discovered reliably from inside the pod.

Alternative considered: both as env vars (`KAFKA_PARTITION_COUNT`, `REPLICA_COUNT`). Simpler but creates a desync risk for partition count, which can change server-side without the env var being updated. Replica count doesn't have this problem — it's always set by the operator via kubectl.

Scaling requires updating both `spec.replicas` and the `REPLICA_COUNT` env var.

### Cross-Thread Offset Commit

The writer thread calls `consumer.commit(offsets, asynchronous=False)` (synchronous) while the consumer thread owns the `Consumer` instance. Synchronous commit is required for at-least-once: the writer must know the commit succeeded before clearing pending state. This is safe because:

- librdkafka's `rd_kafka_t` handle is internally mutex-protected
- `commit()` sends a request processed on librdkafka's background thread; synchronous mode blocks until the broker responds
- The consumer thread is either blocked on backpressure (not in `poll()`) or in `poll()` (librdkafka handles concurrent access)
- `assign()` and `close()` are only called from the consumer thread at startup/shutdown

Fallback if ever needed: queue offsets back to consumer thread via a second queue.

### Flush Triggers

Both time-based and size-based:
- **Size**: accumulated Arrow bytes in pending buffer ≥ `FLUSH_SIZE` (bytes, not records)
- **Time**: elapsed time since last flush ≥ `FLUSH_INTERVAL_MS`

Writer thread uses `queue.get(timeout=remaining_time_until_flush)` to handle both.

### Arrow Conversion

Ported from ducklake-kafka-connect's `SinkRecordToArrowConverter`:

1. Parse JSON via `orjson` (Rust, ~1GB/s)
2. `pa.Table.from_pylist()` builds the columnar batch — PyArrow infers the superset schema across all dicts
3. v1 types: all numbers → DOUBLE, all strings → VARCHAR, nested objects → JSON. No timestamp detection, no type promotion (see Deferred Complexity)

**Important**: `orjson` parses JSON integers as Python `int`, so `pa.Table.from_pylist()` infers INT64 for integer-only columns. A column that's INT64 in batch N and DOUBLE in batch N+1 (because one value was `1.5`) causes type wobble. v1 must cast all numeric columns to DOUBLE after `from_pylist()` to avoid this.

### DuckLake Write

```python
conn.register('arrow_batch', table)
conn.execute("INSERT INTO lake.main.{table} SELECT *, NOW() AS _inserted_at FROM arrow_batch")
conn.unregister('arrow_batch')
```

Zero-copy Arrow scan. Table auto-created and evolved (ADD COLUMN, ALTER COLUMN SET DATA TYPE) to match Arrow schema.

### Table Schema Evolution

The ducklake-kafka-connect connector has two custom layers for schema evolution:

1. **`ArrowSchemaMerge`** — Unifies Arrow schemas within a single batch (records in the same flush with different shapes). Field union, numeric/timestamp type promotion, recursive struct/list/map merging.
2. **`DucklakeTableManager`** — Compares the unified Arrow schema against the DuckLake table and issues DDL (`ADD COLUMN`, `ALTER COLUMN SET DATA TYPE`). Caches known columns to avoid repeated `PRAGMA table_info` round-trips.

**DuckLake handles all the DDL natively.** The extension supports `ADD COLUMN`, `DROP COLUMN`, `ALTER COLUMN SET DATA TYPE` with widening-only enforcement (TINYINT→SMALLINT→INTEGER→BIGINT, FLOAT→DOUBLE, TIMESTAMP→TIMESTAMPTZ). Invalid promotions are rejected by the extension itself.

**DSK2D simplifies this.** The connector's `ArrowSchemaMerge` exists because Kafka Connect can deliver heterogeneous records in the same `put()` call. In DSK2D, `pa.Table.from_pylist()` handles intra-batch schema unification implicitly — PyArrow infers the superset schema across all dicts in the list.

DSK2D schema evolution approach:

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

## Metrics

Prometheus via `prometheus_client`, HTTP on port 8000.

| Metric | Type | Description |
|--------|------|-------------|
| `dsk2d_records_consumed_total` | Counter | Records polled (by partition) |
| `dsk2d_records_written_total` | Counter | Records written to DuckLake |
| `dsk2d_batches_flushed_total` | Counter | Flush cycles completed |
| `dsk2d_backpressure_wait_seconds_total` | Counter | Cumulative consumer block time |
| `dsk2d_records_skipped_total` | Counter | Records skipped (by reason: json_parse, schema) |
| `dsk2d_errors_total` | Counter | Errors by type (kafka/duckdb/arrow/json) |
| `dsk2d_flush_duration_seconds` | Histogram | Time per DuckLake write |
| `dsk2d_flush_size_bytes` | Histogram | Arrow bytes per flush |
| `dsk2d_flush_size_records` | Histogram | Records per flush |
| `dsk2d_buffer_bytes` | Gauge | Current pending Arrow bytes |
| `dsk2d_buffer_utilization` | Gauge | buffer_bytes / buffer_max_bytes |
| `dsk2d_consumer_lag` | Gauge | Highwater - committed (by partition) |
| `dsk2d_last_committed_offset` | Gauge | Last committed offset (by partition) |

## Known Risks and Mitigations (Architect Review)

### Critical

| Risk | Mitigation |
|------|------------|
| Partition count desync | Partition count discovered via `consumer.list_topics()` at startup — no env var. `REPLICA_COUNT` env var must match `spec.replicas`; desync causes uneven assignment but not data loss (some partitions double-assigned, some unassigned). |
| Concurrent DDL from multiple pods (two pods both ALTER TABLE simultaneously) | `ADD COLUMN IF NOT EXISTS` is idempotent — multiple pods racing is harmless. `ALTER COLUMN SET DATA TYPE` widening to the same target is also idempotent. Postgres advisory locks (`pg_advisory_lock(hashtext('dsk2d-schema-' || table))`) available if contention materializes, but unlikely. Cannot designate a single schema-owner pod because schema discovery is distributed (new fields can appear in any partition). In practice, schema changes are rare — the primary use case (events) uses a stable schema that relies on maps/dictionaries for extensibility rather than adding columns. |
| Liveness probe only checks prometheus HTTP, not app health | Add `/healthz` endpoint that checks last-poll and last-flush recency. Pod is unhealthy if either exceeds a threshold. |

### High

| Risk | Mitigation |
|------|------------|
| Duplicate writes on crash (INSERT succeeds, commitSync doesn't) | At-least-once is the design point. Duplicates bounded by flush interval. Downstream consumers must tolerate duplicates. |
| Shutdown sequencing must be explicit | Stop consumer → drain queue → final flush → commit → close consumer. `terminationGracePeriodSeconds: 120` covers S3 latency. |
| Offset tracking must be max-per-partition across accumulated batches | Track `dict[TopicPartition, offset]`, update with max on each batch append. |

### Medium

| Risk | Mitigation |
|------|------------|
| Poison records (malformed JSON) | `orjson.loads()` failure skips record, increments `dsk2d_errors_total{type=json}`, logs. Does not kill batch or pod. |
| Memory limits (256MB buffer + DuckDB + PyArrow + librdkafka) | Profile under load before production. 1Gi limit is a placeholder. |

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

**DSK2D approach:** Poison records get logged, metricked (`dsk2d_records_skipped_total`), and skipped. If the skip rate spikes, fix the root cause and replay from committed offsets.

### Deferred Complexity (not for v1)

| Feature | Status |
|---------|--------|
| Type promotion (int8→int16→int32→int64→float) | v1: all numbers are DOUBLE, all strings are VARCHAR, nested objects are JSON. Add promotion later if storage costs justify it. |
| Timestamp detection heuristic | v1: store as VARCHAR. Let query engine cast. The ISO8601 regex will misfire on non-timestamp strings that happen to match the pattern. Add opt-in timestamp columns later. |

## DuckDB Native Log Routing

DuckDB's internal logs are routed into Python's `logging` module via the log storage callback API. Same pattern as `duckdb-jvm`'s `NativeLogRouter.kt` in `~/src/duckdb-jvm`.

- Logger: `dsk2d.duckdb`
- DuckDB levels mapped: debug/trace→DEBUG, info→INFO, warn→WARNING, error/fatal→ERROR
- Messages prefixed with `[log_type]` (e.g. `[CATALOG]`, `[EXECUTE]`)
- Enabled via `CALL enable_logging(storage='dsk2d')` + `SET logging_level='info'`

## Deployment Strategy

Rolling updates are a poor fit for static partition assignment — during the roll, pods run with different `REPLICA_COUNT` values, causing temporary double-assignment (duplicate writes) or gaps. Since Kafka is the durable buffer, a simpler strategy works:

1. **Canary**: Deploy one pod with the new version. Verify it consumes and flushes correctly (check metrics, lag, error rate).
2. **Graceful shutdown**: Scale the StatefulSet to 0. All pods flush pending writes, commit offsets, and exit. Partitions stop being consumed — Kafka holds the data.
3. **Full redeploy**: Update the image/config, scale back up. Each pod picks up from committed offsets. Zero data loss.

Downtime = time to drain + time to start new pods. With `terminationGracePeriodSeconds: 120` and typical S3 flush latency, expect ~2-3 minutes of no consumption. Kafka buffers this trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically. If someone scales without Helm, partitions will be unevenly or doubly assigned until corrected.

## HTTP Server

Both Prometheus metrics (`/metrics`) and health checks (`/healthz`) run on port 8000. `prometheus_client.start_http_server()` does not support custom routes, so DSK2D uses a custom `http.server.HTTPServer` that serves both:

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
dsk2d/
├── pyproject.toml            # Dependencies and metadata (uv source of truth)
├── uv.lock                   # Resolved dependency lock (committed)
├── .flox/                    # Flox environment
├── Dockerfile
├── k8s/
│   └── statefulset.yaml
└── dsk2d/
    ├── __init__.py
    ├── main.py              # Entry point, config, thread lifecycle, shutdown
    ├── config.py             # Env var → dataclass
    ├── consumer.py           # Consumer thread
    ├── writer.py             # Writer thread
    ├── arrow_converter.py    # JSON → PyArrow Table
    ├── ducklake.py           # DuckDB/DuckLake connection, table mgmt, writes
    └── metrics.py            # Prometheus metric definitions
```
