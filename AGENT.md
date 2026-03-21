# DSK2D — Dead Simple Kafka to DuckLake

## What This Is

A standalone Python app that replaces Kafka Connect for writing Kafka topic data to DuckLake. Two threads, one blocking queue, no framework.

Replaces: [PostHog/ducklake-kafka-connect](https://github.com/PostHog/ducklake-kafka-connect) (~1100 lines of lock management, scheduled executors, two-lock protocols imposed by the Kafka Connect framework).

## Why Not Kafka Connect

Kafka Connect owns the consumer. The connector is a plugin that implements `SinkTask.put()`. Connect calls `put()` with records; if it returns, Connect considers them handled. This creates:

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
       └─ BlockingQueue (capacity bounded by BUFFER_MAX_BYTES)
```

- **No consumer groups.** Each pod computes its partitions from its StatefulSet ordinal and total partition count, uses `consumer.assign()`.
- **Backpressure** is implicit: queue fills → consumer blocks → poll() stops → Kafka waits.
- **Offset commit** is explicit: only after successful DuckLake write.
- **If a pod dies**, its partitions stop being consumed until K8s restarts it. No rebalance.

## Key Design Decisions

### Language: Python

The hot path is all C/C++ (librdkafka, orjson, PyArrow, DuckDB). Python is glue — it touches each record once to pass a parsed dict into a list. Performance bottleneck is S3 write latency, not Python.

If Python ever shows up in profiles, the entire app ports to C with the same structure and similar line count — all libraries have C APIs.

### Static Partition Assignment

Pod ordinal from StatefulSet hostname (e.g. `dsk2d-events-3` → ordinal `3`).

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

`REPLICA_COUNT` and `KAFKA_PARTITION_COUNT` are env vars. Scaling requires updating both the StatefulSet replicas and the env var.

### Cross-Thread Offset Commit

The writer thread calls `consumer.commit()` while the consumer thread owns the `Consumer` instance. This is safe because:

- librdkafka's `rd_kafka_t` handle is internally mutex-protected
- `commit()` enqueues an async request processed on librdkafka's background thread
- The consumer thread is either blocked on backpressure (not in `poll()`) or in `poll()` (librdkafka handles concurrent access)
- `assign()` and `close()` are only called from the consumer thread at startup/shutdown

Fallback if ever needed: queue offsets back to consumer thread via a second queue.

### Flush Triggers

Both time-based and size-based:
- **Size**: accumulated Arrow bytes in pending buffer ≥ `FLUSH_SIZE`
- **Time**: elapsed time since last flush ≥ `FLUSH_INTERVAL_MS`

Writer thread uses `queue.get(timeout=remaining_time_until_flush)` to handle both.

### Arrow Conversion

Ported from ducklake-kafka-connect's `SinkRecordToArrowConverter`:

1. Parse JSON via `orjson` (C, ~1GB/s)
2. Infer PyArrow schema from Python types
3. Timestamp detection: ISO8601 regex, excluding ID-like fields (`_id`, `_uuid`, `_key` suffixes)
4. Schema merging across records with different shapes (type promotion: int→int64, int+float→float, etc.)
5. Schema caching per topic, recomputed on new fields
6. `pa.Table.from_pylist()` builds the columnar batch

### DuckLake Write

```python
conn.register('arrow_batch', table)
conn.execute("INSERT INTO lake.main.{table} SELECT *, NOW() AS _inserted_at FROM arrow_batch")
conn.unregister('arrow_batch')
```

Zero-copy Arrow scan. Table auto-created and evolved (ADD COLUMN, ALTER COLUMN SET DATA TYPE) to match Arrow schema.

### Table Schema Evolution

- New fields → `ALTER TABLE ADD COLUMN`
- Numeric promotion (TINYINT→SMALLINT→INTEGER→BIGINT, FLOAT→DOUBLE) → `ALTER COLUMN SET DATA TYPE`
- Struct/list/map → stored as JSON in DuckDB
- `_inserted_at TIMESTAMP` added automatically, set to `NOW()` on write

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
| `REPLICA_COUNT` / `KAFKA_PARTITION_COUNT` env var desync causes double-writes or gaps | Query `consumer.list_topics()` at startup for partition count. Derive replica count from StatefulSet headless service DNS or K8s API. Eliminate both env vars. |
| Concurrent DDL from multiple pods (two pods both ALTER TABLE simultaneously) | Use `ALTER TABLE ADD COLUMN IF NOT EXISTS`. For type conflicts, use Postgres advisory lock around DDL, or designate lowest-ordinal pod as schema owner. |
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
| No DLQ | Log + metric for v1. DLQ (produce to `{topic}-dlq`) is a later addition — Kafka Connect got this for free from the framework. |

### Deferred Complexity (not for v1)

| Feature | Status |
|---------|--------|
| Type promotion (int8→int16→int32→int64→float) | v1: all numbers are DOUBLE, all strings are VARCHAR, nested objects are JSON. Add promotion later if storage costs justify it. |
| Timestamp detection heuristic | v1: store as VARCHAR. Let query engine cast. The ISO8601 regex will misfire on non-timestamp strings that happen to match the pattern. Add opt-in timestamp columns later. |

## Toolchain

- **Flox**: System dependencies (Python 3.12+, uv)
- **uv**: Python package management. `pyproject.toml` is source of truth, `uv.lock` is committed.

## Dependencies

| Package | Why |
|---------|-----|
| `confluent-kafka>=2.6` | librdkafka Python wrapper |
| `duckdb>=1.2` | DuckDB Python client |
| `pyarrow>=18.0` | Arrow tables, zero-copy DuckDB integration |
| `orjson>=3.10` | Fast JSON parsing (C implementation) |
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
