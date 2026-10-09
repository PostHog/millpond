# Arrow IPC on the Wire: Findings

Experiment branch: `experiment/arrow-message-payload`

## Question

Can a C++ consumer achieve near-zero-copy ingest from Kafka into DuckLake
when the producer ships Arrow IPC on the wire?

## Answer

Yes. The PoC demonstrates:

```
librdkafka recv buffer ──(zero-copy wrap)──> Arrow IPC reader ──(Arrow C Data Interface)──> DuckDB arrow_scan ──> INSERT into DuckLake
```

No `memcpy` occurs between librdkafka's receive buffer and DuckDB's scan.
The only copy is DuckDB materializing into its own column store during
`INSERT`, which is unavoidable regardless of source format.

## What the original doc got wrong

`WICKED_COOL_NEXT_STEPS.md` line 25 says:

> The pipeline is memcpy from librdkafka recv buffer -> Arrow buffer -> DuckDB zero-copy scan.

This implies two hops. In practice there is **one**: librdkafka buffer
**is** the Arrow buffer (wrapped in place via `arrow::Buffer(ptr, len)`).
There is no intermediate copy. The table at line 31 listing `~memcpy` as
the per-record CPU cost is also misleading — the per-record cost is closer
to zero (pointer arithmetic inside Arrow's IPC reader + DuckDB's columnar
materialization on INSERT).

## Buffer ownership — the key invariant

| Question | Answer |
|---|---|
| Who owns `msg->payload`? | librdkafka, until `rd_kafka_message_destroy()` |
| Can we read it in place? | Yes, for the lifetime of the message |
| Can we hold the pointer past `destroy()`? | No |
| How does the consumer handle this? | Option A: non-owning `arrow::Buffer(ptr, len)`. All Arrow + DuckDB work runs synchronously in one loop iteration. `rd_kafka_message_destroy()` is the **last** call in the iteration, after DuckDB has fully materialized the batch. |
| Is there a use-after-free risk? | Only if `INSERT` were ever made async or deferred. It isn't — `duckdb_query` is synchronous, so by the time it returns, DuckDB owns a copy in its column store and the borrowed pointers are dead. |
| Buffer alignment? | librdkafka does not align to Arrow's preferred 64-byte boundary. Arrow IPC tolerates this. DuckDB copies into its own storage on INSERT, so alignment is moot. |

## DuckDB Arrow C API — the `duckdb_arrow_stream` ABI trap

The most significant finding from QE review. The `duckdb_arrow_stream`
typedef is defined as:

```c
typedef struct _duckdb_arrow_stream {
    void *internal_ptr;
} *duckdb_arrow_stream;
```

This looks like it wants a wrapper struct with `internal_ptr` pointing at
your `ArrowArrayStream`. **It does not.** At the ABI boundary, DuckDB's
`duckdb_arrow_scan` implementation (`src/main/capi/arrow-c.cpp`) does:

```cpp
reinterpret_cast<ArrowArrayStream *>(arrow)
```

It interprets the argument directly as an `ArrowArrayStream*`. If you pass
a wrapper struct, DuckDB treats the wrapper's first word (`internal_ptr`,
which is the address of your `ArrowArrayStream`) as the `get_schema`
function pointer and calls it. **Guaranteed segfault.**

Correct usage:

```cpp
ArrowArrayStream c_stream{};
arrow::ExportRecordBatchReader(reader, &c_stream);
duckdb_arrow_scan(con, view, reinterpret_cast<duckdb_arrow_stream>(&c_stream));
```

This is underdocumented. The Python API (`conn.register('arrow_batch', table)`)
routes through a completely different code path and doesn't illustrate the
C ABI contract.

## Smoke-test results

| Metric | Value |
|---|---|
| Platform | Apple Silicon (arm64), Docker native |
| Producer rate (intentionally rate-limited) | 10 batches/sec x 1000 records = 10K rec/sec |
| Per-batch Arrow IPC size (uncompressed, on wire) | ~1.10-1.14 MB |
| Consumer behavior | Kept up trivially, 0 lag across 8 partitions |
| Records ingested (~30s run) | 660,000 |
| Parquet files written to MinIO | 660, ~345-352 KB each |
| Arrow IPC -> parquet compression ratio | ~3.2x |
| Consumer crashes / segfaults | 0 |
| Data lost | 0 |

These numbers do not represent peak consumer throughput. The producer was
rate-limited; the consumer was never saturated. Measuring the ceiling
requires removing the rate limiter or running multiple producer instances.

## Build issues encountered

| # | Symptom | Root cause | Fix |
|---|---|---|---|
| 1 | `wget` exit 8 fetching DuckDB lib | DuckDB names arm64 assets `linux-arm64`, not `linux-aarch64` | `TARGETARCH`-driven `case` in Dockerfile |
| 2 | `libarrow.so.1801 not found` at runtime | Builder pulled Arrow 18.1.0 (SONAME 1801), runtime installed `libarrow1800` (18.0.x) | Pin both stages to `18.1.0-1` |
| 3 | `MSG_SIZE_TOO_LARGE` from producer | 1000 PostHog events x ~1.1 KB avg > 1 MB Kafka default | Bumped broker, topic, and producer `message.max.bytes` to 16 MiB |
| 4 | DuckDB `Parser Error` on `ATTACH` | Inner single quotes in Postgres connstring (`dbname='ducklake'`) closed the outer SQL string literal | Dropped inner quotes (values contain no whitespace) |

## Architecture of the PoC

```
experiments/arrow-payload/
├── producer/arrow_producer.py   # 233 lines: generate events, batch to Arrow IPC, publish
├── consumer/main.cpp            # 250 lines: poll, wrap buffer, arrow_scan, INSERT, commit
├── consumer/CMakeLists.txt      # 35 lines
├── Dockerfile.producer          # python:3.12-slim + pyarrow + confluent-kafka
├── Dockerfile.consumer          # debian bookworm + Arrow 18.1 + DuckDB 1.4.0 + librdkafka
└── docker-compose.yaml          # kafka + postgres + minio + producer + consumer
```

The consumer loop is ~50 lines of load-bearing code:

1. `rd_kafka_consumer_poll` — get message
2. `arrow::Buffer(msg->payload, msg->len)` — zero-copy wrap
3. `arrow::ipc::RecordBatchStreamReader::Open` — parse IPC stream in place
4. `arrow::ExportRecordBatchReader` — export to Arrow C Data Interface
5. `duckdb_arrow_scan` — register as scannable view
6. `INSERT INTO lake.main.events_arrow SELECT *, NOW() FROM view` — materialize
7. `rd_kafka_commit` — commit offset (synchronous, after successful write)
8. `rd_kafka_message_destroy` — release librdkafka buffer (last call)

## What this does not cover

- Peak throughput (producer was rate-limited; consumer never saturated)
- Comparison vs the Python millpond consumer at equal load
- Schema evolution (fixed schema, no ALTER TABLE)
- Compression on the wire (Arrow IPC sent uncompressed; LZ4 could reduce wire size but adds a decompression copy)
- Pure-C implementation (would need Arrow GLib for IPC reading)
- Metrics, health checks, graceful shutdown under load
- Multi-consumer scaling (static `assign()` over all partitions; only one consumer instance ran)

## Implications for the production pipeline

The claim in `WICKED_COOL_NEXT_STEPS.md` holds: if the producer writes
Arrow IPC, the consumer reduces to "three pointers and a loop." The JSON
parse + `from_pylist()` + Python object creation that dominates the current
pipeline's CPU profile disappears entirely.

What changes on the producer side: PostHog's ingestion pipeline would need
to emit Arrow IPC instead of JSON. This is a non-trivial change — the
current pipeline is JSON-native end to end. The consumer savings are
meaningless without the producer change, so the decision is whether the
aggregate system benefit (fewer consumer pods, less Postgres catalog
contention, simpler consumer code) justifies the producer-side work.

The consumer-side complexity delta: 250 lines of C++ vs ~400 lines of
Python (millpond's consumer + arrow_converter + config). The C++ has no
runtime, no GC, no allocator pressure per record. The trade-off is
operational: C++ is harder to debug in production, has no Prometheus
metrics integration without additional work, and requires a separate
build toolchain.
