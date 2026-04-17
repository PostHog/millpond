# Wicked Cool Next Steps

None of this is suggested or implemented for v1. Just for fun.

## Eliminating the JSON→Arrow Penalty

The only CPU-bound work in Millpond is the JSON parse + columnarize step: `orjson.loads()` per record → Python dicts → `pa.Table.from_pylist()`.

### Near-term: simdjson → Arrow (no Python objects)

[arrow-rs ported simdjson's two-pass strategy](https://www.arroyo.dev/blog/fast-arrow-json-decoding/) directly into columnar decoding — SIMD structural indexing in pass 1, then column-at-a-time extraction in pass 2. [~2.5x over the previous arrow-rs JSON reader](https://github.com/apache/arrow-rs/pull/3479). Skips row→dict→columnar entirely, going JSON bytes→Arrow buffers.

This exists in Rust but not as a Python-callable library. [pysimdjson](https://pysimdjson.tkte.ch/) is ~6.6x faster than orjson at parsing, but the moment you materialize full Python dicts (which `pa.Table.from_pylist()` requires), [orjson wins because 95% of the cost is Python object creation, not parsing](https://pysimdjson.tkte.ch/). A `pyarrow_simdjson.table_from_json_lines(bytes_list)` that skips Python objects entirely would be a drop-in replacement.

### Long-term: Arrow IPC on the wire

> **PoC validated** — see branch [`experiment/arrow-message-payload`](https://github.com/PostHog/millpond/tree/experiment/arrow-message-payload) and [`experiments/arrow-payload/FINDINGS.md`](https://github.com/PostHog/millpond/blob/experiment/arrow-message-payload/experiments/arrow-payload/FINDINGS.md) for full results.

If the producer writes Arrow IPC format directly to Kafka, the consumer becomes:

```python
batch = pa.ipc.open_stream(raw_bytes).read_all()  # zero deser, zero columnarize
conn.register('arrow_batch', batch)
conn.execute("INSERT INTO lake.main.{table} SELECT * FROM arrow_batch")
```

No `orjson`, no `from_pylist()`, no Python objects. The pipeline is memcpy from librdkafka recv buffer → Arrow buffer → DuckDB zero-copy scan.

Estimated improvement:

| Metric | JSON (current) | Arrow IPC (wire) |
|--------|---------------|------------------|
| Per-record CPU | orjson parse + dict creation + columnarize | ~memcpy |
| Per-pod throughput | ~200-400K rec/sec | ~1M+ rec/sec (I/O bound) |
| Estimated improvement | — | ~3-5x per pod |
| Pods needed (same load) | N | N/3 to N/5 |

Fewer pods → fewer DuckLake metadata transactions → less Postgres catalog contention. Everything gets better. Requires producer-side changes (PostHog ingestion pipeline).

### Endgame: Arrow IPC + C++ (validated)

The PoC on the experiment branch proves this works end-to-end. The C++ consumer (~250 LOC) achieves true zero-copy from librdkafka's receive buffer through to DuckDB's `arrow_scan`:

```
librdkafka recv buffer ──(zero-copy wrap)──> Arrow IPC reader ──(Arrow C Data Interface)──> DuckDB arrow_scan ──> INSERT into DuckLake
```

Key correction from the original text below: there is no `memcpy` between librdkafka and DuckDB. The librdkafka buffer **is** the Arrow buffer — wrapped in place via `arrow::Buffer(ptr, len)`. The only copy is DuckDB materializing into its own column store during `INSERT`, which is unavoidable regardless of source format.

#### PoC smoke-test results (2026-04-12, Apple Silicon)

| Metric | Value |
|---|---|
| Producer rate (rate-limited) | 10 batches/sec × 1000 records = 10K rec/sec |
| Per-batch Arrow IPC wire size | ~1.10–1.14 MB uncompressed |
| Consumer behavior | Kept up trivially, 0 lag across 8 partitions |
| Records ingested (~30s run) | 660,000 |
| Arrow IPC → parquet compression | ~3.2× (1.12 MB → 350 KB) |
| Consumer crashes / data loss | 0 / 0 |

The consumer was never saturated — these numbers reflect producer-limited throughput, not the consumer ceiling.

#### Gotchas worth knowing

- **`duckdb_arrow_stream` ABI**: The typedef looks like it wants a wrapper struct. It does not — DuckDB `reinterpret_cast`s the argument directly to `ArrowArrayStream*`. Pass `reinterpret_cast<duckdb_arrow_stream>(&c_stream)`, not a wrapper. Guaranteed segfault otherwise.
- **Buffer lifetime**: `msg->payload` is owned by librdkafka until `rd_kafka_message_destroy()`. All Arrow + DuckDB work must complete synchronously before destroy. The PoC uses a non-owning `arrow::Buffer` and calls destroy last in each loop iteration.
- **Buffer alignment**: librdkafka does not align to Arrow's preferred 64-byte boundary. Arrow IPC tolerates this; DuckDB copies on INSERT anyway.

#### Original sketch (preserved for reference)

With Arrow IPC on the wire, the entire consumer reduces to three library calls in a loop:

```c
#include <rdkafka.h>
#include <duckdb.h>
#include <arrow/c/abi.h>

while (running) {
    rd_kafka_message_t *msg = rd_kafka_consumer_poll(consumer, 100);
    if (!msg) continue;

    // Arrow IPC payload — no parsing, no conversion
    struct ArrowArrayStream stream;
    arrow_ipc_import(msg->payload, msg->len, &stream);  // zero-copy view

    // DuckDB ingests via Arrow C Data Interface
    duckdb_arrow_scan(conn, &stream, &result);
    duckdb_query(conn, "INSERT INTO lake.main.events SELECT * FROM arrow_scan", NULL);

    // Commit after successful write
    rd_kafka_offset_store(msg->rktpar, msg->offset);
    rd_kafka_message_destroy(msg);
}
```

~30 lines. No allocator, no GC, no runtime. The message payload *is* the Arrow buffer — you hand a pointer to DuckDB, DuckDB reads it in place. Memory allocation per record: zero. The librdkafka receive buffer, the Arrow buffer, and DuckDB's input are all the same bytes.

The platonic ideal of a data pipeline: three pointers and a loop.
