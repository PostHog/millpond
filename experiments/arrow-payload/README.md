# Experiment: Arrow IPC on the wire (C++ PoC)

## Goal

Validate the claim from `WICKED_COOL_NEXT_STEPS.md` that producing **Arrow IPC**
on the Kafka wire enables a near-zero-copy consumer path:

```
librdkafka recv buffer  ──►  Arrow C Data Interface  ──►  DuckDB arrow_scan  ──►  DuckLake INSERT
```

The librdkafka payload buffer is wrapped in place by Arrow C++ — no `memcpy`
into a separate Arrow buffer. The only "copy" is DuckDB materializing into its
own column store during `INSERT`, which is unavoidable.

## Non-goals

- Pure-C consumer (Arrow IPC reading in pure C requires Arrow GLib; we use
  Arrow C++ + the Arrow C Data Interface to keep the PoC tight). The C++ side
  is still ~150 LOC.
- Full feature parity with the Python consumer (no schema evolution detection,
  no adaptive backpressure, no Prometheus metrics — those layers are
  orthogonal to the zero-copy claim).
- Replacing the Python consumer in production. This is "for fun".

## Architecture

```
┌─────────────┐    Arrow IPC    ┌──────────────────────┐    Arrow C Data Interface    ┌─────────┐    INSERT    ┌──────────┐
│  producer   │ ──────────────► │  arrow-consumer C++  │ ───────────────────────────► │ DuckDB  │ ───────────► │ DuckLake │
│  (Python)   │   record batch  │   librdkafka loop    │   ArrowArrayStream            │ in-mem  │              │  (PG+S3) │
└─────────────┘                 └──────────────────────┘                               └─────────┘              └──────────┘
```

- **Topic**: `test-events-arrow` (separate from the existing `test-events`).
- **Payload format**: Arrow IPC stream — one record batch per Kafka message.
  Each batch contains N PostHog-shaped events.
- **Schema**: fixed at producer startup (no per-batch evolution). See below.
- **Sink**: full DuckLake stack (Postgres catalog + MinIO data) — same services
  as the parent project's `docker-compose.yaml`, separate table:
  `lake.main.events_arrow`.

## Shared contract (producer ↔ consumer)

Both sides MUST agree on these.

| Item | Value |
|---|---|
| Topic | `test-events-arrow` |
| Partitions | `8` |
| Records per batch | `1000` (env: `BATCH_SIZE`) |
| IPC variant | Arrow IPC **stream** format (`ipc.new_stream` / `arrow::ipc::RecordBatchStreamReader`) |
| Compression | none (we want raw memcpy semantics; LZ4 can come later) |
| DuckLake table | `lake.main.events_arrow` |
| Postgres / MinIO | reuses parent stack env vars (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`) |

### Arrow schema (v1)

Top-level columns. Mirrors the PostHog event shape, flattened where convenient.
Properties get JSON-encoded into a VARCHAR column to match the production
Millpond approach (avoids struct unification headaches in the PoC).

| Column | Arrow type |
|---|---|
| `uuid` | `string` |
| `event` | `string` |
| `distinct_id` | `string` |
| `timestamp` | `string` (ISO8601) |
| `team_id` | `int64` |
| `project_id` | `int64` |
| `properties` | `string` (JSON-encoded) |
| `elements_chain` | `string` (nullable, only for `$autocapture`) |

The producer generates events with the existing `test/producer.py` event
generators (pageview, autocapture, identify, etc.) but ships them as columnar
Arrow record batches instead of one-JSON-per-message.

## Repository layout

```
experiments/arrow-payload/
├── README.md              # this file (design + results)
├── docker-compose.yaml    # standalone stack: kafka + postgres + minio + producer + consumer
├── Dockerfile.producer    # Python image with pyarrow + confluent-kafka
├── Dockerfile.consumer    # Debian + librdkafka-dev + libarrow-dev + duckdb headers/lib
├── producer/
│   └── arrow_producer.py  # generates events, batches into Arrow IPC, publishes to Kafka
└── consumer/
    ├── CMakeLists.txt
    └── main.cpp           # librdkafka poll loop → Arrow IPC reader → DuckDB arrow_scan → INSERT
```

## Build & run

```bash
cd experiments/arrow-payload
docker compose up --build
```

Expected output:
- Kafka, Postgres, MinIO healthy
- DuckLake table `events_arrow` created on consumer startup
- Producer logs `[arrow-producer] sent batch N records=1000 bytes=…`
- Consumer logs `[arrow-consumer] flushed batch N records=1000 lag=…`
- Records visible in MinIO under `s3://ducklake/data/main/events_arrow/...`

## What we're measuring

- **Throughput**: records/sec sustained at the consumer.
- **Per-record CPU**: from `perf stat` or just wall time vs records.
- **Memory**: RSS of the consumer process. Should stay flat — no per-record
  allocations beyond what librdkafka and DuckDB need internally.

## First-run results (smoke test, 2026-04-12)

End-to-end works on Apple Silicon with the stack in this directory.

| Metric | Value |
|---|---|
| Producer rate (rate-limited) | 10 batches/sec × 1000 records = **10,000 rec/sec** |
| Per-batch wire size (Arrow IPC stream) | ~1.10–1.14 MB uncompressed |
| Consumer behavior | Kept up with the producer trivially across all 8 partitions; no lag |
| Records ingested into DuckLake (~30s run) | **660,000** across 660 data files |
| Average parquet file size (compressed, on MinIO) | ~345–352 KB per 1000-record batch |
| Compression ratio (Arrow IPC → parquet) | ~3.2× (1.12 MB → 350 KB) |
| DuckLake catalog rows | `ducklake_table` × 1 (`events_arrow`), `ducklake_data_file` × 660 |
| Consumer crashes / segfaults | 0 |
| Data lost | 0 |

These numbers do **not** stress the consumer's peak throughput — the
producer was deliberately rate-limited and ran a single instance. To
measure the consumer ceiling, raise `BATCHES_PER_SECOND` aggressively
(or set it to 0 to disable the limiter entirely) and watch consumer
lag from `kafka-consumer-groups.sh`.

### Things that bit during the smoke test (in case anyone reproduces)

| # | Symptom | Fix |
|---|---|---|
| 1 | `wget` exit 8 fetching `libduckdb-linux-aarch64.zip` | DuckDB ships `libduckdb-linux-arm64.zip`, not `aarch64`. Dockerfile uses `linux-arm64`. |
| 2 | `libarrow.so.1801: cannot open shared object file` | Builder pulled Arrow 18.1.0 (SONAME `1801`), runtime tried to install `libarrow1800` (18.0.x runtime package). Fix: pin both stages to `18.1.0-1` and use `libarrow1801`. |
| 3 | `MSG_SIZE_TOO_LARGE` from producer | 1000 PostHog events with full properties = ~1.1MB > 1MB Kafka default. Bumped broker `KAFKA_CFG_MESSAGE_MAX_BYTES`, topic-level `max.message.bytes`, and producer `message.max.bytes` to 16 MiB. |
| 4 | DuckDB `Parser Error` on `ATTACH 'ducklake:postgres:...'` | Inner single quotes around `dbname='ducklake'` closed the outer SQL string literal early. Fixed by dropping the inner quotes (Postgres connstring values are unquoted-safe when they contain no whitespace). The Python `ducklake.py` has the same shape and is technically vulnerable to the same parser error if a value ever needs quoting — flag for the production code if this turns out to matter. |
| 5 | `duckdb_arrow_scan` segfault (caught by QE review before runtime) | The `_duckdb_arrow_stream { void *internal_ptr; }*` typedef is opaque at the ABI boundary; DuckDB does `reinterpret_cast<ArrowArrayStream *>(arrow)` directly. Pass `&c_stream` cast to the typedef, NOT a wrapper struct. |

## Open questions / known caveats

- **Lifetime of `msg->payload`**: librdkafka owns the buffer until
  `rd_kafka_message_destroy()`. We must hold the message alive across the
  entire `arrow_scan` → `INSERT` window. The C++ consumer wraps the buffer in
  an `arrow::Buffer` with a custom deleter that calls
  `rd_kafka_message_destroy()` once Arrow drops its reference. This makes the
  zero-copy claim defensible.
- **Buffer alignment**: librdkafka does not align its receive buffer to Arrow's
  preferred 64-byte boundary. Arrow IPC tolerates this — readers do not assume
  alignment — but any downstream SIMD kernel that requires alignment would need
  to copy. DuckDB does its own copy into its column store on `INSERT`, so this
  doesn't affect the PoC.
- **`INSERT` materializes**: "zero-copy" here means zero copies in the consumer
  process up to the point DuckDB takes ownership. DuckDB still copies into its
  internal storage during `INSERT`, regardless of source format. This is
  unavoidable and not unique to Arrow IPC.
