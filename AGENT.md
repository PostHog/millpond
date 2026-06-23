# Millpond — Dead Simple Kafka to DuckLake

## Pre-push Checklist

Always run before pushing:

```bash
just lint              # ruff check (currently scoped to millpond/ only — see note)
just fmt-check         # ruff format --check (currently scoped to millpond/ only — see note)
just test              # unit tests
just test-integration  # in-memory DuckDB integration tests — no docker stack, fast
just test-e2e          # full DuckLake stack (~1m)
```

Note: `just lint` and `just fmt-check` only run against `millpond/` today; `tests/` remains uncovered by the recipes. Pre-commit hooks run ruff project-wide, so lint failures still surface — but the just recipes themselves are scope-narrow until they're updated.

All must pass. Do not push with any lint, test, integration, or e2e failures — CI runs all of these on every push and PR (`.github/workflows/ci.yaml`), and the integration/e2e jobs are gating. Catch failures locally rather than on the runner.

For changes to Kafka client code or config handling, also verify with SSL Kafka:

```bash
just up-ssl     # start docker-compose with SSL Kafka
# verify pods connect and flush
just down-ssl
```

Prefer fixup commits over amending and force-pushing.

## Maintenance Tooling

`tools/ducklake_maintenance.py` is a self-contained Python script for DuckLake maintenance operations (snapshot expiry, file cleanup, orphan deletion, checkpoint, tiered compaction, deletion-queue dedup, catalog-side orphan recovery, fsck). It connects to DuckLake using the same env vars as the main app (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`, `DUCKLAKE_DATA_PATH`) but does not import from the `millpond` package. Designed to run as a K8s CronJob reusing the same Docker image.

`connect()` does two ATTACHes against the same upstream Postgres: the DuckLake catalog as `lake` (the `ATTACH_NAME` constant) and a direct Postgres ATTACH as `pg` (the `PG_ATTACH_NAME` constant) used by `postgres_execute` / `postgres_query` for ctid-based DML and advisory-lock acquisition. S3 access is configured via a `CREATE OR REPLACE SECRET s3` (the `S3_SECRET_NAME` constant). On DuckLake 1.5.x (millpond's pinned version) the SECRET manager covers both the catalog driver and ad-hoc httpfs ops like `glob('s3://...')` and `read_parquet('s3://...')`; the legacy `SET s3_*` block is kept alongside the SECRET only for httpfs-pre-secret compatibility, not because the catalog driver needs it. Spills are pointed at `/tmp/duckdb_spill` so they land on the writable cron-pod emptyDir, not the read-only rootfs.

The `compact` subcommand implements tiered compaction: each tier wraps `ducklake_merge_adjacent_files()` with a save/restore of the catalog's `target_file_size` option (which `ducklake_set_option` writes durably and cannot be unset). Tier specs and the steady-state default live in `TIERS` and `DEFAULT_TARGET_FILE_SIZE` at the top of the file. Bin semantics on DuckLake 1.4.x are `min_file_size` inclusive, `max_file_size` exclusive — so the tier ranges `[0, 1 MiB)`, `[1 MiB, 10 MiB)`, `[10 MiB, 64 MiB)` partition the file-size space without overlap. The `compact-probe` subcommand runs `ducklake_merge_adjacent_files()` against one table with a small `max_compacted_files` cap and no `target_file_size` change — used as a lightweight diagnostic from a periodic CronJob. `compact` exposes `--threads` (default 2) and `--memory-limit` (default 4GB) because `ducklake_merge_adjacent_files` over-uses memory relative to input volume in 1.4 and the conservative defaults keep the cron pod alive on real lakes; raise them when the lake fits.

DuckLake stores its catalog tables (`ducklake_data_file`, `ducklake_table`, `ducklake_files_scheduled_for_deletion`, etc.) in a Postgres schema named `__ducklake_metadata_<attach_name>`, where `<attach_name>` is the alias from the `ATTACH … AS <name>` statement. `ducklake_maintenance.py` exposes the alias as the `ATTACH_NAME` constant and the derived `METADATA_SCHEMA = f"__ducklake_metadata_{ATTACH_NAME}"`; any new code that reads the catalog directly (rather than through the `ducklake_*` SQL functions) must reference `METADATA_SCHEMA` so the attach name and schema name never drift.

`tools/ducklake_maintenance.sql` is executed verbatim at every session start, both by `ducklake_maintenance.py`'s `connect()` and by the `just shell` recipe (via the duckdb CLI's `.read` meta-command). The file contains no templating — `__ducklake_metadata_lake` and the rest are written literally so both load paths stay consistent. It defines runtime macros (`count_pending_dups()`, `find_catalog_orphans(data_path)`) and documents the constraints any new recipe must follow: no `LEFT ANTI JOIN` (DuckDB 1.4 limitation; use `LEFT JOIN ... WHERE rhs IS NULL`), no Postgres `ctid` from duckdb-side SQL (use `postgres_execute` / `postgres_query` instead), no literal `glob('s3://...')` inside `CREATE MACRO` bodies (DuckDB 1.4 evaluates them eagerly at macro creation, which would S3-LIST the lake on every connect — pass the path as a parameter), and the advisory-lock key.

The catalog-side orphan-recovery subcommands (`dedup-deletions`, `find-orphans`, `heal-orphans`, `cleanup-all-safe`, `fsck`) form a self-contained recovery toolkit for the failure mode where an interrupted `cleanup-all` leaves the catalog with rows pointing at S3 keys that no longer exist (because the upstream txn rolled back but the S3 deletes are permanent). `heal-orphans` runs two safety gates before deleting: B1 proves `ducklake_data_file` is non-empty AND no orphan path is still live (no vacuous pass on an empty catalog, no false positive that would delete a live file), and B3 aborts if any positional-delete vector references an "orphan" id (such a file is still live for vector lookups). `cleanup-all-safe` is the orchestrator that loops dedup + heal + cleanup-all under one advisory lock until cleanup-all exits clean; `fsck` adds the `ducklake_delete_orphaned_files` S3-side sweep on top. All destructive subcommands take `pg_try_advisory_lock(hashtext('millpond-ducklake-maintenance')::bigint)` on the `pg` ATTACH; the lock is held by the `pg` connection (not the `lake` connection that DuckLake uses internally), so it provides mutual exclusion between maintenance invocations but not catalog-write atomicity against arbitrary writers — document this caveat anywhere the lock is mentioned.

`main()` logs `millpond <version> (maintenance)` on startup using `importlib.metadata.version("millpond")`, mirroring `millpond/main.py`. If `MILLPOND_IMAGE` is set in the env, the image identifier is appended (`image=<value>`). The chart-side wiring is optional — without it, the package version is sufficient to identify which build is running.

`cleanup` and `cleanup-all` log a single structured throughput line on completion: `cleanup throughput: files_processed=N elapsed_s=T rate_obj_s=R queue_depth_after=A`. `files_processed` comes directly from `len(result)` — the count of rows `ducklake_cleanup_old_files` returned — rather than a queue-depth delta. A delta would be misleading whenever any other writer enqueues deletions during the call (the maintenance advisory lock by design only mutexes maintenance invocations, not arbitrary writers); `len(result)` is accurate regardless. `queue_depth_after` is queried with a single post-call snapshot and shows remaining work but is not used in the rate calculation. The line is intentionally suppressed on `--dry-run` because dry-run returns preview rows and a rate computed from those would falsely claim work was done.

If `PUSHGATEWAY_URL` is set, the script pushes two metrics: `maintenance_start_time{operation}` (pushed immediately on start) and `maintenance_duration_seconds{operation, status}` (pushed on completion). This enables Grafana annotation queries for maintenance windows.

DuckDB's HTTP logging and the postgres extension's `pg_debug_show_queries` are off by default — both add per-call overhead that compounds across tens of thousands of S3 deletes. Pass `--debug` at the ducklake_maintenance.py command level to opt back into both for short-lived debugging.

`tools/justfile` wraps the script for interactive use and is copied to `/justfile` in the Docker image. The `shell` recipe pre-ATTACHes both `lake` and `pg`, configures the S3 SECRET, and `.read`s `tools/ducklake_maintenance.sql` so every session starts with the macros loaded; the lifecycle recipes wrap `ducklake_maintenance.py` subcommands. The `DUCKDB` env var can override the duckdb binary path; `DUCKLAKE_MAINTENANCE_SCRIPT`, `DUCKLAKE_MAINTENANCE_SQL`, and `DUCKLAKE_METRICS_SCRIPT` env vars override the in-image paths for dev use.

## State Metrics Daemon

`tools/ducklake_metrics.py` is a long-running Python daemon that runs catalog-side queries against the DuckLake on a per-query schedule and publishes results as Prometheus gauges over HTTP. Same image, same env vars (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`, `DUCKLAKE_DATA_PATH`); intended to run as a small single-replica Deployment alongside the maintenance CronJob. Reuses `ducklake_maintenance.connect()` so the lake + Postgres ATTACHes, S3 secret, `temp_directory`, and the `enable_http_logging` / `pg_debug_show_queries` quiets all match the maintenance side without duplication.

Single-file design by intent: queries are described as YAML and `BUILTIN_YAML` is a string constant in the same module, parsed through the same loader as user-supplied YAML so the two paths can't diverge. User YAML supplied via `DUCKLAKE_METRICS_CONFIG` extends the built-ins by name (user wins on collision); `DUCKLAKE_METRICS_DISABLE` drops named built-ins. The YAML schema is `name`, `help`, `interval_mins` (positive integer; the unit is in the field name so there's no `1m`/`30s` suffix parsing), `labels` (column names → Prometheus label dimensions), `values` (column names → metric suffixes; metric name is `<query_name>_<value>`), and `sql`. Every metric is a gauge — no `type` field. Built-ins reference the metadata schema literally as `__ducklake_metadata_lake` because `connect()` always ATTACHes the lake under that alias; user-supplied SQL is passed through verbatim.

The scheduler is a single-thread min-heap of `(next_monotonic_ts, seq, idx)`; `seq` is a strict tiebreaker so the heap never compares Query objects. Initial schedule fires every query at startup so `/metrics` populates as fast as the catalog will respond. `_run_query` clears each labeled gauge before re-populating so label combinations that drop out between runs (e.g. a partition leaving the top-20) don't linger as stale series; unlabeled gauges skip the clear because `set()` is itself the full state update. Each run reports back to the outer loop as success/failure; per-query failures increment `ducklake_metrics_query_errors_total` and don't kill the daemon.

Reconnect is gated on a global consecutive-failure counter: when `CONSECUTIVE_FAILURE_THRESHOLD` (default 10) runs in a row across all queries fail, the scheduler raises `_ReconnectNeeded` and the outer loop in `main()` drops the connection and re-establishes it via `_connect_with_backoff` (1s → 60s exponential cap). The threshold is set well above the number of built-in queries so a single misbehaving query can't trip the reset on its own. Any successful run resets the streak. `ducklake_metrics_up` reports 1 while connected and 0 during reconnect; `/-/ready` flips true after the first successful connect and stays true thereafter (k8s shouldn't yank traffic for transient catalog flap, and there's no real "traffic" anyway).

HTTP exposition: `/metrics` (Prometheus), `/-/healthy` (k8s liveness — see below), `/-/ready` (k8s readiness — 200 once the scheduler's running). The handler reads from a registry stored on the server object (`srv.registry`) rather than the default Prometheus registry, so integration tests can use an isolated `CollectorRegistry`. `_start_http(port, host="")` defaults to all-interfaces; tests pass `host="127.0.0.1"` because `HTTPServer.server_bind` calls `socket.getfqdn(host)` which on some macOS DNS configs takes 5 seconds for `""`.

`/-/healthy` reflects actual scheduler progress, not just "process is up." A `_Liveness` holder is updated by `_run_query` (sets `current_query_start` at entry, clears it and sets `last_tick` at exit in a `finally` so failures still tick) and read by `_HealthHandler.do_GET`. The handler returns 503 when either (a) a single query has been in flight longer than the liveness timeout, or (b) no scheduler tick has happened in that long. Pre-scheduler-start the endpoint is unconditionally 200 so the startup probe doesn't kill the pod during initial connect backoff. The timeout (default 300s) is configurable via `--liveness-timeout-seconds` / `DUCKLAKE_METRICS_LIVENESS_TIMEOUT` and is sized to allow the slowest legitimate catalog query a comfortable margin. The pure decision lives in `_liveness_status(state, now)` so every restart-or-not case is unit-testable without an HTTP round-trip.

DuckDB `memory_limit` MUST be set explicitly via `--duckdb-memory-limit` / `DUCKLAKE_METRICS_MEMORY_LIMIT` (e.g. `1GB`). DuckDB's default is ~75% of detected RAM, which in a cgroup-limited pod resolves to host RAM and the kernel OOM-kills the pod the moment DuckDB tries to grow. The value is validated ONCE at startup by `ducklake_maintenance._sanitize_setting_value` (alphanumeric + safe-punctuation charset only) before entering the connect-retry loop — putting the validation inside the loop would let the broad `except Exception:` swallow a bad value and retry forever in a 1-second sleep loop. Size the limit well under `resources.limits.memory` to leave headroom for the Python interpreter, the ducklake extension's in-memory catalog model (loads `ducklake_inlined_data_tables` entries at ATTACH time), and HTTP server buffers — ~250-500Mi typical.

Built-ins are deliberately table-unaware (D8 in `state-metrics-plan.md`): no `table_name` label anywhere. A user who needs per-table metrics adds them via the user YAML rather than threading table awareness into the built-in set.

## What This Is

A standalone Python app that replaces Kafka Connect for writing Kafka topic data to a lake table. Single thread, single loop, no framework. One deployment writes to exactly one DuckLake table.

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
        batch = apply_filter(batch, cfg)                 # optional allowlist (no-op if unconfigured)
        pending.append(batch)

    if should_flush(pending):
        consolidated = pa.concat_tables(pending)
        consolidated = apply_sort(consolidated, cfg)     # optional ascending sort (no-op if unconfigured)
        sink.write(consolidated)                         # destination-agnostic
        consumer.commit(offsets, asynchronous=False)
        pending.clear()
```

- **No consumer groups.** Each pod computes its partitions from its StatefulSet ordinal and total partition count, uses `consumer.assign()`.
- **No threads, no queues.** Kafka is the queue. Backpressure is implicit: while flushing to the lake, the consumer simply doesn't call `consume()`. Kafka holds the data.
- **Offset commit** is explicit: only after a successful lake write.
- **If a pod dies**, its partitions stop being consumed until K8s restarts it. No rebalance.


## Key Design Decisions

### Credential Isolation

Millpond has two independent credential paths that must not interfere:

- **Kafka (MSK)**: SASL/OAUTHBEARER via IRSA. The `aws-msk-iam-sasl-signer-python` library uses the standard AWS credential chain (IRSA projected token → STS → temporary creds).
- **S3 (lake data)**: Static IAM credentials via `DUCKDB_S3_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY`. DuckDB's S3 secret does accept `PROVIDER credential_chain` for IRSA — the maintenance tools (`tools/ducklake_maintenance.py`) use that fallback when no static keys are set, for the per-tenant duckling path. The millpond writer itself still uses `PROVIDER config` with static keys because megaduck/viaduck have an IAM-user Secret synced via ExternalSecret and the writer's startup hasn't been migrated. Switching the writer to credential_chain is a separate change.

The S3 credentials do not use the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` — those take precedence in the credential chain and would shadow IRSA for MSK IAM auth. Do not change the S3 credential env var names to the standard AWS names.

`aws-msk-iam-sasl-signer-python` is an optional dependency (`pip install millpond[msk-iam]`) but the Dockerfile always installs it (`--extra msk-iam`). All production deployments use IAM auth. The optional dep is for local dev where boto3/botocore (~15MB) may not be wanted.

### Adaptive Backpressure

`backpressure.py` implements proportional batch sizing based on buffer fullness. The consume batch size scales linearly from `CONSUME_BATCH_SIZE` (max, when buffer is empty) down to 10 (min, when buffer is at flush threshold). No state machine, no mode switching — one formula:

```
fullness = pending_bytes / flush_size
batch_size = max(10, int(max_batch * (1.0 - fullness)))
```

This smooths throughput during catchup without manual tuning. During catchup, the buffer fills quickly, batch size drops, flushes happen frequently with small batches. At steady state, the buffer is mostly empty and batch size stays at max. OOM prevention comes from `queued.max.messages.kbytes` (set in consumer.py), which bounds librdkafka's internal fetch buffer per partition.

Metrics: `millpond_buffer_fullness` (0.0 = empty, can exceed 1.0 if buffer overshoots flush threshold) and `millpond_consume_batch_size_current` for monitoring.

### Language: Python

The hot path is all C/C++ (librdkafka, orjson, PyArrow, DuckDB). Python is glue — it touches each record once to pass a parsed dict into a list. Performance bottleneck is S3 write latency, not Python.

If Python ever shows up in profiles, the entire app ports to C with the same structure and similar line count — all libraries have C APIs.

#### Kafka Consumer Tuning

Two critical tuning knobs for confluent-kafka-python:

1. **Use `consume(num_messages=N)` batch API, not `poll()`**. The librdkafka C extension's per-call overhead is fixed, so one `consume(num_messages=N)` is cheaper than N `poll()` calls — the FFI-amortization argument is conceptual (it is not stated as such in the [docstring](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.consume), which only describes the API contract). The recommendation matches community consensus (see [confluent-kafka-python #580](https://github.com/confluentinc/confluent-kafka-python/issues/580) for the API-shape discussion). [#597](https://github.com/confluentinc/confluent-kafka-python/issues/597) reports a 4× throughput gap, but as ProcessPoolExecutor vs ThreadPoolExecutor under the GIL with both legs using `consume()` — not as `consume()` vs `poll()` — so don't cite it for the latter.

2. **Set `fetch.min.bytes` to 1MB+** (default is 1 byte). This is the single biggest throughput lever — reduces fetch request count dramatically by letting the broker accumulate data before responding. Trade latency for throughput. Pair with `fetch.max.wait.ms=500`. See [Kafka consumer config docs](https://kafka.apache.org/documentation/#consumerconfigs_fetch.min.bytes) and [Confluent throughput optimization guide](https://docs.confluent.io/cloud/current/client-apps/optimizing/throughput.html).

### Static Partition Assignment

Pod ordinal from StatefulSet hostname (e.g. `millpond-events-3` → ordinal `3`).

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

**Partition count**: discovered at startup via `admin.list_topics(topic=cfg.topic, timeout=30)` (an `AdminClient`, not the consumer instance). No env var — eliminates desync risk if partitions are added server-side.

**Replica count**: set via `REPLICA_COUNT` env var (matches `spec.replicas` in the StatefulSet). This is operator-controlled and can't be discovered reliably from inside the pod.

Alternative considered: both as env vars (`KAFKA_PARTITION_COUNT`, `REPLICA_COUNT`). Simpler but creates a desync risk for partition count, which can change server-side without the env var being updated. Replica count doesn't have this problem — it's always set by the operator via kubectl.

Scaling requires updating both `spec.replicas` and the `REPLICA_COUNT` env var.

**`auto.offset.reset=earliest`**: required. With `assign()`, if a partition has no committed offset (new partition, or `GROUP_ID` changed), the default `latest` silently drops all existing data. `earliest` replays from the beginning — safe for at-least-once.

**`group.id`**: defaults to `millpond-{topic}-{ducklake_table}` (see `load()` in `config.py`). Used only for offset storage in `__consumer_offsets` (no consumer group semantics). Changing `group.id` loses all committed offsets and triggers a full replay from `earliest`.

**Monitoring caveat**: because we use `assign()` instead of `subscribe()`, standard consumer group monitoring tools (`kafka-consumer-groups.sh`, Burrow, etc.) show empty output or stale data. Use Millpond's own `millpond_consumer_lag` metric for lag monitoring, and `millpond_last_committed_offset` for offset tracking.

### Flush Triggers

Both time-based and size-based:
- **Size**: accumulated Arrow bytes in pending buffer ≥ `FLUSH_SIZE` (bytes, not records)
- **Time**: elapsed time since last flush ≥ `FLUSH_INTERVAL_MS`

`consume(timeout=remaining_until_flush)` handles both: it returns early with data (check size), or times out (check time). Single thread, no coordination needed. Synchronous commit (`asynchronous=False`) after each successful write — required for at-least-once correctness.

### Arrow Conversion

Ported from ducklake-kafka-connect's `SinkRecordToArrowConverter`:

1. Parse JSON via `orjson` (Rust, ~1GB/s)
2. `pa.Table.from_pylist()` builds the columnar batch — PyArrow infers the superset schema across all dicts
3. v1 types: integers → INT64, floats → FLOAT64, all strings → VARCHAR, nested objects → JSON. No timestamp detection (see Deferred Complexity)

**Important**: `orjson` parses JSON integers as Python `int`, so `pa.Table.from_pylist()` infers INT64 for integer-only columns. A column that's INT64 in batch N and FLOAT64 in batch N+1 (because one value was `1.5`) causes type wobble. `_normalize_numeric_types()` casts all integers to INT64 and all floats to FLOAT64 after `from_pylist()` to prevent this.

**Caveat**: `pa.Table.from_pylist()` infers the schema from the first record's keys only. `arrow_converter.py` works around this by pre-scanning all records to build the full key union and passing an explicit schema to `from_pylist()`. The pre-scan is effectively free (pointer iteration over dict keys).

**`pa.null()` columns are filtered.** `_drop_null_typed_columns()` runs after type normalization and drops any column whose Arrow type is `pa.null()`. In normal use `_build_schema` falls back to `pa.string()` for keys that are None in every record, so this filter is defensive: a column with no schema information is a column with no data, and dropping it at the converter costs nothing — the column gets reintroduced with a real type on the next batch that has a non-null value.

### The Sink

`main.py` constructs a `DuckLakeSink` directly (`millpond/ducklake.py`) and calls exactly three methods on it: `write(batch)`, `reset_caches()`, `close()`. One implementation, no abstraction layer.

Per-Sink instance state: the table-ensured cache and the SchemaManager both live on the Sink instance, not at module level. Two Sink instances in the same process correctly do not share cache — each owns its own connection handle too. `reset_caches()` is called only by the write-retry loop in `main.py` after a failed write; the sink does not self-reset on internal recovery, it surfaces the failure and lets the retry path drive cache invalidation.

Empty-batch contract: callers must not invoke `write()` with a zero-row batch. `main.py` gates on `pending_records > 0` before flushing. Defensively, DuckLake creates the table eagerly on any call (including empty); the divergence from the gate isn't exercised in steady state, but the contract is documented on the `DuckLakeSink` docstring so any future caller knows it.

Reserved-column contract: source-schema columns must not collide with sink-managed metadata column names. `ducklake.py`'s `check_reserved_collision(batch_schema, reserved)` runs at the top of the module-level `write()` — raises `ValueError("Source schema column(s) [...] collide with DuckLake-reserved metadata column names...")` before any sink-specific work. `RESERVED_COLUMNS` is `{"_inserted_at", "year", "month", "day", "hour"}` — DuckLake produces only `_inserted_at` itself; the four partition cols stay reserved for historical reasons and because they're the conventional output names of the `DUCKLAKE_PARTITION_BY` expression. `SAFE_IDENTIFIER` (the regex for column names safe to embed in generated SQL) lives in `schema.py`.

### Optional record handling (filter + sort)

Two optional pre-sink stages, both implemented in `main.py` so the sink stays a pure write surface.

**Filter** (`_apply_filter` in `main.py`) runs immediately after `_convert_batch` and before records enter the pending buffer. Drops records whose value in `cfg.filter_keep_field` is not in `cfg.filter_values`. Tracks two skip reasons distinctly on `records_skipped_total`:

- `filter_field_missing` — column absent from batch schema, null for that row, or column type is not in the allowlist (integer / string / large_string only)
- `filter_excluded` — column present, value not in the allowlist

The column-type allowlist exists because PyArrow's `safe=True` cast happily coerces ints to bool/float/timestamp/date and silently produces semantically-wrong matches. Rejecting non-integer/non-string columns up front turns "silent surprising match" into "explicit `filter_field_missing` signal."

Cast direction is values → column (the small array to the big one's type), not column → fixed type. Three properties fall out of that:

1. Schema drift across batches is handled by construction (the cast is re-evaluated against the live column type each call, not against a type chosen at config load).
2. The hot path makes one pass over the column (the final `table.filter`) — `pc.is_in` returns null for null inputs natively, and `column.null_count` is O(num_chunks).
3. The cast site is wrapped in `try/except (ArrowInvalid, ArrowNotImplementedError, ArrowTypeError)` so an unexpected column shape lands a batch in `filter_field_missing` rather than killing the consume loop.

`MILLPOND_FILTER_DROP_FIELD_NAME` is reserved at the config layer (mutex with keep, both empty or exactly one set) and explicitly rejected at startup. The denylist implementation lives in a future commit; the namespace is locked today so that change doesn't require operator env-var churn.

**Sort** (`_apply_sort` in `main.py`) runs inside `_flush()` after `pa.concat_tables` but before `sink.write()`. The sink sees pre-sorted data; sink-side partition columns (year/month/day/hour, computed by the ducklake extension) are not in scope by design — operators specify sort keys against the source schema.

Missing-field handling: if any `cfg.sort_by` field is absent from the batch, the whole sort is skipped (rather than partially sorting on available keys, which would silently differ from intent). Records still flow through unsorted. The metric is `sort_skipped_total{reason="field_missing"}`, deliberately distinct from `records_skipped_total` because no data is being dropped — only the layout improvement is.

Log dedup: `_sort_missing_fields_warned` (module-level set) prevents per-flush log floods under sustained misconfiguration. One warning per distinct missing-fields pattern per pod lifetime; the metric is the always-on signal.

Cost: ~50–200 ms per flush at production batch sizes (mostly `pa.Table.take()`'s full-column rewrite); peak memory ~2× the flush buffer during the take. Sort coverage lives in `TestApplySort` in `tests/unit/test_main.py` — both stages run upstream of the sink boundary, so the sink stays a pure write surface.

### DuckLake Initialization

At startup, `ducklake.py` must:

```python
conn = duckdb.connect(config.ducklake_connection)
conn.execute("LOAD httpfs")       # must load before ducklake — race condition with S3 access
conn.execute("LOAD ducklake")
pg_connstr = f"host={config.rds_host} port={config.rds_port} dbname='{config.rds_database}' user='{config.rds_username}' password='{config.rds_password}'"
conn.execute(f"ATTACH 'ducklake:postgres:{pg_connstr}' AS lake (DATA_PATH '{config.ducklake_data_path}')")
```

Extensions are pre-installed in the Docker image at build time (no runtime network dependency). `httpfs` must be loaded before `ducklake` to avoid a race condition where ducklake tries to access S3 before httpfs is available.

### DuckLake Write

```python
conn.register('arrow_batch', table)
conn.execute("INSERT INTO lake.main.{table} SELECT *, NOW() AS _inserted_at FROM arrow_batch")
conn.unregister('arrow_batch')
```

Zero-copy Arrow scan. Table auto-created and evolved (ADD COLUMN, ALTER COLUMN SET DATA TYPE) to match Arrow schema. `_inserted_at` is added by the SQL `NOW()` at INSERT time, so rows in a single flush can have microsecond drift in their timestamps.

### Hive-Style Partitioning

If `DUCKLAKE_PARTITION_BY` is set, `_ensure_table()` runs `ALTER TABLE SET PARTITIONED BY (...)` after table creation. DuckLake writes files into Hive-style directories (`year=2026/month=3/day=23/hour=21/*.parquet`).

- Partitioning is applied on first write per pod lifetime (idempotent — `SET PARTITIONED BY` with the same expression is a no-op, safe for multiple pods and restarts)
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

The SchemaManager (`millpond/schema.py`):

1. `pa.Table.from_pylist()` infers superset schema across all records in the batch
2. Before write, compare `table.schema` against cached DuckLake table schema
3. New field → `ALTER TABLE ADD COLUMN IF NOT EXISTS`
4. Wider type → `ALTER TABLE ALTER COLUMN SET DATA TYPE` (DuckLake enforces widening-only)
5. Incompatible change → DuckLake rejects it, log + metric + skip (per-column; does not abort the flush)
6. `_inserted_at TIMESTAMP` added automatically, set to `NOW()` on write

Concurrency: DuckLake's idempotent DDL (`ADD COLUMN IF NOT EXISTS`) handles multi-pod races silently.

**Why schema evolution swallows per-column failures.** This is deliberate, not a bug: DuckLake runs one ALTER per column (`ADD COLUMN` or `ALTER COLUMN SET DATA TYPE`). Each statement is its own transaction. If column N's ALTER fails (e.g. an invalid narrowing the extension rejects), columns 1..N-1 have already committed; the rest can still proceed. Aborting the whole flush would discard the legitimate successes. So `SchemaManager.evolve()` logs the failure, bumps `errors_total{type="schema"}`, and continues with the remaining columns; the failed column simply stays at its old type and the batch's values for it land NULL (or get cast if DuckDB can coerce them).

**Column type coercion (`MILLPOND_TYPED_COLUMNS`).** JSON carries no type schema, so inference can diverge from the destination column, and DuckLake's widening-only evolution then rejects the narrowing `ALTER` every flush (logged + `errors_total{type="schema"}`, swallowed per the per-column failure path above) while the write stalls under DuckLake at INSERT. Two cases on the duckling backfill's `posthog.events`: date-times infer `VARCHAR` vs a `TIMESTAMPTZ` column, and `project_id` (the one numeric column the producer serializes as explicit JSON `null` — no serde skip) infers `VARCHAR` in an all-null batch vs `BIGINT`. `arrow_converter.coerce_typed_columns()` (called from `main.py` right after `convert()`, gated on the env var) pins named columns to a target type (`timestamptz`/`bigint`/`double`/`boolean`/`varchar`, via the `_COERCERS` registry) *before* `evolve()`, so the inferred type already matches the table and no ALTER is issued; it also makes millpond-created tables use the right type from the start. `timestamptz` parses the ClickHouse-events wire string (space-separated, UTC implied, 0/3/6 fractional digits) via cast-to-naive + `assume_timezone` (PyArrow's `strptime`/`%f` doesn't parse it; a direct tz-aware cast demands a zone offset); other targets are plain casts. A column already the target type, or absent from the batch, is skipped. Non-fatal and type-consistent per column: a present, configured column is always emitted as the target type — values that can't be cast are **nulled** (only the unconvertible ones; good values kept), never left as the source type, so buffered batches stay concat-compatible at flush (`pa.concat_tables`) instead of raising `ArrowTypeError` outside the write-retry path. Failures bump `errors_total{type="column_coercion"}` (loud via metrics) and never raise on the consume path (a raise there would unwind past the offset bookkeeping). Clean coercions count via `columns_coerced_total{target_type=...}`.

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
| `millpond_records_written_total` | Counter | Records written to the lake |
| `millpond_batches_flushed_total` | Counter | Flush cycles completed (by trigger: `size` or `time`) |
| `millpond_records_skipped_total` | Counter | Records skipped (by reason: json_parse, schema) |
| `millpond_errors_total` | Counter | Errors by type (kafka, write_retry, offset_commit, schema, …) |
| `millpond_arrow_conversion_seconds` | Histogram | Time to convert JSON to Arrow table |
| `millpond_flush_duration_seconds` | Histogram | Time per lake write |
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
| Partition count desync | Partition count discovered via `admin.list_topics()` at startup — no env var. `REPLICA_COUNT` env var must match `spec.replicas`; desync causes uneven assignment but not data loss (some partitions double-assigned, some unassigned). |
| Concurrent DDL from multiple pods (two pods both evolve schema simultaneously) | `ADD COLUMN IF NOT EXISTS` is idempotent — multiple pods racing is harmless. `ALTER COLUMN SET DATA TYPE` widening to the same target is also idempotent. Cannot designate a single schema-owner pod because schema discovery is distributed (new fields can appear in any partition). In practice, schema changes are rare — the primary use case (events) uses a stable schema that relies on maps/dictionaries for extensibility rather than adding columns. |
| Liveness probe only checks prometheus HTTP, not app health | **Mitigated.** `/healthz` and `/readyz` endpoints at `millpond/server.py` check `last_poll` recency against `max_poll_age_s=300`; pod reports 503 if no poll in the last 5 minutes. |

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
| Type promotion (int8→int16→int32→int64→float) | v1: integers normalized to INT64, floats to FLOAT64, all strings VARCHAR, nested objects JSON. Add finer promotion later if storage costs justify it. |
| Timestamp detection heuristic | v1: store as VARCHAR. Let query engine cast. The ISO8601 regex will misfire on non-timestamp strings that happen to match the pattern. Add opt-in timestamp columns later. When adding this, port the ID field heuristic from ducklake-kafka-connect's `SinkRecordToArrowConverter`: fields ending in `_uuid`, `uuid`, `_id`, `id`, `_key`, `key` must be forced to VARCHAR to prevent UUID strings like `"2024-02-28T23:59:59Z"` from being mis-inferred as timestamps. |

## DuckDB Logging

Unlike the JVM client (which supports custom log storage callbacks — see `duckdb-jvm`'s `NativeLogRouter.kt`), the Python client only supports `memory`, `stdout`, and `file` log storage. No way to route DuckDB internal logs into Python's `logging` module.

For now, DuckDB logging is left at defaults. If needed, enable with `CALL enable_logging(storage='stdout')` and DuckDB will write to stderr in CSV format alongside Python's structured logs.

## Releases

Every merge to `main` triggers `.github/workflows/release.yaml`:

1. Auto-bumps patch version from latest git tag (`v0.0.1` → `v0.0.2`)
2. Builds a source tarball (`millpond-v0.0.X.tar.gz`) containing `pyproject.toml`, `uv.lock`, and all source — attached to the GitHub release
3. Builds and pushes Docker image to `ghcr.io/posthog/millpond:<tag>` and `:latest`
4. Creates GitHub release with auto-generated changelog

The tarball is the primary artifact for external Docker builds (e.g. `posthog-cloud-infra`). It includes the lockfile so `uv sync --frozen` produces reproducible installs with pinned binary wheels. Do not distribute standalone wheels — they lack the lockfile and resolve unpinned deps from PyPI.

## Deployment Strategy

Rolling updates are a poor fit for static partition assignment — during the roll, pods run with different `REPLICA_COUNT` values, causing temporary double-assignment (duplicate writes) or gaps. Since Kafka is the durable buffer, a simpler strategy works:

1. **Canary**: Deploy one pod with the new version. Verify it consumes and flushes correctly (check metrics, lag, error rate).
2. **Graceful shutdown**: Scale the StatefulSet to 0. All pods flush pending writes, commit offsets, and exit. Partitions stop being consumed — Kafka holds the data.
3. **Full redeploy**: Update the image/config, scale back up. Each pod picks up from committed offsets. Zero data loss.

Downtime = time to drain + time to start new pods. With `terminationGracePeriodSeconds: 120` and typical S3 flush latency, expect ~2-3 minutes of no consumption. Kafka buffers this trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically. If someone scales without Helm, partitions will be unevenly or doubly assigned until corrected.

## HTTP Server

Prometheus metrics and health checks on port 8000 via a custom `http.server.HTTPServer` (`prometheus_client.start_http_server()` doesn't support custom routes):

- `GET /metrics` → Prometheus exposition format (via `prometheus_client.generate_latest()`)
- `GET /healthz` → liveness: 200 if started and last poll within `max_poll_age_s` (300s), 503 otherwise
- `GET /readyz` → readiness: 200 if started and last poll within `max_poll_age_s` (300s), 503 otherwise. A consumer with no incoming data is still ready — it's sitting in its consume-write loop.

## Toolchain

- **Flox**: System dependencies (Python 3.12+, uv)
- **uv**: Python package management. `pyproject.toml` is source of truth, `uv.lock` is committed.

## Dependencies

| Package | Why |
|---------|-----|
| `confluent-kafka>=2.6` | librdkafka Python wrapper |
| `duckdb==1.5.2` | DuckDB Python client (pinned; the ducklake extension is sensitive to minor-version moves) |
| `pyarrow>=18.0` | Arrow tables, zero-copy DuckDB integration |
| `orjson>=3.10` | Fast JSON parsing (Rust) |
| `prometheus-client>=0.21` | Metrics exposition |
| `pytz>=2024.1` | Required by duckdb's TIMESTAMPTZ Python conversion (1.5.x doesn't accept stdlib zoneinfo) |
| `pyyaml>=6.0.3` | `tools/ducklake_metrics.py` query definitions |
| `opentelemetry-sdk>=1.30` | Structured-log OTLP export plumbing for millpond's PostHog Logs export |
| `opentelemetry-exporter-otlp-proto-http>=1.30` | OTLP/HTTP exporter — ships millpond logs to PostHog Logs when `POSTHOG_PROJECT_TOKEN` is set |
| `opentelemetry-instrumentation-logging>=0.50b0,<0.64` | Ships the non-deprecated `LoggingHandler` (SDK 1.42 deprecated the `sdk._logs` one); ceiling bounds surprise since the handler's module path isn't the package's documented public surface |

## Project Structure

```
millpond/
├── pyproject.toml            # Dependencies and metadata (uv source of truth)
├── uv.lock                   # Resolved dependency lock (committed)
├── .flox/                    # Flox environment
├── .github/workflows/
│   ├── ci.yaml               # Format, lint, unit tests on PR/push
│   └── release.yaml          # Auto-version, tarball, Docker image, GitHub release
├── Dockerfile                # Builds one image with the `millpond` console-script entry point
├── docker-compose.yaml       # DuckLake dev stack (Kafka, Postgres, MinIO, Grafana)
├── docker-compose.ssl.yaml   # Overlay adding SSL Kafka to docker-compose.yaml
├── k8s/
│   ├── statefulset.yaml
│   ├── service.yaml          # Headless service for StatefulSet
│   └── pdb.yaml              # PodDisruptionBudget
├── millpond/                 # The writer
│   ├── __init__.py
│   ├── main.py               # Entry point, main loop, signal handling
│   ├── config.py             # Env var → dataclass; startup validation
│   ├── arrow_converter.py    # JSON → PyArrow Table (orjson + from_pylist + numeric normalization)
│   ├── ducklake.py           # DuckLake backend: connect, write, DuckLakeSink class
│   ├── schema.py             # DuckLake SchemaManager
│   ├── consumer.py           # Kafka consumer + AdminClient for partition discovery
│   ├── backpressure.py       # Adaptive batch sizing
│   ├── metrics.py            # Prometheus metric definitions
│   ├── logging_config.py     # Two-phase logging setup + PostHog Logs OTLP attach
│   ├── structured_logging.py # JSON formatter + optional OTLP/HTTP export to PostHog Logs
│   └── server.py             # HTTP server for /metrics, /healthz, /readyz
├── tools/
│   ├── ducklake_maintenance.py     # Self-contained DuckLake maintenance script (K8s CronJob, DuckLake-only)
│   ├── ducklake_maintenance.sql    # Macros loaded at session start
│   ├── ducklake_metrics.py         # Long-running DuckLake state-metrics daemon (DuckLake-only)
│   ├── justfile                    # Interactive wrapper for ducklake_maintenance.py + DuckDB CLI shell
│   └── sizing-calculator.html      # Interactive flush/object sizing calculator
├── tests/
│   ├── unit/                 # Fast, no external deps
│   ├── integration/          # In-memory DuckDB write/metrics tests — no docker stack
│   └── e2e/                  # Full docker-compose DuckLake stack via testcontainers
└── test/                     # Dev fixtures (producer.py, ducklake-init.sql)
```

`tools/` scripts are DuckLake-specific (maintenance and state-metrics).
