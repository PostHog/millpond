# tools/

Operational utilities that ship in the millpond Docker image but live outside the `millpond` package. Same env vars as the main app (`DUCKLAKE_RDS_*`, `DUCKDB_S3_*`, `DUCKLAKE_DATA_PATH`); none of these import from `millpond`.

In the image:

```
/app/tools/ducklake_maintenance.py
/app/tools/ducklake_maintenance.sql
/app/tools/ducklake_metrics.py
/justfile                        (copy of tools/justfile)
```

For local dev, the `DUCKLAKE_MAINTENANCE_SCRIPT`, `DUCKLAKE_MAINTENANCE_SQL`, and `DUCKLAKE_METRICS_SCRIPT` env vars override the in-image paths so `just …` recipes work from a checkout.

---

## ducklake_maintenance.py

Self-contained CLI for DuckLake catalog and storage maintenance. Designed to run as a K8s CronJob reusing the same image and credentials as the main app. Subcommands:

| Group | Subcommand | Purpose |
|---|---|---|
| Snapshot lifecycle | `expire` | Expire snapshots older than N days |
| File lifecycle | `cleanup` | Delete files scheduled for deletion older than N days |
| File lifecycle | `cleanup-all` | Delete all files scheduled for deletion regardless of age |
| File lifecycle | `dedup-deletions` | Drop duplicate rows from `ducklake_files_scheduled_for_deletion` (workaround for DuckLake bug c5) |
| File lifecycle | `find-orphans` | List catalog rows whose S3 key no longer exists (read-only) |
| File lifecycle | `heal-orphans` | Delete those catalog rows; gated by safety checks B1 (data-file table non-empty AND no orphan path is still live) and B3 (no positional-delete vector references an orphan id) |
| File lifecycle | `cleanup-all-safe` | Loop dedup + heal-orphans + cleanup-all under one advisory lock until cleanup-all exits clean |
| File lifecycle | `fsck` | `cleanup-all-safe` + `ducklake_delete_orphaned_files` (S3-side sweep) |
| File lifecycle | `orphans` | Delete S3-side orphaned files (no catalog row references them) |
| File lifecycle | `maintain` | `expire` + `cleanup` |
| File lifecycle | `checkpoint` | DuckLake `CHECKPOINT` (integrated merge + expire + cleanup) |
| Compaction | `compact --tier {1,2,3}` | Tiered compaction; bin ranges `[0, 1 MiB)` → `~5 MiB`, `[1 MiB, 10 MiB)` → `~32 MiB`, `[10 MiB, 64 MiB)` → `~128 MiB`. Bounds DuckDB resource use via `--threads` (default 2) and `--memory-limit` (default 4GB); raise on lakes that fit comfortably |
| Compaction | `compact-probe` | Lightweight diagnostic: merge up to N adjacent files in one table, no `target_file_size` change |

All destructive subcommands take `pg_try_advisory_lock(hashtext('millpond-ducklake-maintenance')::bigint)` on the `pg` ATTACH; concurrent invocations bail rather than racing each other's DELETEs. The lock provides mutual exclusion *between maintenance invocations* — it does not serialize against arbitrary catalog writers (e.g. ingest pods).

Every `cleanup` / `cleanup-all` (skipped on `--dry-run`) logs a single structured throughput line: `cleanup throughput: files_processed=N elapsed_s=T rate_obj_s=R queue_depth_after=A`. `--debug` flips DuckDB's HTTP logging and the postgres extension's `pg_debug_show_queries` back on for short-lived debugging; both are off by default because they add per-call overhead that compounds across tens of thousands of S3 deletes.

If `PUSHGATEWAY_URL` is set, the script pushes `maintenance_start_time{operation}` (on start) and `maintenance_duration_seconds{operation, status}` (on completion) to a Prometheus Pushgateway, enabling Grafana annotation queries for maintenance windows.

## ducklake_maintenance.sql

Executed verbatim at every session start by both `ducklake_maintenance.py`'s `connect()` and the `just shell` recipe. No templating — `__ducklake_metadata_lake` and friends are written literally so both load paths stay consistent. Defines runtime macros:

- `count_pending_dups()` — duplicate-row count in the pending-deletion queue
- `find_catalog_orphans(data_path)` — catalog rows whose S3 key no longer exists, scanned via `glob()` against the live S3 listing

The header documents the constraints any new recipe must follow:

- No `LEFT ANTI JOIN` (DuckDB 1.4 doesn't have it; use `LEFT JOIN … WHERE rhs IS NULL` or `NOT EXISTS (…)`)
- No Postgres `ctid` from duckdb-side SQL — the duckdb postgres extension doesn't expose system columns; use `postgres_execute` / `postgres_query` for ctid-based DML
- No literal `glob('s3://…')` inside `CREATE MACRO` bodies — DuckDB 1.4 evaluates them eagerly at macro creation, which would S3-LIST the lake on every connect. Pass the path as a parameter and call `glob()` at macro invocation time.
- Advisory-lock key (`hashtext('millpond-ducklake-maintenance')::bigint`) is the single source of truth — duplicate it nowhere else.

## ducklake_metrics.py

Long-running Prometheus-exposition daemon for catalog-side lake-state metrics. Single Python file. Single thread for queries (one DuckDB connection isn't safe for concurrent calls anyway), separate thread for the HTTP server. Reuses `ducklake_maintenance.connect()`.

Endpoints: `/metrics`, `/-/healthy` (k8s liveness — always 200 while the process answers), `/-/ready` (k8s readiness — 200 after the first successful connect; never gated on individual query completion so slow queries can't block rollout).

Queries are described in YAML and parsed through one loader regardless of whether they came from the embedded `BUILTIN_YAML` constant or from a file pointed at by `DUCKLAKE_METRICS_CONFIG`. User YAML extends the built-ins by name (user wins on collision); `DUCKLAKE_METRICS_DISABLE` drops named built-ins.

YAML field reference:

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | yes | Becomes the metric prefix; matches `^[a-zA-Z_][a-zA-Z0-9_]*$` |
| `help` | string | yes | Prometheus HELP line |
| `interval_mins` | positive integer | yes | Whole minutes, ≥1. The unit is in the field name — no `1m`/`30s` suffix parsing |
| `sql` | string | yes | Must return columns named in `labels` + `values` |
| `labels` | list[string] | optional | Column names → Prometheus label dimensions |
| `values` | list[string] | yes | Column names → metric suffixes; metric name is `<name>_<value>` |

Every metric is a gauge — no `type` field. For a query with `labels: [band]` and `values: [count, bytes]`, the registered metrics are `<name>_count{band="…"}` and `<name>_bytes{band="…"}`.

Built-in queries (lake-wide; no `table_name` label by design):

| Name | Labels | Values | Source |
|---|---|---|---|
| `ducklake_pending_deletes` | — | `total`, `unique_paths`, `dup_rows` | `ducklake_files_scheduled_for_deletion` |
| `ducklake_files_per_band` | `band` | `count`, `bytes` | `ducklake_data_file` |
| `ducklake_compaction_candidates` | `tier` | `count` | `ducklake_data_file`; tier buckets match `ducklake_maintenance.py`'s `TIERS` (`tier1` < 1 MiB, `tier2` [1, 10) MiB, `tier3` [10, 64) MiB, `large` ≥ 64 MiB, plus `total`) |
| `ducklake_snapshots` | — | `count`, `oldest_seconds_ago`, `newest_seconds_ago` | `ducklake_snapshot`; CASTs `snapshot_time` (VARCHAR) to TIMESTAMPTZ before time arithmetic |
| `ducklake_files_per_partition_top20` | `partition` | `count` | Composite partition values joined with `/`; live files without partition_value rows surface as `<none>` |
| `ducklake_catalog` | `suffix` | `format_version` | `ducklake_metadata` row with `key='version'` and `scope IS NULL`. Numeric `major.minor` (extracted via `regexp_extract`) lands in the gauge value; any trailing tag DuckLake attaches (`-dev1` on main after `MigrateV10`, future `-rcN`/`-betaN` shapes) lands in the `suffix` label. Empty `suffix=""` for clean releases. Polled every 60 minutes — value changes only on a DuckLake upgrade |

Self-metrics (always on):

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `ducklake_metrics_up` | gauge | — | 1 while connected; 0 during reconnect |
| `ducklake_metrics_query_duration_seconds` | gauge | `query` | Wall-clock duration of the most recent run |
| `ducklake_metrics_query_last_success_timestamp` | gauge | `query` | Unix ts of the most recent successful run |
| `ducklake_metrics_query_errors_total` | counter | `query` | Cumulative failed runs |

Reconnect: connect failures retry with exponential backoff (1s → 60s cap). Once connected, transient query failures only increment the per-query error counter and log; the daemon stays up. After `CONSECUTIVE_FAILURE_THRESHOLD` (default 10) runs in a row across all queries fail, the scheduler raises `_ReconnectNeeded` and the outer loop drops the connection and reconnects via the same backoff. Any successful run resets the streak.

Env vars (in addition to the `DUCKLAKE_*` / `DUCKDB_*` set used by `ducklake_maintenance.py`):

| Variable | Default | Notes |
|---|---|---|
| `DUCKLAKE_METRICS_PORT` | `9100` | HTTP listen port |
| `DUCKLAKE_METRICS_CONFIG` | unset | Path to user-supplied queries YAML |
| `DUCKLAKE_METRICS_DISABLE` | unset | Comma-separated names to skip from built-ins |

## justfile

Recipe wrapper for both maintenance and metrics. Copied to `/justfile` in the image.

Groups visible in `just --list`:

- `[interactive]` — `shell` opens a DuckDB session with `lake` and `pg` ATTACHed, S3 SECRET configured, `ducklake_maintenance.sql` macros loaded
- `[lifecycle]` — every snapshot/file maintenance subcommand of `ducklake_maintenance.py`, both `*` and `*-dry-run` variants
- `[compaction]` — tiered compaction recipes plus `compact-probe`
- `[bootstrap]` — `bootstrap-index-*` per-index recipes (idempotent `CREATE INDEX CONCURRENTLY IF NOT EXISTS` against the DuckLake catalog schema) and `bootstrap-indexes` umbrella; one-shot use against a freshly instantiated DuckLake
- `[metrics]` — `ducklake-metrics`, `ducklake-metrics-with-config`, `ducklake-metrics-list`

The bootstrap recipes shell out to `psql` directly rather than going through the DuckDB ATTACH path used by everything else: `CREATE INDEX CONCURRENTLY` cannot run inside a transaction block, and the duckdb postgres extension wraps every `postgres_execute` call in one. They reuse the same `DUCKLAKE_RDS_*` env vars; `PGPASSWORD` is exported via env (not args) so the password doesn't show up in `ps`, and every interpolated credential goes through just's `quote()` builtin so values containing `'`, `"`, `$`, or `\` are shell-safe. The umbrella runs the 8 builds sequentially — same-relation `CONCURRENTLY` builds serialize on Postgres's `ShareUpdateExclusiveLock` anyway, and sequential output keeps the operator log linear.

Path overrides for dev use: `DUCKDB` (binary path), `DUCKLAKE_MAINTENANCE_SCRIPT`, `DUCKLAKE_MAINTENANCE_SQL`, `DUCKLAKE_METRICS_SCRIPT`.

The `_setup` constant mirrors what `ducklake_maintenance.connect()` does (S3 secret, ATTACH lake + pg, `temp_directory`) so an interactive `just shell` ends up with the same wired-up session as a maintenance subcommand. The two-layer escaping for the Postgres connection string is documented inline.

## sizing-calculator.html

Single self-contained HTML page (hosted at <https://posthog.github.io/millpond/sizing-calculator.html>) that estimates millpond pod memory and Parquet object size from inputs like message rate, partitions, and flush settings. No backend; opens in any browser. See README's "Object Sizing" section for the underlying model.
