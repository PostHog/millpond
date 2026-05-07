# Plan: DuckLake state-metrics daemon

Goes in `tools/` as a single Python file, `ducklake_metrics.py`. Small Python daemon that runs catalog-side queries on a schedule and exposes Prometheus metrics. Reuses the millpond image. New deps added via `uv add`. Built-in queries are embedded in the file (as a YAML string parsed through the same loader as user-supplied YAML); user can supply additional YAML to extend or override.

## Shape

Single file. Connects once via `maintenance.connect()` — that already configures the lake ATTACH, the Postgres extension ATTACH, the S3 secret, `temp_directory`, and the `enable_http_logging` / `pg_debug_show_queries` quiets from a14. Reusing it means the daemon inherits all that without duplication. Queries run sequentially on per-query schedules. Prometheus served via `prometheus_client.start_http_server`.

## Layout (one file)

`tools/ducklake_metrics.py` contains:

- `Query` dataclass — name, help, sql, interval_minutes, labels[], values[]
- `BUILTIN_YAML` — multi-line string constant with the built-in queries
- `load_queries(user_yaml_path) -> list[Query]` — parses built-in + optional user YAML, merges by name (user wins)
- `run_query(conn, q, gauges)` — executes, updates gauges, captures errors
- `main()` — schedule loop + Prom server + health endpoints

## YAML schema

```yaml
queries:
  - name: ducklake_files_per_band
    help: Live data files grouped by size band
    interval_mins: 1        # whole minutes only; minimum 1
    labels: [band]          # column names → Prom labels
    values: [file_count, total_bytes]
    sql: |
      SELECT ... GROUP BY band
```

`values: [a, b]` → emits `<name>_a` and `<name>_b`. Single-value queries: `values: [count]`. Type is always gauge (no `type` field — kept simple).

## Scheduling

Single thread, min-heap of `(next_run_ts, query)`. Sleep until top, run, requeue. Sequential — duckdb conn isn't thread-safe across writes anyway and queries are short (catalog reads).

## Multi-row handling

Each tick: `Gauge.clear()` then re-populate from rows. Avoids stale label combinations.

## Self-metrics

| Metric | Type | Labels |
|---|---|---|
| `state_metrics_query_duration_seconds` | gauge | query |
| `state_metrics_query_errors_total` | counter | query |
| `state_metrics_query_last_success_timestamp` | gauge | query |
| `state_metrics_up` | gauge | — |

## Built-in queries (FOLLOWUPS b2–b6)

Built-ins are lake-wide — no `table_name` label. User-supplied YAML may emit table-specific or table-labeled metrics if the operator wants them; that's their call. The built-in set keeps the daemon table-unaware.

| ID | Name | Labels | Values |
|---|---|---|---|
| b2 | `ducklake_files_per_band` | `band` | `count`, `bytes` |
| b3 | `ducklake_compaction_candidates` | `tier` (`tier1/2/3/large/total`) | `count` |
| b4 | `ducklake_pending_deletes` | — | `total`, `unique_paths`, `dup_rows` |
| b5 | `ducklake_snapshots` | — | `count`, `oldest_seconds_ago`, `newest_seconds_ago` |
| b6 | `ducklake_files_per_partition_top20` | `partition` | `count` |
| b7 | placeholder, no SQL until DuckLake c6 lands | — | — |

## K8s-native health

| Endpoint | Semantics |
|---|---|
| `/-/healthy` (liveness) | 200 if process responsive — not gated on query freshness |
| `/-/ready` (readiness) | 200 once Prom port is open and first tick has been scheduled — not gated on any individual query completing |
| `/metrics` | served by `prometheus_client` |

Readiness must NOT wait for slow queries. The Prom port opens immediately at startup; `/metrics` may briefly return only self-metrics until the first scheduled queries run.

## Config (env-driven, matches `maintenance.py`)

| Var | Purpose |
|---|---|
| `MILLPOND_PG_*` | catalog connection (already used) |
| `MILLPOND_S3_*` | S3 secret |
| `STATE_METRICS_PORT` | default `9100` |
| `STATE_METRICS_CONFIG` | optional path to user YAML |
| `STATE_METRICS_DISABLE` | comma-separated names to skip from built-ins |

## Failure mode

Catalog unreachable: log + retry forever with exponential backoff; `state_metrics_up = 0`; process does not exit. Liveness still 200 (process is alive, just disconnected — k8s shouldn't restart it because RDS is flapping).

## Deps & justfile

`uv add prometheus_client pyyaml`. New `justfile` recipe `state-metrics` for local run. K8s manifest is chart territory — out of scope for this PR.

## Decisions (resolved)

| # | Question | Resolution |
|---|---|---|
| D1 | Single-file vs subdir | single file: `tools/ducklake_metrics.py` |
| D2 | Multi-lake support | one lake per pod |
| D3 | Built-ins embedded vs separate YAML file | embedded as YAML string in the .py — same loader path |
| D4 | Histogram for query duration | gauge only |
| D5 | Healthcheck endpoint | k8s-native: `/-/healthy` (liveness) + `/-/ready` (readiness) |
| D6 | First-scrape semantics | open Prom port immediately; do not block on slow queries |
| D7 | Failure mode if catalog unreachable | retry with backoff; `up = 0`; don't exit |
| D8 | Per-table breakdown | built-ins are lake-wide; user YAML may go table-specific |
| D9 | Interval format | YAML field `interval_mins` (positive integer, whole minutes; minimum 1). Unit is encoded in the field name — no suffix parsing. |
