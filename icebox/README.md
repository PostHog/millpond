# icebox

A polling-daemon committer for high-concurrency Iceberg writes. Each
icebox instance fronts exactly one Iceberg `(namespace, table)` pair
and serializes commits from many concurrent millpond writer pods,
eliminating PyIceberg's REST-catalog optimistic-concurrency contention.

The v6 polling-daemon design is documented in
[`../docs/icebox-self-healing-recovery.md`](../docs/icebox-self-healing-recovery.md);
this README is the operational quick-reference for the running code.

## Why

The millpond writer's direct Iceberg sink commits via PyIceberg's REST
client. Each commit attaches a branch-snapshot requirement
(`expected id != actual id`); two writers committing at the same flush
cadence race against the catalog and the loser retries with
exponential backoff. Under sustained dual-writer load at
`FLUSH_INTERVAL_MS=5000` (let alone the 32 writers production targets),
retries pile up and the pod exits.

icebox solves this with a writer/daemon split:

```
32× millpond writers ──INSERT───▶ icebox_files (PG)
                       (Parquet to S3)              │
                                                    ▼
                                              icebox daemon
                                                    │
                                                    ├──tick──▶ Lakekeeper
                                                    └──tick──▶ Kafka offset commit
```

- **Writers write parquet to S3** and INSERT a row into the
  `icebox_files` PG table (the writer-side `IceboxClient` uses a small
  psycopg pool; see [`millpond/icebox_sink.py`](../millpond/icebox_sink.py)).
  `ON CONFLICT (file_path) DO NOTHING` makes writer replay idempotent
  (201 on a new row, 409 on a same-path replay).
- **One icebox per `(Iceberg namespace, table)` per environment.** Each
  deployment owns its own PG schema (`ICEBOX_PG_SCHEMA`) and serves
  exactly one Iceberg table. The chart enforces `replicas: 1` +
  `strategy.type: Recreate`; SKIP LOCKED is belt-and-suspenders in case
  two daemons ever overlap during a rollout.
- **The daemon polls** `icebox_files` every `ICEBOX_COMMITTER_CADENCE_SECONDS`
  (default 60s) with `SELECT … WHERE result='pending' AND inserted_at <
  now() - interval '<age_filter_seconds>' FOR UPDATE SKIP LOCKED LIMIT
  <batch_size>`. Each tick commits one batch (default 100 rows) to
  Iceberg as a single snapshot.
- **Kafka offsets are advanced by the daemon**, not by the writers.
  Writers use `consumer.assign()` and never join the consumer group;
  the daemon holds the offset-commit responsibility for the group via
  the Kafka AdminClient. The offset commit runs **after** the PG
  transaction commits — so the invariant *Kafka offset committed iff
  PG knows the file's fate* survives daemon crashes between PG COMMIT
  and AdminClient ack (next tick covers the gap via cumulative-offset
  semantics).

## Failure model

| Failure | What the daemon does |
|---|---|
| No pending rows | Vacuous tick — stamp heartbeat, return |
| Iceberg transport failure (`requests.{Timeout, ConnectionError, HTTPError, RequestException}` / `TimeoutError` from `with_timeout` / `CommitFailedException` / `CommitStateUnknownException`) | Rows stay `pending` (no UPDATE). No Kafka commit. Stamp heartbeat. Next tick retries |
| Iceberg rejects a single row at `build_data_file` (e.g., partition_values missing) | That row marked `failed` inline; the rest of the batch proceeds normally. Kafka offsets advance past the full batch (the failed row's events are lost from Iceberg; downstream UUID dedup is irrelevant here — operator audits via `result='failed'`) |
| Iceberg rejects the whole batch with a non-transport error | Every row marked `failed`. Kafka offsets advanced past the batch (the "make progress" tradeoff: pipeline keeps moving, operator audits later) |
| Daemon dies mid-tick | PG tx rolls back; rows stay `pending`. Next tick re-claims them |
| Daemon dies after Iceberg commit, before PG UPDATE | Iceberg has the files; PG rolled back. Next tick re-commits — PyIceberg silently accepts the duplicate file_path (verified against pinned 0.11.1). Downstream UUID dedup absorbs the duplicate at query time. Cost: 2x read of those events until snapshot expiration |
| Daemon dies after PG COMMIT, before Kafka offset commit | PG committed; Kafka behind. Writer replays from Kafka; same deterministic file_path hits `ON CONFLICT DO NOTHING`. Next tick advances offsets cumulatively |
| Lakekeeper unreachable | Tick increments `icebox_lakekeeper_failures_total` and returns. After enough failed ticks the heartbeat goes stale → `/healthz` 503 → k8s restarts the pod (no longer doing useful work anyway) |
| PG unreachable | `daemon_loop` catches `psycopg.Error`, increments `icebox_pg_unreachable_total`, sleeps the cadence, retries. If the pool is closed (e.g., shutdown), the loop exits |

The doc's "Failure modes" table and the v6 invariants section have the
full discussion: <../docs/icebox-self-healing-recovery.md>.

## Probe endpoints

The daemon binds `ICEBOX_API_HOST:ICEBOX_API_PORT` (default
`0.0.0.0:8000`) with a `ThreadingHTTPServer` serving:

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | k8s liveness. 200 if `status.last_committer_heartbeat` is within `cadence × heartbeat_stale_multiple` (default 3×). 503 on stale, NULL, or PG unreachable. Boot-seeded by `main.py` before the daemon thread starts so a slow first tick doesn't race the kubelet probe |
| `GET /metrics` | Prometheus exposition |

There is no `/v1/files` / `POST` surface anymore — writers INSERT
directly. The v6 rewrite [removed the HTTP API
entirely](../docs/icebox-self-healing-recovery.md#what-this-doesnt-try-to-do).

## Metrics

See `icebox/metrics.py` for the full list with descriptions and bucket
boundaries. Headline metrics for dashboards/alerting:

| Metric | Type | Use |
|---|---|---|
| `icebox_files_count{result}` | gauge | Backlog signal (`pending`), audit queue size (`failed`), throughput trend (`committed`) |
| `icebox_files_oldest_pending_seconds` | gauge | Drain-rate signal. -1 when no pending rows |
| `icebox_files_bytes{result}` | gauge | "How much data is stuck" — alert on `pending` / `failed` sum |
| `icebox_tick_duration_seconds{outcome}` | histogram | End-to-end tick budget. Outcomes: `success`, `vacuous`, `transport_failure`, `batch_failure` |
| `icebox_iceberg_commit_duration_seconds` | histogram | Lakekeeper p99 visible without Lakekeeper-side instrumentation |
| `icebox_kafka_commit_duration_seconds` | histogram | AdminClient cost (runs post-tx, outside the pool conn) |
| `icebox_batch_size` | histogram | Tick batch size. Saturating the top bucket = increase `ICEBOX_COMMITTER_MAX_PENDING_FILES` |
| `icebox_files_committed_total` / `_failed_total` / `icebox_records_committed_total` | counter | Throughput / audit growth rate |
| `icebox_last_success_at` | gauge (unix time) | Alert: `now() - last_success_at > N AND files_count{result='pending'} > 0` |
| `icebox_ticks_total{outcome}` | counter | Combined with `last_success_at` distinguishes "alive but stuck" from "alive and progressing" |
| `icebox_lakekeeper_failures_total` / `icebox_batch_failures_total` / `icebox_iceberg_timeout_total` / `icebox_pg_unreachable_total` | counter | Failure-mode partitioning for incident attribution |
| `icebox_iceberg_table_*` (6 gauges) | gauge | Iceberg snapshot summary values (cumulative + per-tick delta). Updated free on every successful commit — no extra Lakekeeper round-trip |

## Configuration

All config is env-driven via `icebox/config.py`. The chart's
`icebox.yaml` template wires values 1:1 to env vars. Selected
high-impact knobs:

| Env var | Default | Notes |
|---|---|---|
| `ICEBOX_PG_HOST` / `DATABASE` / `PASSWORD` / `SCHEMA` | (required) | Postgres connection essentials |
| `ICEBOX_PG_PORT` / `SSLMODE` / `USERNAME` | `5432` / `require` / `lakekeeper` | The username default exists because the icebox reuses Lakekeeper's PG role (see "Operator prereqs" below) |
| `ICEBOX_ICEBERG_CATALOG_URI` / `NAMESPACE` / `TABLE` | (required) | Lakekeeper catalog + this deployment's table |
| `ICEBOX_ICEBERG_WAREHOUSE` | `ingest` | Warehouse name on the catalog |
| `ICEBOX_KAFKA_BOOTSTRAP_SERVERS` / `TOPIC` / `GROUP_ID` | (required) | Kafka group whose offsets the daemon advances on the writer's behalf |
| `ICEBOX_KAFKA_EXTRA_CONFIG` | `{}` | JSON dict merged into the AdminClient config (e.g., security.protocol, sasl.*) |
| `ICEBOX_COMMITTER_CADENCE_SECONDS` | `60` | Tick interval |
| `ICEBOX_COMMITTER_MAX_PENDING_FILES` | `100` | Max rows per tick. Lower → bad-batch blast radius smaller; higher → fewer Iceberg snapshots per unit ingest |
| `ICEBOX_AGE_FILTER_SECONDS` | `60` | Pending rows younger than this aren't eligible — gives the writer time to accumulate enough files for a worthwhile snapshot |
| `ICEBOX_ICEBERG_TIMEOUT_S` | `5` | Wall-clock budget for the Iceberg commit (via `with_timeout`). Bounds row-lock hold time during Lakekeeper degradation |
| `ICEBOX_COMMITTER_HEARTBEAT_STALE_MULTIPLE` | `3.0` | `/healthz` returns 503 when `now() - heartbeat > cadence × stale_multiple` |
| `ICEBOX_PSYCOPG_POOL_MIN` / `_MAX` | `1` / `4` | Pool budget. Daemon tick holds 1 conn across the Iceberg commit; `refresh_state_gauges` holds 1; probes need ≥1 more. Max floor is 3 |
| `ICEBOX_API_HOST` / `_PORT` | `0.0.0.0` / `8000` | Bind address for the probe HTTP server (`/healthz` + `/metrics`) |

## Structured logging + PostHog Logs

JSON-by-default (`ICEBOX_LOG_FORMAT=json`). When `POSTHOG_PROJECT_TOKEN`
is set the daemon additionally exports log records to PostHog Logs via
standard OTLP/HTTP (`https://us.i.posthog.com/i/v1/logs` by default;
override via `POSTHOG_LOGS_ENDPOINT`). The `BatchLogRecordProcessor` is
flushed on the SIGTERM drain path so in-flight batches reach PostHog
before the process exits.

### Resource attribute taxonomy

Split by ownership so the app and the chart never set the same key.

**App-owned** (passed by `icebox/main.py` → `setup_logging`):

| Attr | Value | Why |
|---|---|---|
| `service.name` | `icebox` (constant) | One binary, one service. Per-instance differentiation is on `service.instance.id` |
| `service.namespace` | `millpond` (override via `ICEBOX_SERVICE_NAMESPACE`) | OTel-semconv "logical service grouping" |
| `service.instance.id` | The consumer key, e.g. `events-icebox` (from `ICEBOX_SERVICE_INSTANCE_ID`) | The per-`(namespace, table)` axis |
| `service.version` | The millpond package version | |
| `messaging.system` | `kafka` (only when Kafka attrs are set) | OTel messaging semconv |
| `messaging.destination.name` | The Kafka topic | OTel messaging semconv |
| `messaging.kafka.consumer.group` | The icebox-side consumer group id | OTel messaging semconv |
| `icebox.iceberg.warehouse` / `namespace` / `table` | Lakekeeper warehouse, namespace, table | Vendor-prefixed (no OTel semconv coverage for Iceberg today) |

**Chart-owned** (`OTEL_RESOURCE_ATTRIBUTES` env on the icebox
Deployment — auto-merged into the resource):

- `deployment.environment` (e.g. `managed-warehouse-prod-us`)
- `k8s.cluster.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`
- `host.hostname`

## Tests

| Path | Scope |
|---|---|
| `tests/unit/test_icebox_*.py` | Unit tests for each module: schema/DDL, postgres_sync SQL + helpers, daemon tick paths (success / vacuous / transport failure / batch failure / per-row partitioning), `with_timeout`, iceberg commit shape, structured logging |
| `tests/integration/test_icebox_*.py` | testcontainers Postgres + mocked Lakekeeper. Covers happy path, age filter, SKIP LOCKED concurrency (event-gated to actually exercise the disjoint claim), transport failure, batch failure, crash mid-tick (PG side), heartbeat-on-every-exit-path, daemon-loop drain budget, Kafka commit ordering (post-tx, outside the pool conn), writer-side INSERT + replay 409, probe `/healthz` (200/503 stale/503 NULL/503 PG-unreachable) + `/metrics`, migration idempotency, boot-sequence heartbeat seed |

Real-Lakekeeper docker-compose coverage is deferred (per the v6 doc's
"Out of scope" section).

## Known design constraints

- One icebox per `(Iceberg namespace, table)`. Each deployment owns its
  own PG schema; schema isolation is enforced via
  `options=-csearch_path=<schema>` on every pool connection.
- Schema names are validated as lowercase ASCII identifiers at
  config-load time (no PG protocol support for parameterized session
  options).
- Chart enforces `replicas: 1` + `strategy.type: Recreate`. SKIP LOCKED
  is the row-coordination primitive; if multi-replica becomes
  warranted, the design is natively safe (writer replay absorbs the
  brief Kafka-offset reorder window).
- The PG advisory lock that the cycle-era code used is **gone**. The
  v6 rewrite replaces it with SKIP LOCKED + chart-level singleton; the
  Aurora-failover concern that previously applied to the lock is no
  longer relevant.

## Operational notes

### TCP keepalives

Neither the psycopg pool nor the Kafka AdminClient sets TCP keepalives
explicitly. NLB/ELB default idle timeout is 350s; an idle connection
beyond that gets silently RST and the next query races
dead-connection detection. Add `keepalives=1 keepalives_idle=30
keepalives_interval=10 keepalives_count=3` on the psycopg conninfo if
this becomes operationally visible.

### Operator prereqs for the bootstrap helpers

`ensure_database_exists` requires the icebox PG user to have
`CREATEDB`. `ensure_schema_exists` requires `CREATE` on the configured
database. The helpers wrap `InsufficientPrivilege` errors with
actionable messages pointing at the required GRANTs, but the preferred
long-term fix is provisioning the database + schema via Terraform so
the helpers become no-ops.

### Connection budget at scale

Per-pod budget: `ICEBOX_PSYCOPG_POOL_MAX` (default 4) connections per
icebox pod. At 6 iceboxes per env that's 24 conns to a single PG
instance, shared with Lakekeeper's own pool. Confirmed comfortable on
the megaberg PG.

### Snapshot expiration

Out of scope for this PR. Per-tick commits produce one Iceberg
snapshot per non-vacuous tick; the `metadata.json` manifest list
grows monotonically without an external `expire_snapshots` job.
Filed as a separate concern; the v6 doc has the operational
discussion.
