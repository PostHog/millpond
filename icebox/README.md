# icebox

A writer/committer split for high-concurrency Iceberg writes. Each
icebox instance fronts exactly one Iceberg `(namespace, table)` pair
and serializes commits from many concurrent millpond writer pods
through a single committer thread, eliminating PyIceberg's REST-catalog
optimistic-concurrency contention.

## Why

The millpond writer's Iceberg sink path commits via PyIceberg's REST
client. Each writer-to-Lakekeeper commit attaches a branch-snapshot
requirement (`expected id != actual id`); two writers committing at
the same flush cadence race against the catalog and the loser retries
with exponential backoff. Under sustained dual-writer load with
`FLUSH_INTERVAL_MS=5000` (let alone the 32 writers the production
deployment targets), retries pile up on the *next* round of commits
and exhaust the budget. The pod exits. The original discovery is
also commented in `docker-compose.iceberg.yaml`.

icebox solves this with a producer/consumer split:

```
32× millpond writers ──POST /v1/files──▶ icebox ──cycle──▶ Lakekeeper
                                          │
                                          └──cycle──▶ Kafka offset commit
```

- **Writers write parquet to S3** as before, but instead of calling
  PyIceberg's commit, they `POST /v1/files` with file metadata to the
  icebox. The POST returns 201 once the row lands in the icebox's
  Postgres.
- **One icebox per (Iceberg namespace, table) per environment.** Each
  deployment owns its own PG schema and serves exactly one table. The
  committer thread inside the icebox is a singleton enforced via a
  per-schema PG advisory lock derived from the schema name.
- **The committer batches** every claimed file row into one cycle and
  produces one Iceberg snapshot per cycle (default cadence 60s). Many
  writers, one committer per table → zero OCC contention.
- **Offsets are committed by the icebox**, not by the writers. The
  writers use `consumer.assign()` and never join the consumer group;
  the icebox holds the offset-commit responsibility for the group
  on the writers' behalf. This is what guarantees the
  exactly-one-snapshot-per-cycle / exactly-once-from-Kafka invariant
  through the writer-committer split.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `POST /v1/files` | Writer registers a parquet file. 201 on accept, 409 on idempotent replay, 400 on body/schema mismatch, 429 on queue full, 503 on degraded or stale heartbeat. |
| `GET /v1/status` | Operator-facing observability snapshot (pending files, last cycle, last committed snapshot id, consecutive failures). |
| `GET /readyz` | Readiness: PG reachable AND committer heartbeat fresh. Downstream (Lakekeeper, Kafka) outages do NOT fail readyz — the icebox keeps accepting POSTs and stages files for future cycles. |
| `GET /healthz` | Liveness only — the API process is responsive. No PG round-trip. |
| `GET /metrics` | Prometheus exposition: pending files, oldest pending age, consecutive failures, committer heartbeat age, cycle counts and duration histogram, post counts by HTTP status. |

## Structured logging + PostHog Logs

Logs are JSON-by-default (`ICEBOX_LOG_FORMAT=json`). Every log line
emitted inside a committer cycle is automatically stamped with
`cycle_id` via a `ContextVar` — no per-call-site plumbing — so a
cycle's complete trace is grep-friendly.

When `POSTHOG_PROJECT_TOKEN` is set, the icebox additionally exports
log records to PostHog Logs via standard OTLP/HTTP
(`https://us.i.posthog.com/i/v1/logs` by default; override via
`POSTHOG_LOGS_ENDPOINT`). The `BatchLogRecordProcessor` is flushed on
the SIGTERM drain path so in-flight batches make it out before the
process exits.

### Resource attribute taxonomy

Split by ownership so the app and the chart never set the same key.

**App-owned** (passed by `icebox/main.py` → `setup_logging`):

| Attr | Value | Why |
|---|---|---|
| `service.name` | `icebox` (constant) | One binary, one service. Per-instance differentiation is on `service.instance.id`. |
| `service.namespace` | `millpond` (default; override via `ICEBOX_SERVICE_NAMESPACE`) | OTel-semconv "logical service grouping". **Note:** earlier versions misused this for the PG schema. Filters that targeted the old per-deployment value (`service.namespace=icebox_events_icebox` etc.) must migrate to `service.instance.id=<consumer-key>`. |
| `service.instance.id` | The consumer key, e.g. `events-icebox` (from `ICEBOX_SERVICE_INSTANCE_ID`) | This IS the per-(namespace, table) axis. |
| `service.version` | The millpond package version | |
| `messaging.system` | `kafka` (only when Kafka attrs are set) | OTel messaging semconv. |
| `messaging.destination.name` | The Kafka topic | OTel messaging semconv (chosen over vendor-prefixed `icebox.kafka.topic` for interop with OTel-aware tooling). |
| `messaging.kafka.consumer.group` | The icebox-side consumer group id | OTel messaging semconv. |
| `icebox.iceberg.warehouse` / `namespace` / `table` | Lakekeeper warehouse, namespace, table | Vendor-prefixed because OTel semconv has no Iceberg coverage today. |

**Chart-owned** (set via the chart's `OTEL_RESOURCE_ATTRIBUTES` env on
the icebox Deployment — auto-merged into the resource by
`Resource.create()`):

- `deployment.environment` (e.g. `managed-warehouse-prod-us`)
- `k8s.cluster.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`
- `host.hostname`

These are not passed by the app — keeping env/cluster concerns out of
icebox code.

### Per-record attributes

| Attr | Source | Why |
|---|---|---|
| `icebox.cycle_id` | The `cycle_id_var` ContextVar set by `committer.run_cycle` | Per-record, NOT Resource. Resource attrs describe the *process*; `cycle_id` describes a *single cycle* inside it. Moving it to Resource would silently break per-cycle filtering. |

The stdout JSON formatter additionally emits a plain `cycle_id` field
in the body for stdout-only readers. PostHog Logs queries should use
`attributes.icebox.cycle_id`.

## Tests

| Path | Scope |
|---|---|
| `tests/unit/test_icebox_*.py` | Unit tests for each module: API perimeter checks (backpressure ordering, fingerprint mismatch, namespace/table mismatch, redaction), committer state machine, recovery branches, schema-fingerprint cache, JSON formatter + ContextVar propagation, metric accounting. |
| `tests/integration/test_icebox_e2e.py` | In-process e2e against testcontainers Postgres + a PyIceberg `SqlCatalog` against SQLite. Fast feedback for refactor regressions. |
| `tests/integration/test_icebox_docker.py` | End-to-end against the actual `icebox` Docker image built from the repo Dockerfile, talking to testcontainers Postgres + MinIO + tabulario/iceberg-rest + Redpanda. Covers image boot path, real cycle producing real Iceberg snapshot, SIGKILL recovery, SIGTERM drain (idle + mid-cycle), 32-concurrent POST burst, same-schema advisory-lock contention, and downstream-outage graceful degradation. |

The Docker integration suite pins the build platform to `linux/amd64`
by default to match what the chart publishes; Apple-Silicon devs are
auto-detected and fall back to `linux/arm64` for native iteration
(see `_resolve_test_platform` in the test module).

## Operational notes

The rest of this file captures **deferred operational concerns** and
**known limitations** for operators reading the code. Everything here
is intentional non-coverage in the current PR — not undiscovered.

## Deferred operational concerns

### Aurora failover and the advisory lock

The committer holds a session-scoped PG advisory lock on a dedicated
connection (see `postgres_sync.committer_advisory_lock_id` +
`committer.committer_loop`). The session-scoped semantics mean the lock
evaporates with its TCP socket — a dead committer's lock auto-releases,
which is the design's primary recovery mechanism.

**What this design does NOT handle today**: an Aurora failover (or any
TCP RST) on the held lock connection mid-cycle. The pool doesn't
health-check held connections; the committer keeps running, believing
it still holds the lock, while a freshly-elected Aurora primary has no
record of the lock. If a second pod were running at that instant, it
could acquire its own "lock" and the singleton-committer invariant
would be briefly violated.

At the current deployment shape — replicas=1 with `strategy.type:
Recreate` — there's no second pod to acquire, so this is benign. If
anyone bumps replicas to 2 (e.g., to attempt blue/green), the lock
becomes load-bearing during a failover window.

**Mitigations to add when this becomes a real risk:**
- Add a `pg_advisory_lock_is_held(<lock_id>)` check at cycle start; if
  False, log + re-acquire (or shut down and let K8s restart).
- Add a periodic heartbeat query on the lock_conn (e.g., `SELECT 1`)
  so the pool catches the dead TCP within seconds instead of at
  next-shutdown.

### TCP keepalives

Neither pool (`psycopg_pool.ConnectionPool` nor `asyncpg.create_pool`)
sets TCP keepalives. NLB/ELB default idle timeout is 350s; an idle
asyncpg connection beyond that gets silently RST and the next query
races dead-connection detection.

**To add:** `keepalives=1 keepalives_idle=30 keepalives_interval=10
keepalives_count=3` on the psycopg conninfo, equivalent settings on
asyncpg. Both reviewers flagged this; it's separate-concern and not
required for mw-dev rollout.

### Operator prereqs for the bootstrap helpers

`ensure_database_exists` requires the icebox PG user to have
`CREATEDB`. `ensure_schema_exists` requires `CREATE` on the configured
database. The bootstrap helpers wrap `InsufficientPrivilege` errors
with actionable messages pointing at the required GRANTs, but the
preferred long-term fix is provisioning the database + schema via
Terraform so the helpers become no-ops.

The reuse of Lakekeeper's PG user means the icebox inherits whatever
grants the Lakekeeper installer configured. As of this writing, that
user does NOT have `CREATEDB`. Operator action item: either grant
`CREATEDB` to the lakekeeper user OR provision the icebox database
manually before first pod deploy.

### Connection budget at scale

Per-pod budget: 1 lock conn (held outside the pool) + asyncpg pool
(max 8) + psycopg pool (max 2) = up to 11 connections per pod. At 6
iceboxes per env that's 66 connections to a single PG instance,
shared with Lakekeeper's own pool. Confirmed sufficient on the
megaberg PG; revisit if instance class drops or if writer pods start
holding PG connections too.

## Known design constraints

- One icebox per (Iceberg namespace, table). Each deployment is
  configured with `ICEBOX_PG_SCHEMA`, `ICEBOX_ICEBERG_NAMESPACE`,
  `ICEBOX_ICEBERG_TABLE` and serves exactly one Iceberg table.
- Each deployment owns its own PG schema; schema isolation is enforced
  via `options=-csearch_path=<schema>` on every pool connection.
- Schema names are validated as lowercase ASCII identifiers at
  config-load time. The validation comment in `config.py` explains
  why (no PG protocol support for parameterized session options).
- Advisory lock id is derived deterministically from the schema name
  (SHA-256 prefix → signed int8). **DO NOT** rotate the derivation —
  it has no documented migration playbook and would silently break
  the singleton-committer invariant during a transition.

## Schema fingerprint validation

Writers send `schema_fingerprint` (SHA-256 of the Iceberg-Schema
`model_dump_json` of the augmented batch schema). The committer
validates against the table's current schema fingerprint and rejects
mismatches with 400. This is the only defense against silent schema
drift between writer and committer.

The fingerprint check currently happens at the committer (after the
file is registered in PG). Moving it to the API perimeter (where the
mismatch can be rejected synchronously instead of stalling the whole
cycle batch) is a planned follow-up.
