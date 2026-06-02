# icebox — operational notes

The icebox is a writer/committer split for high-concurrency Iceberg writes.
See `ICEBOX-PLAN.md` at the repo root for the design.

This file captures **deferred operational concerns** and **known
limitations** for operators reading the code. Everything here is
intentional non-coverage in the current PR — not undiscovered.

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
