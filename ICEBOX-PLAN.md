# Icebox — Iceberg's Inbox

Design plan for the icebox sink + service. This document is the source of
truth for the architecture; code follows. Review before implementation.

---

## Why

Millpond's existing iceberg sink has every writer commit directly to a
single Iceberg table. With 32 writers and an Iceberg commit window of ~6s,
the OCC conflict probability is ~80% per commit even at `flush.multiplier=2`.
Multiplier alone is a linear-improvement treadmill; getting conflict rate
below 10% requires `multiplier=32`, which costs ~6 TiB of fleet memory and
pushes per-commit data freshness past 30 minutes.

Iceberg's commit protocol is designed for "few writers, large commits." 32
concurrent writers to one table is fighting the design.

The icebox decouples writes from commits while preserving millpond's
contract that **"Kafka offsets committed ≡ data is in Iceberg."** Writers
produce parquet files and register them; a single committer batches
registered files into one Iceberg commit and advances Kafka offsets in the
same transaction. One committer ⇒ trivially zero OCC contention. Writers
remain ordinal-based and independent of each other.

Think of the icebox as **a load balancer for Iceberg**. Iceberg's commit
protocol was designed for a small number of well-behaved clients. The
icebox makes millpond look like a small number of well-behaved clients
(specifically, one), regardless of how many writers actually feed it.

This is roughly the Flink IcebergSink pattern, minus Flink's checkpoint
coordinator. Postgres replaces Flink's state backend; HTTP replaces the
control-channel messaging.

### Why this matters for the growth trajectory

The 32 writers for `events-iceberg` is the lower bound. Under naive
per-writer commits, every growth dimension multiplies the rate of
commit *attempts* on the catalog:

| Scenario | Writers committing |
|---|---|
| Today, `events` only | 32 |
| All 6 iceberg consumers enabled | ~80 across 6 tables |
| Plus 5 future topics | ~160 across 11 tables |
| Mid-backfill: events scaled 32 → 128 for 12 hours | 256+ across tables |
| Two simultaneous backfills | 384+ |

Each writer attempts roughly 1 commit per `flush.intervalMs`, with
additional attempts on each OCC conflict. We haven't measured
Lakekeeper's actual commit-handler latency or its sustained
throughput, so we can't quantify the ceiling. We do know:

1. **The 6s collision window we observed** is end-to-end (load_table →
   parquet write → manifest writes → commit_table return), dominated
   by the writer-side parquet write — NOT Lakekeeper's handler.
   Lakekeeper's commit_table call itself is probably ~hundreds of ms.

2. **OCC contention bites long before throughput** does. With 32
   concurrent writers committing every 60-120s, the OCC conflict
   probability per attempt is ~80% (see math elsewhere in this doc).
   Most commit attempts fail not because the catalog is slow but
   because their expected-snapshot-id is stale.

3. **Every growth dimension makes both worse.** More writers per table
   → higher conflict probability AND more attempt rate. More tables →
   more parallel commit streams. Backfills → temporary spikes that
   would saturate whatever the actual ceiling is.

With the icebox, catalog load is **decoupled from writer count**.
Regardless of how many writers feed it, the icebox commits at its own
cadence (~1/min by default). A backfill that quadruples writer count
produces the same catalog load profile. So does adding 5 more topics.
The icebox absorbs growth in every dimension without re-touching the
catalog.

(Worth measuring Lakekeeper's actual commit_table handler latency as a
follow-up — useful for capacity-planning the icebox cadence and for
spotting catalog-side regressions. Not a blocker for the design
decision.)

---

## Validated assumptions

Two Kafka-side assumptions were empirically verified via a Docker-based
harness before committing to this design:

1. **A process other than the consumer can commit offsets on its behalf,
   for partitions it never assigned.** Millpond uses `consumer.assign()`
   (not `subscribe()`), so no member ever JoinGroups; the consumer
   group is in "Empty" state from the coordinator's perspective. Any
   process can commit offsets via either:
   - `Consumer.commit(offsets=...)` with the same `group.id`
   - `AdminClient.alter_consumer_group_offsets(group_id, offsets)`

   Both work. A subsequent consumer with `assign(... OFFSET_STORED ...)`
   correctly resolves to those committed offsets. Verified across
   multiple topics in a single commit call.

2. **librdkafka delivers proportionally across partitions under
   continuous arrival.** A skewed-production test (one hot partition at
   10×, seven others at baseline) showed the consumer received 10× more
   from the hot partition while every partition's recv/sent ratio was
   identical (86.25% across all 8, including the hot one). No
   starvation of cold partitions; no round-robin throttling of hot
   partitions. Per-partition lag in production tracks production rates,
   not consumer-side fairness artifacts.

Implication: the icebox's Kafka-side mechanism (committer writes
offsets on behalf of writers; multi-topic consumers safely handle skewed
workloads) is feasible AND scales naturally with the partition-by-team
hot-customer pattern in `clickhouse_events_json`.

### WarpStream compatibility

Our validation harness ran against an Apache Kafka broker
(`confluentinc/cp-kafka` in Docker). Our production Kafka backend is
WarpStream, which is Kafka-API-compatible but implements semantics over
S3. Confirmed via [WarpStream's docs](https://docs.warpstream.com/warpstream/kafka/reference/protocol-and-feature-support):

- WarpStream Agents act as consumer group coordinators and explicitly
  support committing offsets for any consumer group
- The OffsetCommit protocol message is supported (this is what
  `Consumer.commit(offsets=...)` and `AdminClient.alter_consumer_group_offsets()`
  both send)
- librdkafka is an officially supported client
- The "group must be empty" caveat for `alter_consumer_group_offsets`
  is satisfied by our `consumer.assign()` setup (no JoinGroup means the
  group is permanently in Empty state)

Not empirically tested against WarpStream; relying on the docs +
identical protocol-level mechanism. If we ever see weirdness in prod,
the validation harness pattern is easy to re-run against WarpStream
directly.

---

## How Lakekeeper actually works (relevant context)

Worth pinning down because it shapes both the icebox design and what an
upstream contribution would look like.

Lakekeeper is an Iceberg REST catalog. We verified by reading its
migrations (`crates/lakekeeper/migrations/05_table.sql`) that the
`table` table in PG has a column:

```sql
"metadata" jsonb not null
```

…which stores the **full TableMetadata** — schemas, partition specs,
snapshot tree, refs, all of it. The S3 `metadata.json` file is a
write-through publication, not Lakekeeper's source of truth. The PG row
is.

Implication: Lakekeeper's `commit_table` handler:

1. PG `SELECT FOR UPDATE` on the table row
2. Validate `AssertRefSnapshotId` against the existing `metadata` jsonb
3. Apply updates → build new TableMetadata
4. PUT new metadata.json to S3 (for external readers' spec compliance)
5. UPDATE the row with new `metadata` + new `metadata_location`
6. COMMIT

The OCC mechanism is PG row-level locking + a check inside the jsonb.
Not S3-based, not filesystem-based. Per-commit cost is dominated by
step 4's S3 PUT (~50-200ms typical) — but the OCC check itself is
fast (PG row lock + jsonb field comparison).

Lakekeeper does NOT write a `version-hint.text` sidecar file. We
verified by grepping the Lakekeeper repo — zero references. This
means: external readers that don't speak the REST catalog protocol have
no authoritative way to determine the current metadata version from S3
alone. The catalog is required.

### The architectural critique behind why icebox exists

Once you accept that:

- A catalog with a backing store is required for safe concurrent writes
- That backing store can hold the full table state (Lakekeeper does)
- The catalog is the read entry point ("what's current?")

…there's no architectural justification for ALSO writing the metadata
file chain (manifests, manifest lists, metadata.json) to S3. It's pure
redundancy. Lakekeeper writes them for Iceberg spec compliance, not
because it needs them itself.

This is what DuckLake correctly identified and discarded: metadata
lives in PG only, no S3 metadata chain at all. The Iceberg design pays
the cost of "no backing store assumed" while every modern deployment
has one.

For our purposes, the icebox **absorbs the cost of Iceberg's
redundant write protocol** so millpond doesn't have to feel it. We're
not fixing Iceberg; we're shielding millpond from it. One committer at
1/min cadence means the per-commit overhead is paid once per minute
across the whole fleet, regardless of writer count.

---

## Naming

> Icebox — Iceberg's Inbox.

| Thing | Name |
|---|---|
| The new millpond sink (writer side) | `millpond/icebox.py` |
| The companion service (committer + REST API) | `icebox/` (package in this repo) |
| The CLI entrypoint for the service | `icebox` (binary, declared in `pyproject.toml` `[project.scripts]`) |
| The PG database | `icebox` |
| The PG schema/tables | `icebox.files`, `icebox.status` |
| The sink config marker | `icebox: {}` (per-consumer block in millpond values) |
| `MILLPOND_DESTINATION` env value | `icebox` |

---

## Architecture

### Data flow

```
  ┌──────────────┐  parquet file        ┌─────┐
  │ Millpond     │ ───────────────────► │ S3  │
  │ writer pod   │                      └─────┘
  │ (ordinal N)  │
  │              │  POST /v1/files       ┌──────────────┐
  │              │ ──────────────────►  │ Icebox       │
  └──────────────┘                       │ REST server  │
                                         │              │
                                         │  background  │
                                         │  committer   │
                                         └──────┬───────┘
                                                │
                                                ▼
                                         ┌──────────────┐
                                         │ Lakekeeper   │
                                         │ (Iceberg     │
                                         │  catalog)    │
                                         └──────────────┘
```

### The atomic commit cycle (icebox-side, every ~60s)

The naive shape "wrap everything in a PG transaction and hope" doesn't
actually provide atomicity, because PyIceberg's commit and Kafka's
offset commit are external resources that PG ROLLBACK cannot unwind.
Instead, the design uses a Saga / 2PC-flavored pattern with PG as the
durable coordinator log and a **cycle_id** embedded in the Iceberg
snapshot summary as the synchronization point. This is the canonical
pattern Flink, Kafka Connect, and Tabular all use (Flink calls it
`flink.checkpoint-id`).

```
1. BEGIN PG TXN
2. Generate new cycle_id (UUID)
3. INSERT into commit_cycles (cycle_id, started_at=now())
4. UPDATE icebox.files SET cycle_id = <new>
     WHERE committed_at IS NULL AND cycle_id IS NULL  -- claim files
5. COMMIT PG TXN
   -- the claim survives crash; if we die here, the rows are tagged

6. Construct DataFile records from writer-supplied parquet stats
   (see "DataFile construction without footer reads" below).

7. Call producer.append_data_file(...) inside a transaction whose
   snapshot summary contains "icebox.cycle_id": <id>.
   On success → Iceberg snapshot K landed with the cycle_id in its
   summary, observable by anyone via load_table.

8. BEGIN PG TXN
9. UPDATE commit_cycles SET iceberg_snapshot_id = K WHERE cycle_id = <id>
10. COMMIT PG TXN
    -- now we know the snapshot landed.

11. kafka.commit(...)

12. BEGIN PG TXN
13. UPDATE commit_cycles SET kafka_committed_at = now() WHERE cycle_id = <id>
14. UPDATE icebox.files SET committed_at = now(), iceberg_snapshot_id = K
     WHERE cycle_id = <id>
15. UPDATE commit_cycles SET completed_at = now() WHERE cycle_id = <id>
16. COMMIT PG TXN
```

### Recovery (icebox restart with an in-flight cycle)

For each cycle_id with `completed_at IS NULL`, scan the table's
**`snapshots`** array (NOT just `current-snapshot-id`) for any
snapshot whose `summary` contains `posthog.icebox.cycle_id = <id>`:

| Cycle_id found in any snapshot's summary? | PG state | Action |
|---|---|---|
| Yes (let K = that snapshot's id) | `iceberg_snapshot_id` set, `kafka_committed_at IS NULL` | The snapshot landed in a previous attempt. kafka.commit() is idempotent. Run kafka.commit, UPDATE state, mark cycle completed. |
| Yes (let K = that snapshot's id) | `iceberg_snapshot_id` NOT set | The snapshot landed but the committer crashed before recording K in PG. UPDATE PG with K, then proceed as above. |
| Yes | `kafka_committed_at IS NOT NULL` | Both landed, committer crashed before marking cycle complete. UPDATE completed_at, done. |
| No | (any) | Previous attempt did NOT commit. Re-run from step 6 (build DataFiles, append, commit). New snapshot will carry the same cycle_id. No duplicate files in Iceberg because no previous snapshot referenced these files. |

The "scan the snapshots array, not just current" detail is
load-bearing. Possible failure scenario: committer calls
`append_data_file` → Lakekeeper succeeds server-side → server's ACK
HTTP response times out / network drops → committer thinks it failed
→ retry could double-commit. **But** another writer's commit can
advance `current-snapshot-id` between our attempts, so the cycle_id
we tagged the previous successful snapshot with is no longer in
`current-snapshot-id.summary` — it's in the snapshot_log. We must
walk the full log to find it.

The cycle_id in the Iceberg snapshot summary is the load-bearing
synchronization point: it definitively answers "did THIS attempt's
commit land?" without ambiguity, given we look at the entire snapshot
history not just the current pointer. No partial-success double-
counting, no "treat ValueError as success" hand-waving.

**Snapshot summary key**: `posthog.icebox.cycle_id`. Namespaced under
`posthog.` to avoid colliding with Iceberg/Spark conventions
(`spark.app.id`, etc.) that use unnamespaced or `spark.`-prefixed
keys.

### DataFile construction without footer reads

PyIceberg's high-level `add_files(paths)` API reads every parquet
file's footer from S3 to extract column statistics for the manifest.
At ~320 files in a 10-minute backlog × ~100ms per footer read, that's
~30 seconds of synchronous S3 I/O inside the commit path — held PG
locks, head-of-line blocking, real production hazard.

The lower-level path: the writer already has the parquet stats from
PyArrow's `ParquetWriter.writer.metadata` at write time. The writer
ships those stats with the POST body; the committer constructs
`DataFile` records directly without any S3 GETs.

POST body fields (in addition to file_path, kafka_offsets,
partition_values, record_count, file_size):

```json
{
  ...
  "parquet_stats": {
    "column_sizes":      {"<iceberg_field_id>": bytes, ...},
    "value_counts":      {"<iceberg_field_id>": rows, ...},
    "null_value_counts": {"<iceberg_field_id>": nulls, ...},
    "nan_value_counts":  {"<iceberg_field_id>": nans, ...},
    "lower_bounds":      {"<iceberg_field_id>": <typed value>, ...},
    "upper_bounds":      {"<iceberg_field_id>": <typed value>, ...},
    "split_offsets":     [bytes, ...]
  }
}
```

**Wire format rules** (load-bearing — wrong encoding silently breaks
Iceberg readers' partition pruning):

1. **Keys are Iceberg field IDs as JSON strings**, NOT parquet column
   indices. The writer must hold the Iceberg schema (with field IDs)
   at write time — same mapping the existing `iceberg.py` already
   resolves via `assign_fresh_schema_ids`.

2. **Bound values are TYPED JSON over the wire**, not opaque bytes:
   - int/long → JSON number
   - float/double → JSON number
   - string → JSON string
   - date → JSON string `"YYYY-MM-DD"`
   - timestamp/timestamptz → JSON string ISO-8601 with microsecond
     precision
   - binary/fixed → base64-encoded string
   - decimal → JSON string of the decimal value

3. **The COMMITTER, not the writer, converts typed values to Iceberg
   single-value-serialization bytes** via
   `pyiceberg.conversions.to_bytes(iceberg_type, python_value)`. This
   is what populates `DataFile.lower_bounds: dict[int, bytes]`.
   Writers don't compute Iceberg-spec bytes; they just ship typed
   values. Conversion responsibility lives entirely in the committer
   where the Iceberg schema is authoritatively known.

4. **Integration test required**: a round-trip test that writes typed
   bounds → builds DataFile via committer → reads back via PyIceberg's
   manifest reader → confirms decoded bounds match the original typed
   values. This is the only test that catches "wrong encoding produces
   wrong query results" bugs.

5. PyArrow's `statistics.min/max` does NOT match this format and
   cannot be used as-is. The writer extracts typed values from
   PyArrow's stats (via `ParquetWriter.writer.metadata` after write),
   applies type-aware conversion to JSON-serializable form, and
   includes them in the POST.

Committer code (PyIceberg 0.11.1 — verified against `.venv`):

```python
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.conversions import to_bytes
from pyiceberg.typedef import Record

# PyIceberg 0.11.1's DataFile.__init__ is positional via a Record-style
# _data tuple. The supported public constructor is the classmethod
# DataFile.from_args(_table_format_version=int, **kwargs) which binds
# kwargs through `super()._bind`. Keyword-only invocation is required.
data_file = DataFile.from_args(
    _table_format_version=tx.table_metadata.format_version,
    content=DataFileContent.DATA,
    file_path=row.file_path,
    file_format=FileFormat.PARQUET,
    partition=Record(*partition_tuple_from_spec(row, table.spec())),
    record_count=row.record_count,
    file_size_in_bytes=row.file_size,
    column_sizes=row.parquet_stats["column_sizes"],
    value_counts=row.parquet_stats["value_counts"],
    null_value_counts=row.parquet_stats["null_value_counts"],
    nan_value_counts=row.parquet_stats.get("nan_value_counts", {}),
    lower_bounds=encode_bounds(row.parquet_stats["lower_bounds"], table.schema()),
    upper_bounds=encode_bounds(row.parquet_stats["upper_bounds"], table.schema()),
    key_metadata=None,
    split_offsets=row.parquet_stats.get("split_offsets"),
    equality_ids=None,
    sort_order_id=None,
)

with table.transaction() as tx:
    snapshot_props = {"posthog.icebox.cycle_id": str(cycle_id)}
    with tx._append_snapshot_producer(snapshot_props, branch="main") as producer:
        for df in data_files:
            producer.append_data_file(df)
```

**Required helpers:**

- `partition_tuple_from_spec(row, spec)` — converts the writer's JSON
  `partition_values` dict (which loses int32/int distinction over JSON)
  into a tuple shaped per the table's `PartitionSpec`, with each value
  coerced to the spec's field type. Required because PyIceberg's
  Record construction is positional and type-strict; JSON-deserialized
  numbers come through as `int`/`float` regardless of the source type.

- `encode_bounds(typed_values_by_field_id, schema)` — wraps
  `pyiceberg.conversions.to_bytes(iceberg_type, python_value)` over the
  writer's typed JSON. Returns `dict[int, bytes]` per Iceberg's
  single-value-serialization spec (little-endian ints, IEEE floats,
  UTF-8 strings, etc.). This is the ONLY correct encoding for the
  manifest's `lower_bounds`/`upper_bounds`. PyArrow's parquet
  `statistics.min/max` encoding is DIFFERENT and cannot be reused
  verbatim.

This eliminates the synchronous footer-read cost regardless of batch
size. The committer's per-cycle wall time is now bounded by
Lakekeeper's commit_table handler latency (one S3 PUT for metadata.json
+ one for manifest + PG row update), not by `O(files)` S3 GETs.

### Async-vs-sync inside the icebox process

FastAPI auto-wraps SYNC request handlers via `run_in_threadpool`, but
that auto-wrapping does NOT apply to `asyncio.create_task`-spawned
background workers. A naive `async def commit_loop()` that calls
PyIceberg's synchronous `add_files` (or `producer.append_data_file`)
blocks the entire event loop — incoming POST handlers stop responding
for the duration of `add_files`.

Design choice for v1: **run the committer in a dedicated thread**, not
as an asyncio task. Spawned at startup via `threading.Thread(target=
commit_loop, daemon=True).start()`. Uses sync `psycopg` (not asyncpg)
inside the committer thread. The REST API stays async with `asyncpg`
and FastAPI's normal model.

Trade-off: two DB driver libraries (sync + async) in the same process.
Acceptable for the simpler isolation. Alternative: keep async
throughout with `loop.run_in_executor(None, sync_op, ...)` around each
PyIceberg call. Either works; thread is more idiomatic for "long-
running background worker."

**Connection-pool budgets:** Lakekeeper's RDS has a `max_connections`
limit. The icebox now consumes from TWO pools against the same DB
(asyncpg for the REST API + psycopg for the committer). Pin both
pool sizes explicitly in code:
- `asyncpg.create_pool(min_size=2, max_size=8)` for the API layer
- `psycopg.connection_pool` `min_size=1, max_size=2` for the committer
  thread (the committer is single-threaded; 1-2 connections is
  plenty)

Total: ≤ 10 PG connections from the icebox pod. Document this in the
icebox's `config.py` defaults so anyone tuning it sees the budget.

**Committer thread liveness:** the committer thread can crash silently
while the FastAPI server stays alive — `/healthz` returns 200, K8s
doesn't restart. Add a watchdog:

- Committer thread writes `icebox.status.last_committer_heartbeat =
  now()` at the start of every cycle and after every step in the
  state machine
- API handler for `POST /v1/files` checks
  `now() - last_committer_heartbeat > 3 * cadence` before accepting;
  returns 503 if stale, with the heartbeat age in the body
- A Prometheus gauge `icebox_committer_heartbeat_age_seconds` drives
  the alerting

The heartbeat is in PG (not in-memory) so it survives process restart
and is alert-visible from outside the icebox.

### The writer flow (millpond `icebox.py` sink, every flush)

```
1. Compute `_inserted_at` once per flush (single timestamp source of
   truth for both partition_values AND the deterministic S3 path).

2. Derive partition_values from _inserted_at:
   {"year": Y, "month": M, "day": D, "hour": H}

3. Compute deterministic S3 path using the SAME timestamp's partition
   values plus the kafka_offsets fingerprint:
     s3://<bucket>/warehouses/ingest/kafka/<table>/data/
       year=YYYY/month=MM/day=DD/hour=HH/
       writer-<ordinal>-<sha256(sorted-offsets)>.parquet

4. Write parquet to S3 at that path. PyArrow's ParquetWriter exposes
   column-level stats during write — capture them into a structure
   the icebox API accepts.

5. POST /v1/files to the icebox:
     {file_path, writer_ordinal, kafka_offsets, partition_values,
      record_count, file_size, schema_version, schema_fingerprint,
      parquet_stats, protocol_version}

6. Handle response:
     201 Created → no further action (writer does NOT call kafka.commit;
                   the icebox owns offset commits as part of its
                   atomic cycle)
     409 Conflict (file already registered) → no further action
                  (idempotent replay)
     429 / 503 → absorbed inside the sink (sleep + retry with
                 bounded timeout; see "Writer error handling"). If
                 the icebox stays degraded long enough to exhaust the
                 internal retry budget, the sink raises a real error
                 and the existing _write_with_retry + pod-restart
                 path takes over.
     400 Bad Request with protocol_version mismatch → fail loud
         (deploy mismatch; writer and icebox running incompatible
         image versions)

7. Writer NEVER calls kafka.commit() — that's the icebox's job, run
   atomically with the Iceberg commit via the cycle_id mechanism
   above. Writers committing their own offsets would degrade the
   "Iceberg snapshot landed ≡ Kafka offsets advanced" invariant to a
   weaker "data is durable in icebox PG, will eventually appear in
   Iceberg" — acceptable for many use cases, but we want the stronger
   contract.
```

### Writer error handling — backpressure absorbed inside the sink

The icebox sink absorbs backpressure responses (429/503) internally
without surfacing them to millpond's main loop. On a 429/503, the
sink sleeps for the response's `Retry-After` (or a sensible default
bounded by a max) and retries the POST. This continues until either:

- The POST succeeds (most cases — icebox catches up within a cycle or two)
- A timeout threshold is exceeded — at which point the sink raises a
  REAL error, which DOES go through `_write_with_retry`. After the
  existing 3-attempt retry budget is exhausted, the pod crashes and
  K8s restarts it. By design: sustained backpressure for many minutes
  IS a degraded condition worth paging on.

Why absorb in the sink instead of surfacing to main:

- **Kafka is already our durable queue.** When `write()` blocks waiting
  for the icebox to come back, the main loop blocks → `consumer.poll()`
  doesn't run → Kafka offsets don't advance → broker holds records on
  our behalf. We get backpressure for free via Kafka's own queueing
  semantics. An explicit `consumer.pause()` would just stop our fetch
  RPCs to the broker, which the broker would notice as "client stopped
  fetching" — the same end state.
- **Simpler code path.** No new exception class threading through
  `_write_with_retry`. No special-case handling in main. The sink
  looks like any other sink: write() either succeeds or it doesn't.
- **Matches ducklake sink behavior.** The ducklake sink already
  retries PG hiccups internally without telling main; treating the
  icebox sink the same way is consistent.

The Kafka client buffer growth during a backpressure window is
naturally bounded by `queued.max.messages.kbytes` (16MB in millpond's
config) — when full, librdkafka stops fetching even without an
explicit pause. So the worst-case memory growth during sustained
backpressure is bounded by the consumer's local buffer, not by the
icebox queue depth.

### Failure modes and recovery

| Failure | Outcome | Recovery |
|---|---|---|
| Writer crashes after S3 write, before POST | Orphan parquet in S3, no icebox row | Replay from last (icebox-)committed Kafka offset → re-write same path (S3 no-op, idempotent by deterministic path) → POST succeeds |
| Writer crashes after POST, before next iteration | Icebox row exists, writer hasn't advanced internal state | Replay → re-write same path (S3 no-op) → POST returns 409 (UNIQUE catches duplicate) → writer continues |
| Icebox crashes after PG row claim (cycle_id assigned), before append_data_file | PG has claimed rows for cycle_id, no Iceberg snapshot | Recovery sees cycle_id without `iceberg_snapshot_id`; Lakekeeper has no snapshot tagged with cycle_id; re-run `append_data_file` for the cycle. Same files, same cycle_id, new snapshot. |
| Icebox crashes after append_data_file (Iceberg snapshot landed), before PG records iceberg_snapshot_id | Iceberg has snapshot tagged with cycle_id, PG doesn't know yet | Recovery sees cycle_id without `iceberg_snapshot_id` in PG; Lakekeeper IS tagged with cycle_id; advance state to "snapshot landed", proceed to kafka.commit. |
| Icebox crashes after kafka.commit, before PG records kafka_committed_at | Iceberg + Kafka both done, PG doesn't know | Recovery sees cycle_id with iceberg_snapshot_id but no kafka_committed_at; re-run kafka.commit (idempotent — same offsets twice is a no-op); advance state. |
| Icebox crashes after PG update of icebox.files but before commit_cycles completed_at | The cycle is functionally done but the cycle row isn't marked completed | Recovery sees completed_at IS NULL, all other fields set; UPDATE completed_at=now() and move on. |
| Icebox down for an extended period | Writers see 503 → pause consumer.fetch → Kafka lag grows (visible in standard dashboards) | Writers resume on icebox recovery. No replay needed (icebox-committed offsets are stable). |
| PG down | Writers see HTTP timeout/error → pause | Same |
| Iceberg catalog (Lakekeeper) down | Committer fails to commit → `consecutive_failures` increments → REST API returns 503 → writers pause | Same |
| Writer pod restarted while icebox is healthy | New writer assigns its partitions with `OFFSET_STORED`; resumes from last icebox-committed offset; any uncommitted data is replayed from Kafka (re-written at the same deterministic S3 path, POSTs return 409 if already registered) | Bounded by icebox cadence. Typical: ≤ 1 min of replay. |
| Writer pod AND icebox both restarting | Worst case: new writer comes up during icebox-down window, sees 503, pauses. Once icebox returns, writer resumes from last icebox-committed offset. | Bounded by `cadence + icebox_recovery_time`. Typically 1-2 min during routine Helm rollout. |

Orphan parquet files (writer crashed between S3 write and POST) are
cleaned up by a maintenance procedure scanning the warehouse data path
for files not referenced by any snapshot and not present in
`icebox.files`. PyIceberg 0.11.1 does NOT ship a `remove_orphan_files`
operation (that's a Spark/Java thing); we'll roll our own as a
follow-up. For v1, accept that orphan files accumulate slowly; address
when storage costs flag it.

### Backpressure (bounded queue depth)

The icebox REST handler enforces two backpressure conditions:

```python
@app.post("/v1/files", status_code=201)
def register_file(req):
    pending = await db.scalar(
        "SELECT count(*) FROM icebox.files WHERE committed_at IS NULL"
    )
    if pending >= MAX_PENDING:
        raise HTTPException(429, {
            "queue_depth": pending,
            "retry_after_s": COMMITTER_INTERVAL_S,
        })

    if await committer.is_degraded():    # consecutive_failures >= 2
        raise HTTPException(503, {
            "consecutive_failures": ...,
            "retry_after_s": COMMITTER_INTERVAL_S * 2,
        })

    try:
        row = await db.fetchrow("""
            INSERT INTO icebox.files (...) VALUES (...) RETURNING id, staged_at
        """, ...)
        return {"row_id": row.id, "queued_at": row.staged_at}
    except asyncpg.UniqueViolationError:
        existing = await db.fetchrow(
            "SELECT id, staged_at FROM icebox.files WHERE file_path = $1",
            req.file_path
        )
        # 409 with the same shape — idempotent replay-friendly
        raise HTTPException(409, {"row_id": existing.id, "queued_at": existing.staged_at})
```

Maximum replay window on writer crash: `2 × COMMITTER_INTERVAL_S` (~2 min
default). Bounded by construction.

### Why the icebox is single-replica

Multiple icebox replicas committing concurrently → multiple `add_files`
calls racing on the same table → OCC contention returns from the other end,
defeating the entire purpose.

The Deployment uses `strategy.type: Recreate` (not `RollingUpdate`) so old
and new pods never overlap during image upgrades. Brief 503 during the
restart; writers back off; resume on recovery.

Leader election + multi-replica is an explicit non-goal for v1.

---

## REST API

Base URL within the cluster: `http://millpond-icebox:8000` (resolved via the
Helm-templated Service in the millpond namespace).

### POST /v1/files

Register a new parquet file as pending Iceberg commit.

Request body:

```json
{
  "protocol_version": 1,
  "file_path": "s3://posthog-megaberg-mw-prod-us/warehouses/ingest/kafka/events/data/year=2026/month=06/day=01/hour=14/writer-20-3f8c9a2b1e7d4f0a.parquet",
  "writer_ordinal": 20,
  "kafka_offsets": {"20": 1245678, "52": 9876543, "84": 3456789, "...": "..."},
  "partition_values": {"year": 2026, "month": 6, "day": 1, "hour": 14},
  "record_count": 69937,
  "file_size": 536924264,
  "schema_version": "v1",
  "schema_fingerprint": "<sha256 hex>",
  "parquet_stats": {
    "column_sizes":      {"1": 1234, "...": "..."},
    "value_counts":      {"1": 69937, "...": "..."},
    "null_value_counts": {"1": 0, "...": "..."},
    "lower_bounds":      {"1": "<base64 min>", "...": "..."},
    "upper_bounds":      {"1": "<base64 max>", "...": "..."},
    "split_offsets":     [123, 456789, "..."]
  }
}
```

`protocol_version` exists to detect deploy mismatches. If the writer
and icebox run different image versions during a partial rollout (a
real failure mode given Pydantic silently accepts missing fields with
defaults), version mismatch produces a loud 400 instead of silent
field loss. v1 of the protocol uses `"protocol_version": 1`. Future
schema-incompatible changes increment.

Responses:

| Status | Meaning | Body |
|---|---|---|
| 201 Created | Registered successfully | `{row_id, queued_at}` |
| 409 Conflict | Already registered (idempotent replay) | `{row_id, queued_at}` (same shape; client treats as success) |
| 429 Too Many Requests | Queue full | `{queue_depth, retry_after_s}` |
| 503 Service Unavailable | Icebox degraded (consecutive commit failures) | `{consecutive_failures, retry_after_s}` |
| 400 Bad Request | Validation error | `{error}` |

### GET /v1/status

Observability endpoint. Returns aggregated state of the icebox.

Response:

```json
{
  "pending_files": 17,
  "pending_files_by_table": {"kafka.events": 14, "kafka.person": 3},
  "oldest_pending_age_seconds": 47,
  "in_flight_cycles": 1,
  "last_success_at": "2026-06-01T14:23:45Z",
  "last_cycle_at": "2026-06-01T14:24:45Z",
  "last_commit_duration_seconds": 1.23,
  "last_commit_file_count": 32,
  "consecutive_failures": 0,
  "last_committed_iceberg_snapshot": 1234567890123,
  "kafka_offsets_high_watermark": {"events": {"20": 1245678, "...": "..."}}
}
```

`oldest_pending_age_seconds` is the load-bearing "are we falling
behind?" metric. `pending_files` count alone tells you nothing; a
queue of 1000 files all from the last 30 seconds is healthy, a queue
of 5 files where the oldest is 20 minutes old is broken.

These same metrics are ALSO exposed via Prometheus, not just this
JSON endpoint. Dashboards/alerts go off the metrics; the JSON endpoint
is for human-triage curl-from-jumphost.

### GET /healthz

Liveness probe (process is alive). Returns 200 unconditionally if the
HTTP server is running. Decoupled from downstream health — Lakekeeper
being slow or PG being slow does NOT cause the icebox pod to fail
liveness and get K8s-restarted.

### GET /readyz

Readiness probe. Returns 200 if:
- PG is reachable (cheap query, no extended work)
- The HTTP server has finished initialization (FastAPI startup events
  completed)

Returns 503 if either of the above fails. Notably **NOT** tied to
downstream health (Lakekeeper reachability, `consecutive_failures`).
The reason: tying readiness to downstream health causes a K8s pod
restart loop while the downstream is recovering — the worst response.
Downstream health is a separate Prometheus metric driving alerts, not
K8s lifecycle.

---

## Postgres schema

Database name: `icebox`. Lives on the existing Lakekeeper Postgres
instance (same RDS host, separate DATABASE). Migrations applied at icebox
container startup (idempotent `CREATE TABLE IF NOT EXISTS`).

```sql
CREATE SCHEMA IF NOT EXISTS icebox;

CREATE TABLE IF NOT EXISTS icebox.commit_cycles (
    cycle_id             uuid PRIMARY KEY,
    started_at           timestamptz NOT NULL DEFAULT now(),
    iceberg_snapshot_id  bigint,         -- set when append_data_file lands
    kafka_committed_at   timestamptz,    -- set when kafka.commit lands
    completed_at         timestamptz     -- set when PG UPDATE marks rows committed
);

CREATE INDEX IF NOT EXISTS commit_cycles_incomplete_idx
    ON icebox.commit_cycles (started_at)
    WHERE completed_at IS NULL;

CREATE TABLE IF NOT EXISTS icebox.files (
    id                   bigserial PRIMARY KEY,
    file_path            text NOT NULL UNIQUE,
    writer_ordinal       int NOT NULL,
    kafka_offsets        jsonb NOT NULL,
    partition_values     jsonb NOT NULL,
    record_count         bigint NOT NULL,
    file_size            bigint NOT NULL,
    schema_version       text NOT NULL,
    schema_fingerprint   text NOT NULL,  -- SHA-256 of the writer's parquet schema; icebox rejects mismatches against the table's current schema
    parquet_stats        jsonb NOT NULL, -- column_sizes/value_counts/lower_bounds/upper_bounds/null_counts captured by the writer's ParquetWriter; the committer uses these to construct DataFile records without footer reads
    cycle_id             uuid REFERENCES icebox.commit_cycles(cycle_id),  -- claimed by a commit cycle; null until claimed
    staged_at            timestamptz NOT NULL DEFAULT now(),
    committed_at         timestamptz,
    iceberg_snapshot_id  bigint
);

CREATE INDEX IF NOT EXISTS files_unclaimed_idx
    ON icebox.files (staged_at)
    WHERE committed_at IS NULL AND cycle_id IS NULL;

CREATE INDEX IF NOT EXISTS files_in_flight_idx
    ON icebox.files (cycle_id)
    WHERE committed_at IS NULL AND cycle_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS icebox.status (
    id                   int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    last_success_at      timestamptz,
    consecutive_failures int NOT NULL DEFAULT 0,
    last_cycle_at        timestamptz
);
INSERT INTO icebox.status (id) VALUES (1) ON CONFLICT DO NOTHING;
```

The state-machine columns on `commit_cycles` are the load-bearing
recovery anchor. The four nullable timestamp/id columns
(`iceberg_snapshot_id`, `kafka_committed_at`, `completed_at`) plus the
external check against Lakekeeper's snapshot summary uniquely identify
where any in-flight cycle stopped.

---

## Repository layout

```
millpond/
├── pyproject.toml          # declares both scripts
├── millpond/               # existing Kafka consumer package
│   ├── main.py
│   ├── consumer.py
│   ├── ducklake.py
│   ├── iceberg.py
│   ├── icebox.py           # NEW — the icebox sink
│   ├── sink.py             # existing — make_sink dispatch updated
│   └── ...
├── icebox/                 # NEW package — REST server + committer
│   ├── __init__.py
│   ├── main.py             # boot: FastAPI + background committer thread
│   ├── api.py              # routes (async; asyncpg)
│   ├── committer.py        # commit loop (sync; psycopg). Owns the
│   │                       # state machine described above. Runs in
│   │                       # a dedicated thread, not on the event loop.
│   ├── postgres_async.py   # asyncpg helpers for the API layer
│   ├── postgres_sync.py    # psycopg helpers for the committer thread
│   ├── iceberg.py          # PyIceberg DataFile construction + low-
│   │                       # level _append_snapshot_producer wrapper
│   │                       # (NOT high-level add_files; see plan body)
│   ├── kafka.py            # offset commit via either Consumer.commit
│   │                       # or AdminClient.alter_consumer_group_offsets
│   ├── schema.py           # Pydantic models + migration DDL
│   └── config.py
├── shared/                 # NEW — types/models shared between millpond + icebox
│   ├── __init__.py
│   ├── models.py           # Pydantic models for REST + DB rows
│   └── paths.py            # deterministic file-path helper
├── tests/
│   ├── unit/
│   │   ├── test_icebox_sink.py      # NEW — sink response handling
│   │   └── test_icebox_api.py       # NEW — REST API + backpressure
│   │   └── test_icebox_committer.py # NEW — commit cycle idempotency
│   └── integration/
│       └── test_icebox_e2e.py       # NEW — testcontainers PG + mock Lakekeeper
├── Dockerfile              # builds both binaries into one image
└── tools/                  # unchanged
```

`pyproject.toml` additions:

```toml
[project.scripts]
millpond = "millpond.main:main"
icebox   = "icebox.main:main"

[project.optional-dependencies]
icebox = [
    "fastapi>=0.110",
    "uvicorn>=0.27",
    "asyncpg>=0.29",
    "httpx>=0.27",          # also used by the sink
]
```

The Dockerfile installs the project with the `icebox` extra; both
binaries end up on PATH. Pod manifests select via `command:`.

---

## Helm chart additions (millpond chart, gated)

Add to `charts/millpond/values.yaml`:

```yaml
# Icebox companion service. Gated on icebox.enabled — set to true in per-env
# values when any consumer uses the icebox sink. Single replica; structural.
icebox:
  enabled: false
  replicas: 1               # MUST be 1 (no leader election)
  image:
    repository: ""          # defaults to millpond image
    tag: ""
  command: ["icebox"]
  postgres:
    host: ""
    database: icebox
    username: ""            # v1: reuse Lakekeeper's existing PG credentials
                            # (separate role + ESO secret is a follow-up)
    passwordSecretName: ""  # v1: reuse Lakekeeper's existing secret name
  iceberg:
    catalogUri: ""          # in-cluster Lakekeeper URL
    warehouse: ingest
  committer:
    cadenceSeconds: 60
    maxPendingFiles: 1000
    degradedFailureThreshold: 2
  service:
    port: 8000
  resources:
    requests:
      cpu: 200m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 1Gi
```

New chart templates (all gated on `{{- if .Values.icebox.enabled }}`):

- `charts/millpond/templates/icebox-deployment.yaml` — single-replica
  Deployment with `strategy.type: Recreate`, FastAPI/uvicorn entrypoint,
  `/readyz` for the K8s readinessProbe
- `charts/millpond/templates/icebox-service.yaml` — ClusterIP on port 8000
- ~~`charts/millpond/templates/icebox-external-secret.yaml`~~ — **dropped
  for v1**; reuses Lakekeeper's existing PG ExternalSecret. Add a
  dedicated ESO secret for the icebox in a follow-up once we want
  separate icebox-specific role + grants.
- `charts/millpond/templates/icebox-migration-job.yaml` — Helm
  post-install/post-upgrade hook to run `CREATE TABLE IF NOT EXISTS`
  (mirror the megaberg warehouse-bootstrap-job pattern)

The sink's `icebox_url` is computed from a chart helper as
`http://{{ .Release.Name }}-icebox:{{ .Values.icebox.service.port }}`, so
writers in the same release auto-discover the endpoint without explicit
configuration.

---

## Implementation

Two bundled PRs, one per repo. Commit often within each branch; PR rarely.

1. **millpond repo PR (bundled)**: `icebox/` package + `millpond/icebox.py`
   sink + `pyproject.toml` script declaration + Dockerfile update + tests
   for both. Single PR, multiple commits.
2. **charts repo PR (bundled)**: millpond chart additions (templates +
   values for the icebox Deployment/Service/ExternalSecret/migration-Job)
   + per-env values for mw-dev with `icebox.enabled: true` + flip
   mw-dev `events-iceberg` to the new sink. Single PR.
3. **Manual one-shot before chart PR merges**: create the `icebox` PG
   database + user on the Lakekeeper RDS in mw-dev; create the password
   secret in AWS Secrets Manager. Cloud-infra terragrunt to formalize
   these later.
4. **Iterate**: tune cadence + threshold values in prod-us during
   promotion. Probably another small charts PR for the prod-us values
   flip.
5. **Retire legacy `iceberg` sink** in a follow-up once icebox is
   proven across both envs.

---

## Open questions

- **PG instance choice**: confirmed Lakekeeper's existing PG, new
  database. But — should the icebox database have its own user/role,
  or share with Lakekeeper? Answer: separate role with grants only on
  the `icebox` database (least privilege).

- **Schema version handling**: writers include both `schema_version`
  (free-form producer tag) and `schema_fingerprint` in the POST body.

  Canonical form for the fingerprint: SHA-256 of
  `pyiceberg.schema.Schema.model_dump_json()` of the **Iceberg
  Schema** (with field IDs assigned). The writer derives this via the
  existing path in `millpond/iceberg.py`:
  `_pyarrow_to_schema_without_ids(arrow_schema)` →
  `assign_fresh_schema_ids(iceberg_schema)` → `.model_dump_json()` →
  SHA-256.

  The committer computes the table's fingerprint from
  `table.schema().model_dump_json()`. Compare; on mismatch, return
  400 with both fingerprints in the body for triage.

  Field renames or field-ID reassignments change the fingerprint
  even if columns are logically equivalent. That's intentional —
  it forces explicit schema migration via the (future)
  schema-evolution-through-icebox path, not silent acceptance of
  unintended renames.

  v1 of the committer rejects mismatches loudly. Schema evolution
  itself (writer wants to add a column → table needs
  `update_schema`) is a follow-up; for v1 schemas are assumed
  stable. When schemas DO need to evolve, the right path is for
  writers to POST the new schema to a dedicated endpoint and the
  icebox runs `update_schema` as part of the next commit cycle.
  Don't let writers race `update_schema` against each other; that's
  the OCC problem on the schema-commit path and reintroduces what
  icebox solves on the data path.

- **PartitionSpec stability**: v1 assumes the partition spec is fixed
  at `(year, month, day, hour)` identity. Writers hard-code these
  partition values; the committer constructs DataFile records with
  matching partition tuples. If someone runs `update_spec`, the
  committer's DataFile construction breaks (partition tuple shape
  mismatch). v1 calls this out as an invariant; `update_spec` is
  out-of-scope. Same follow-up bucket as schema evolution.

- **Committer per-cycle latency at scale**: the v1 uses the low-level
  `_append_snapshot_producer` + writer-supplied parquet stats path
  (NOT the high-level `add_files` that reads footers), so per-cycle
  S3 GETs are zero regardless of file count. Per-cycle latency is now
  bounded by Lakekeeper's `commit_table` handler latency — one S3 PUT
  for new metadata.json, one for the new manifest (which IS linear in
  file count, since the manifest contains an entry per file). At
  ~320 files in a 10-minute backlog the manifest is hundreds of KB,
  not MB. Worth measuring at scale but not load-bearing for v1.

- **Per-table backpressure**: v1 uses a single global `MAX_PENDING`
  threshold. One slow table's outage would 429 every writer for every
  healthy table. Not different from DuckLake's "backing-DB-down"
  failure mode today, so accept it for v1. Per-table thresholds (or
  per-table icebox dispatch with separate commit pipelines) is a
  follow-up once we have multiple iceberg consumers active and a real
  noisy-neighbor incident to motivate the work.

- **Orphan file cleanup**: PyIceberg 0.11.1 does NOT ship a
  `remove_orphan_files` operation — that's Java/Spark-only. We'll
  need to roll our own as a small Python script. The script must be
  icebox-aware: it should JOIN against `icebox.files` and skip any
  path present there regardless of age. For v1 we accept that orphan
  files accumulate slowly; we'll address when storage costs flag it
  or when we wire up a maintenance CronJob. Plain `remove_orphan_files`
  without icebox awareness would delete our staged-but-not-yet-
  committed files if the icebox stays offline longer than the
  cleanup `older_than` threshold — don't run it that way.

- **Snapshot history expiration**: similar to orphan cleanup —
  PyIceberg 0.11.1 doesn't auto-expire old snapshots. metadata.json
  grows on every commit (snapshot history accumulates). At 1
  commit/min × 24h = ~1440 snapshots/day; metadata.json grows MBs
  over weeks. Readers feel this since metadata.json is the entry
  point for every load_table. Mitigation: write our own snapshot-
  expiry script alongside the orphan-cleanup one. Same follow-up
  bucket.

- **Backpressure threshold tuning**: `MAX_PENDING=1000` and
  `degradedFailureThreshold=2` are educated guesses. Tune in prod-us
  after first deployment.

- **REST API auth**: v1 ships unauthenticated (network-level isolation
  via K8s NetworkPolicy is the only gate). If/when icebox is exposed to
  external producers, add bearer token + per-producer scopes.

- **PyIceberg pin canary expansion**: `tests/unit/test_pyiceberg_pin.py`
  currently asserts two private symbols (`_pyarrow_to_schema_without_ids`,
  `assign_fresh_schema_ids`). The icebox committer adds more private-
  symbol dependencies that the canary MUST cover, otherwise a PyIceberg
  bump silently breaks the committer:
  - `Transaction._append_snapshot_producer(snapshot_props, branch=...)`
    — signature + that it returns a context manager whose
    `__exit__` triggers the commit_table call
  - `DataFile.from_args(_table_format_version, **kwargs)` — classmethod
    signature + that it accepts the keyword fields enumerated in the
    Committer code block above
  - `DataFile._data` tuple length == 16 (or whatever the spec version
    requires); assert positional layout matches the property order
  - `pyiceberg.conversions.to_bytes(iceberg_type, value)` — signature
    + at least one round-trip assertion per primitive type
    (int, long, string, float, double, date, timestamp, timestamptz)
  Drift in any of these means the icebox committer will TypeError or
  silently corrupt manifests; the canary should fail loudly at install
  time, not at first production commit.

- **PG credentials for v1**: the plan says "reuse Lakekeeper's
  existing PG credentials" but also "icebox database has its own
  role" — these contradict. v1 resolves it as: bootstrap creates the
  `icebox` database, then `GRANT CREATE, CONNECT ON DATABASE icebox`
  + `GRANT ALL ON SCHEMA icebox` to Lakekeeper's existing PG user.
  Lakekeeper's user can write to icebox tables. Operationally weird
  (Lakekeeper's pod can technically corrupt icebox state if
  compromised), but the same blast radius as today (same pod has full
  write to its own catalog DB). Follow-up: split into a dedicated
  icebox role with grants only on the icebox database.

---

## Non-goals (v1)

- Multi-replica icebox with leader election. Single-replica with
  `Recreate` strategy is the design choice.
- Exactly-once semantics across writer crash + icebox recovery in the
  literal transactional sense. The system is at-least-once on writer
  side (deterministic paths + UNIQUE constraint dedupe make it
  effectively exactly-once) and the icebox cycle is exactly-once at the
  Iceberg + Kafka level via the in-transaction sequence.
- General-purpose icebox service for non-millpond producers. The REST
  API is shaped to support this eventually, but the docs/auth/SLA
  story isn't in scope for v1.
- Per-writer-tables fallback. We're committing to the single-table
  architecture; per-writer-tables stays as a "if icebox doesn't work"
  contingency plan, not a parallel implementation.

---

## Out of scope / future work

- Move `IceboxClient` from inline `httpx` calls in the sink to a
  dedicated `shared/icebox_client.py` once a second producer exists.
- Add bearer-token authentication on POST /v1/files.
- Per-table icebox routing (today we assume one icebox handles one
  table per release; if multiple tables share the icebox, schema
  routing logic is needed).
- Compaction job that runs `rewrite_data_files` on the icebox-fed
  table — separate from the icebox itself.
- Icebox-aware orphan cleanup wrapper (see Open Questions above).
- Snapshot history expiration script (see Open Questions above).
- Per-table backpressure thresholds + per-table icebox dispatch
  (see Open Questions above).
- **Puffin sidecar support for bloom filters and richer statistics.**
  Iceberg v3 defines Puffin as a sidecar format for bloom filters,
  theta sketches, and other large per-column indexes that don't fit
  in manifest metadata. PyIceberg 0.11.1 has partial Puffin support
  (read; write is patchier). For the icebox, the natural extension:
  writers compute bloom filters during parquet write (PyArrow doesn't
  expose this directly; would need a separate pass or a Rust helper),
  ship the Puffin blob (or its serializable description) in the POST
  body, committer writes the Puffin sidecar file to S3 alongside the
  manifest and registers it in the snapshot's statistics-files list.
  Cuts query-time false positives dramatically on selective predicates
  (e.g., distinct_id = X queries). Real future work; not in v1.
- Dedicated icebox PG role (not Lakekeeper's user reused) with grants
  scoped to the `icebox` database only.

### Upstream Lakekeeper contribution (separate concern)

Once external icebox is stable, there's value in proposing a similar
pattern upstream in Lakekeeper as a generic "staged commits" feature.

Treat this as **entirely separate from our v1**. The upstream design
would NOT be a 1:1 port:

- Lakekeeper-native staged commits would be generic (not Kafka-aware),
  so the atomic Iceberg+Kafka contract our v1 provides would NOT
  carry over. Producers using a Lakekeeper-native version would
  commit their own offsets and accept eventual-consistency on the
  Iceberg side.
- Producers wanting strict ordering would use optional commit
  callbacks (webhooks).
- Built-in orphan-cleanup awareness (cleanup naturally joins against
  staged_files in the same component).

Migration from our v1 to a hypothetical Lakekeeper-native v2 would
be a behavioral change for writers — NOT a cosmetic URL change. Our
v1 has a stronger contract than the proposed v2 would. That's a
deliberate v1 design choice, not a bug.

Open this as a follow-up after our internal icebox proves the pattern
in production for several weeks. Not part of any v1 commitment.
