# drop-partitions: server-side partition drop for DuckLake (v3 — sign-off ready)

Status: v3. Review trail: two adversarial agent reviews (engine fidelity;
ops/concurrency) folded into v2; independent verification pass
(`partition-drop-findings.md`) confirmed both, corrected two mechanism
claims, and added M1-M3 + nits — all folded here. Superseded v1/v2 text is
REMOVED, not layered. Implementation awaits Jakob's sign-off.

Target: `tools/ducklake_maintenance.py` new op `drop-partitions`, sibling of
the PG-native `expire_snapshots`. First consumer: megaduck `events_nrt`
14-day retention; recurring daily thereafter.

## Why not DuckLake DELETE

1. DELETE scans every candidate file to prove full coverage (no
   metadata-delete path; delete vectors also require the scan and pay the
   same commit conflict). The compacted era is tens of TiB.
2. The fork's OCC conflicts insert-vs-delete at TABLE level
   (`ducklake_transaction_state.cpp:209, 225`). Mechanism (corrected per
   findings M3): PK retries DO advance the baseline (fresh head read,
   `ducklake_metadata_manager.cpp:4457-4460`), but the first conflict CHECK
   that sees a concurrent insert throws with `can_retry=false`
   (`:1930, :1944`) — immediate, deterministic abort. Any DELETE whose
   scan+commit window overlaps an nrt insert (~every 26s) dies after paying
   its full scan. Observed live 2026-08-26, both conflict classes.
3. The nrt writer feeds realtime pipelines and cannot be paused for hours.

## What the engine's whole-file-drop commit writes (fork
`merge-upstream-2026-08-26`, verified thrice)

| # | Write | Source |
|---|---|---|
| 1 | `INSERT INTO ducklake_snapshot VALUES ({id}, NOW(), {schema_version}, {next_catalog_id}, {next_file_id})` | metadata_manager.cpp:4416-4418 |
| 2 | `UPDATE ducklake_data_file SET end_snapshot = {id} WHERE end_snapshot IS NULL AND data_file_id IN (...)` | :2584-2594 |
| 3 | `UPDATE ducklake_table_stats SET record_count=<abs>, file_size_bytes=<abs>, next_row_id=<abs>` | :4953-4956 |
| 4 | `INSERT INTO ducklake_snapshot_changes VALUES ({id}, 'deleted_from_table:<t>', <author>, <msg>, <extra>)` | :4427-4433; token transaction_state.cpp:391 |

Plus per-column `ducklake_table_column_stats` no-op UPDATEs (values
unchanged while live rows remain), and a DELETE of those rows if the table
empties — we refuse the empty case (assertion c). `ducklake_schema_versions`
is not written. `next_row_id` is monotonic, never recomputed downward —
carried forward untouched.

### F1: the stats ObjectCache clobber (and why we bump next_file_id)

`GetTableStats` caches per-process on `(snapshot.next_file_id, table_id)`
(`ducklake_catalog.cpp:818-823`); invalidation is local-process only. A drop
does not bump `next_file_id`, so a conflict-aborted millpond writer reopens
with a cache HIT on pre-drop stats and its absolute stats write resurrects
the dropped counts — silent, cross-process. This is an engine bug too (a
real DELETE has it); filed on the fork-fix list.

The tool closes it: **write the snapshot with `next_file_id = baseline + 1`**
(the engine's inlined-commit idiom, transaction_state.cpp:1167-1170). Every
process's next stats read cache-MISSes and reloads the decremented row.
Costs one file id; ids are non-contiguous everywhere. No collision risk:
in-flight writers allocate below the head counter.

## Tool design

### Enumeration — `list-droppable-partitions`, read-only, once per RUN

Enumeration never writes and runs outside every drop transaction (selection
inside a writing txn would make snapshot allocation a livelock: a
multi-minute snapshot vs ~3 commits/s). Its output — the leaf manifest — is
advisory; every drop re-verifies in its own transaction:

- One aggregated fpv pass (bitmap via the fpv table_id index), pivot to
  `(data_file_id, partition_hour)`, join live `ducklake_data_file`.
- **Spec-generic, not table-specific**: the tool resolves the table's
  LIVE partition spec from `ducklake_partition_info`/
  `ducklake_partition_column` at run start — column list, transforms,
  key order — and locates the time keys BY TRANSFORM TYPE
  (year/month/day[/hour]), never by hardcoded index. Any spec works:
  (team, y, m, d, h) on events_nrt, (y, m, d) on ducklings, etc.
- **Partition-spec pinning (data-loss class)**: fpv `key_index` meaning
  is PER-FILE via `data_file.partition_id`. Select only
  `df.partition_id = $LIVE_SPEC_ID`; ABORT the run if any
  candidate-window file carries a different or NULL partition_id (a
  file written under an older spec has different key semantics).
  Remedy for old-spec/rotted files: the existing
  `repair-partition-values` cron step (runbook link).
- fpv sanity: every doomed file has exactly the pinned spec's key-index
  set, one well-typed value per index; non-conforming files excluded +
  counted + reported; count above threshold aborts.
- Partition time composed from the spec's time-transform keys via
  `make_timestamptz(..., 'UTC')` (hour key optional per spec) — never
  string comparison. Cutoff aligned to the spec's finest time grain.
- **Structural floor, applied at selection** (safe there: snapshot ids only
  move forward): never select files with `begin_snapshot` newer than the
  newest snapshot older than (retention − 48h). **Implementation note
  (2026-08-28):** under megaduck's 3-day expire no snapshot row that old
  survives, so the floor degenerates to `min(surviving snapshot_id) − 1` —
  the expiry horizon (~3d) becomes the effective floor, not 12d. The cursor
  guard is the primary straggler protection; the floor is defense-in-depth.
  (v3's original formulation is vacuous wherever expire < retention − 48h.)
- **One leaf per operation — no batching in the mutating path**: the
  drop op REQUIRES a fully resolved partition tuple
  (e.g. `team_id=2,year=2026,month=8,day=1,hour=13`) and drops exactly
  that leaf in one transaction / one snapshot. Leaves are small by
  construction (events_nrt ceiling ~800 files = writer-count x
  flush-rate); `max_files` remains as a sanity ceiling that ABORTS on a
  pathological leaf. Atomicity is therefore trivial: a leaf is dropped
  whole or not at all.
- **Campaigns are a driver loop over a read-only enumeration**: a
  companion `list-droppable-partitions` subcommand runs the aggregated
  fpv pass once (read-only) and emits the leaf manifest — tuple,
  structured values, file/row/byte counts, max begin_snapshot — for
  every leaf wholly below the cutoff. The campaign driver (or the
  operator) loops the drop op over the manifest, oldest first.
  **Implementation note (2026-08-28):** per-leaf drops re-SELECT by
  tuple (indexed lookups) rather than consuming a manifest id list —
  fresher, and equivalent because the floor excludes all
  post-enumeration arrivals; manifest file-id lists are audit-only and
  opt-in (`--with-file-ids`), since they're tens-hundreds of MB of dead
  JSON at campaign scale. The manifest remains advisory like all
  selection; everything re-verifies in-txn. Progress is per-leaf
  durable, resumable, and trivially rate-limited.
- **Guard interaction**: the fresh per-invocation cursor guard applies
  to the WHOLE leaf — if any member file is guard-ineligible
  (straggler with begin_snapshot >= guard), the invocation SKIPS the
  leaf entirely (exit code + metric; it ages into eligibility). The
  one remaining partial-leaf source is a compaction interleave: a
  doomed file merged mid-flight leaves its rows in a NEW
  same-partition file outside the manifest — the next enumeration
  picks it up. Transient, self-healing, visible via the skipped-id
  metric.

### Per-leaf transaction (short, <500ms target)

**Execution vehicle: a DIRECT libpq connection (psql/psycopg from the
maintenance image), NOT the duckdb-attach `postgres_execute` path.**
Rationale (2026-08-28): postgres_execute imposes REPEATABLE READ (would
need overriding), returns no result sets (would force `:SNAP` through
chained-CTE/`DO $$` contortions — findings N1), and its duckdb teardown
core-dumps after success (observed on the compaction jobs, exit 134,
'wedged duckdb connection'). Direct libpq gives native READ COMMITTED,
RETURNING to the driver, plain BEGIN/COMMIT with driver-side assertions
between statements, and no duckdb lifecycle at all. All reviewed SQL,
assertions, and retry classes are unchanged. `SET LOCAL
statement_timeout` and `lock_timeout` (5-10s) inside each txn.

Transaction contents:

1. Snapshot row: `max(snapshot_id)+1` (the universal PK race),
   `clock_timestamp()` (not NOW() — transaction-start time would backdate),
   current `schema_version`, `next_catalog_id` carried,
   **`next_file_id = baseline + 1`** (F1).
2. File UPDATE, re-verifying per row — and this is where the guards bind:
   `SET end_snapshot = :SNAP WHERE data_file_id IN (<leaf manifest>) AND
   end_snapshot IS NULL AND begin_snapshot < :GUARD` — the **fresh
   per-invocation cursor guard applied in the WHERE** (findings M2: a
   flush-failure rewind between enumeration and this drop must exclude the
   file here, not just at selection). `RETURNING record_count,
   file_size_bytes` feeds everything downstream.
3. Stats: RELATIVE decrement from the RETURNING sums — never absolute,
   never from the pre-selected set. Relative is what makes both concurrent
   orderings correct (below).
4. `ducklake_snapshot_changes`: `'deleted_from_table:' || $TABLE`, author
   `'drop-partitions'`, message with campaign/leaf id.

Assertions (driver-side checks between statements, inside the open txn; any failure ROLLBACKs the leaf drop and is classified non-retryable):

- (a) **Tolerance, not equality** (findings M1): rows updated ≤ manifest id
  count is EXPECTED (compaction may have hard-deleted members; rewound
  guard may exclude members). Skipped ids are logged + counted (metric).
  Strict arm: re-verify skipped ids and abort ONLY if one is STILL LIVE
  and guard-eligible — our UPDATE missed a row it should have hit.
- (b) Stats row values ≥ decrement (no negative write — engine parity with
  `SubtractDroppedFileStat`), AND exactly one stats row updated (no PK on
  `ducklake_table_stats`).
- (c) Decrement must not take `record_count` to 0 (refuse the engine's
  distinct empty-table path).
- (d) No **LIVE** `ducklake_delete_file` row (`end_snapshot IS NULL`)
  references a leaf file — scoped to live vectors (findings N2: a
  historical ended vector must not wedge every run).
- (e) Fork version pin: `ducklake_metadata` version must equal the verified
  value ('1.0' / '1.1-dev1' family); abort otherwise; re-validate on fork
  upgrades before moving the pin.

Retry policy: 23505 (snapshot PK), 40001, 55P03/lock_timeout retryable with
jittered backoff; 57014 (statement_timeout) burns attempts; assertion
assertion failures non-retryable. Cap 50 attempts per leaf, then abort the run.
Expected ~4-5 attempts per leaf at ~3 commits/s with <500ms transactions
(collision window ≈ txn duration).

## Concurrency correctness (the crux, both orderings)

- **We commit first (id N)**: millpond's in-flight commit PK-collides,
  retries with a fresh head read, its conflict checker consumes our
  `deleted_from_table:<t>` token from `ducklake_snapshot_changes`
  (STRING_AGG over `snapshot_id > baseline`), hard-aborts
  (`can_retry=false`), and millpond's `_write_with_retry` reopens a fresh
  statement whose baseline includes our drop — with the F1 bump forcing a
  stats cache MISS, its absolute stats write is computed post-drop.
  Correct. Cost: one aborted flush attempt, identical to a real DELETE.
- **millpond commits first**: our PK insert collides, we retry, our
  relative decrement applies on top of its committed absolute write.
  Correct.
- **Compaction** (findings M3 — the advisory lock does NOT mutex it; no
  lock site covers `compact`): the real protection is the token. Our
  committed snapshot hard-aborts a concurrent compaction commit on the same
  table (transaction_state.cpp:267, `can_retry=false` → per-table failure;
  the cron continues with other tables). The reverse order — compaction
  commits between enumeration and a leaf drop — is absorbed by READ COMMITTED +
  the tolerant UPDATE (its members simply skip) + RETURNING-derived stats.
  **Expected observable**: `maintenance_compact_tables_failed` blips for
  events_nrt during campaign runs; document in the runbook.
- **millpond error metrics** (findings N4): the token-conflict
  `TransactionException` does not match `_is_commit_contention` substrings
  (main.py:291-310), so each drop can tick
  `errors_total{type="write_retry"}`. Preferred: add "Transaction conflict"
  to the classifier in the same millpond PR; otherwise document the
  expected spike against the MillpondErrorsHigh threshold.

## Consumers

| Consumer | Behavior on drop snapshots | Gate |
|---|---|---|
| viaduck fleet (main changefeed) | Never reads `changes_made`; plans on `begin_snapshot` ranges; delete-only snapshots skip cleanly | Cursor guard (below) |
| viaduck single-destination duckling | **Crash-loops by design** on any drop (`end_snapshot > cursor` OR the token) regardless of file age — the witness encodes "immutable," not "log with retention" | Not deployed against events_nrt today (branch never merged). Preflight: image-digest inventory of everything attaching megaduck; first prod drop is a CANARY leaf + consumer sweep (runtime confirmation is impossible — the witness is latent code) |
| clickhouse-ducklake (findings N10) | Direct megaduck ATTACH, head reads only — a drop snapshot is invisible to head reads | List + verify head-only during validation |
| duckgres / Trino | Never attach megaduck | none |

Witness redesign (separate viaduck work, not this tool): narrow
`_assert_no_deletes` to the Kafka invariant — fatal iff an ended file's
content is at/after my cursor, or my cursor left the retained window.

### Cursor guard

- Derived per LEAF DROP from viaduck's durable committed cursor store (direct
  SQL) — never scraped gauges (flush-failure rewinds move cursors
  BACKWARD). Min across ALL pipelines/destinations incl. metrics-only.
- Applied per leaf drop: whole-leaf skip if any member is ineligible,
  plus the per-file WHERE as defense-in-depth (M2); floored structurally at
  selection (retention − 48h).
- Run-level abort: any consumer lag > retention − 48h — that, not the
  guard value, is the actual loss condition.
- `--no-viaduck-guard` exists but prints the lag-vs-retention numbers it
  overrides.

## Rollback — manual procedure, ships AFTER the drop path

A drop is reversible until expiry processes its snapshot (≥3 days). The
procedure is a first-class OCC commit, never raw mutation:

- NEW snapshot via the same PK-retry machinery; un-end the files; stats
  corrected by RECOMPUTATION verified against `sum(record_count)` of live
  files (never a blind re-increment — findings F7: interleaved absolute
  writes make blind increments wrong); a conflict token so concurrent
  writers rebase; snapshot S's row LEFT IN PLACE (empty snapshot, expiry
  ages it; deleting it would re-open the id-reuse OCC hole).
- Preconditions, hard-abort otherwise: S within retention; zero affected
  files in `files_scheduled_for_deletion`; fpv/stats rows intact for every
  affected file.
- Semantics caveat (findings N9): un-ending makes the files visible at ALL
  snapshots ≥ begin_snapshot again — a history rewrite. Acceptable for an
  emergency procedure; stated here and in the runbook.
- Drill the procedure on dev before prod relies on it.

## Operations

- Advisory lock (`hashtext('millpond-ducklake-maintenance')`): acquired /
  released PER LEAF DROP with pacing sleeps so expire/cleanup crons interleave.
  Same-session re-acquisition is REFCOUNTED (findings N7) — pair
  acquire/release exactly, or `pg_advisory_unlock_all()` per leaf drop.
  Ownership re-verified each leaf drop (a dropped pg session loses the lock
  silently). Wall-clock budget per invocation.
- Table identity resolved by `schema.table_name` + cross-check; bare
  table_id refused.
- Per-leaf structured log: leaf tuple, snapshot_id, files ended, rows, bytes, skipped
  ids + reason, retries, txn_ms, window, guard value. Pushgateway gauges:
  `files_dropped_total`, `leaves_dropped_total`, `leaves_skipped_total`, `retries`, `aborts`,
  `skipped_files_total`, `excluded_rot_files`, `last_success_timestamp`.
- Dry-run default; `--execute` to write. Cutoff younger than 7 days refused
  without `--force-young-cutoff`.

## Prerequisite one-liner (separate PR, before the campaign)

Pin the millpond writer's timezone (`SET TimeZone='UTC'` in `connect()` or
`TZ=UTC` in the chart) — partition transforms bind session TZ at write time
and today's UTC is only de-facto from the base image (findings N5: delete
the hazard class instead of sampling for it). The fpv-vs-column-stats
cross-check below stays as independent verification.

## Validation protocol (mw-dev + synthesized scale)

1. Scratch partitioned table; real engine DELETE on one partition vs the
   tool on an equivalent one; diff the catalog deltas. Expected deviations
   ONLY: `next_file_id` +1, `clock_timestamp` vs NOW, author/message, and
   the engine's per-column no-op `table_column_stats` UPDATEs.
2. **Two-PROCESS stats-clobber loop** with a cache-hit precondition: prove
   the writer process held the pre-drop cached stats entry (populate via a
   pre-drop read; observe the post-drop MISS) — without the precondition
   the test passes vacuously.
3. Synthesized ≥1M-row catalog: EXPLAIN ANALYZE the enumeration and a leaf drop;
   isolation-level assertion inside the drop transaction (READ COMMITTED).
4. Sustained ≥3 synthetic commits/s during a leaf-drop loop: writer convergence,
   zero lost inserts, stats invariant (`sum(record_count) of live files ==
   table_stats.record_count`) holds throughout.
5. Forced compaction interleave ending a doomed file mid-run: the leaf drop skips
   it, stats correct, run continues (M1 tolerance path).
6. Forced cursor REWIND across a sampled guard between selection and
   drop: whole leaf skipped by the guard re-check (M2 path).
7. FLEET viaduck at lag reading through drop snapshots (a duckling reader
   fatals BY DESIGN — that is the gate, not a validation failure).
8. Crash injection: driver kill mid-retry / between leaves; pg session
   drop (lock-loss re-verify); two concurrent operators.
9. fpv rot fuzz: missing/duplicated/NULL/non-numeric/collapsed indexes;
   old-spec partition_id files; cutoff-boundary file.
10. TZ cross-check: fpv hour values vs `file_column_stats` min/max for
    sampled files.
11. Expiry end-to-end: dropped files reach `files_scheduled_for_deletion`
    and physical cleanup after retention; rollback drill before prod.

Success claim: "validated against the enumerated hazard list" — never
"100% safe"; this tool forges engine commits.

## Rollout

1. Millpond PR: the tool + TZ pin + `_is_commit_contention` classifier
   addition + unit tests for SQL generation; validation results attached.
2. mw-dev full protocol pass.
3. Prod: consumer image-digest inventory → CANARY leaf (one small
   tenant-hour, watched) + consumer sweep → one full hour of leaves →
   day-sized manifest loops for the
   14-day campaign → recurring daily cron step (guard fully automated).
4. Post-campaign: `VACUUM ANALYZE ducklake_data_file` once expiry has
   flushed the ended rows; watch the expiry queue depth throughout.
