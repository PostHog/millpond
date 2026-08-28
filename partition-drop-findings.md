# drop-partitions: review findings

Adversarial review of `DROP_PARTITIONS_PLAN.md` (v1 and v2), verified against:

- `~/src/ducklake` @ `merge-upstream-2026-08-26` (177d492e) — the fork
- `~/src/millpond` — writer + `tools/ducklake_maintenance.py`
- `~/src/viaduck`, `~/src/duckgres`, `~/src/charts`, `~/src/posthog-cloud-infra` — consumers/deploy
- `millpond/ducklake.sql` — catalog schema dump

All file:line citations below were read directly. The postgres_scanner extension
source (what `postgres_execute` actually wraps) is not checked out locally; the
in-repo claim that it uses REPEATABLE READ (`ducklake_maintenance.py:1221-1227`)
remains a validation item — v2 already treats it as one.

## Verified load-bearing claims (both review rounds)

| Claim | Evidence |
|---|---|
| Engine whole-file-drop commit = exactly the 4 writes in the plan's table | `ducklake_metadata_manager.cpp:4416-4418` (snapshot), `:2584-2594` (FlushDrop), `:4953-4956` (absolute stats), `:4427-4433` (changes); token `ducklake_transaction_state.cpp:391` |
| Snapshot id = `max+1`; no sequences/reservations/gaps | `ducklake_transaction_state.cpp:1937-1939` |
| insert-vs-delete conflicts at table level, both directions | `ducklake_transaction_state.cpp:209, 225` |
| compaction-vs-delete conflicts at table level, both directions | `:223-224` (deleter checks), `:266-271` (compactor checks, emits `merge_adjacent:<id>` at `:401`) |
| delete-vs-delete same table: no table-level conflict; file-level check via `GetFilesDeletedOrDroppedAfterSnapshot` | `:228-252`, `:1818-1826` |
| `changes_made` = one comma-joined VARCHAR per snapshot; PK is `snapshot_id` alone | `ducklake.sql:460-467`; parse `ducklake_transaction_changes.cpp:36-104`; agg `ducklake_metadata_manager.cpp:4443-4446` |
| `next_row_id` never recomputed downward; safe to leave untouched | delete path `:1004-1015`; compaction carries forward `:868` ("monotonic") |
| No `files_scheduled_for_deletion` write at drop time; expiry cascades later; millpond PG-native expire handles ended-by-anyone files identically | `ducklake_maintenance.py:481-490, 536-563`; fork: scheduling only in compaction/expire/`DeleteOverwrittenDeleteFiles` |
| Column order of `ducklake_snapshot` / `ducklake_snapshot_changes` matches the plan's positional INSERTs | `ducklake.sql:444-467` |
| Catalog `version` is `'1.0'` or `'1.1-dev1'` on this fork | `ducklake_initializer.cpp:175-178`; `ducklake_metadata_manager_v1_1.cpp:35-38`; migration `:442` |
| Millpond writer: insert-only (no compaction/delete/expire), never writes `ducklake_table_stats`, retries ALL write exceptions with fresh statement baseline | `millpond/ducklake.py`, `millpond/main.py:313-339` (`_write_with_retry`) |
| Writer TZ is **not pinned** — de-facto UTC via `python:3.12-slim` base image only; `year/month/day/hour` transforms bind session TZ at write time | `millpond/ducklake.py:310-379` (no `SET TimeZone`); no `TZ` in Dockerfile/k8s/charts; `ducklake_partition_data.cpp:283-301` (contrast epoch_* UTC normalization at `:264-268`) |
| events_nrt partition expression = `team_id,year,month,day,hour(_inserted_at)` → key indexes 0..4 | `charts/argocd/millpond/values/common.yaml:58`; layout confirmed in `events_nrt.txt` |
| Viaduck fleet never reads `changes_made`; plans purely on `begin_snapshot` ranges; delete-only snapshots skipped cleanly; cursor = highest durably-committed snapshot | `viaduck/feed.py:344-351, 589-596`; `main.py:1672-1676`; `delivery.py:846-859` |
| Viaduck `single_destination` (duckling) **crash-loops** on any drop snapshot (`end_snapshot > cursor` or `deleted_from_table` token); not deployed against events_nrt today | `viaduck/single_destination.py:499-541` |
| duckgres / Trino never attach the megaduck catalog | duckgres: per-org ducklings only; Trino: per-org catalogs via control plane |
| Engine whole-file drop does NOT bump `next_file_id`; engine invalidates the local stats cache keyed on the new snapshot's `next_file_id` | `ducklake_transaction_state.cpp:1958` → `ducklake_catalog.cpp:1129`; bumps only on new files / inlined idiom `:1167-1170` |

## F1: the stats ObjectCache clobber (v2's key addition — independently verified)

`GetTableStats` cache key is `StatsCacheKey(snapshot.next_file_id, table_id)` +
`schema_version` guard (`ducklake_catalog.cpp:818-823`). The cache is per-process
(DuckDB ObjectCache). `invalidate_table_stats_cache` is **local-process only**.

Without `next_file_id = baseline + 1` in the hand-written snapshot:

1. Millpond flush PK-collides on our snapshot, conflict-checks, hard-aborts
   (`TransactionException`, `can_retry=false` — see "Corrections" below).
2. `_write_with_retry` re-executes the INSERT; `reset_caches()` clears only
   Python-side state (`ducklake.py:641-643`), NOT the DuckDB ObjectCache.
3. The fresh DuckLake transaction's baseline snapshot carries the SAME
   `next_file_id` → `StatsCacheKey` HIT → stale pre-drop stats → its absolute
   `ducklake_table_stats` write resurrects the dropped counts. Silent, cross-process.

With the bump: fresh baseline → cache MISS → reload → correct absolute write.
Id waste: one id per batch; ids are non-contiguous everywhere already. No
collision risk — in-flight writers allocate file ids below the head's
`next_file_id`; the bump only moves the counter.

**This is also an engine bug**: a pure engine DELETE doesn't bump `next_file_id`
either, so an engine drop has the same cross-process stale-stats resurrection.
The tool works around it; fork fix is filed separately per the plan. The
validation byte-diff (v1 step 4) will show the `next_file_id` delta — expected,
not a deviation (v2 line 104-105 already anticipates).

**Validation ask:** the two-process clobber test must prove the writer process
actually HELD a cached stats entry at the pre-drop key (populate via a pre-drop
read; observe the miss after). Otherwise the test can pass vacuously.

## v1 blockers → v2 status

| # | v1 finding | v2 status |
|---|---|---|
| B1 | `RETURNING` can't reach the driver through void `postgres_execute` | Fixed: chained-CTE single statement (v2 lines 26-28); see nit N1 |
| B2 | retry set must include 40001 (+40P01/55P03), not just 23505 | Fixed (line 23-24: PK/40001/lock_timeout, cap 50) |
| B3 | rollback must not `DELETE FROM ducklake_snapshot` (id-reuse OCC hole: conflict windows are `snapshot_id > baseline`, `ducklake_metadata_manager.cpp:4443-4446`) | Fixed: rollback is a new OCC commit, S left in place (lines 80-89) |
| B4 | advisory lock does NOT mutex compaction (all 11 lock sites ≤ `ducklake_maintenance.py:1691`; `compact` = `:2109+`) | **Text still false (v1 lines 226-229 uncorrected) — see M3**; race itself covered by token + retries |
| C1 | stats decrement from rows actually ended (`UPDATE ... RETURNING`), never the pre-selected set | Fixed (lines 39-41) |
| C2 | partition-key indexes are per-spec-epoch (`ducklake_data_file.partition_id` → `ducklake_partition_info`/`_column`) | Fixed, stronger: spec resolution + abort on mismatch (lines 52-62) |
| C3 | engine deletes `ducklake_table_column_stats` when a drop empties the table | Fixed by refusal: assertion (c) (lines 46-47) |
| C4 | assert stats rowcount + non-negative (else later engine deletes throw non-retryable `InternalException`, `ducklake_transaction_state.cpp:983-988`) | Fixed: assertion (b) (lines 44-45); rowcount → nit N3 |
| C5 | cursor guard from durable SQL store, not gauges; duckling gate | Fixed, stronger (lines 65-78: SQL cursor, rewind rationale, structural floor, lag abort) |

## Remaining must-fix (against v2)

### M1 — Assertion (a) contradicts the liveness-tolerance design

Lines 39-41 tolerate per-row liveness changes (RETURNING-derived decrement);
assertion (a) (`updated rowcount == batch id-list count`, lines 42-43)
RAISE-aborts on exactly that condition. A compaction that ends a batch file
between selection and batch is PERMANENT: every retry re-fails (a), burns all
50 attempts, aborts the run — for the benign race the validation protocol
itself forces ("forced compaction interleave ending a doomed file mid-run",
line 108).

Fix: tolerate `<=`; decrement from RETURNING (already mandated); log/metric
skipped ids. A stricter check, if wanted: re-verify skipped ids and abort only
if a skipped file is STILL LIVE (our UPDATE missed a row it should have hit —
that one genuinely shouldn't happen).

### M2 — The per-batch cursor guard is derived but never applied

Lines 71-74 derive the guard per batch (correct: flush-failure rewinds move
cursors backward below a sampled value — their own rationale), but the batch
contents (lines 30-50) re-verify only liveness. A backward cursor move between
selection and batch N silently drops a file a destination hasn't consumed —
the exact loss condition the guard exists to close.

Fix: apply `begin_snapshot < $GUARD` per batch — in the batch UPDATE's WHERE
(requires M1's tolerance) or as a driver-side re-filter of each chunk against
the fresh guard. The structural floor (lines 75-76) is safe to apply once at
selection: snapshot ids move only forward; the cursor does not.

### M3 — Superseded v1 text is still live and hazardous

"v2 REVISIONS supersede anything contradictory below" is not enough for blocks
that describe concrete, different procedures:

- **v1 gate 6 (lines 272-279)**: the DELETE-snapshot rollback, described as
  "provided as a `--rollback-snapshot S` subcommand" — v2 cancels the
  subcommand entirely. Strike it.
- **v1 gate 2 (lines 259-264)**: operator-supplied
  `min(viaduck_dest_last_snapshot_id)` from metrics — superseded by lines
  71-74 (durable SQL cursor, never gauges). Strike or annotate.
- **v1 lines 226-229**: "mutexes expire/cleanup/compaction" — false for
  compaction, and v2 never states what actually protects the race. Add the
  real story: our `deleted_from_table:<t>` token hard-aborts a concurrent
  compaction commit (`ducklake_transaction_state.cpp:267`, `can_retry=false`
  → per-table failure, cron continues at `ducklake_maintenance.py:2347`);
  the reverse order is 40001/EPQ + RETURNING-derived decrement on our side.
  Document expected `maintenance_compact_tables_failed` for events_nrt
  during the campaign.
- **v1 lines 129-131**: "retry baseline never advances" — wrong; PK retries
  DO advance (`ducklake_transaction_state.cpp:1932-1939`, fresh head read at
  `ducklake_metadata_manager.cpp:4457-4460`). The deterministic DELETE failure
  is the conflict throw at `can_retry=false` (`:1930, :1944, :1979-1991`).
  Conclusion unchanged; the document is the audit trail — fix the mechanism.

## Nits

1. **Assertion plumbing**: `GET DIAGNOSTICS` reflects only the top-level
   statement. Capture per-CTE counts/sums via aggregate CTEs + `SELECT ... INTO`
   PL/pgSQL variables inside `DO $$ ... $$`; assertions as `RAISE` with a
   distinct SQLSTATE classified NON-retryable (vs retryable
   23505/40001/55P03; 57014 statement_timeout should burn attempts against the
   cap). Data-modifying CTEs share one statement snapshot and cannot see each
   other's effects — the chain passes only RETURNING values, which is legal.
2. **Assertion (d)**: scope to LIVE delete files (`end_snapshot IS NULL`); a
   historical ended vector would otherwise wedge every run permanently.
3. Fold "exactly one `ducklake_table_stats` row updated" into assertion (b)'s
   capture — no PK on that table (`ducklake.sql:542-547`).
4. **Rollout expectations**: expect a small millpond
   `errors_total{type="write_retry"}` spike per drop — the token-conflict
   `TransactionException` ("Transaction conflict - attempting to insert into
   table …") does not match `_is_commit_contention` substrings
   (`main.py:291-310`). Accept+document, or add "Transaction conflict" to the
   classifier.
5. **TZ**: the fpv-vs-`file_column_stats` min/max cross-check (line 115) is a
   good independent verification. Still pin `TZ=UTC` in the writer chart (or
   `SET TimeZone` in `connect()`) — one line that deletes the class instead of
   sampling for it.
6. "Expected ~1.8 attempts at 3 commits/s" implies a ~220ms collision window;
   with <500ms batches it's ~4-5. Immaterial given cap-50 — fix or drop.
7. Per-batch lock: same-session advisory-lock re-acquisition is refcounted —
   pair acquire/release carefully or `pg_advisory_unlock_all()` per batch
   (existing notes: `ducklake_maintenance.py:619-620, 1676-1677`).
8. Old-spec `partition_id` files abort the run forever (lines 57-58) —
   deliberate and right; link the remedy in the runbook:
   `repair-partition-values` already exists as a cron step.
9. Rollback doc: un-ending files makes them visible at ALL snapshots ≥
   `begin_snapshot`, including the dropped window — a history rewrite.
   Acceptable for an emergency procedure; say it in the manual procedure.
10. Consumer list: add clickhouse-ducklake (direct megaduck ATTACH, head reads
    only — harmless to a delete snapshot, but outside the cursor guard's
    "has consumed" semantics).

## Open questions from v1 — answered

| Question | Answer |
|---|---|
| `max(snapshot_id)+1` safe? Other id paths? | Yes; only path. PK race + retry is the universal mechanism. |
| Any reader besides the conflict checker parses `changes_made`? | In the fork: only `ducklake_snapshots()` display UDF (`ducklake_metadata_manager.cpp:4989-5008`). Consumers: fleet viaduck never reads it; duckling treats the token as fatal (undeployed); duckgres/Trino don't attach megaduck. |
| `next_row_id` recomputed downward anywhere? | No. Safe to leave untouched. |
| Missing `{SNAPSHOT_ID}` writers on this fork? | The 4 writes, PLUS `ducklake_table_column_stats` no-op UPDATEs (or DELETE when the table empties) — handled by assertion (c) refusal + documented diff delta. `ducklake_schema_versions` not written on pure drop. No catalog/server-side locks taken by the engine at all. |
| Writer TZ actually UTC? | Not pinned anywhere; de-facto UTC from the base image. v2 validates by cross-check; nit N5 pins it. |

## Validation protocol — coverage check

v2 additions (lines 102-117) cover the v1 gaps: two-process clobber loop,
forced compaction interleave, viaduck-at-lag with forced flush-failure rewind,
crash injection, dual operators (advisory lock), fpv rot fuzz, cutoff boundary,
in-call isolation assertion, TZ cross-check.

Still to add:

- Cache-hit precondition assertion for the two-process clobber test (above).
- Rollback drill on dev when the rollback ships (v2 correctly demotes it to a
  manual procedure shipping after the drop path — drill the procedure before
  prod relies on it).
- Specify "fleet" in v1 validation step 5's "dev viaduck continues" — a
  duckling reader fatals by design (that's the gate at lines 65-70).

## Bottom line

v1: approach sound, 4 blockers. v2: all four resolved, F1 is a genuine catch
(engine bug included). Fix M1 (assertion contradiction), M2 (guard not applied
per batch), M3 (hazardous superseded text) and this is sign-off ready. The
"validated against the enumerated hazard list, not 100% safe" framing
(v2 lines 116-117) is the right posture for a tool that forges engine commits.
