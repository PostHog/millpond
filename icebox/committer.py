"""Committer thread + cycle state machine.

Runs in a dedicated thread (NOT an asyncio task) because PyIceberg's
commit path is synchronous; running it on the event loop would block
incoming POSTs. See ICEBOX-PLAN.md "Async-vs-sync inside the icebox
process".

Per-cycle state machine:

  steady-state                            recovery (handles a crash at any step)

  1. claim_files(cycle_id, max)           - incomplete_cycles() finds in-flight cycles
  2. insert_cycle(cycle_id)               - find_snapshot_for_cycle() asks Lakekeeper
  3. validate schema fingerprints           if our snapshot_id is in the log
  4. build_data_files(rows)               - if YES: continue from step 7 (Kafka)
  5. iceberg_commit (PyIceberg)           - if NO:  release file claims, files re-enter
     → returns iceberg_snapshot_id        the unclaimed pool, cycle row stays as a record

  6. mark_iceberg_committed(snapshot_id)
  7. kafka_commit (AdminClient)
  8. mark_kafka_committed
  9. complete_cycle (sets completed_at +
     stamps files with snapshot_id)
  10. record_success

Failure handling:
  - Iceberg commit raises → release_cycle_claim, record_failure, exit cycle
  - Kafka commit raises → leave cycle stuck (iceberg already committed),
    record_failure, exit cycle. Next iteration's recovery path picks it
    up by checking Lakekeeper's snapshot_log.

Cadence:
  - Each cycle ends in time.sleep(cadence_seconds) before the next.
  - On exception inside a cycle, cadence is still honored (don't tight-
    loop a broken catalog).
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg_pool import ConnectionPool

from icebox import iceberg as ib
from icebox import kafka as kf
from icebox import postgres_sync as ps
from icebox.config import Config
from icebox.schema import CommitCycleRow

log = logging.getLogger(__name__)


# Floor on the sleep between cycles in committer_loop, even if the
# last cycle ran longer than the configured cadence. Prevents a tight
# failure loop from hammering Lakekeeper.
#
# 5s default: at the 60s baseline cadence, that's 12 attempts/min in
# failure mode — plenty for Lakekeeper to recover or for ops to notice,
# and the cost of cycle thrashing at prod scale (PG conn churn, manifest
# writes that may partially commit before failing) is non-trivial.
MIN_INTER_CYCLE_SLEEP_SECONDS = 5.0

# How long to wait between attempts to acquire the committer advisory
# lock at startup. Short enough that a normal Helm rollout transition
# (old pod terminating, new pod starting) doesn't make the new pod look
# stuck; long enough that the lock check doesn't busy-loop.
ADVISORY_LOCK_RETRY_SECONDS = 5.0


@dataclass
class CycleResult:
    """Outcome of one cycle iteration — used by tests + committer-loop telemetry."""

    cycle_id: UUID | None = None
    file_count: int = 0
    iceberg_snapshot_id: int | None = None
    kafka_offsets_committed: dict[int, int] = field(default_factory=dict)
    success: bool = False
    error: str | None = None
    skipped_reason: str | None = None  # "no_files" | "schema_mismatch" | ...


# A thin abstraction so tests can supply mocks for the Iceberg + Kafka
# side effects without standing up a real catalog or broker.
@dataclass
class CommitterDeps:
    """Side-effect callables the committer depends on. Tests pass mocks."""

    load_table: Callable[[], Any]  # returns pyiceberg Table
    commit_data_files: Callable[..., int] = ib.commit_data_files  # returns snapshot_id
    find_snapshot_for_cycle: Callable[..., int | None] = ib.find_snapshot_for_cycle
    build_data_file: Callable[..., Any] = ib.build_data_file
    kafka_admin: Any = None  # confluent_kafka AdminClient — built once at startup
    kafka_commit_offsets: Callable[..., None] = kf.commit_offsets


def run_cycle(
    *,
    cfg: Config,
    pg_pool: ConnectionPool,
    deps: CommitterDeps,
) -> CycleResult:
    """Execute one committer cycle. Returns CycleResult so the caller
    can observe outcome without scraping logs.
    """
    cycle_id = uuid4()
    result = CycleResult(cycle_id=cycle_id)

    # ---- Step 1-3: claim files in a PG transaction -----------------------
    with pg_pool.connection() as conn:
        with conn.transaction():
            ps.insert_cycle(conn, cycle_id=cycle_id)
            claimed_ids = ps.claim_files(
                conn,
                cycle_id=cycle_id,
                max_files=cfg.committer_max_pending_files,
            )

    if not claimed_ids:
        log.info("run_cycle: no files to claim — vacuous cycle %s", cycle_id)
        result.skipped_reason = "no_files"
        # Still considered a "success" for status semantics: the loop
        # ran and we're not falling behind, just nothing to do.
        result.success = True
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.update_heartbeat(conn)
        return result

    result.file_count = len(claimed_ids)
    log.info("run_cycle: cycle %s claimed %d files", cycle_id, len(claimed_ids))

    # ---- Step 4-5: build DataFiles + iceberg commit ----------------------
    table = deps.load_table()
    try:
        with pg_pool.connection() as conn:
            rows = ps.files_for_cycle(conn, cycle_id=cycle_id)

        # Schema fingerprint validation. Per the plan, we currently
        # treat any mismatch as a loud failure that releases the claims
        # — the writer must update its schema or the icebox catches up.
        from shared.fingerprint import schema_fingerprint

        table_fp = schema_fingerprint(table.schema())
        for row in rows:
            row_fp = row[8]  # schema_fingerprint column
            if row_fp != table_fp:
                log.error(
                    "run_cycle: schema fingerprint mismatch on file id=%s — "
                    "row=%s table=%s",
                    row[0], row_fp, table_fp,
                )
                result.skipped_reason = "schema_mismatch"
                result.error = (
                    f"Schema fingerprint mismatch: writer={row_fp} "
                    f"table={table_fp} on file_id={row[0]}"
                )
                with pg_pool.connection() as conn:
                    with conn.transaction():
                        ps.release_cycle_claim(conn, cycle_id=cycle_id)
                        ps.record_failure(conn)
                        ps.update_heartbeat(conn)
                return result

        data_files = []
        kafka_offset_dicts: list[dict[str, int]] = []
        for row in rows:
            (_id, file_path, _writer_ordinal, kafka_offsets, partition_values,
             record_count, file_size, _schema_version, _schema_fingerprint,
             parquet_stats, _cycle_id, _staged_at, _committed_at,
             _iceberg_snapshot_id) = row
            df = deps.build_data_file(
                table=table,
                file_path=file_path,
                record_count=record_count,
                file_size=file_size,
                partition_values=partition_values,
                parquet_stats=parquet_stats,
            )
            data_files.append(df)
            kafka_offset_dicts.append(kafka_offsets)

        snapshot_id = deps.commit_data_files(
            table=table,
            data_files=data_files,
            cycle_id=cycle_id,
        )
        result.iceberg_snapshot_id = snapshot_id
        log.info(
            "run_cycle: cycle %s iceberg-committed snapshot_id=%s",
            cycle_id, snapshot_id,
        )

    except Exception as exc:
        log.exception(
            "run_cycle: iceberg-commit step failed for cycle %s: %s",
            cycle_id, exc,
        )
        result.error = f"iceberg-commit failed: {exc}"
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.release_cycle_claim(conn, cycle_id=cycle_id)
                ps.record_failure(conn)
                ps.update_heartbeat(conn)
        return result

    # ---- Step 6: persist iceberg_snapshot_id BEFORE attempting Kafka -----
    with pg_pool.connection() as conn:
        with conn.transaction():
            ps.mark_iceberg_committed(
                conn, cycle_id=cycle_id, snapshot_id=snapshot_id
            )

    # ---- Step 7: kafka commit --------------------------------------------
    try:
        max_offsets = kf.merge_max_offsets(kafka_offset_dicts)
        deps.kafka_commit_offsets(
            deps.kafka_admin,
            group_id=cfg.kafka_group_id,
            topic=cfg.kafka_topic,
            max_offsets=max_offsets,
        )
        result.kafka_offsets_committed = max_offsets
        log.info(
            "run_cycle: cycle %s kafka-committed %d partitions",
            cycle_id, len(max_offsets),
        )
    except Exception as exc:
        # Iceberg already committed; we're now in the "stuck cycle"
        # state. Next iteration's recovery path will see this cycle and
        # complete it.
        log.exception(
            "run_cycle: kafka-commit failed for cycle %s (iceberg already "
            "committed snapshot_id=%s — recovery will finalize): %s",
            cycle_id, snapshot_id, exc,
        )
        result.error = f"kafka-commit failed (cycle stuck): {exc}"
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.record_failure(conn)
                ps.update_heartbeat(conn)
        return result

    # ---- Step 8-10: mark kafka committed + finalize ----------------------
    with pg_pool.connection() as conn:
        with conn.transaction():
            ps.mark_kafka_committed(conn, cycle_id=cycle_id)
            ps.complete_cycle(conn, cycle_id=cycle_id, snapshot_id=snapshot_id)
            ps.record_success(conn)
            ps.update_heartbeat(conn)

    result.success = True
    log.info("run_cycle: cycle %s complete", cycle_id)
    return result


def recover_in_flight_cycles(
    *,
    cfg: Config,
    pg_pool: ConnectionPool,
    deps: CommitterDeps,
) -> list[CycleResult]:
    """At startup, walk incomplete_cycles and rationalize each into
    either: completed (with retry as needed) OR released (claims back
    into the unclaimed pool for re-batching).

    Each cycle gets a freshly-loaded Table (via deps.load_table). This
    is load-bearing — between recovery iterations, the catalog could
    advance (another writer, an admin rewrite), and a stale handle's
    `snapshots()` list wouldn't include subsequent commits.

    Returns one CycleResult per cycle examined.
    """
    results: list[CycleResult] = []
    with pg_pool.connection() as conn:
        cycles = ps.incomplete_cycles(conn)
    if len(cycles) >= ps.INCOMPLETE_CYCLES_LIMIT_DEFAULT:
        # The query has a safety LIMIT. Hitting it means we likely have
        # more stuck cycles than we're seeing — page ops, don't silently
        # truncate.
        log.error(
            "recover_in_flight_cycles: hit INCOMPLETE_CYCLES_LIMIT_DEFAULT=%d "
            "stuck cycles. The icebox has more stuck cycles than the recovery "
            "scan returns; manual investigation required.",
            ps.INCOMPLETE_CYCLES_LIMIT_DEFAULT,
        )
    else:
        log.info("recover_in_flight_cycles: %d in-flight cycles to inspect", len(cycles))
    for cycle in cycles:
        results.append(_recover_one(cycle, cfg=cfg, pg_pool=pg_pool, deps=deps))
    return results


def _recover_one(
    cycle: CommitCycleRow,
    *,
    cfg: Config,
    pg_pool: ConnectionPool,
    deps: CommitterDeps,
) -> CycleResult:
    """Decide what to do with one in-flight cycle. Three branches:

    A) iceberg_snapshot_id is NULL on PG side. Ask Lakekeeper if our
       cycle_id shows up in any snapshot.summary in the snapshot_log:
       - YES → backfill snapshot_id, fall through to kafka step.
       - NO  → release_cycle_claim, delete the cycle row (no Iceberg
               snapshot ever landed for it), files re-batch into a
               fresh cycle.

    B) iceberg_snapshot_id is set AND kafka_committed_at is NULL —
       Iceberg commit landed, Kafka commit didn't. Retry Kafka commit.

    C) iceberg_snapshot_id AND kafka_committed_at are both set —
       only finalize remains.

    Recovery does NOT advance `consecutive_failures` on failure paths
    (kafka-retry-failure, etc.). Old failures from a previous
    process's lifetime shouldn't preemptively force degraded mode
    before steady-state has a chance to run a healthy cycle. Heartbeat
    is still pumped in every exit path so the API knows the committer
    is alive.
    """
    result = CycleResult(cycle_id=cycle.cycle_id)
    log.info("_recover_one: cycle=%s state=%r", cycle.cycle_id, cycle)

    table = deps.load_table()
    snapshot_id: int | None = cycle.iceberg_snapshot_id

    if snapshot_id is None:
        snapshot_id = deps.find_snapshot_for_cycle(table, cycle.cycle_id)
        if snapshot_id is None:
            log.info(
                "_recover_one: cycle=%s not in snapshot_log — releasing claims",
                cycle.cycle_id,
            )
            # Heartbeat update on branch A: a long recovery walking many
            # released-no-iceberg cycles otherwise wouldn't update the
            # heartbeat at all, leaving the API 503'ing legitimate POSTs.
            # See ICEBOX-PLAN.md "Committer thread liveness".
            #
            # Also: the cycle row is now a zombie (no Iceberg snapshot
            # ever landed for it). DELETE it so it doesn't accumulate
            # against incomplete_cycles' LIMIT and silently block
            # newer stuck cycles from being seen.
            with pg_pool.connection() as conn:
                with conn.transaction():
                    ps.release_cycle_claim(conn, cycle_id=cycle.cycle_id)
                    ps.delete_cycle_row(conn, cycle_id=cycle.cycle_id)
                    ps.update_heartbeat(conn)
            result.skipped_reason = "released_no_iceberg_commit"
            result.success = True  # successful recovery, not a successful cycle
            return result
        # Backfill: cycle had no recorded snapshot_id but Lakekeeper has
        # the cycle's snapshot — record it.
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.mark_iceberg_committed(
                    conn, cycle_id=cycle.cycle_id, snapshot_id=snapshot_id
                )
        log.info(
            "_recover_one: cycle=%s backfilled snapshot_id=%s",
            cycle.cycle_id, snapshot_id,
        )

    result.iceberg_snapshot_id = snapshot_id

    # Kafka commit (idempotent: same offsets, same group, same topic).
    if cycle.kafka_committed_at is None:
        with pg_pool.connection() as conn:
            rows = ps.files_for_cycle(conn, cycle_id=cycle.cycle_id)
        kafka_offset_dicts = [row[3] for row in rows]  # kafka_offsets column
        max_offsets = kf.merge_max_offsets(kafka_offset_dicts)
        try:
            deps.kafka_commit_offsets(
                deps.kafka_admin,
                group_id=cfg.kafka_group_id,
                topic=cfg.kafka_topic,
                max_offsets=max_offsets,
            )
        except Exception as exc:
            log.exception(
                "_recover_one: kafka commit failed for cycle %s — leaving "
                "stuck for next attempt: %s",
                cycle.cycle_id, exc,
            )
            result.error = f"kafka-commit failed during recovery: {exc}"
            # Heartbeat update on the kafka-retry-failure exit so a
            # series of stuck recovery cycles doesn't trip the API's
            # stale-heartbeat backpressure. Without this, recovery
            # walking many sequentially-failing cycles silently freezes
            # writers via 503.
            with pg_pool.connection() as conn:
                with conn.transaction():
                    ps.update_heartbeat(conn)
            return result
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.mark_kafka_committed(conn, cycle_id=cycle.cycle_id)

    # Finalize. Heartbeat is updated here so a long recovery (multiple
    # stuck cycles to drain) doesn't leave the API serving stale-503s.
    with pg_pool.connection() as conn:
        with conn.transaction():
            ps.complete_cycle(conn, cycle_id=cycle.cycle_id, snapshot_id=snapshot_id)
            ps.record_success(conn)
            ps.update_heartbeat(conn)
    result.success = True
    log.info("_recover_one: cycle=%s recovered to complete", cycle.cycle_id)
    return result


def committer_loop(
    *,
    cfg: Config,
    pg_pool: ConnectionPool,
    deps: CommitterDeps,
    stop_event: threading.Event,
) -> None:
    """Thread target. Runs forever (until stop_event is set), invoking
    run_cycle on cadence.

    Recovery runs ONCE at startup before the steady-state loop begins.
    """
    log.info("committer_loop: starting")

    # PG advisory lock — singleton guarantee for the committer. Acquire
    # on a DEDICATED connection that we hold for the lifetime of this
    # thread; pool acquisitions for cycle work go through pg_pool
    # separately. Session-scoped advisory locks die with the connection,
    # so a dead committer's lock evaporates with TCP timeout.
    #
    # Lock id is derived from cfg.pg_schema so multiple iceboxes sharing
    # a PG instance (one per (topic, table)) each hold their own lock —
    # events doesn't block person, etc. Same schema → same lock id, so
    # two replicas of the SAME icebox still serialize correctly.
    #
    # We loop with a small sleep so a Helm rollout (old pod terminating
    # holds the lock until its connection closes; new pod waits) is a
    # graceful handoff, not a startup crash.
    lock_id = ps.committer_advisory_lock_id(cfg.pg_schema)
    lock_conn: psycopg.Connection | None = None
    while not stop_event.is_set():
        # Transient PG issues during pool checkout (TCP reset, server
        # restart) should NOT kill the committer thread — log + retry.
        try:
            candidate_conn = pg_pool.getconn()
        except Exception as exc:
            log.warning(
                "committer_loop: pg_pool.getconn() failed during lock "
                "acquisition (%s); retrying in %.1fs",
                exc,
                ADVISORY_LOCK_RETRY_SECONDS,
            )
            stop_event.wait(ADVISORY_LOCK_RETRY_SECONDS)
            continue

        # try_acquire_committer_lock can raise on PG transport errors.
        # try/finally ensures the conn is returned to the pool either
        # way; otherwise a raise here would permanently leak a pool slot
        # (which at psycopg_pool_max=2 is half the budget).
        try:
            acquired = ps.try_acquire_committer_lock(candidate_conn, lock_id=lock_id)
        except Exception as exc:
            try:
                pg_pool.putconn(candidate_conn)
            except Exception:
                log.exception(
                    "committer_loop: failed to return conn after lock-acquire error"
                )
            log.warning(
                "committer_loop: try_acquire_committer_lock raised (%s); "
                "retrying in %.1fs",
                exc,
                ADVISORY_LOCK_RETRY_SECONDS,
            )
            stop_event.wait(ADVISORY_LOCK_RETRY_SECONDS)
            continue

        if acquired:
            lock_conn = candidate_conn
            log.info("committer_loop: acquired singleton advisory lock")
            break
        pg_pool.putconn(candidate_conn)
        log.info(
            "committer_loop: advisory lock held by another committer; "
            "retrying in %.1fs",
            ADVISORY_LOCK_RETRY_SECONDS,
        )
        stop_event.wait(ADVISORY_LOCK_RETRY_SECONDS)

    if lock_conn is None:
        # Lock was never acquired — exited the while loop because
        # stop_event fired during the wait-for-lock retry sleep.
        log.info("committer_loop: stop requested before lock acquired")
        return

    # Stamp the heartbeat BEFORE recovery so the API doesn't accept POSTs
    # against an icebox where the committer thread might be dead. Without
    # this, last_committer_heartbeat=NULL is treated as "not stale" and
    # writes flow in even if the committer never starts. With this stamp,
    # a dead committer becomes stale within `cadence × stale_multiple`
    # seconds and the API switches to 503.
    try:
        with pg_pool.connection() as conn:
            with conn.transaction():
                ps.update_heartbeat(conn)
        log.info("committer_loop: initial heartbeat stamped")
    except Exception as exc:
        log.exception("committer_loop: failed to stamp initial heartbeat: %s", exc)
        # Don't crash — API will see None heartbeat and treat it as
        # not-stale, which is the pre-existing boot behavior.

    try:
        recover_in_flight_cycles(cfg=cfg, pg_pool=pg_pool, deps=deps)
    except Exception as exc:
        log.exception("committer_loop: recovery failed at startup: %s", exc)
        # Don't crash — let the API come up so /readyz can surface the
        # degraded state. The next steady-state iteration will retry.

    while not stop_event.is_set():
        start = time.monotonic()
        try:
            run_cycle(cfg=cfg, pg_pool=pg_pool, deps=deps)
        except Exception as exc:
            # run_cycle catches its own errors and returns CycleResult.
            # An exception leaking here is a bug or an unexpected PG
            # outage; log and keep going.
            log.exception("committer_loop: unhandled error in cycle: %s", exc)
            try:
                with pg_pool.connection() as conn:
                    with conn.transaction():
                        ps.record_failure(conn)
                        ps.update_heartbeat(conn)
            except psycopg.Error:
                log.exception(
                    "committer_loop: failed to record_failure after cycle error"
                )

        elapsed = time.monotonic() - start
        # Floor the sleep at MIN_INTER_CYCLE_SLEEP so a tight failure loop
        # (e.g., Lakekeeper sustained 5xx) doesn't hammer the catalog. A
        # 60s cadence cycle that takes 75s would otherwise loop with 0s
        # sleep; the floor gives downstreams breathing room.
        sleep_for = max(
            MIN_INTER_CYCLE_SLEEP_SECONDS,
            cfg.committer_cadence_seconds - elapsed,
        )
        stop_event.wait(sleep_for)

    # Release the advisory lock explicitly on graceful shutdown so a fast
    # pod restart doesn't have to wait for the kernel-level TCP timeout
    # on the dying connection. Best-effort: any failure here is logged
    # but doesn't keep the loop from returning.
    if lock_conn is not None:
        try:
            ps.release_committer_lock(lock_conn, lock_id=lock_id)
            log.info("committer_loop: released advisory lock")
        except Exception:
            log.exception("committer_loop: error releasing advisory lock")
        finally:
            try:
                pg_pool.putconn(lock_conn)
            except Exception:
                log.exception("committer_loop: error returning lock conn to pool")
    log.info("committer_loop: stop_event set, exiting")
