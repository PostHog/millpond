"""v6 polling-daemon: replaces the cycle-based committer.

The daemon's one job is to try to commit pending Parquet files into
Iceberg and advance Kafka offsets accordingly. It stays up no matter
what — no Kafka traffic, all Iceberg commits failing, PG unreachable,
any mix — and surfaces progress (or its absence) via metrics, not via
its process state. See docs/icebox-self-healing-recovery.md (v6).

Per tick:

  1. SELECT FOR UPDATE SKIP LOCKED a batch of pending rows older than
     the age filter.
  2. Build DataFiles from row data (no S3 reads).
  3. commit_data_files via Iceberg, wrapped in with_timeout because
     PyIceberg has no native timeout.
  4. Classify any exception:
       - requests transport / TimeoutError → transient. Rows weren't
         UPDATEd so they stay pending. Heartbeat fires; tx commits and
         releases the row locks. Next tick re-SELECTs them.
       - CommitFailedException → transient (OCC conflict). Same handling.
       - anything else → batch failure. Mark rows result='failed',
         ADVANCE Kafka offsets past them so the writer makes progress,
         heartbeat fires, return.
  5. Success: UPDATE rows result='committed' + iceberg_snapshot_id,
     commit Kafka offsets, heartbeat.

Heartbeat fires on every tick-exit path. The daemon's liveness probe
checks staleness; staleness signals a stuck thread, not a Lakekeeper
outage. Lakekeeper outages are observed via icebox_lakekeeper_failures_total
+ icebox_last_success_at, not via /healthz.

Design notes:

  - The entire tick is one PG transaction. The Iceberg RPC runs
    inside the tx; row locks hold for the RPC duration (bounded by
    cfg.iceberg_timeout_s, see with_timeout).
  - No advisory lock. SKIP LOCKED gives row-level safety, the chart's
    replicas:1 enforces singleton-per-(ns, table). Two daemons
    accidentally running take disjoint slices (correctness preserved;
    Kafka offset commits may briefly reorder at the coordinator —
    writer replays a small range, ON CONFLICT no-ops on PG).
  - No pre-commit dedup. PyIceberg's _FastAppendFiles silently accepts
    duplicate file_paths (verified); downstream UUID-dedup absorbs the
    rare crash-after-Iceberg-before-PG-UPDATE.
"""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
import requests.exceptions
from psycopg_pool import ConnectionPool
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException

from icebox import iceberg as ib
from icebox import kafka as kf
from icebox import metrics
from icebox import postgres_sync as ps
from icebox.config import Config
from icebox.schema import IceboxPendingFileRow
from icebox.timeout import with_timeout

log = logging.getLogger(__name__)


# Tick outcome labels — must match metrics._TICK_OUTCOMES.
OUTCOME_SUCCESS = "success"
OUTCOME_VACUOUS = "vacuous"
OUTCOME_TRANSPORT_FAILURE = "transport_failure"
OUTCOME_BATCH_FAILURE = "batch_failure"


# Floor on the sleep between ticks even if the last tick ran long.
# Prevents a sustained Lakekeeper-down loop from churning PG conns.
# At cadence=60s a stuck tick still sleeps 5s before retrying — slow
# enough not to thrash, fast enough that recovery is noticed.
MIN_INTER_TICK_SLEEP_SECONDS = 5.0


@dataclass
class TickResult:
    """Outcome of one tick — surface for tests and for the loop's
    metrics step. The loop bumps icebox_ticks_total / observes
    icebox_tick_duration_seconds against `outcome`."""

    outcome: str
    file_count: int = 0
    snapshot_id: int | None = None
    error: str | None = None


@dataclass
class DaemonDeps:
    """Side-effect callables for the daemon. Tests pass mocks; the
    main.py wiring binds the real implementations.

    `load_table` is called once per tick (the Iceberg table is read
    via the catalog with no on-S3 footer reads). `build_data_file`,
    `commit_data_files`, `kafka_commit_offsets` are the real PyIceberg
    + confluent_kafka calls, swappable in tests.
    """

    load_table: Callable[[], Any]
    build_data_file: Callable[..., Any] = ib.build_data_file
    commit_data_files: Callable[..., ib.CommitResult] = ib.commit_data_files
    kafka_admin: Any = None
    kafka_commit_offsets: Callable[..., None] = kf.commit_offsets


# Exception types that mean "Lakekeeper isn't telling us the commit
# definitively failed; try again." We treat CommitStateUnknownException
# as transient too — if the commit did land, the next tick's idempotent
# replay (via the writer regenerating the same file_path) is harmless;
# if it didn't, retry is what we want.
_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    TimeoutError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.HTTPError,
    requests.exceptions.RequestException,  # parent — belt-and-suspenders
    CommitFailedException,
    CommitStateUnknownException,
)


def daemon_tick(
    conn: psycopg.Connection,
    *,
    cfg: Config,
    table: Any,
    deps: DaemonDeps,
) -> TickResult:
    """One iteration of the polling loop. Returns a TickResult so the
    loop (and tests) can read the outcome without re-doing the work.

    The entire body runs inside the caller's `with conn.transaction():`
    — row locks acquired by the SELECT survive until commit/rollback.
    """
    tick_start = time.monotonic()

    rows = ps.claim_pending_batch(
        conn,
        batch_size=cfg.committer_max_pending_files,
        age_seconds=cfg.age_filter_seconds,
    )
    if not rows:
        ps.update_heartbeat(conn)
        elapsed = time.monotonic() - tick_start
        metrics.ICEBOX_TICK_DURATION_SECONDS.labels(outcome=OUTCOME_VACUOUS).observe(elapsed)
        metrics.ICEBOX_TICKS_TOTAL.labels(outcome=OUTCOME_VACUOUS).inc()
        return TickResult(outcome=OUTCOME_VACUOUS)

    metrics.ICEBOX_BATCH_SIZE.observe(len(rows))

    # Build DataFiles. A KeyError here (e.g., partition_values missing a
    # spec column because the writer's partition spec drifted) is a
    # batch-failure case — handled in the broad except below.
    try:
        data_files = [
            deps.build_data_file(
                table=table,
                file_path=r.file_path,
                record_count=r.record_count,
                file_size=r.file_size,
                partition_values=r.partition_values,
                parquet_stats=r.parquet_stats,
            )
            for r in rows
        ]
    except Exception as exc:
        # Treat data-file construction failures as batch failures: the
        # row's metadata is wrong for the table's current state.
        # Mark + advance + heartbeat.
        log.error("daemon_tick: build_data_file failed: %s", exc, exc_info=True)
        return _handle_batch_failure(conn, cfg, rows, exc, tick_start, deps)

    # Iceberg commit. with_timeout defends against a wedged Lakekeeper
    # holding our PG row locks indefinitely.
    iceberg_start = time.monotonic()
    try:
        commit_result = with_timeout(
            cfg.iceberg_timeout_s,
            lambda: deps.commit_data_files(table=table, data_files=data_files),
        )
    except TimeoutError as exc:
        metrics.ICEBOX_ICEBERG_TIMEOUT_TOTAL.inc()
        metrics.ICEBOX_LAKEKEEPER_FAILURES_TOTAL.inc()
        metrics.ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS.observe(
            time.monotonic() - iceberg_start
        )
        log.warning("daemon_tick: iceberg commit timed out; will retry",
                    exc_info=True)
        ps.update_heartbeat(conn)
        elapsed = time.monotonic() - tick_start
        metrics.ICEBOX_TICK_DURATION_SECONDS.labels(
            outcome=OUTCOME_TRANSPORT_FAILURE
        ).observe(elapsed)
        metrics.ICEBOX_TICKS_TOTAL.labels(outcome=OUTCOME_TRANSPORT_FAILURE).inc()
        return TickResult(outcome=OUTCOME_TRANSPORT_FAILURE, error=str(exc))
    except _TRANSIENT_EXCEPTIONS as exc:
        metrics.ICEBOX_LAKEKEEPER_FAILURES_TOTAL.inc()
        metrics.ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS.observe(
            time.monotonic() - iceberg_start
        )
        log.warning("daemon_tick: iceberg commit transient failure; will retry",
                    exc_info=True)
        ps.update_heartbeat(conn)
        elapsed = time.monotonic() - tick_start
        metrics.ICEBOX_TICK_DURATION_SECONDS.labels(
            outcome=OUTCOME_TRANSPORT_FAILURE
        ).observe(elapsed)
        metrics.ICEBOX_TICKS_TOTAL.labels(outcome=OUTCOME_TRANSPORT_FAILURE).inc()
        return TickResult(outcome=OUTCOME_TRANSPORT_FAILURE, error=str(exc))
    except Exception as exc:
        metrics.ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS.observe(
            time.monotonic() - iceberg_start
        )
        return _handle_batch_failure(conn, cfg, rows, exc, tick_start, deps)

    metrics.ICEBOX_ICEBERG_COMMIT_DURATION_SECONDS.observe(
        time.monotonic() - iceberg_start
    )
    snapshot_id = commit_result.snapshot_id

    # Success path — UPDATE then Kafka commit. The Kafka commit lives
    # OUTSIDE PG (it's an AdminClient RPC) so a failure there can't
    # roll back the UPDATE; cumulative offset semantics cover any
    # missed commit on the next tick.
    ps.mark_committed(conn, ids=[r.id for r in rows], snapshot_id=snapshot_id)
    metrics.ICEBOX_FILES_COMMITTED_TOTAL_V6.inc(len(rows))
    metrics.ICEBOX_RECORDS_COMMITTED_TOTAL.inc(sum(r.record_count for r in rows))

    _try_commit_kafka_offsets(deps, cfg, rows, context="success")

    metrics.ICEBOX_LAST_SUCCESS_AT.set_to_current_time()
    ps.update_heartbeat(conn)

    elapsed = time.monotonic() - tick_start
    metrics.ICEBOX_TICK_DURATION_SECONDS.labels(outcome=OUTCOME_SUCCESS).observe(elapsed)
    metrics.ICEBOX_TICKS_TOTAL.labels(outcome=OUTCOME_SUCCESS).inc()
    return TickResult(
        outcome=OUTCOME_SUCCESS,
        file_count=len(rows),
        snapshot_id=snapshot_id,
    )


def _handle_batch_failure(
    conn: psycopg.Connection,
    cfg: Config,
    rows: Sequence[IceboxPendingFileRow],
    exc: BaseException,
    tick_start: float,
    deps: DaemonDeps,
) -> TickResult:
    """Mark rows failed, advance Kafka offsets past them, heartbeat.

    This is the "make progress" path. We can't fix bad data; better to
    stamp the audit trail and unblock the writer than to loop on the
    same broken batch forever.
    """
    metrics.ICEBOX_BATCH_FAILURES_TOTAL.inc()
    ps.mark_failed(conn, ids=[r.id for r in rows])
    metrics.ICEBOX_FILES_FAILED_TOTAL.inc(len(rows))

    _try_commit_kafka_offsets(deps, cfg, rows, context="batch_failure")

    ps.update_heartbeat(conn)
    log.error(
        "daemon_tick: batch failure; %d row(s) marked failed, offsets advanced: %s",
        len(rows), exc, exc_info=True,
    )
    elapsed = time.monotonic() - tick_start
    metrics.ICEBOX_TICK_DURATION_SECONDS.labels(outcome=OUTCOME_BATCH_FAILURE).observe(elapsed)
    metrics.ICEBOX_TICKS_TOTAL.labels(outcome=OUTCOME_BATCH_FAILURE).inc()
    return TickResult(
        outcome=OUTCOME_BATCH_FAILURE,
        file_count=len(rows),
        error=str(exc),
    )


def _try_commit_kafka_offsets(
    deps: DaemonDeps,
    cfg: Config,
    rows: Sequence[IceboxPendingFileRow],
    *,
    context: str,
) -> None:
    """Commit Kafka offsets for the rows' consumer group; log + swallow
    failures. Cumulative offset semantics mean the next successful tick
    covers any gap, so a transient AdminClient failure here is fine.

    `context` is just a log tag ("success" vs "batch_failure") for
    grep-clarity in case the same partition's offsets get committed
    twice in close succession.
    """
    if not rows:
        return
    max_offsets = kf.merge_max_offsets([r.kafka_offsets for r in rows])
    if not max_offsets:
        return
    kafka_start = time.monotonic()
    try:
        deps.kafka_commit_offsets(
            deps.kafka_admin,
            group_id=cfg.kafka_group_id,
            topic=cfg.kafka_topic,
            max_offsets=max_offsets,
        )
    except Exception:
        log.warning(
            "daemon_tick(%s): kafka commit_offsets failed; next tick "
            "covers the gap",
            context,
            exc_info=True,
        )
    finally:
        metrics.ICEBOX_KAFKA_COMMIT_DURATION_SECONDS.observe(
            time.monotonic() - kafka_start
        )


def refresh_state_gauges(conn: psycopg.Connection) -> None:
    """Populate the live `icebox_files_count{result}`, oldest-pending,
    and `icebox_files_bytes{result}` gauges from PG. Called by the
    loop after each tick so /metrics scrapes see the latest values
    without needing the /metrics handler itself to hit PG.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT result, COUNT(*), COALESCE(SUM(file_size), 0) "
            "FROM icebox_files GROUP BY result"
        )
        rows = cur.fetchall()
    # Reset every label to 0 first so a result that drops to zero
    # (e.g., the failed audit gets cleared by an operator) actually
    # reads as 0 instead of holding its last seen non-zero value.
    for result in ("pending", "committed", "failed"):
        metrics.ICEBOX_FILES_COUNT.labels(result=result).set(0)
        metrics.ICEBOX_FILES_BYTES.labels(result=result).set(0)
    for result, count, total_bytes in rows:
        metrics.ICEBOX_FILES_COUNT.labels(result=result).set(count)
        metrics.ICEBOX_FILES_BYTES.labels(result=result).set(total_bytes)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM (now() - MIN(inserted_at))) "
            "FROM icebox_files WHERE result='pending'"
        )
        row = cur.fetchone()
    oldest = row[0] if row and row[0] is not None else -1
    metrics.ICEBOX_FILES_OLDEST_PENDING_SECONDS.set(float(oldest))


def daemon_loop(
    *,
    cfg: Config,
    pg_pool: ConnectionPool,
    deps: DaemonDeps,
    stop_event: threading.Event,
) -> None:
    """Thread target. Runs until `stop_event` is set, invoking
    `daemon_tick` once per cadence interval.

    Layout per iteration:

      1. Open a PG transaction.
      2. Load the Iceberg table (fresh, no caching across ticks — the
         catalog can advance underneath us).
      3. Run the tick.
      4. Refresh state gauges from PG (so /metrics is current).
      5. Commit the transaction.
      6. Sleep to fill out the cadence.

    A PG-pool checkout failure (transient Aurora hiccup, conn timeout)
    is logged + counted + the loop sleeps and retries. Lakekeeper /
    PyIceberg failures are handled INSIDE the tick.
    """
    metrics.initialize_outcome_counters()
    log.info("daemon_loop: starting (cadence=%.1fs, batch_size=%d)",
             cfg.committer_cadence_seconds, cfg.committer_max_pending_files)

    while not stop_event.is_set():
        start = time.monotonic()
        try:
            with pg_pool.connection() as conn:
                with conn.transaction():
                    table = deps.load_table()
                    daemon_tick(conn, cfg=cfg, table=table, deps=deps)
                # State gauges run in their own (read-only) tx so they
                # don't block the row-lock release from the tick's tx.
                refresh_state_gauges(conn)
        except psycopg.Error as exc:
            metrics.ICEBOX_PG_UNREACHABLE_TOTAL.inc()
            log.warning("daemon_loop: PG error in tick (%s); retrying",
                        exc, exc_info=True)
        except Exception as exc:
            # Anything else escaping daemon_tick is a bug. Log loudly,
            # keep going — the daemon's one job is to stay up.
            log.exception("daemon_loop: unhandled error in tick: %s", exc)

        elapsed = time.monotonic() - start
        sleep_for = max(
            MIN_INTER_TICK_SLEEP_SECONDS,
            cfg.committer_cadence_seconds - elapsed,
        )
        stop_event.wait(sleep_for)

    log.info("daemon_loop: stop_event set, exiting")
