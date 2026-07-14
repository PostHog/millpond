"""Unit tests for tools/ducklake_maintenance.py.

Coverage tier A: pure helpers, log message shape, argparse plumbing.
Coverage tier B: orchestrator retry/dispatch logic with mocked sub-calls.

Tier C (macros against a stubbed catalog) and tier D (full e2e against a
real lake) are intentionally out of scope here — they need either an
in-process duckdb with stubbed schemas or the docker-compose stack.
"""

import logging
import re
from unittest.mock import MagicMock, patch

import duckdb
import ducklake_maintenance
import pytest

# ---------------------------------------------------------------------------
# Tier A — pure helpers and log shape
# ---------------------------------------------------------------------------


class TestSqlStringLiteral:
    def test_plain(self):
        assert ducklake_maintenance._sql_string_literal("plain") == "'plain'"

    def test_embedded_quote_doubled(self):
        assert ducklake_maintenance._sql_string_literal("with'quote") == "'with''quote'"

    def test_already_doubled_quotes_are_escaped_again(self):
        # The helper has no idea whether the input was pre-escaped; doubling
        # is a one-way transform consistent with SQL string-literal rules.
        assert ducklake_maintenance._sql_string_literal("two''already") == "'two''''already'"

    def test_empty(self):
        assert ducklake_maintenance._sql_string_literal("") == "''"


class TestBytesToHuman:
    """Round-trip DuckLake's stored byte-count format back to a units-suffixed form."""

    def test_clean_mib(self):
        assert ducklake_maintenance._bytes_to_human("67108864") == "64MiB"
        assert ducklake_maintenance._bytes_to_human("134217728") == "128MiB"
        assert ducklake_maintenance._bytes_to_human("5242880") == "5MiB"

    def test_clean_gib(self):
        assert ducklake_maintenance._bytes_to_human("1073741824") == "1GiB"
        assert ducklake_maintenance._bytes_to_human("2147483648") == "2GiB"

    def test_clean_kib(self):
        assert ducklake_maintenance._bytes_to_human("1024") == "1KiB"
        assert ducklake_maintenance._bytes_to_human("4096") == "4KiB"

    def test_picks_largest_clean_unit(self):
        # 64 MiB = 65536 KiB; the converter picks MiB, not KiB.
        assert ducklake_maintenance._bytes_to_human("67108864") == "64MiB"

    def test_non_power_of_1024_returns_none(self):
        assert ducklake_maintenance._bytes_to_human("12345678") is None

    def test_zero_or_negative_returns_none(self):
        assert ducklake_maintenance._bytes_to_human("0") is None
        assert ducklake_maintenance._bytes_to_human("-1024") is None

    def test_non_integer_returns_none(self):
        assert ducklake_maintenance._bytes_to_human("128MiB") is None
        assert ducklake_maintenance._bytes_to_human("") is None
        assert ducklake_maintenance._bytes_to_human(None) is None


class TestLogCleanupThroughput:
    """The throughput line is keyed off ``files_processed`` (rows the
    operation actually returned) rather than a queue-depth delta — concurrent
    writers can change the queue mid-run, making any delta misleading or
    even negative."""

    def test_typical(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance._log_cleanup_throughput(
                "cleanup-all", files_processed=50, elapsed_s=10.0, queue_depth_after=950
            )
        msg = caplog.records[0].getMessage()
        assert "cleanup-all throughput" in msg
        assert "files_processed=50" in msg
        assert "elapsed_s=10.0" in msg
        assert "rate_obj_s=5.0" in msg
        assert "queue_depth_after=950" in msg
        # No before-snapshot in the new shape — explicit assertion that the
        # racy field is gone, in case anyone re-introduces it.
        assert "queue_depth_before" not in msg

    def test_zero_elapsed_does_not_divide_by_zero(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance._log_cleanup_throughput("cleanup", 0, 0.0, 0)
        msg = caplog.records[0].getMessage()
        assert "rate_obj_s=0.0" in msg

    def test_full_drain(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance._log_cleanup_throughput("cleanup-all", 47023, 9405.0, 0)
        msg = caplog.records[0].getMessage()
        assert "files_processed=47023" in msg
        assert "rate_obj_s=5.0" in msg
        assert "queue_depth_after=0" in msg


class TestArgparse:
    """The new subcommands must reach the dispatch with the expected fields."""

    def setup_method(self):
        self.parser = ducklake_maintenance.build_parser()

    def test_dedup_deletions_dry_run(self):
        args = self.parser.parse_args(["dedup-deletions", "--dry-run"])
        assert args.command == "dedup-deletions"
        assert args.dry_run is True

    def test_dedup_deletions_real(self):
        args = self.parser.parse_args(["dedup-deletions"])
        assert args.dry_run is False

    def test_purge_orphan_stats_dry_run(self):
        args = self.parser.parse_args(["purge-orphan-stats", "--dry-run"])
        assert args.command == "purge-orphan-stats"
        assert args.dry_run is True

    def test_purge_orphan_stats_real(self):
        args = self.parser.parse_args(["purge-orphan-stats"])
        assert args.dry_run is False

    def test_cleanup_all_rejects_dry_run(self):
        """--dry-run used to be accepted and silently no-op — operators
        believed they had previewed something. Must now fail loudly."""
        with pytest.raises(SystemExit):
            self.parser.parse_args(["cleanup-all", "--dry-run"])

    def test_find_orphans(self):
        args = self.parser.parse_args(["find-orphans"])
        assert args.command == "find-orphans"

    def test_heal_orphans(self):
        args = self.parser.parse_args(["heal-orphans"])
        assert args.command == "heal-orphans"
        assert args.dry_run is False

    def test_heal_orphans_dry_run(self):
        args = self.parser.parse_args(["heal-orphans", "--dry-run"])
        assert args.dry_run is True

    def test_cleanup_all_safe_default_iterations(self):
        args = self.parser.parse_args(["cleanup-all-safe"])
        assert args.command == "cleanup-all-safe"
        assert args.max_iterations == 10

    def test_cleanup_all_safe_override(self):
        args = self.parser.parse_args(["cleanup-all-safe", "--max-iterations", "3"])
        assert args.max_iterations == 3

    def test_cleanup_all_safe_zero_rejected(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["cleanup-all-safe", "--max-iterations", "0"])

    def test_fsck_dry_run(self):
        args = self.parser.parse_args(["fsck", "--dry-run"])
        assert args.command == "fsck"
        assert args.dry_run is True
        assert args.max_iterations == 10

    def test_fsck_real(self):
        args = self.parser.parse_args(["fsck", "--max-iterations", "5"])
        assert args.dry_run is False
        assert args.max_iterations == 5

    def test_compact_threads_memory_defaults(self, monkeypatch):
        # argparse defaults read COMPACTION_* env at parser-build time, so a
        # lingering shell override (the justfile exports COMPACTION_MAX_FILES)
        # would make this test flaky — isolate and rebuild the parser.
        for var in ("COMPACTION_THREADS", "COMPACTION_MEMORY_LIMIT", "COMPACTION_MAX_FILES"):
            monkeypatch.delenv(var, raising=False)
        parser = ducklake_maintenance.build_parser()
        args = parser.parse_args(["compact", "--tier", "1"])
        assert args.threads == 2
        assert args.memory_limit == "4GB"
        assert args.max_compacted_files == 80  # fallback aligned with justfile export

    def test_compact_threads_memory_override(self):
        args = self.parser.parse_args(
            ["compact", "--tier", "2", "--threads", "8", "--memory-limit", "16GB", "--max-compacted-files", "5000"]
        )
        assert args.threads == 8
        assert args.memory_limit == "16GB"
        assert args.max_compacted_files == 5000

    def test_compact_tuning_env_overrides(self, monkeypatch):
        """The K8s CronJob passes no CLI args; chart values tune via env."""
        monkeypatch.setenv("COMPACTION_THREADS", "4")
        monkeypatch.setenv("COMPACTION_MEMORY_LIMIT", "16GB")
        monkeypatch.setenv("COMPACTION_MAX_FILES", "50000")
        parser = ducklake_maintenance.build_parser()
        args = parser.parse_args(["compact", "--tier", "1"])
        assert args.threads == 4
        assert args.memory_limit == "16GB"
        assert args.max_compacted_files == 50000

    def test_compact_cli_beats_env(self, monkeypatch):
        monkeypatch.setenv("COMPACTION_MEMORY_LIMIT", "16GB")
        parser = ducklake_maintenance.build_parser()
        args = parser.parse_args(["compact", "--tier", "1", "--memory-limit", "8GB"])
        assert args.memory_limit == "8GB"

    def test_expire_snapshots_defaults(self):
        args = self.parser.parse_args(["expire-snapshots"])
        assert args.days == 7
        assert args.batch_size == 1000
        assert args.num_batches is None
        assert args.dry_run is False

    def test_expire_snapshots_num_batches(self):
        args = self.parser.parse_args(["expire-snapshots", "--num-batches", "10"])
        assert args.num_batches == 10

    def test_expire_snapshots_num_batches_zero_rejected(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["expire-snapshots", "--num-batches", "0"])


# ---------------------------------------------------------------------------
# Tier B — orchestrators with mocked sub-calls
# ---------------------------------------------------------------------------


class TestExpireSnapshots:
    """Verify expire_snapshots batch loop and num_batches limit."""

    def _make_conn(self, batch_rows):
        """Return a mock conn whose execute().fetchall() cycles through batch_rows.

        Each element of batch_rows is either a list of (snapshot_id,) tuples
        (one batch) or an empty list (signals end-of-data). After the sequence
        is exhausted, subsequent calls return [].

        The mock also accepts _pg_query_one / _pg_execute call patterns:
        postgres_query returns a single-row result for the cutoff SELECT and
        per-batch COUNT queries; postgres_execute is a no-op.
        """
        conn = MagicMock()
        iter_batches = iter(batch_rows + [[]])  # sentinel empty list at end

        def fake_execute(sql, *args, **kwargs):
            result = MagicMock()
            if "postgres_query" in sql and "ducklake_snapshot" in sql and "SELECT snapshot_id" in sql:
                # batch SELECT — return next batch
                result.fetchall.return_value = next(iter_batches, [])
            elif "postgres_query" in sql and "NOW()" in sql:
                # cutoff timestamp query
                result.fetchone.return_value = ("2026-06-07 00:00:00+00",)
            elif "postgres_query" in sql and "COUNT(*)" in sql:
                # dead-file count — return 0 to skip DML
                result.fetchone.return_value = (0,)
            elif "postgres_query" in sql and "pg_try_advisory_lock" in sql:
                result.fetchone.return_value = (True,)
            else:
                result.fetchall.return_value = []
                result.fetchone.return_value = (0,)
            return result

        conn.execute.side_effect = fake_execute
        return conn

    def test_processes_all_batches_when_no_limit(self, caplog):
        batches = [[(1,), (2,)], [(3,), (4,)], [(5,)]]
        conn = self._make_conn(batches)
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=None, dry_run=False)
        assert any("no expired snapshots remaining" in r.message for r in caplog.records)
        batch_logs = [r for r in caplog.records if "batch" in r.message and "snapshot_ids" in r.message]
        assert len(batch_logs) == 3

    def test_stops_after_num_batches(self, caplog):
        batches = [[(1,), (2,)], [(3,), (4,)], [(5,)]]
        conn = self._make_conn(batches)
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=2, dry_run=False)
        batch_logs = [r for r in caplog.records if "snapshot_ids" in r.message]
        assert len(batch_logs) == 2
        assert any("reached --num-batches limit" in r.message for r in caplog.records)

    def test_num_batches_one_processes_exactly_one(self, caplog):
        batches = [[(1,)], [(2,)], [(3,)]]
        conn = self._make_conn(batches)
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=1, dry_run=False)
        batch_logs = [r for r in caplog.records if "snapshot_ids" in r.message]
        assert len(batch_logs) == 1

    def test_dry_run_returns_without_processing(self, caplog):
        conn = self._make_conn([])
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=None, dry_run=True)
        assert any("dry-run" in r.message for r in caplog.records)
        # No batch processing log lines
        assert not any("snapshot_ids" in r.message for r in caplog.records)

    def test_empty_catalog_logs_done(self, caplog):
        conn = self._make_conn([])
        with caplog.at_level(logging.INFO, logger="maintenance"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=None, dry_run=False)
        assert any("no expired snapshots remaining" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Tier B — orchestrators with mocked sub-calls
# ---------------------------------------------------------------------------


class TestExpireSnapshotsSafety:
    """Pin the three safety properties of expire_snapshots: head guard,
    structural bridging predicate, and per-batch atomicity."""

    def _run(self, batch_rows, dry_run=False):
        conn = TestExpireSnapshots()._make_conn(batch_rows)
        ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=None, dry_run=dry_run)
        return [str(c.args[0]) for c in conn.execute.call_args_list]

    def test_head_guard_in_batch_select(self):
        """The newest snapshot must NEVER be expired — DuckLake resolves all
        state from MAX(snapshot_id); expiring it bricks the catalog (fork
        built-in has the same guard)."""
        calls = self._run([[(1,), (2,)]])
        batch_selects = [c for c in calls if "ORDER BY snapshot_id LIMIT" in c]
        assert batch_selects, "no batch SELECT issued"
        for c in batch_selects:
            assert "MAX(snapshot_id)" in c and "snapshot_id !=" in c

    def test_head_guard_in_dry_run_count(self):
        calls = self._run([], dry_run=True)
        counts = [c for c in calls if "COUNT(*)" in c and "ducklake_snapshot" in c]
        assert counts and all("MAX(snapshot_id)" in c for c in counts)

    def test_dead_predicate_is_structural_not_time_based(self):
        """Bridging must be 'no surviving snapshot in [begin, end)' — a
        snapshot_time comparison can classify a file dead while a later-batch
        or head-retained snapshot still references it."""
        calls = self._run([[(1,), (2,)]])
        dml = [c for c in calls if "postgres_execute" in c and "ducklake_data_file" in c]
        assert dml, "no batch DML issued"
        for c in dml:
            assert "live.snapshot_id NOT IN" in c
            assert "live.snapshot_time" not in c

    def test_batch_is_single_transaction(self):
        """Queue INSERTs, file cascade, and snapshot DELETEs must ship in ONE
        postgres_execute call (one txn): a crash between separate calls
        leaves surviving snapshot rows whose data-file rows are gone."""
        calls = self._run([[(1,), (2,)]])
        dml = [c for c in calls if "postgres_execute" in c]
        # exactly one DML call for the single batch
        assert len(dml) == 1, f"expected 1 batch txn, got {len(dml)}"
        sql = dml[0]
        assert "SET LOCAL statement_timeout" in sql and "SET LOCAL lock_timeout" in sql
        # schedule-before-drop and snapshots-last invariants
        q_ins = sql.index("ducklake_files_scheduled_for_deletion")
        d_file = sql.index("DELETE FROM public.ducklake_data_file ")
        d_snap = sql.rindex("DELETE FROM public.ducklake_snapshot WHERE")
        assert q_ins < d_file < d_snap

    def test_empty_catalog_issues_no_dml(self):
        calls = self._run([])
        assert not any("postgres_execute" in c for c in calls)

    def test_dml_failure_propagates_and_stops(self):
        """A failed batch txn (lock_timeout, network, anything) must raise and
        stop the loop — never swallow and re-select the same ids (spin) or
        continue to later batches on top of an unapplied one."""
        conn = TestExpireSnapshots()._make_conn([[(1,), (2,)], [(3,), (4,)]])
        inner = conn.execute.side_effect

        def failing_execute(sql, *args, **kwargs):
            if "postgres_execute" in sql:
                raise RuntimeError("lock_timeout")
            return inner(sql, *args, **kwargs)

        conn.execute.side_effect = failing_execute
        with pytest.raises(RuntimeError, match="lock_timeout"):
            ducklake_maintenance.expire_snapshots(conn, days=7, batch_size=1000, num_batches=None, dry_run=False)
        selects = [c for c in conn.execute.call_args_list if "ORDER BY snapshot_id LIMIT" in str(c.args[0])]
        assert len(selects) == 1, "loop must stop at the failed batch, not continue"

    def test_batch_size_cap_rejected_at_parse(self):
        parser = ducklake_maintenance.build_parser()
        args = parser.parse_args(["expire-snapshots", "--batch-size", "20000"])
        assert args.batch_size == 20000  # argparse allows; main() rejects
        with pytest.raises(SystemExit):
            ducklake_maintenance.main(["expire-snapshots", "--batch-size", "20000", "--dry-run"])

    def test_expire_builtin_takes_advisory_lock(self):
        conn = MagicMock()
        ducklake_maintenance.expire(conn, days=7, dry_run=False)
        assert any("pg_try_advisory_lock" in str(c.args[0]) for c in conn.execute.call_args_list), (
            "expire() must mutex against expire-snapshots/cleanup"
        )

    def test_expire_builtin_dry_run_skips_lock(self):
        conn = MagicMock()
        ducklake_maintenance.expire(conn, days=7, dry_run=True)
        assert not any("pg_try_advisory_lock" in str(c.args[0]) for c in conn.execute.call_args_list)


class TestCleanupFamilyAdvisoryLock:
    """Every mutating cleanup-family entry point must take the maintenance
    advisory lock; dry-run paths must not."""

    def _conn(self):
        conn = MagicMock()

        def fake_execute(sql, *args, **kwargs):
            result = MagicMock()
            if "pg_try_advisory_lock" in sql:
                result.fetchone.return_value = (True,)
            elif "COUNT(*)" in sql:
                result.fetchone.return_value = (0,)
            else:
                result.fetchall.return_value = []
            return result

        conn.execute.side_effect = fake_execute
        return conn

    def _locked(self, conn):
        return any("pg_try_advisory_lock" in str(c.args[0]) for c in conn.execute.call_args_list)

    def test_cleanup_locks(self):
        conn = self._conn()
        ducklake_maintenance.cleanup(conn, days=1, dry_run=False)
        assert self._locked(conn)

    def test_cleanup_dry_run_skips_lock(self):
        conn = self._conn()
        ducklake_maintenance.cleanup(conn, days=1, dry_run=True)
        assert not self._locked(conn)

    def test_cleanup_all_locks(self):
        conn = self._conn()
        ducklake_maintenance.cleanup_all(conn)
        assert self._locked(conn)

    def test_orphans_locks_and_dry_run_skips(self):
        conn = self._conn()
        ducklake_maintenance.orphans(conn, dry_run=False)
        assert self._locked(conn)
        conn2 = self._conn()
        ducklake_maintenance.orphans(conn2, dry_run=True)
        assert not self._locked(conn2)

    def test_checkpoint_locks(self):
        conn = self._conn()
        ducklake_maintenance.checkpoint(conn)
        assert self._locked(conn)


class TestRepairDiscoveryNameGate:
    """Catalog-derived table names outside the strict identifier shape must
    be skipped loudly, never interpolated into SQL."""

    def _conn(self, names):
        conn = MagicMock()
        result = MagicMock()
        result.fetchall.return_value = [(n,) for n in names]
        conn.execute.return_value = result
        return conn

    def test_hostile_name_skipped_with_warning(self, caplog):
        hostile = "events_x' UNION SELECT 1,2 --"
        conn = self._conn(["events", hostile, "persons_2024"])
        with caplog.at_level(logging.WARNING):
            out = ducklake_maintenance._repair_partition_values_discover_tables(conn)
        assert ("events", "events") in out
        assert ("persons", "persons_2024") in out
        assert all(hostile != name for _, name in out)
        assert "refusing to interpolate" in caplog.text

    def test_plain_names_pass(self):
        conn = self._conn(["events", "events_nrt", "persons", "persons_v2"])
        out = ducklake_maintenance._repair_partition_values_discover_tables(conn)
        assert len(out) == 4


class TestRepairSqlEscaping:
    """Pin that the two catalog-name interpolation sites route through
    _sql_string_literal. The discovery gate keeps quote-bearing names out of
    the composed flow, but resolve/log_outliers are independently callable
    belts -- a revert to plain f-string quoting here must fail a test, not
    pass silently. Names are fed directly, bypassing the gate."""

    # A name with one embedded single quote. _sql_string_literal doubles it
    # for the inner literal, then postgres_query's outer wrapper doubles
    # again, so the fully-composed SQL contains a 4-quote run around it.
    NAME = "events_o" + "'" + "brien"
    ESCAPED = "events_o" + "'" * 4 + "brien"
    UNESCAPED = "'events_o" + "'" + "brien'"

    def test_log_outliers_escapes_table_names(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        ducklake_maintenance._repair_partition_values_log_outliers(conn, [("events", self.NAME)])
        sql = conn.execute.call_args[0][0]
        assert self.ESCAPED in sql
        assert self.UNESCAPED not in sql

    def test_resolve_escapes_table_name(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        ducklake_maintenance._repair_partition_values_resolve(conn, "events", self.NAME)
        sql = conn.execute.call_args[0][0]
        assert ("t.table_name = " + "'" * 2 + "events_o" + "'" * 4 + "brien" + "'" * 2) in sql


class TestRepairPreFlightPredicate:
    """The pre-flight must gate on the fpv INDEX SET, not the row count.

    The collapsed shape (N rows all stacked on the top partition_key_index —
    ducklake_add_data_files rot) has the RIGHT row count at the WRONG indexes.
    A bare COUNT(...) <> N check passed it, so the pre-flight short-circuited
    "clean" over real rot and _execute never ran, while the dashboard's
    partition-value-corruption metric (distinct-index-set based) kept
    flagging the same files. The predicate must mirror _count_broken /
    _execute's post-condition."""

    def _sql(self):
        conn = MagicMock()
        result = MagicMock()
        result.fetchone.return_value = (True,)
        conn.execute.return_value = result
        assert ducklake_maintenance._repair_partition_values_pre_flight_any_rot(conn) is True
        return conn.execute.call_args[0][0]

    def test_gates_on_index_set_not_row_count(self):
        sql = self._sql()
        assert "IS DISTINCT FROM ARRAY[0,1,2]::bigint[]" in sql, "events must compare the index SET"
        assert "IS DISTINCT FROM ARRAY[0,1]::bigint[]" in sql, "persons must compare the index SET"
        assert "COUNT(fpv.partition_key_index) <> " not in sql, "row-count check passes the collapsed shape"

    def test_aggregate_matches_count_broken(self):
        # Non-DISTINCT array_agg (catches duplicated rows per index) with the
        # NULL-filtered COALESCE-to-empty shape used by _count_broken.
        sql = self._sql()
        assert "array_agg(fpv.partition_key_index ORDER BY fpv.partition_key_index)" in sql
        assert "FILTER (WHERE fpv.partition_key_index IS NOT NULL)" in sql

    def test_expected_arrays_derive_from_spec(self):
        # Single source of truth: the arrays come from
        # _REPAIR_PARTITION_VALUE_SPEC, so a spec change can't desync the
        # pre-flight from the repair.
        sql = self._sql()
        for kind, spec in ducklake_maintenance._REPAIR_PARTITION_VALUE_SPEC.items():
            expected = "ARRAY[" + ",".join(str(idx) for idx, _ in spec) + "]::bigint[]"
            assert expected in sql, f"{kind} expected-index array missing"


class TestPurgeOrphanStats:
    """Verify purge_orphan_stats predicate shape, dry-run gating, and ordering."""

    def _make_conn(self, before=(5, 120), after=(0, 0), lock_acquired=True):
        """Mock conn: counts query returns `before` first, `after` afterwards."""
        conn = MagicMock()
        counts = iter([before, after, after])
        executed = []

        def fake_execute(sql, *args, **kwargs):
            result = MagicMock()
            if "postgres_query" in sql and "table_rows" in sql:
                result.fetchone.return_value = next(counts)
            elif "postgres_query" in sql and "pg_try_advisory_lock" in sql:
                result.fetchone.return_value = (lock_acquired,)
            elif "postgres_execute" in sql:
                executed.append(sql)
            return result

        conn.execute.side_effect = fake_execute
        conn._executed = executed
        return conn

    def test_dry_run_reports_without_deleting(self, caplog):
        conn = self._make_conn(before=(5, 120))
        with caplog.at_level(logging.INFO):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=True)
        assert conn._executed == []
        assert "5 orphaned table-stats rows, 120 orphaned column-stats rows" in caplog.text

    def test_zero_orphans_skips_lock_and_delete(self):
        conn = self._make_conn(before=(0, 0))
        ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert conn._executed == []
        # advisory lock must not be taken when there is nothing to do
        assert not any("pg_try_advisory_lock" in str(c) for c in conn.execute.call_args_list)

    def test_real_run_single_txn_table_stats_first(self, caplog):
        """Both DELETEs must ship in ONE postgres_execute call (one REPEATABLE
        READ transaction) with table_stats first. Two separate transactions
        would expose a "table_stats present, column_stats gone" state that
        crashes the commit path's conflict read (unguarded GetValue on the
        NULL column_id the LEFT JOIN produces)."""
        conn = self._make_conn(before=(5, 120), after=(0, 0))
        with caplog.at_level(logging.INFO):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert len(conn._executed) == 1
        sql = conn._executed[0]
        assert "SET LOCAL statement_timeout" in sql
        assert "SET LOCAL lock_timeout" in sql
        ts_pos = sql.index("DELETE FROM public.ducklake_table_stats")
        cs_pos = sql.index("DELETE FROM public.ducklake_table_column_stats")
        assert ts_pos < cs_pos
        assert "deleted ~5 table-stats and ~120 column-stats rows" in caplog.text

    def test_lock_failure_aborts_before_any_delete(self):
        conn = self._make_conn(before=(5, 120), lock_acquired=False)
        with pytest.raises(RuntimeError, match="advisory lock"):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert conn._executed == []

    def test_churn_during_run_clamps_deleted_at_zero(self, caplog):
        """More orphans after than before (pathological churn) must not log
        negative deleted counts."""
        conn = self._make_conn(before=(5, 120), after=(7, 150))
        with caplog.at_level(logging.INFO):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert "deleted ~0 table-stats and ~0 column-stats rows" in caplog.text
        assert "7/150 orphans remain" in caplog.text

    def test_predicate_uses_not_exists_never_not_in(self):
        """NOT IN goes three-valued on NULL table_ids: it skips NULL-key rows
        (metric/purge divergence) and deletes NOTHING if the live set ever
        contained a NULL. The predicate must be NOT EXISTS."""
        conn = self._make_conn(before=(1, 1), after=(0, 0))
        ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        for sql in conn._executed:
            assert "NOT EXISTS" in sql
            assert "NOT IN" not in sql
            assert "end_snapshot IS NULL" in sql

    def test_large_purge_logs_vacuum_hint(self, caplog):
        conn = self._make_conn(before=(1_000, 200_000), after=(0, 0))
        with caplog.at_level(logging.INFO):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert "VACUUM (ANALYZE)" in caplog.text

    def test_small_purge_no_vacuum_hint(self, caplog):
        conn = self._make_conn(before=(5, 120), after=(0, 0))
        with caplog.at_level(logging.INFO):
            ducklake_maintenance.purge_orphan_stats(conn, dry_run=False)
        assert "VACUUM" not in caplog.text


class TestCleanupAllSafe:
    """Verify the dedup → heal → cleanup-all retry loop."""

    def _patches(self):
        return (
            patch("ducklake_maintenance._acquire_advisory_lock"),
            patch("ducklake_maintenance.dedup_deletions"),
            patch("ducklake_maintenance.heal_orphans"),
            patch("ducklake_maintenance.cleanup_all"),
        )

    def test_succeeds_on_first_attempt(self):
        lock, dedup, heal, cleanup = (p.start() for p in self._patches())
        try:
            ducklake_maintenance.cleanup_all_safe(MagicMock(), max_iterations=10)
        finally:
            patch.stopall()
        # Lock taken once for the whole orchestration.
        assert lock.call_count == 1
        # One pass of dedup + heal + cleanup_all.
        assert dedup.call_count == 1
        assert heal.call_count == 1
        assert cleanup.call_count == 1

    def test_retries_after_io_exception_then_succeeds(self):
        lock, dedup, heal, cleanup = (p.start() for p in self._patches())
        # First attempt: cleanup_all raises (simulating the c1 NoSuchKey crash).
        # Second attempt: dedup + heal mop up the fresh orphans, cleanup succeeds.
        cleanup.side_effect = [duckdb.IOException("simulated NoSuchKey"), None]
        try:
            ducklake_maintenance.cleanup_all_safe(MagicMock(), max_iterations=10)
        finally:
            patch.stopall()
        assert lock.call_count == 1, "lock taken once for the whole orchestration"
        assert dedup.call_count == 2, "dedup re-runs to clean up after the crash"
        assert heal.call_count == 2, "heal re-runs to clean up after the crash"
        assert cleanup.call_count == 2

    def test_exhausts_iterations_and_raises(self):
        lock, dedup, heal, cleanup = (p.start() for p in self._patches())
        cleanup.side_effect = duckdb.IOException("persistent crash")
        try:
            with pytest.raises(RuntimeError, match="exhausted 3 iterations"):
                ducklake_maintenance.cleanup_all_safe(MagicMock(), max_iterations=3)
        finally:
            patch.stopall()
        assert dedup.call_count == 3
        assert heal.call_count == 3
        assert cleanup.call_count == 3


class TestFsck:
    """Verify the dry-run vs real dispatch and ordering."""

    def test_dry_run_delegates_to_dry_run_subcalls(self):
        with (
            patch("ducklake_maintenance.dedup_deletions") as dedup,
            patch("ducklake_maintenance.heal_orphans") as heal,
            patch("ducklake_maintenance.orphans") as s3_orphans,
            patch("ducklake_maintenance.cleanup_all_safe") as orch,
        ):
            conn = MagicMock()
            ducklake_maintenance.fsck(conn, dry_run=True, max_iterations=10)
        # Dry-run must run heal_orphans so its B1/B3 gates execute.
        dedup.assert_called_once_with(conn, dry_run=True)
        heal.assert_called_once_with(conn, dry_run=True)
        s3_orphans.assert_called_once_with(conn, dry_run=True)
        orch.assert_not_called()

    def test_real_run_calls_orchestrator_and_s3_sweep(self):
        with (
            patch("ducklake_maintenance.dedup_deletions") as dedup,
            patch("ducklake_maintenance.heal_orphans") as heal,
            patch("ducklake_maintenance.orphans") as s3_orphans,
            patch("ducklake_maintenance.cleanup_all_safe") as orch,
        ):
            conn = MagicMock()
            ducklake_maintenance.fsck(conn, dry_run=False, max_iterations=7)
        # Real path goes through cleanup_all_safe (which itself calls dedup +
        # heal under the lock); it must NOT call heal/dedup directly here, or
        # we'd be running them outside the lock.
        orch.assert_called_once_with(conn, 7)
        s3_orphans.assert_called_once_with(conn, dry_run=False)
        dedup.assert_not_called()
        heal.assert_not_called()

    def test_dry_run_propagates_gate_failure(self):
        """A real fsck would abort once heal-orphans hits a failed gate; the
        dry-run must surface the same outcome rather than reporting healthy."""
        with (
            patch("ducklake_maintenance.dedup_deletions"),
            patch("ducklake_maintenance.heal_orphans") as heal,
            patch("ducklake_maintenance.orphans"),
        ):
            heal.side_effect = RuntimeError("safety gate B1 failed: ...")
            with pytest.raises(RuntimeError, match="safety gate B1"):
                ducklake_maintenance.fsck(MagicMock(), dry_run=True, max_iterations=10)


class TestSetCompactionTuning:
    """Pure SQL emission; no real connection needed."""

    def test_emits_expected_sets(self):
        conn = MagicMock()
        ducklake_maintenance._set_compaction_tuning(conn, threads=4, memory_limit="8GB")
        executed = [c.args[0] for c in conn.execute.call_args_list]
        assert "SET threads = 4" in executed
        assert "SET memory_limit = '8GB'" in executed
        assert "SET preserve_insertion_order = false" in executed
        assert "SET http_timeout = 600000" in executed

    def test_rejects_injection_in_memory_limit(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match="Illegal character"):
            ducklake_maintenance._set_compaction_tuning(conn, threads=2, memory_limit="4GB'; DROP TABLE x; --")
        # Sanitization happens before any execute; conn must not have been touched.
        conn.execute.assert_not_called()


class TestHeartbeat:
    """Heartbeat line formatting and degradation; no real threads or sleeps."""

    def _line(self, conn, label, elapsed, prev_net=None, interval_s=60.0):
        """Helper: call _heartbeat_line and return just the string."""
        line, _ = ducklake_maintenance._heartbeat_line(conn, label, elapsed, prev_net, interval_s)
        return line

    def test_no_query_running_elapsed_only(self, monkeypatch):
        monkeypatch.setattr(ducklake_maintenance, "_rss_bytes", lambda: None)
        monkeypatch.setattr(ducklake_maintenance, "_net_bytes", lambda: None)
        conn = MagicMock()
        conn.query_progress.return_value = -1.0
        assert self._line(conn, "x", 60.0) == "x: 60s elapsed"

    def test_network_rate_shown_when_prev_net_available(self, monkeypatch):
        monkeypatch.setattr(ducklake_maintenance, "_rss_bytes", lambda: None)
        monkeypatch.setattr(ducklake_maintenance, "_net_bytes", lambda: (2_000_000_000, 500_000_000))
        conn = MagicMock()
        conn.query_progress.return_value = -1.0
        prev_net = (1_000_000_000, 300_000_000)  # 1GB rx, 300MB tx before
        # over 10s: rx=(2G-1G)/10/1024^2=95.4MiB/s, tx=(500M-300M)/10/1024^2=19.1MiB/s
        line = self._line(conn, "x", 60.0, prev_net=prev_net, interval_s=10.0)
        assert "↓95.4/↑19.1 MiB/s" in line

    def test_network_rate_wrap_shows_zero_not_negative(self, monkeypatch):
        """32-bit counter wrap must not produce a negative rate in the log."""
        monkeypatch.setattr(ducklake_maintenance, "_rss_bytes", lambda: None)
        monkeypatch.setattr(ducklake_maintenance, "_net_bytes", lambda: (100, 100))
        conn = MagicMock()
        conn.query_progress.return_value = -1.0
        prev_net = (2**32 - 1, 2**32 - 1)  # just before wrap
        line = self._line(conn, "x", 60.0, prev_net=prev_net, interval_s=10.0)
        assert "↓0.0/↑0.0 MiB/s" in line
        assert "-" not in line.split("net=")[-1]

    def test_network_rate_absent_without_prev_net(self, monkeypatch):
        monkeypatch.setattr(ducklake_maintenance, "_rss_bytes", lambda: None)
        monkeypatch.setattr(ducklake_maintenance, "_net_bytes", lambda: (1_000_000, 1_000_000))
        conn = MagicMock()
        conn.query_progress.return_value = -1.0
        line = self._line(conn, "x", 60.0, prev_net=None)
        assert "MiB/s" not in line

    def test_rss_bytes_returns_positive_or_none(self):
        rss = ducklake_maintenance._rss_bytes()
        assert rss is None or rss > 0

    def test_net_bytes_returns_tuple_or_none(self):
        net = ducklake_maintenance._net_bytes()
        assert net is None or (isinstance(net, tuple) and len(net) == 2 and all(v >= 0 for v in net))


def _compact_conn(tables, merge_rows=None, fail_tables=(), candidates=(100, 1000)):
    """A duckdb-conn-shaped MagicMock driving the per-table compact() flow.

    tables: (schema, table) pairs the candidate-driven enumeration returns
    (the mock returns them verbatim — production orders by backlog DESC).
    merge_rows: {table_name: result rows} for its merge CALL (default []).
    fail_tables: table names whose merge CALL raises.
    """
    conn = MagicMock()
    # The CALL's table name is its second positional arg — parse it instead of
    # substring-matching, so a mock defect can't masquerade as a table failure.
    call_re = re.compile(r"ducklake_merge_adjacent_files\('[^']*', '([^']*)'")

    def _execute(sql, *a, **k):
        res = MagicMock()
        if "HAVING COUNT(*) >= 2" in sql:
            # candidate-driven table enumeration
            res.fetchall.return_value = list(tables)
        elif "ducklake_merge_adjacent_files" in sql:
            m = call_re.search(sql)
            assert m, f"unparseable merge CALL: {sql}"
            name = m.group(1)
            if name in fail_tables:
                raise RuntimeError("DuckLakeCompactor: Files have different hive partition path")
            res.fetchall.return_value = (merge_rows or {}).get(name, [])
        elif "ducklake_options" in sql:
            res.fetchone.return_value = ("134217728",)  # 128MiB, restore path
        elif "ducklake_data_file" in sql:
            res.fetchone.return_value = candidates
        return res

    conn.execute.side_effect = _execute
    return conn


def _merge_calls(conn):
    return [c.args[0] for c in conn.execute.call_args_list if "ducklake_merge_adjacent_files" in c.args[0]]


class TestCompactSql:
    """compact() must pass the per-run file cap through to the merge CALL(s)."""

    def test_merge_runs_under_heartbeat(self, monkeypatch):
        """The heartbeat must wrap each per-table merge and be stopped afterwards."""
        hb = MagicMock()
        start = MagicMock(return_value=hb)
        monkeypatch.setattr(ducklake_maintenance, "_start_heartbeat", start)

        conn = _compact_conn([("posthog", "events")])
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=1, memory_limit="16GB", max_compacted_files=25000
        )

        start.assert_called_once_with(conn, "compact tier-1 posthog.events")
        hb.set.assert_called_once()

    def test_single_table_call_includes_max_compacted_files(self):
        """--table invocation keeps the direct one-CALL form (no schema arg)."""
        conn = MagicMock()
        result = MagicMock()
        # candidate-count read, then ducklake_options read for target_file_size restore
        result.fetchone.side_effect = [(614342, 242861636470), ("128MiB",)]
        result.fetchall.return_value = []
        conn.execute.return_value = result

        ducklake_maintenance.compact(
            conn, tier=1, table="events", dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=100000
        )

        merge_calls = _merge_calls(conn)
        assert len(merge_calls) == 1
        assert "'events'" in merge_calls[0]
        assert "schema =>" not in merge_calls[0]
        assert "max_compacted_files => 100000" in merge_calls[0]
        assert "max_file_size =>" in merge_calls[0]

    def test_dry_run_does_not_merge(self):
        conn = MagicMock()
        result = MagicMock()
        result.fetchone.return_value = (614342, 242861636470)
        conn.execute.return_value = result

        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=True, threads=2, memory_limit="16GB", max_compacted_files=100000
        )

        assert not _merge_calls(conn)


class TestCompactPerTable:
    """Catalog-wide compaction is one CALL per live table: a poisoned table
    (e.g. mixed hive-path conventions from an add_data_files backfill — the
    production `events` incident) must not abort the other tables' compaction."""

    def test_catalog_wide_iterates_tables_with_schema(self):
        conn = _compact_conn([("posthog", "events"), ("posthog", "persons")])
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        calls = _merge_calls(conn)
        assert len(calls) == 2
        assert "schema => 'posthog'" in calls[0] and "'events'" in calls[0]
        assert "schema => 'posthog'" in calls[1] and "'persons'" in calls[1]
        assert all("max_compacted_files => 10" in c for c in calls)

    def test_poisoned_table_does_not_abort_run(self, caplog, monkeypatch):
        hb = MagicMock()
        start = MagicMock(return_value=hb)
        monkeypatch.setattr(ducklake_maintenance, "_start_heartbeat", start)
        conn = _compact_conn(
            [("posthog", "events"), ("posthog", "persons")],
            merge_rows={"persons": [("posthog", "persons", 4, 1)]},
            fail_tables=("events",),
        )
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            n_failed = ducklake_maintenance.compact(
                conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
            )
        calls = _merge_calls(conn)
        assert len(calls) == 2, "persons must still be compacted after events fails"
        assert n_failed == 1
        assert "1/2 table(s) failed" in caplog.text
        assert "posthog.events" in caplog.text
        # Heartbeat stopped on BOTH paths, including the failed table.
        assert start.call_count == 2
        assert hb.set.call_count == 2
        # target_file_size restored despite the partial failure.
        assert any("ducklake_set_option" in c.args[0] and "128MiB" in c.args[0] for c in conn.execute.call_args_list)

    def test_all_tables_failed_raises(self):
        conn = _compact_conn(
            [("posthog", "events"), ("posthog", "persons")],
            fail_tables=("events", "persons"),
        )
        with pytest.raises(RuntimeError, match="all 2 table"):
            ducklake_maintenance.compact(
                conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
            )

    def test_single_table_catalog_poisoned_does_not_raise(self):
        """1/1 failed must NOT raise: raising would re-wedge the recipe chain
        (later tiers + cleanup-all) forever — the exact incident mode this
        change fixes. The failure surfaces via the returned count/gauge."""
        conn = _compact_conn([("posthog", "events")], fail_tables=("events",))
        n_failed = ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        assert n_failed == 1

    def test_failure_budget_and_skip_interplay_does_not_raise(self):
        """fail + budget-exhaust + never-reached must not trip the all-failed
        policy: failed(1) < total(3)."""
        conn = _compact_conn(
            [("posthog", "aaa"), ("posthog", "bbb"), ("posthog", "ccc")],
            merge_rows={"bbb": [("posthog", "bbb", 10, 1)]},
            fail_tables=("aaa",),
        )
        n_failed = ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        calls = _merge_calls(conn)
        assert len(calls) == 2, "ccc must be deferred once bbb exhausts the budget"
        assert n_failed == 1

    def test_malformed_result_rows_fail_the_table_not_the_run(self, caplog):
        """A fork/version drift in the CALL's result schema must fail that
        table's accounting loudly, not poison the run-wide aggregation."""
        conn = _compact_conn(
            [("posthog", "events"), ("posthog", "persons")],
            merge_rows={"events": [("posthog", "events", 3)], "persons": [("posthog", "persons", 4, 1)]},
        )
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            n_failed = ducklake_maintenance.compact(
                conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
            )
        assert n_failed == 1
        assert "unexpected merge result shape" in caplog.text
        assert len(_merge_calls(conn)) == 2, "persons still compacts after events' malformed rows"

    def test_budget_is_global_across_tables(self):
        """A table that consumes the whole file budget stops the loop; later
        tables wait for the next run (preserves the old catalog-scope bound)."""
        conn = _compact_conn(
            [("posthog", "events"), ("posthog", "persons")],
            merge_rows={"events": [("posthog", "events", 10, 1)]},
        )
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        calls = _merge_calls(conn)
        assert len(calls) == 1 and "'events'" in calls[0]

    def test_remaining_budget_passed_to_next_table(self):
        conn = _compact_conn(
            [("posthog", "events"), ("posthog", "persons")],
            merge_rows={"events": [("posthog", "events", 3, 1)]},
        )
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        calls = _merge_calls(conn)
        assert len(calls) == 2
        assert "max_compacted_files => 10" in calls[0]
        assert "max_compacted_files => 7" in calls[1]

    def test_skips_non_identifier_table_names(self, caplog):
        conn = _compact_conn([("posthog", "events"), ("posthog", "bad'name")])
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            ducklake_maintenance.compact(
                conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
            )
        calls = _merge_calls(conn)
        assert len(calls) == 1 and "'events'" in calls[0]
        assert "non-identifier" in caplog.text

    def test_empty_catalog_is_a_noop(self):
        conn = _compact_conn([])
        n_failed = ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        assert not _merge_calls(conn)
        assert n_failed == 0

    def test_dry_run_does_not_enumerate_tables(self):
        """Dry-run must stay cheap against a wedged catalog: no enumeration,
        no merge CALLs, just the candidate count."""
        conn = MagicMock()
        result = MagicMock()
        result.fetchone.return_value = (100, 1000)
        conn.execute.return_value = result
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=True, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        assert not any("HAVING COUNT(*) >= 2" in c.args[0] for c in conn.execute.call_args_list)

    def test_enumeration_is_candidate_driven_backlog_first(self):
        """The table list must come from the tier's candidate files — sized to
        the tier band, >= 2 files per table (singletons can't merge), most
        backlogged first — NOT information_schema. Alphabetical enumeration
        over all live tables let 2-file cosmetic merges starve the real
        backlog out of the budget (observed: 3,074 tables, budget gone on
        five billing tables, the 15k-candidate events table never reached)."""
        conn = _compact_conn([("posthog", "events")])
        ducklake_maintenance.compact(
            conn, tier=1, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        enum_calls = [c.args[0] for c in conn.execute.call_args_list if "HAVING COUNT(*) >= 2" in c.args[0]]
        assert len(enum_calls) == 1
        sql = enum_calls[0]
        assert "ducklake_data_file" in sql and "ducklake_table" in sql and "ducklake_schema" in sql
        assert "ORDER BY COUNT(*) DESC" in sql, "most backlogged table must be served first"
        assert "file_size_bytes < 1048576" in sql, "enumeration must be scoped to the tier's size band"
        assert not any("information_schema" in c.args[0] for c in conn.execute.call_args_list)

    def test_enumeration_band_includes_min_for_upper_tiers(self):
        conn = _compact_conn([("posthog", "events")])
        ducklake_maintenance.compact(
            conn, tier=2, table=None, dry_run=False, threads=2, memory_limit="16GB", max_compacted_files=10
        )
        sql = next(c.args[0] for c in conn.execute.call_args_list if "HAVING COUNT(*) >= 2" in c.args[0])
        assert "file_size_bytes >= 1048576" in sql and "file_size_bytes < 10485760" in sql


class TestScopedTargetFileSize:
    """Read-and-restore round-trip for target_file_size; warns only on real failure."""

    def _conn(self, prior_value):
        """A duckdb-conn shaped MagicMock that returns ``prior_value`` from the
        ducklake_options read and accepts the ducklake_set_option calls."""
        conn = MagicMock()
        # Each conn.execute() returns a result object whose fetchone() is
        # configured per call. We only care about the first fetchone (the
        # ducklake_options read); the set_option CALLs return result objects
        # that are never .fetchone()'d.
        result = MagicMock()
        result.fetchone.return_value = (prior_value,) if prior_value is not None else None
        conn.execute.return_value = result
        return conn

    def test_no_warning_when_prior_converts_cleanly_to_default(self, caplog):
        # 134217728 bytes == 128 MiB == DEFAULT_TARGET_FILE_SIZE. The conversion
        # succeeded; the warning must NOT fire just because the converted form
        # equals the default — that's a healthy install, not a failure.
        conn = self._conn("134217728")
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
                pass
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert warnings == [], "no warning expected on a clean default-value round-trip"

    def test_no_warning_when_prior_converts_cleanly_to_non_default(self, caplog):
        # 64 MiB — operator value, not the default.
        conn = self._conn("67108864")
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
                pass
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_warning_when_conversion_fails(self, caplog):
        # 12345678 isn't a clean power of 1024; _bytes_to_human returns None,
        # and we genuinely lose the operator's value — that's the case where
        # the warning is informative.
        conn = self._conn("12345678")
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
                pass
        warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) == 1
        assert "could not be converted" in warnings[0]
        assert "12345678" in warnings[0]

    def test_no_warning_when_prior_unset(self, caplog):
        conn = self._conn(None)
        with caplog.at_level(logging.WARNING, logger="maintenance"):
            with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
                pass
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []

    def test_restores_to_converted_prior(self):
        conn = self._conn("67108864")
        with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
            pass
        # The last execute call should be the restore SET to '64MiB'.
        last_sql = conn.execute.call_args_list[-1].args[0]
        assert "target_file_size" in last_sql
        assert "'64MiB'" in last_sql

    def test_restores_to_default_when_prior_unset(self):
        conn = self._conn(None)
        with ducklake_maintenance._scoped_target_file_size(conn, "5MiB"):
            pass
        last_sql = conn.execute.call_args_list[-1].args[0]
        assert f"'{ducklake_maintenance.DEFAULT_TARGET_FILE_SIZE}'" in last_sql


class TestConnectS3SecretProvider:
    """connect() chooses between PROVIDER config (static keys) and
    PROVIDER credential_chain (IRSA / instance profile / env / shared config)
    based on whether DUCKDB_S3_ACCESS_KEY_ID + DUCKDB_S3_SECRET_ACCESS_KEY are
    set together. The static-keys path is the megaduck/viaduck case (an IAM
    user's keys synced via ExternalSecret); the credential_chain path is the
    per-tenant duckling case (PodIdentityAssociation, no static-key Secret).

    `patch.dict(..., clear=True)` is intentional — it wipes the developer's
    local `AWS_*` env so a future change that accidentally adds a fallback
    on those variables shows up in the unit tests rather than only failing on
    a CI runner that happens to inherit the right shell env. The downside is
    that we can't assert anything about how credential_chain RESOLVES creds
    — only that the SECRET DDL is well-formed; resolution is an SDK runtime
    concern and is covered by the diagnostic probe at connect-time."""

    _BASE_ENV = {
        "DUCKLAKE_RDS_HOST": "h",
        "DUCKLAKE_RDS_PASSWORD": "p",
        "DUCKLAKE_DATA_PATH": "s3://bucket/",
        "DUCKDB_S3_REGION": "us-east-1",
    }

    def _executed_sql(self, conn: MagicMock) -> list[str]:
        return [call.args[0] for call in conn.execute.call_args_list if call.args]

    def _connect_with_env(self, env: dict[str, str]) -> MagicMock:
        conn = MagicMock()
        # ducklake_table_insertions and similar queries fetchone()s are
        # exercised after connect() returns; nothing in connect() itself reads
        # results, so a bare MagicMock is enough. The mock also bypasses the
        # diagnostic probe's glob() call — that path is covered by separate
        # real-DuckDB tests.
        with patch.dict("os.environ", env, clear=True), patch("ducklake_maintenance.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value = conn
            # The credential_chain probe catches duckdb.Error; expose the real
            # exception class so a raise() inside the test isn't swallowed by
            # the broad except clause.
            mock_duckdb.Error = duckdb.Error
            ducklake_maintenance.connect()
        return conn

    def test_static_keys_set_emits_provider_config_with_keys(self):
        env = {**self._BASE_ENV, "DUCKDB_S3_ACCESS_KEY_ID": "AKIA_X", "DUCKDB_S3_SECRET_ACCESS_KEY": "SECRET_X"}
        conn = self._connect_with_env(env)
        secret_sql = next(s for s in self._executed_sql(conn) if "CREATE OR REPLACE SECRET s3" in s)
        assert "PROVIDER config" in secret_sql
        assert "KEY_ID 'AKIA_X'" in secret_sql
        assert "SECRET 'SECRET_X'" in secret_sql
        assert "credential_chain" not in secret_sql

    def test_static_keys_set_also_emits_set_s3_access_key_id(self):
        """T1 (review feedback): the static-keys path must continue to emit
        the legacy `SET s3_access_key_id` / `SET s3_secret_access_key` rows
        even though DuckLake 1.5.x runs through the SECRET manager — a
        future refactor that drops them on the static path could silently
        regress any 1.4-compat consumer or any future code path that reads
        SET-level credentials. Pinning them keeps the catalog driver's
        legacy fallback path intact."""
        env = {**self._BASE_ENV, "DUCKDB_S3_ACCESS_KEY_ID": "AKIA_X", "DUCKDB_S3_SECRET_ACCESS_KEY": "SECRET_X"}
        conn = self._connect_with_env(env)
        executed = self._executed_sql(conn)
        assert any(sql == "SET s3_access_key_id = 'AKIA_X'" for sql in executed), executed
        assert any(sql == "SET s3_secret_access_key = 'SECRET_X'" for sql in executed), executed

    def test_no_keys_emits_provider_credential_chain_with_no_key_id_or_secret(self):
        """Defends against a refactor that keeps the KEY_ID '' SECRET '' string
        even on the IRSA path — DuckDB would parse those as literal empty
        credentials and credential_chain would never run.

        The credential-pair exclusion is asserted via a regex anchored on the
        `KEY_ID 'X' SECRET 'Y'` pair shape, not a fragile `'SECRET ''` substring
        check (which would trip on any future addition of a `SECRET_NAME` style
        keyword)."""
        import re

        conn = self._connect_with_env(self._BASE_ENV)
        secret_sql = next(s for s in self._executed_sql(conn) if "CREATE OR REPLACE SECRET s3" in s)
        assert "PROVIDER credential_chain" in secret_sql
        assert "PROVIDER config" not in secret_sql
        assert "KEY_ID" not in secret_sql
        assert re.search(r"\bSECRET\s+'[^']*'", secret_sql) is None, (
            f"credential_chain path must not embed KEY_ID/SECRET literals, got: {secret_sql!r}"
        )
        assert "REGION 'us-east-1'" in secret_sql

    def test_no_keys_skips_set_s3_access_key_id(self):
        """The SET s3_access_key_id loop must not emit lines with an empty
        string value when keys are unset. Empty-string credentials shadow the
        SECRET in the catalog driver and cause silent auth failures."""
        conn = self._connect_with_env(self._BASE_ENV)
        for sql in self._executed_sql(conn):
            assert not sql.startswith("SET s3_access_key_id"), f"unexpected SET s3_access_key_id: {sql!r}"
            assert not sql.startswith("SET s3_secret_access_key"), f"unexpected SET s3_secret_access_key: {sql!r}"

    def test_only_key_id_set_refuses_at_startup(self):
        env = {**self._BASE_ENV, "DUCKDB_S3_ACCESS_KEY_ID": "AKIA_X"}
        with pytest.raises(RuntimeError, match="must be set together"):
            self._connect_with_env(env)

    def test_only_secret_set_refuses_at_startup(self):
        env = {**self._BASE_ENV, "DUCKDB_S3_SECRET_ACCESS_KEY": "SECRET_X"}
        with pytest.raises(RuntimeError, match="must be set together"):
            self._connect_with_env(env)

    def test_partial_config_error_names_both_env_vars_for_log_grepping(self):
        """M4 (review feedback): the error message names both real env var
        names so a log scraper that greps either DUCKDB_S3_ACCESS_KEY_ID or
        DUCKDB_S3_SECRET_ACCESS_KEY matches the failure."""
        env = {**self._BASE_ENV, "DUCKDB_S3_ACCESS_KEY_ID": "AKIA_X"}
        with pytest.raises(RuntimeError) as exc:
            self._connect_with_env(env)
        msg = str(exc.value)
        assert "DUCKDB_S3_ACCESS_KEY_ID=set" in msg, msg
        assert "DUCKDB_S3_SECRET_ACCESS_KEY=unset" in msg, msg

    def test_credential_chain_path_honors_endpoint_override(self):
        """A custom endpoint (MinIO in dev, FIPS endpoint in prod, etc.) is
        still propagated under the credential_chain path."""
        env = {**self._BASE_ENV, "DUCKDB_S3_ENDPOINT": "minio.local:9000", "DUCKDB_S3_USE_SSL": "false"}
        conn = self._connect_with_env(env)
        secret_sql = next(s for s in self._executed_sql(conn) if "CREATE OR REPLACE SECRET s3" in s)
        assert "PROVIDER credential_chain" in secret_sql
        assert "ENDPOINT 'minio.local:9000'" in secret_sql
        assert "USE_SSL false" in secret_sql

    def test_credential_chain_requires_explicit_region(self):
        """M2 (review feedback): credential_chain path refuses to start
        without DUCKDB_S3_REGION. Silently defaulting to us-east-1 when a
        per-tenant duckling is in eu-west-1 would resolve creds against
        a region whose bucket may not exist."""
        env = {k: v for k, v in self._BASE_ENV.items() if k != "DUCKDB_S3_REGION"}
        with pytest.raises(RuntimeError, match="DUCKDB_S3_REGION"):
            self._connect_with_env(env)

    def test_static_keys_keeps_us_east_1_default_when_region_unset(self):
        """Counterpoint to test_credential_chain_requires_explicit_region:
        megaduck/viaduck have shipped without DUCKDB_S3_REGION set, so the
        legacy us-east-1 default is preserved on the static-keys path.
        Defends against a refactor that requires region uniformly and
        breaks existing deployments."""
        env = {k: v for k, v in self._BASE_ENV.items() if k != "DUCKDB_S3_REGION"}
        env.update({"DUCKDB_S3_ACCESS_KEY_ID": "AKIA_X", "DUCKDB_S3_SECRET_ACCESS_KEY": "SECRET_X"})
        conn = self._connect_with_env(env)
        secret_sql = next(s for s in self._executed_sql(conn) if "CREATE OR REPLACE SECRET s3" in s)
        assert "REGION 'us-east-1'" in secret_sql


class TestConnectSecretDdlAgainstRealDuckdb:
    """T3 (review feedback): the mocked-conn tests above assert the SECRET
    DDL strings, but a typo in the SECRET grammar (unquoted boolean, missing
    keyword) would pass the string assertions while breaking production.
    These tests execute the same DDL templates against a real DuckDB so a
    syntax regression fails CI rather than only failing on a deployed pod."""

    def test_static_keys_secret_ddl_parses_against_real_duckdb(self):
        """Real DuckDB parses the static-keys SECRET DDL emitted on the
        config-provider branch. Regression lock for the megaduck/viaduck
        path that still uses static keys."""
        conn = duckdb.connect()
        try:
            conn.execute(
                "CREATE OR REPLACE SECRET s3 ("
                "TYPE s3, PROVIDER config, "
                "KEY_ID 'AKIA_TEST', SECRET 'SECRET_TEST', "
                "REGION 'us-east-1', ENDPOINT 's3.us-east-1.amazonaws.com', "
                "URL_STYLE 'vhost', USE_SSL true)"
            )
            rows = conn.execute("SELECT name, type, provider FROM duckdb_secrets() WHERE name = 's3'").fetchall()
            assert rows, "real DuckDB did not register the static-keys SECRET"
            name, type_, provider = rows[0]
            assert name == "s3"
            assert type_ == "s3"
            assert provider == "config"
        finally:
            conn.close()

    def test_credential_chain_secret_ddl_parses_against_real_duckdb(self):
        """Real DuckDB parses the credential_chain SECRET DDL emitted on the
        IRSA branch. This is the regression lock for the actual goal of this
        PR — a typo'd keyword or missing quote on the new branch is the most
        likely silent breakage and the unit string-asserts can't catch it.

        DuckDB validates credential_chain at CREATE time by attempting
        provider resolution. On a clean CI/dev box with no AWS creds the
        CREATE raises `Secret Validation Failure: ... Credential Chain` —
        which is exactly the production diagnostic surface we want, so we
        accept it as a pass: a typo in the keywords would produce a `Parser`
        / `Catalog` error class, not a `Secret Validation Failure`. The
        message is matched explicitly to keep this test from masking a real
        regression."""
        conn = duckdb.connect()
        try:
            try:
                conn.execute(
                    "CREATE OR REPLACE SECRET s3 ("
                    "TYPE s3, PROVIDER credential_chain, "
                    "REGION 'us-east-1', ENDPOINT 's3.us-east-1.amazonaws.com', "
                    "URL_STYLE 'vhost', USE_SSL true)"
                )
            except duckdb.Error as e:
                msg = str(e)
                assert "Secret Validation Failure" in msg or "Credential Chain" in msg, (
                    f"DuckDB rejected the SECRET DDL with an unexpected error class — "
                    f"likely a SECRET-grammar regression: {msg}"
                )
                return  # acceptable on no-creds runner; provider keyword is valid
            rows = conn.execute("SELECT name, type, provider FROM duckdb_secrets() WHERE name = 's3'").fetchall()
            assert rows, "real DuckDB did not register the credential_chain SECRET"
            name, type_, provider = rows[0]
            assert name == "s3"
            assert type_ == "s3"
            assert provider == "credential_chain"
        finally:
            conn.close()


class TestAcquireAdvisoryLock:
    """The lock-helper SQL must use single quotes around the inner literal."""

    def test_emits_single_postgres_query_call_with_doubled_quotes(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (True,)
        ducklake_maintenance._acquire_advisory_lock(conn)
        sent = conn.execute.call_args_list[0].args[0]
        # Outer single-quote-wrapped literal (postgres_query 2nd arg) and
        # inner single quotes around 'millpond-...' must be doubled to
        # survive the duckdb-side parser (regression test for a real bug).
        assert "postgres_query('pg', '" in sent
        assert "''millpond-ducklake-maintenance''" in sent

    def test_raises_when_lock_held_by_another_session(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (False,)
        with pytest.raises(RuntimeError, match="advisory lock"):
            ducklake_maintenance._acquire_advisory_lock(conn)
