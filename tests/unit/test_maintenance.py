"""Unit tests for tools/maintenance.py.

Coverage tier A: pure helpers, log message shape, argparse plumbing.
Coverage tier B: orchestrator retry/dispatch logic with mocked sub-calls.

Tier C (macros against a stubbed catalog) and tier D (full e2e against a
real lake) are intentionally out of scope here — they need either an
in-process duckdb with stubbed schemas or the docker-compose stack.
"""

import logging
from unittest.mock import MagicMock, patch

import duckdb
import maintenance
import pytest

# ---------------------------------------------------------------------------
# Tier A — pure helpers and log shape
# ---------------------------------------------------------------------------


class TestSqlStringLiteral:
    def test_plain(self):
        assert maintenance._sql_string_literal("plain") == "'plain'"

    def test_embedded_quote_doubled(self):
        assert maintenance._sql_string_literal("with'quote") == "'with''quote'"

    def test_already_doubled_quotes_are_escaped_again(self):
        # The helper has no idea whether the input was pre-escaped; doubling
        # is a one-way transform consistent with SQL string-literal rules.
        assert maintenance._sql_string_literal("two''already") == "'two''''already'"

    def test_empty(self):
        assert maintenance._sql_string_literal("") == "''"


class TestLogCleanupThroughput:
    def test_typical(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            maintenance._log_cleanup_throughput("cleanup-all", 1000, 950, 10.0)
        msg = caplog.records[0].getMessage()
        assert "cleanup-all throughput" in msg
        assert "files_processed=50" in msg
        assert "queue_depth_before=1000" in msg
        assert "queue_depth_after=950" in msg
        assert "elapsed_s=10.0" in msg
        assert "rate_obj_s=5.0" in msg

    def test_zero_elapsed_does_not_divide_by_zero(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            maintenance._log_cleanup_throughput("cleanup", 0, 0, 0.0)
        msg = caplog.records[0].getMessage()
        assert "rate_obj_s=0.0" in msg

    def test_full_drain(self, caplog):
        with caplog.at_level(logging.INFO, logger="maintenance"):
            maintenance._log_cleanup_throughput("cleanup-all", 47023, 0, 9405.0)
        msg = caplog.records[0].getMessage()
        assert "files_processed=47023" in msg
        assert "rate_obj_s=5.0" in msg


class TestArgparse:
    """The new subcommands must reach the dispatch with the expected fields."""

    def setup_method(self):
        self.parser = maintenance.build_parser()

    def test_dedup_deletions_dry_run(self):
        args = self.parser.parse_args(["dedup-deletions", "--dry-run"])
        assert args.command == "dedup-deletions"
        assert args.dry_run is True

    def test_dedup_deletions_real(self):
        args = self.parser.parse_args(["dedup-deletions"])
        assert args.dry_run is False

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

    def test_compact_threads_memory_defaults(self):
        args = self.parser.parse_args(["compact", "--tier", "1"])
        assert args.threads == 2
        assert args.memory_limit == "4GB"

    def test_compact_threads_memory_override(self):
        args = self.parser.parse_args(["compact", "--tier", "2", "--threads", "8", "--memory-limit", "16GB"])
        assert args.threads == 8
        assert args.memory_limit == "16GB"


# ---------------------------------------------------------------------------
# Tier B — orchestrators with mocked sub-calls
# ---------------------------------------------------------------------------


class TestCleanupAllSafe:
    """Verify the dedup → heal → cleanup-all retry loop."""

    def _patches(self):
        return (
            patch("maintenance._acquire_advisory_lock"),
            patch("maintenance.dedup_deletions"),
            patch("maintenance.heal_orphans"),
            patch("maintenance.cleanup_all"),
        )

    def test_succeeds_on_first_attempt(self):
        lock, dedup, heal, cleanup = (p.start() for p in self._patches())
        try:
            maintenance.cleanup_all_safe(MagicMock(), max_iterations=10)
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
            maintenance.cleanup_all_safe(MagicMock(), max_iterations=10)
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
                maintenance.cleanup_all_safe(MagicMock(), max_iterations=3)
        finally:
            patch.stopall()
        assert dedup.call_count == 3
        assert heal.call_count == 3
        assert cleanup.call_count == 3


class TestFsck:
    """Verify the dry-run vs real dispatch and ordering."""

    def test_dry_run_delegates_to_dry_run_subcalls(self):
        with (
            patch("maintenance.dedup_deletions") as dedup,
            patch("maintenance.heal_orphans") as heal,
            patch("maintenance.orphans") as s3_orphans,
            patch("maintenance.cleanup_all_safe") as orch,
        ):
            conn = MagicMock()
            maintenance.fsck(conn, dry_run=True, max_iterations=10)
        # Dry-run must run heal_orphans so its B1/B3 gates execute.
        dedup.assert_called_once_with(conn, dry_run=True)
        heal.assert_called_once_with(conn, dry_run=True)
        s3_orphans.assert_called_once_with(conn, dry_run=True)
        orch.assert_not_called()

    def test_real_run_calls_orchestrator_and_s3_sweep(self):
        with (
            patch("maintenance.dedup_deletions") as dedup,
            patch("maintenance.heal_orphans") as heal,
            patch("maintenance.orphans") as s3_orphans,
            patch("maintenance.cleanup_all_safe") as orch,
        ):
            conn = MagicMock()
            maintenance.fsck(conn, dry_run=False, max_iterations=7)
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
            patch("maintenance.dedup_deletions"),
            patch("maintenance.heal_orphans") as heal,
            patch("maintenance.orphans"),
        ):
            heal.side_effect = RuntimeError("safety gate B1 failed: ...")
            with pytest.raises(RuntimeError, match="safety gate B1"):
                maintenance.fsck(MagicMock(), dry_run=True, max_iterations=10)


class TestSetCompactionTuning:
    """Pure SQL emission; no real connection needed."""

    def test_emits_expected_sets(self):
        conn = MagicMock()
        maintenance._set_compaction_tuning(conn, threads=4, memory_limit="8GB")
        executed = [c.args[0] for c in conn.execute.call_args_list]
        assert "SET threads = 4" in executed
        assert "SET memory_limit = '8GB'" in executed
        assert "SET preserve_insertion_order = false" in executed
        assert "SET http_timeout = 600000" in executed

    def test_rejects_injection_in_memory_limit(self):
        conn = MagicMock()
        with pytest.raises(ValueError, match="Illegal character"):
            maintenance._set_compaction_tuning(conn, threads=2, memory_limit="4GB'; DROP TABLE x; --")
        # Sanitization happens before any execute; conn must not have been touched.
        conn.execute.assert_not_called()


class TestAcquireAdvisoryLock:
    """The lock-helper SQL must use single quotes around the inner literal."""

    def test_emits_single_postgres_query_call_with_doubled_quotes(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (True,)
        maintenance._acquire_advisory_lock(conn)
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
            maintenance._acquire_advisory_lock(conn)
