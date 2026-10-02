"""drop-orphan-inline-tables against a real Postgres.

The op is DDL + DML on a live catalog database, so the properties that
matter (the right tables dropped, registry and pg_class kept consistent, the
non-empty guard, keyset batching) only show up against a real server. A
scripted fake would only echo the SQL back.

The Postgres server and the per-test catalog database come from the shared
fixtures in conftest.py (sources and skip contract: postgres_server.py).
"""

from __future__ import annotations

import glob
import logging
import os
import shutil
import subprocess
import time

import ducklake_maintenance as dm
import psycopg
import pytest
from prometheus_client import CollectorRegistry, Gauge

from tests.integration.postgres_server import _conninfo, _has_local_server

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Synthetic catalog: only the columns the op and the metric read.
# ---------------------------------------------------------------------------


def _make_catalog(pg, snapshots: range) -> None:
    pg.execute(
        "CREATE TABLE ducklake_snapshot (snapshot_id BIGINT PRIMARY KEY);"
        "CREATE TABLE ducklake_table (table_id BIGINT, table_name VARCHAR, "
        "  begin_snapshot BIGINT, end_snapshot BIGINT);"
        "CREATE TABLE ducklake_inlined_data_tables (table_id BIGINT, table_name VARCHAR, schema_version BIGINT)"
    )
    pg.execute(
        "INSERT INTO ducklake_snapshot SELECT generate_series(%s::bigint, %s::bigint)",
        (snapshots.start, snapshots.stop - 1),
    )


def _add_table(pg, table_id: int, begin: int, end: int | None, schema_versions=(1,), rows: int = 0) -> list[str]:
    pg.execute(
        "INSERT INTO ducklake_table VALUES (%s, %s, %s, %s)",
        (table_id, f"t{table_id}", begin, end),
    )
    names = []
    for sv in schema_versions:
        names.append(_add_inline(pg, table_id, sv, rows=rows))
    return names


def _add_inline(pg, table_id: int, schema_version: int, rows: int = 0, name: str | None = None) -> str:
    name = name or f"ducklake_inlined_data_{table_id}_{schema_version}"
    pg.execute(
        f'CREATE TABLE "{name}" (row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, a INTEGER, b VARCHAR)'
    )
    if rows:
        pg.execute(f"INSERT INTO \"{name}\" SELECT g, 1, NULL, g, 'x' FROM generate_series(1, %s::int) g", (rows,))
    pg.execute("INSERT INTO ducklake_inlined_data_tables VALUES (%s, %s, %s)", (table_id, name, schema_version))
    return name


def _relation_exists(pg, name: str) -> bool:
    return pg.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{name}",)).fetchone()[0]


def _registry(pg) -> set[str]:
    return {r[0] for r in pg.execute("SELECT table_name FROM ducklake_inlined_data_tables").fetchall()}


def _gauges():
    reg = CollectorRegistry()
    return reg, {
        "dropped": Gauge("dropped", "", registry=reg),
        "skipped_nonempty": Gauge("skipped_nonempty", "", registry=reg),
        "remaining": Gauge("remaining", "", registry=reg),
        "budget_exhausted": Gauge("budget_exhausted", "", registry=reg),
    }


def _metric_count(pg) -> int:
    """Run the ducklake_unreachable_inline_tables metric's own SQL, retargeted
    from the DuckDB alias to the Postgres schema."""
    import ducklake_metrics

    q = next(q for q in ducklake_metrics.load_queries(None, set()) if q.name == "ducklake_unreachable_inline_tables")
    return pg.execute(q.sql.replace("__ducklake_metadata_lake.", "public.")).fetchone()[0]


def _expected_cap(gucs) -> int:
    """The cap, restated from the finding rather than recomputed with the
    production helper: a quarter of the lock table's documented floor at the
    5 measured lock-table entries per DROP, clamped to [10, 2000]."""
    max_locks, max_conns, max_prepared = gucs
    return max(10, min(2000, int(max_locks * (max_conns + max_prepared) * 0.25 / 5)))


def _run(dry_run=False, batch_size=500, max_batches=None, run_budget_s=None):
    reg, gauges = _gauges()
    dm.drop_orphan_inline_tables(dry_run, batch_size, max_batches, gauges, run_budget_s=run_budget_s)
    return {k: g._value.get() for k, g in gauges.items()}


# Retained snapshots are 100..200 in every synthetic case below.
RETAINED = range(100, 201)


class TestDropOrphanInlineTables:
    def test_orphan_dropped_and_registry_row_deleted(self, pg):
        _make_catalog(pg, RETAINED)
        orphan = _add_table(pg, 1, begin=10, end=50)  # ended before the retained range
        missing_rel = _add_table(pg, 2, begin=10, end=60)
        pg.execute(f'DROP TABLE "{missing_rel[0]}"')  # registry row whose table is already gone
        # table_id 3 has no ducklake_table row at all (reaped by expiry).
        unregistered_parent = _add_inline(pg, 3, 1)

        out = _run()

        for name in (*orphan, unregistered_parent):
            assert not _relation_exists(pg, name)
        assert _registry(pg) == set()
        assert out == {"dropped": 2, "skipped_nonempty": 0, "remaining": 0, "budget_exhausted": 0}

    def test_reachable_and_live_tables_kept(self, pg):
        _make_catalog(pg, RETAINED)
        live = _add_table(pg, 1, begin=10, end=None, schema_versions=(1, 2, 3))  # live, incl. superseded versions
        dropped_but_retained = _add_table(pg, 2, begin=10, end=150)  # dropped, still visible at snapshot 100..149
        ends_at_lo_plus_one = _add_table(pg, 3, begin=10, end=101)  # visible at snapshot 100 only
        ends_at_lo = _add_table(pg, 4, begin=10, end=100)  # end is exclusive: not visible at 100
        orphan = _add_table(pg, 5, begin=10, end=20)

        assert _metric_count(pg) == 2
        out = _run()

        kept = {*live, *dropped_but_retained, *ends_at_lo_plus_one}
        assert _registry(pg) == kept
        assert all(_relation_exists(pg, n) for n in kept)
        assert not any(_relation_exists(pg, n) for n in (*ends_at_lo, *orphan))
        assert out["dropped"] == 2 and out["remaining"] == 0

    def test_non_empty_orphan_skipped(self, pg, caplog):
        _make_catalog(pg, RETAINED)
        full = _add_table(pg, 1, begin=10, end=20, rows=3)
        empty = _add_table(pg, 2, begin=10, end=20)

        out = _run()

        assert _registry(pg) == set(full)
        assert _relation_exists(pg, full[0])
        assert pg.execute(f'SELECT COUNT(*) FROM "{full[0]}"').fetchone()[0] == 3
        assert not _relation_exists(pg, empty[0])
        assert out == {"dropped": 1, "skipped_nonempty": 1, "remaining": 1, "budget_exhausted": 0}
        assert "skipping non-empty orphan" in caplog.text

    def test_unexpected_table_name_never_dropped(self, pg):
        _make_catalog(pg, RETAINED)
        pg.execute("INSERT INTO ducklake_table VALUES (1, 't1', 10, 20)")
        odd = _add_inline(pg, 1, 1, name="not_an_inlined_table")

        _run()

        assert _relation_exists(pg, odd)
        assert _registry(pg) == {odd}

    def test_dry_run_makes_no_changes(self, pg, caplog):
        _make_catalog(pg, RETAINED)
        orphans = [n for i in range(1, 6) for n in _add_table(pg, i, begin=10, end=20, rows=1 if i == 5 else 0)]
        live = _add_table(pg, 9, begin=10, end=None)
        before_registry = _registry(pg)

        caplog.set_level("INFO")
        _run(dry_run=True, batch_size=2)

        assert _registry(pg) == before_registry
        assert all(_relation_exists(pg, n) for n in (*orphans, *live))
        assert "registry_rows=6 orphaned=5 orphaned_present_in_pg_class=5" in caplog.text
        assert "would_drop=4" in caplog.text and "skipped_nonempty=1" in caplog.text

    def test_batches_and_max_batches(self, pg, caplog):
        _make_catalog(pg, RETAINED)
        # 7 orphans, the 2nd non-empty: keyset paging must step past it
        # instead of reselecting it in every batch.
        for i in range(1, 8):
            _add_table(pg, i, begin=10, end=20, rows=1 if i == 2 else 0)
        _add_table(pg, 50, begin=10, end=None)

        caplog.set_level("INFO")
        out = _run(batch_size=3, max_batches=2)
        assert out == {"dropped": 5, "skipped_nonempty": 1, "remaining": 2, "budget_exhausted": 0}
        assert "batch 2:" in caplog.text and "batch 3:" not in caplog.text

        caplog.clear()
        out = _run(batch_size=3)
        assert out == {"dropped": 1, "skipped_nonempty": 1, "remaining": 1, "budget_exhausted": 0}
        assert _registry(pg) == {"ducklake_inlined_data_2_1", "ducklake_inlined_data_50_1"}

    def test_metric_and_predicate_count_the_same_rows(self, pg):
        _make_catalog(pg, RETAINED)
        for i, (begin, end) in enumerate([(10, 20), (10, 100), (10, 101), (10, None), (150, None), (190, 195)]):
            _add_table(pg, i + 1, begin=begin, end=end)
        _add_inline(pg, 77, 1)  # no ducklake_table row

        count = pg.execute(
            f"SELECT COUNT(*) FROM ducklake_inlined_data_tables idt WHERE {dm._inline_orphan_predicate('idt')}"
        ).fetchone()[0]
        assert count == _metric_count(pg) == 3

    def test_refuses_with_empty_snapshot_table(self, pg):
        _make_catalog(pg, range(0))
        _add_table(pg, 1, begin=10, end=None)
        with pytest.raises(RuntimeError, match="ducklake_snapshot is empty"):
            _run()
        assert _registry(pg) == {"ducklake_inlined_data_1_1"}

    def test_refuses_when_maintenance_lock_held(self, pg, catalog_db):
        _make_catalog(pg, RETAINED)
        orphan = _add_table(pg, 1, begin=10, end=20)
        with psycopg.connect(_conninfo(catalog_db), autocommit=True) as other:
            other.execute(f"SELECT pg_advisory_lock({dm.ADVISORY_LOCK_KEY_SQL})")
            with pytest.raises(dm._LockContended):
                _run()
        assert _relation_exists(pg, orphan[0])

    def test_main_runs_without_duckdb(self, pg, monkeypatch):
        """Direct-pg dispatch: the whole point is to work when the DuckLake
        ATTACH cannot complete, so main() must never call connect()."""
        _make_catalog(pg, RETAINED)
        orphan = _add_table(pg, 1, begin=10, end=20)

        def _no_attach(*a, **kw):
            raise AssertionError("connect() must not be called")

        monkeypatch.setattr(dm, "connect", _no_attach)
        monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
        dm.main(["drop-orphan-inline-tables", "--batch-size", "10"])
        assert not _relation_exists(pg, orphan[0])

    # --- bounds: the lock budget and the wall-clock budget -----------------

    def test_effective_batch_size_is_derived_from_the_servers_gucs(self, pg, caplog):
        """The cap comes from THIS server's GUCs. The formula is restated here
        from the finding (not recomputed with the production helper, which
        would make the test a tautology), and the EFFECTIVE size is read off
        the loop by counting batches, not just off the pre-loop log line."""
        _make_catalog(pg, RETAINED)
        for i in range(1, 8):
            _add_table(pg, i, begin=10, end=20)
        gucs = pg.execute(dm._INLINE_LOCK_GUC_SQL).fetchone()
        capacity = gucs[0] * (gucs[1] + gucs[2])
        cap = _expected_cap(gucs)
        # The stock/CNPG-default case is pinned numerically, whatever this
        # server happens to be configured as.
        assert _expected_cap((64, 100, 0)) == 320

        caplog.set_level("INFO")
        # Ask for 3 (well under any cap) and count the batches: 7 orphans in
        # pages of 3 is 3 batches, so the size the loop USED was 3.
        _run(batch_size=3)

        assert (
            f"max_locks_per_transaction={gucs[0]} max_connections={gucs[1]} max_prepared_transactions={gucs[2]} "
            f"capacity={capacity}"
        ) in caplog.text
        assert f"batch_cap={cap} batch_size=3 requested=3" in caplog.text
        assert "batch 3:" in caplog.text and "batch 4:" not in caplog.text

    def test_a_request_above_the_cap_is_lowered_and_one_at_the_cap_is_not(self, pg, caplog):
        _make_catalog(pg, RETAINED)
        _add_table(pg, 1, begin=10, end=20)
        cap = _expected_cap(pg.execute(dm._INLINE_LOCK_GUC_SQL).fetchone())

        caplog.set_level("INFO")
        _run(batch_size=cap + 1)
        assert f"--batch-size {cap + 1} lowered to {cap}" in caplog.text
        assert f"batch_cap={cap} batch_size={cap} requested={cap + 1}" in caplog.text
        # P3: this is INFO on the servers where it fires every single run.
        assert [r.levelno for r in caplog.records if "lowered to" in r.getMessage()] == [logging.INFO]

        caplog.clear()
        _add_table(pg, 2, begin=10, end=20)
        _run(batch_size=cap)
        assert "lowered to" not in caplog.text

    def test_run_budget_stops_between_batches_and_the_next_run_converges(self, pg, caplog, monkeypatch):
        _make_catalog(pg, RETAINED)
        for i in range(1, 8):
            _add_table(pg, i, begin=10, end=20)

        # Let the batch itself run unbounded (so this exercises the
        # BETWEEN-batches check) and burn the budget right after it commits.
        real_batch = dm._drop_inline_batch

        def slow(conn, after, batch_size, dry_run, deadline=None):
            res = real_batch(conn, after, batch_size, dry_run, None)
            time.sleep(0.05)
            return res

        monkeypatch.setattr(dm, "_drop_inline_batch", slow)
        caplog.set_level("INFO")
        out = _run(batch_size=2, run_budget_s=0.01)

        assert out == {"dropped": 2, "skipped_nonempty": 0, "remaining": 5, "budget_exhausted": 1}
        assert "batch 1:" in caplog.text and "batch 2:" not in caplog.text
        assert "truncated=False" in caplog.text  # the batch completed; the RUN stopped
        assert "budget_exhausted=true" in caplog.text and "remaining_orphans=5" in caplog.text
        assert len(_registry(pg)) == 5

        # Convergence: keyset paging restarts from the predicate, so a second
        # run (here unbudgeted, and without the slow wrapper) finishes the
        # backlog with no saved cursor. Restore just the batch function —
        # monkeypatch.undo() would also drop the fixture's DUCKLAKE_RDS_* env.
        monkeypatch.setattr(dm, "_drop_inline_batch", real_batch)
        caplog.clear()
        out = _run(batch_size=2)
        assert out == {"dropped": 5, "skipped_nonempty": 0, "remaining": 0, "budget_exhausted": 0}
        assert _registry(pg) == set()

    def test_an_exhausted_budget_truncates_the_batch_after_one_row(self, pg, caplog):
        """statement_timeout bounds a statement, not the ~3+3n statements of a
        batch, so the deadline is enforced inside the row loop: the row in
        flight finishes and commits WITH its registry row, and nothing else
        is taken. One row always goes through, so a run cannot no-op."""
        _make_catalog(pg, RETAINED)
        for i in range(1, 6):
            _add_table(pg, i, begin=10, end=20)

        caplog.set_level("INFO")
        out = _run(batch_size=5, run_budget_s=0.001)

        assert out == {"dropped": 1, "skipped_nonempty": 0, "remaining": 4, "budget_exhausted": 1}
        assert "selected=1 dropped=1" in caplog.text and "truncated=True" in caplog.text
        assert "batch 2:" not in caplog.text
        # Atomic: exactly the dropped table's registry row is gone.
        assert _registry(pg) == {f"ducklake_inlined_data_{i}_1" for i in range(2, 6)}
        assert not _relation_exists(pg, "ducklake_inlined_data_1_1")

    def test_out_of_shared_memory_inside_the_transaction_halves_and_completes(self, pg, monkeypatch, caplog):
        """The real psycopg error, raised from INSIDE the batch transaction, so
        the retry has to survive an aborted transaction as well as shrink."""
        _make_catalog(pg, RETAINED)
        orphans = [n for i in range(1, 13) for n in _add_table(pg, i, begin=10, end=20)]
        real_has_rows = dm._inline_table_has_rows
        raised = []

        def flaky(cur, table_name):
            if not raised:
                raised.append(table_name)
                raise psycopg.errors.OutOfMemory("out of shared memory")
            return real_has_rows(cur, table_name)

        monkeypatch.setattr(dm, "_inline_table_has_rows", flaky)
        monkeypatch.setattr(dm.time, "sleep", lambda *_: None)

        caplog.set_level("INFO")
        out = _run(batch_size=20)

        assert raised  # the error really came from inside the transaction
        assert "out of shared memory (53200): batch size 20 -> 10" in caplog.text
        assert "retries=1 batch_size_start=20 batch_size_final=10" in caplog.text
        assert out == {"dropped": 12, "skipped_nonempty": 0, "remaining": 0, "budget_exhausted": 0}
        assert not any(_relation_exists(pg, n) for n in orphans)
        assert _registry(pg) == set()

    def test_skipped_share_is_reported(self, pg, caplog):
        """Skipped rows are re-walked by every run and never converge, so the
        run says what share of its work they were."""
        _make_catalog(pg, RETAINED)
        _add_table(pg, 1, begin=10, end=20, rows=1)  # non-empty: skipped forever
        _add_table(pg, 2, begin=10, end=20)

        caplog.set_level("INFO")
        _run()
        assert "skipped_nonempty=1 skipped_invalid_name=0 skipped_share=50.0%" in caplog.text

    def test_main_passes_the_run_budget_and_registers_the_gauge(self, pg, monkeypatch, caplog):
        """CLI -> dispatch wiring: without `run_budget_s=args.run_budget_s` in
        main() every other test here still passes."""
        _make_catalog(pg, RETAINED)
        for i in range(1, 6):
            _add_table(pg, i, begin=10, end=20)
        gauge_names = []
        real_gauge = dm.Gauge

        def recording_gauge(*a, **kw):
            gauge_names.append(a[0])
            return real_gauge(*a, **kw)

        monkeypatch.setattr(dm, "Gauge", recording_gauge)
        monkeypatch.setattr(dm, "connect", lambda *a, **kw: pytest.fail("connect() must not be called"))
        monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)

        caplog.set_level("INFO")
        dm.main(["drop-orphan-inline-tables", "--batch-size", "2", "--run-budget-s", "0.001"])

        assert "budget_exhausted=true" in caplog.text
        assert "maintenance_inline_orphans_budget_exhausted" in gauge_names
        assert _registry(pg)  # rows left for the next tick


class TestJustfileDelivery:
    """The chain wrapper is what the cron actually runs; an unwired flag there
    is invisible to every python-level test."""

    def test_default_wrapper_renders_the_run_budget(self):
        just = shutil.which("just")
        if not just:
            pytest.skip("just not installed")
        justfile = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "tools", "justfile"
        )
        out = subprocess.run(
            [just, "--justfile", justfile, "--dry-run", "drop-orphan-inline-tables-default"],
            capture_output=True,
            text=True,
            timeout=120,
        )
        rendered = out.stdout + out.stderr
        assert out.returncode == 0, rendered
        assert "drop-orphan-inline-tables --batch-size '500'" in rendered
        assert "--run-budget-s '300'" in rendered
        # The budget is an environment knob, so a tenant can be raised from
        # the CronJob spec without touching the chain args.
        raised = subprocess.run(
            [just, "--justfile", justfile, "--dry-run", "drop-orphan-inline-tables-default"],
            capture_output=True,
            text=True,
            env={**os.environ, "MILLPOND_INLINE_RUN_BUDGET_S": "900"},
        )
        assert raised.returncode == 0, raised.stdout + raised.stderr
        assert "--run-budget-s '900'" in raised.stdout + raised.stderr
        assert "--max-batches" not in rendered

    def test_empty_run_budget_renders_an_unbudgeted_command(self):
        """An operator draining a backlog by hand must be able to pass ""."""
        just = shutil.which("just")
        if not just:
            pytest.skip("just not installed")
        justfile = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "tools", "justfile"
        )
        out = subprocess.run(
            [just, "--justfile", justfile, "--dry-run", "drop-orphan-inline-tables", "500", "", ""],
            capture_output=True,
            text=True,
            timeout=120,
        )
        rendered = out.stdout + out.stderr
        assert out.returncode == 0, rendered
        assert "--run-budget-s" not in rendered
        assert "--max-batches" not in rendered


class TestLocalServerDetection:
    """`initdb` on PATH does not imply a local server: brew's libpq ships the
    client programs without `postgres`, and initdb fails at run time instead
    of letting this module fall through to docker."""

    def _patch(self, monkeypatch, *, which, executable):
        monkeypatch.setattr(shutil, "which", lambda name: which.get(name))
        monkeypatch.setattr(glob, "glob", lambda pattern: [])
        monkeypatch.setattr(os, "access", lambda path, mode: path in executable)

    def test_no_client_programs(self, monkeypatch):
        self._patch(monkeypatch, which={}, executable=set())
        assert _has_local_server() is False

    def test_initdb_without_pg_ctl(self, monkeypatch):
        self._patch(monkeypatch, which={"initdb": "/bin/initdb"}, executable={"/bin/postgres"})
        assert _has_local_server() is False

    def test_client_programs_without_a_server(self, monkeypatch):
        # The brew-libpq case that errored the whole module before this gate.
        self._patch(
            monkeypatch,
            which={"initdb": "/opt/homebrew/bin/initdb", "pg_ctl": "/opt/homebrew/bin/pg_ctl"},
            executable=set(),
        )
        assert _has_local_server() is False

    def test_server_beside_initdb(self, monkeypatch):
        self._patch(
            monkeypatch,
            which={"initdb": "/usr/lib/postgresql/17/bin/initdb", "pg_ctl": "/usr/lib/postgresql/17/bin/pg_ctl"},
            executable={"/usr/lib/postgresql/17/bin/postgres"},
        )
        assert _has_local_server() is True


class TestAgainstRealDuckLakeCatalog:
    def test_live_tables_still_readable_after_drop(self, catalog_db, pg, tmp_path):
        """Real DuckLake catalog: drop leaked inlined tables, then re-ATTACH
        and read the live table's inlined rows through DuckLake."""
        import duckdb

        conn = duckdb.connect()
        try:
            conn.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")
        except Exception:
            pytest.skip("ducklake/postgres extensions unavailable (offline?)")
        attach = (
            f"ATTACH 'ducklake:postgres:host={catalog_db['host']} port={catalog_db['port']} "
            f"dbname={catalog_db['dbname']} user={catalog_db['user']} password={catalog_db['password']}' "
            f"AS lake (DATA_PATH '{tmp_path}/data')"
        )
        conn.execute(attach)
        conn.execute("CREATE TABLE lake.keep (a INTEGER)")
        conn.execute("INSERT INTO lake.keep VALUES (1), (2)")
        for _ in range(3):
            conn.execute("CREATE TABLE lake.churn (a INTEGER, b VARCHAR)")
            conn.execute("DROP TABLE lake.churn")
        conn.close()

        # Simulate the leak: the snapshots that could see the churn tables
        # are gone, but their inlined tables and registry rows are not.
        keep_id = pg.execute("SELECT table_id FROM ducklake_table WHERE table_name = 'keep'").fetchone()[0]
        churn_end = pg.execute("SELECT MAX(end_snapshot) FROM ducklake_table WHERE table_name = 'churn'").fetchone()[0]
        pg.execute("DELETE FROM ducklake_snapshot WHERE snapshot_id < %s", (churn_end,))
        leaked = pg.execute(
            "SELECT table_name FROM ducklake_inlined_data_tables WHERE table_id <> %s", (keep_id,)
        ).fetchall()
        assert len(leaked) == 3

        out = _run()

        assert out["dropped"] == 3 and out["remaining"] == 0
        assert not any(_relation_exists(pg, name) for (name,) in leaked)
        conn = duckdb.connect()
        conn.execute("LOAD ducklake; LOAD postgres;")
        conn.execute(attach)
        assert conn.execute("SELECT a FROM lake.keep ORDER BY a").fetchall() == [(1,), (2,)]
        conn.execute("INSERT INTO lake.keep VALUES (3)")
        assert conn.execute("SELECT COUNT(*) FROM lake.keep").fetchone()[0] == 3
        conn.close()
