"""drop-orphan-inline-tables against a real Postgres.

The op is DDL + DML on a live catalog database, so the properties that
matter (the right tables dropped, registry and pg_class kept consistent, the
non-empty guard, keyset batching) only show up against a real server. A
scripted fake would only echo the SQL back.

Postgres source, first available wins:
  1. MILLPOND_TEST_PG_DSN — an existing server; the test creates and drops a
     throwaway database on it (needs CREATEDB).
  2. initdb / pg_ctl on PATH (or under /usr/lib/postgresql/*/bin, where the
     GitHub ubuntu runner keeps them) — a throwaway cluster in tmp.
  3. docker — a throwaway postgres:17 container on a random 127.0.0.1 port.
Otherwise the module skips (fails when MILLPOND_REQUIRE_DOCKER_STACK is set,
same contract as the hoglake suites).
"""

from __future__ import annotations

import contextlib
import glob
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid

import ducklake_maintenance as dm
import psycopg
import pytest
from prometheus_client import CollectorRegistry, Gauge

from tests.hoglake_stack import stack

pytestmark = pytest.mark.integration

_USER = "ducklake"
_PASSWORD = "ducklake"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_ready(conninfo: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            psycopg.connect(conninfo, connect_timeout=2).close()
            return
        except psycopg.OperationalError:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.5)


def _pg_bin(name: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    candidates = sorted(glob.glob(f"/usr/lib/postgresql/*/bin/{name}"))
    return candidates[-1] if candidates else None


@contextlib.contextmanager
def _local_cluster():
    initdb, pg_ctl = _pg_bin("initdb"), _pg_bin("pg_ctl")
    datadir = tempfile.mkdtemp(prefix="millpond-pg-")
    port = _free_port()
    try:
        subprocess.run(
            [initdb, "-D", datadir, "-U", _USER, "--auth=trust"], check=True, capture_output=True, timeout=120
        )
        # TCP only: tmp paths on macOS exceed the unix-socket path limit.
        opts = f"-p {port} -c listen_addresses=127.0.0.1 -c unix_socket_directories=''"
        subprocess.run(
            [pg_ctl, "-D", datadir, "-o", opts, "-w", "-l", os.path.join(datadir, "log"), "start"],
            check=True,
            capture_output=True,
            timeout=120,
        )
        yield {"host": "127.0.0.1", "port": port, "user": _USER, "password": _PASSWORD, "dbname": "postgres"}
    finally:
        subprocess.run([pg_ctl, "-D", datadir, "-m", "immediate", "stop"], capture_output=True, timeout=60)
        shutil.rmtree(datadir, ignore_errors=True)


@contextlib.contextmanager
def _docker_cluster():
    port = _free_port()
    name = f"millpond-pg-it-{uuid.uuid4().hex[:8]}"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            name,
            "-e",
            f"POSTGRES_USER={_USER}",
            "-e",
            f"POSTGRES_PASSWORD={_PASSWORD}",
            "-p",
            f"127.0.0.1:{port}:5432",
            "--tmpfs",
            "/var/lib/postgresql/data",
            "postgres:17",
        ],
        check=True,
        capture_output=True,
        timeout=600,
    )
    try:
        yield {"host": "127.0.0.1", "port": port, "user": _USER, "password": _PASSWORD, "dbname": "postgres"}
    finally:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, timeout=60)


def _conninfo(p: dict) -> str:
    return f"host={p['host']} port={p['port']} user={p['user']} password={p['password']} dbname={p['dbname']}"


@pytest.fixture(scope="module")
def pg_server():
    dsn = os.environ.get("MILLPOND_TEST_PG_DSN")
    if dsn:
        info = psycopg.conninfo.conninfo_to_dict(dsn)
        yield {
            "host": info.get("host", "127.0.0.1"),
            "port": int(info.get("port", 5432)),
            "user": info.get("user", _USER),
            "password": info.get("password", ""),
            "dbname": info.get("dbname", "postgres"),
        }
        return
    if _pg_bin("initdb") and _pg_bin("pg_ctl"):
        cm = _local_cluster()
    elif stack.docker_available():
        cm = _docker_cluster()
    else:
        stack.require_or_skip("no Postgres available (no MILLPOND_TEST_PG_DSN, no initdb/pg_ctl, no docker)")
    with cm as server:
        _wait_ready(_conninfo(server))
        yield server


@pytest.fixture()
def catalog_db(pg_server, monkeypatch):
    """A fresh database per test, with the DUCKLAKE_RDS_* env pointing at it
    so the op connects through the real _pg_direct_connect()."""
    dbname = f"it_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(_conninfo(pg_server), autocommit=True) as admin:
        admin.execute(f'CREATE DATABASE "{dbname}"')
    server = {**pg_server, "dbname": dbname}
    monkeypatch.setenv("DUCKLAKE_RDS_HOST", server["host"])
    monkeypatch.setenv("DUCKLAKE_RDS_PORT", str(server["port"]))
    monkeypatch.setenv("DUCKLAKE_RDS_DATABASE", dbname)
    monkeypatch.setenv("DUCKLAKE_RDS_USERNAME", server["user"])
    monkeypatch.setenv("DUCKLAKE_RDS_PASSWORD", server["password"] or "unused")
    try:
        yield server
    finally:
        with psycopg.connect(_conninfo(pg_server), autocommit=True) as admin:
            admin.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')


@pytest.fixture()
def pg(catalog_db):
    with psycopg.connect(_conninfo(catalog_db), autocommit=True) as conn:
        yield conn


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
    }


def _metric_count(pg) -> int:
    """Run the ducklake_unreachable_inline_tables metric's own SQL, retargeted
    from the DuckDB alias to the Postgres schema."""
    import ducklake_metrics

    q = next(q for q in ducklake_metrics.load_queries(None, set()) if q.name == "ducklake_unreachable_inline_tables")
    return pg.execute(q.sql.replace("__ducklake_metadata_lake.", "public.")).fetchone()[0]


def _run(dry_run=False, batch_size=500, max_batches=None):
    reg, gauges = _gauges()
    dm.drop_orphan_inline_tables(dry_run, batch_size, max_batches, gauges)
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
        assert out == {"dropped": 2, "skipped_nonempty": 0, "remaining": 0}

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
        assert out == {"dropped": 1, "skipped_nonempty": 1, "remaining": 1}
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
        assert out == {"dropped": 5, "skipped_nonempty": 1, "remaining": 2}
        assert "batch 2:" in caplog.text and "batch 3:" not in caplog.text

        caplog.clear()
        out = _run(batch_size=3)
        assert out == {"dropped": 1, "skipped_nonempty": 1, "remaining": 1}
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
