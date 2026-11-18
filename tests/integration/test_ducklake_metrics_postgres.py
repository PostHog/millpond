"""ducklake_metrics postgres catalog mode against a real DuckLake catalog.

The catalog is built by DuckDB's ducklake extension on a throwaway Postgres
(shared fixtures in conftest.py, sources in postgres_server.py), so the
tables, column types and rows are the ones a production catalog has. The
tests then:

  * run --once in postgres mode end to end (connect, every built-in, push to
    a local HTTP sink) with DuckDB made unusable, and check every built-in
    succeeds;
  * check that each built-in's Postgres form returns the same samples as its
    DuckDB form over the DuckLake ATTACH of the same catalog;
  * check the transaction contract: read-only, statement_timeout, no
    transaction left open after a query.
"""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb
import ducklake_maintenance
import ducklake_metrics as dm
import psycopg
import pytest
from prometheus_client import CollectorRegistry
from prometheus_client.parser import text_string_to_metric_families

from tests.integration.postgres_server import _conninfo

pytestmark = pytest.mark.integration

TENANT = "it"

# Seconds-ago values move between the two runs; everything else must match exactly.
_TIME_DEPENDENT = {"ducklake_snapshots_oldest_seconds_ago", "ducklake_snapshots_newest_seconds_ago"}


class _Sink:
    """Local stand-in for the vmagent Prometheus import endpoint."""

    def __init__(self):
        sink = self
        self.bodies: list[str] = []

        class H(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - stdlib signature
                sink.bodies.append(self.rfile.read(int(self.headers.get("Content-Length", "0"))).decode())
                self.send_response(204)
                self.end_headers()

            def log_message(self, *args):  # noqa: A002
                pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.srv.server_address[1]}/api/v1/import/prometheus"

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


def _attach_sql(db: dict, data_path: str) -> str:
    return (
        f"ATTACH 'ducklake:postgres:host={db['host']} port={db['port']} dbname={db['dbname']} "
        f"user={db['user']} password={db['password']}' AS lake (DATA_PATH '{data_path}')"
    )


def _duckdb_with_lake(db: dict, data_path: str, with_pg_attach: bool = False) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;")
    except Exception:
        pytest.skip("ducklake/postgres extensions unavailable (offline?)")
    conn.execute(_attach_sql(db, data_path))
    if with_pg_attach:
        conn.execute(
            f"ATTACH 'host={db['host']} port={db['port']} dbname={db['dbname']} user={db['user']} "
            f"password={db['password']}' AS {ducklake_maintenance.PG_ATTACH_NAME} (TYPE postgres)"
        )
    return conn


@pytest.fixture()
def real_catalog(catalog_db, pg, tmp_path):
    """A DuckLake catalog with every shape the built-ins report on: data files
    in several size bands and partitions, a live delete file, inlined data,
    live and dropped tables, config rows, and leaked inlined-data tables."""
    data_path = f"{tmp_path}/data"
    conn = _duckdb_with_lake(catalog_db, data_path)
    conn.execute("CALL lake.set_option('data_inlining_row_limit', 10)")
    conn.execute("CREATE TABLE lake.events (day VARCHAR, n BIGINT, payload VARCHAR)")
    conn.execute("ALTER TABLE lake.events SET PARTITIONED BY (day)")
    # Several data files per partition (tier1 sizes), so mergeable groups exist.
    for day in ("2026-05-01", "2026-05-01", "2026-05-02", "2026-05-02", "2026-05-03"):
        conn.execute(f"INSERT INTO lake.events SELECT '{day}', range, 'x' FROM range(1000)")
    # One larger file (random payload does not compress) in its own partition.
    conn.execute("INSERT INTO lake.events SELECT '2026-05-04', range, md5(range::VARCHAR) FROM range(60000)")
    # Delete files on the 2026-05-02 files (more rows than the inlining limit, so not inlined).
    conn.execute("DELETE FROM lake.events WHERE day = '2026-05-02' AND n < 500")
    # Inlined data: fewer rows than the inlining limit.
    conn.execute("CREATE TABLE lake.small (a INTEGER)")
    conn.execute("INSERT INTO lake.small VALUES (1), (2)")
    # Dropped tables (churn) with inlined data.
    for _ in range(3):
        conn.execute("CREATE TABLE lake.churn (a INTEGER, b VARCHAR)")
        conn.execute("INSERT INTO lake.churn VALUES (1, 'x')")
        conn.execute("DROP TABLE lake.churn")
    conn.close()
    # Leak the churn tables' inlined data: drop the snapshots that could see
    # them (the same simulation as the drop-orphan-inline-tables tests).
    churn_end = pg.execute("SELECT MAX(end_snapshot) FROM ducklake_table WHERE table_name = 'churn'").fetchone()[0]
    pg.execute("DELETE FROM ducklake_snapshot WHERE snapshot_id < %s", (churn_end,))
    # More commits after the leak, so the snapshot bounds and ages differ,
    # and a table-scoped option, so ducklake_config has a non-empty scope_id.
    conn = _duckdb_with_lake(catalog_db, data_path)
    for i in range(3):
        conn.execute(f"INSERT INTO lake.small VALUES ({i})")
    conn.execute("CALL lake.set_option('data_inlining_row_limit', 5, table_name => 'small')")
    conn.close()
    # Pending deletions with one duplicated path (the dup_rows pathology).
    pg.execute(
        "INSERT INTO ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, schedule_start) "
        "VALUES (9001, 'a.parquet', true, now()), (9002, 'a.parquet', true, now()), (9003, 'b.parquet', true, now())"
    )
    # Fresh planner statistics for the catalog-size query. Best effort: a
    # non-superuser MILLPOND_TEST_PG_DSN role may not ANALYZE system catalogs.
    try:
        pg.execute("ANALYZE pg_catalog.pg_class, pg_catalog.pg_attribute")
    except psycopg.Error:
        pass
    return {"db": catalog_db, "data_path": data_path}


def _samples_from_registry(registry: CollectorRegistry) -> dict[tuple, float]:
    return {(s.name, tuple(sorted(s.labels.items()))): s.value for fam in registry.collect() for s in fam.samples}


def _samples_from_text(body: str) -> dict[tuple, float]:
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for fam in text_string_to_metric_families(body)
        for s in fam.samples
    }


def _query_samples(samples: dict[tuple, float], queries: list[dm.Query]) -> dict[tuple, float]:
    """Only the query gauges (no self-metrics), keyed by (metric, labels)."""
    names = {f"{q.name}_{v}" for q in queries for v in q.values}
    return {k: v for k, v in samples.items() if k[0] in names}


def _forbid_duckdb(monkeypatch):
    def _no_duckdb(*a, **kw):
        raise AssertionError("postgres catalog mode must not open DuckDB")

    monkeypatch.setattr(ducklake_maintenance, "connect", _no_duckdb)
    monkeypatch.setattr(dm, "_connect_once", _no_duckdb)
    monkeypatch.setattr(dm.duckdb, "connect", _no_duckdb)


class TestPostgresCatalogMode:
    def test_once_runs_every_builtin_without_duckdb(self, real_catalog, monkeypatch):
        queries = dm.load_queries(None, set())
        _forbid_duckdb(monkeypatch)
        sink = _Sink()
        try:
            rc = dm._run_once(queries, TENANT, sink.url, "1GB", dm.CATALOG_MODE_POSTGRES, dm.PgOptions())
        finally:
            sink.close()

        assert rc == 0
        samples = _samples_from_text(sink.bodies[0])
        succeeded = {
            dict(labels)["query"]
            for (name, labels) in samples
            if name == "ducklake_metrics_query_last_success_timestamp"
        }
        errors = {
            dict(labels)["query"]: v
            for (name, labels), v in samples.items()
            if name.startswith("ducklake_metrics_query_errors")
        }
        assert succeeded == {q.name for q in queries}
        assert not any(errors.values()), errors
        # The new catalog-size query reports real numbers.
        rows = {name: v for (name, _), v in samples.items() if name.startswith("ducklake_pg_catalog_size_")}
        assert rows["ducklake_pg_catalog_size_pg_attribute_bytes"] > 0
        assert rows["ducklake_pg_catalog_size_pg_attribute_rows"] > 0
        assert rows["ducklake_pg_catalog_size_pg_class_rows"] > 0
        # Sanity: the catalog really has the shapes the comparison test relies on.
        assert samples[("ducklake_unreachable_inline_tables_total", (("tenant", TENANT),))] == 3
        assert samples[("ducklake_delete_files_files", (("tenant", TENANT),))] >= 1

    def test_main_auto_mode_selects_postgres_and_never_opens_duckdb(self, real_catalog, monkeypatch, caplog):
        """Mirror of test_main_runs_without_duckdb for drop-orphan-inline-tables:
        with DUCKLAKE_RDS_HOST set and only built-ins, auto must pick postgres."""
        _forbid_duckdb(monkeypatch)
        monkeypatch.delenv("DUCKLAKE_METRICS_CATALOG_MODE", raising=False)
        monkeypatch.delenv("DUCKLAKE_METRICS_CONFIG", raising=False)
        monkeypatch.setenv("DUCKLAKE_METRICS_MEMORY_LIMIT", "1GB")
        sink = _Sink()
        caplog.set_level("INFO")
        try:
            with pytest.raises(SystemExit) as exc:
                dm.main(["--once", "--tenant", TENANT, "--push-url", sink.url])
        finally:
            sink.close()
        assert exc.value.code == 0
        assert "Catalog mode: postgres (requested auto)" in caplog.text
        assert "is ignored in postgres catalog mode" in caplog.text
        assert len(sink.bodies) == 1

    def test_postgres_form_matches_duckdb_form(self, real_catalog, monkeypatch):
        """Every built-in with both forms returns the same samples: the Postgres
        form directly over psycopg, the DuckDB form over the DuckLake ATTACH."""
        queries = dm.load_queries(None, set())
        both = [q for q in queries if q.sql is not None]
        db, data_path = real_catalog["db"], real_catalog["data_path"]

        # DuckDB form only: drop server_sql so _fetch_duckdb runs `sql`.
        duck_queries = [dm.Query(**{**q.__dict__, "server_sql": None}) for q in both]
        duck_reg = CollectorRegistry()
        duck_gauges = dm._build_query_gauges(duck_queries, duck_reg)
        duck_sm = dm._build_self_metrics(duck_reg)
        conn = _duckdb_with_lake(db, data_path)
        try:
            for q in duck_queries:
                assert dm._run_query(conn, q, duck_gauges[q.name], duck_sm, TENANT), q.name
        finally:
            conn.close()

        # duckdb catalog mode as shipped: server_sql through postgres_query()
        # on the `pg` attach.
        ship_reg = CollectorRegistry()
        ship_gauges = dm._build_query_gauges(both, ship_reg)
        ship_sm = dm._build_self_metrics(ship_reg)
        conn = _duckdb_with_lake(db, data_path, with_pg_attach=True)
        try:
            for q in both:
                assert dm._run_query(conn, q, ship_gauges[q.name], ship_sm, TENANT), q.name
        finally:
            conn.close()

        # Postgres catalog mode.
        pg_reg = CollectorRegistry()
        pg_gauges = dm._build_query_gauges(both, pg_reg)
        pg_sm = dm._build_self_metrics(pg_reg)
        catalog = dm._connect_postgres(dm.PgOptions())
        try:
            for q in both:
                assert dm._run_query(catalog, q, pg_gauges[q.name], pg_sm, TENANT), q.name
        finally:
            catalog.close()

        duck = _query_samples(_samples_from_registry(duck_reg), both)
        shipped = _query_samples(_samples_from_registry(ship_reg), both)
        direct = _query_samples(_samples_from_registry(pg_reg), both)
        # Every built-in produced at least one sample, so the comparison covers all of them.
        for q in both:
            assert any(k[0].startswith(q.name + "_") for k in direct), f"{q.name}: no samples"
        for other_name, other in (("shipped", shipped), ("duckdb", duck)):
            assert set(direct) == set(other), (other_name, set(direct) ^ set(other))
            for key, value in direct.items():
                if key[0] in _TIME_DEPENDENT:
                    assert abs(value - other[key]) < 60, (other_name, key, value, other[key])
                else:
                    assert value == other[key], (other_name, key, value, other[key])


class TestPostgresTransactionContract:
    def _run(self, catalog, q):
        reg = CollectorRegistry()
        gauges = dm._build_query_gauges([q], reg)
        sm = dm._build_self_metrics(reg)
        ok = dm._run_query(catalog, q, gauges[q.name], sm, TENANT)
        return ok, reg

    def test_success_leaves_no_open_transaction(self, catalog_db):
        catalog = dm._connect_postgres(dm.PgOptions())
        try:
            q = dm.Query(name="t_ok", help="t", sql=None, server_sql="SELECT 1 AS n", interval_seconds=60, values=["n"])
            ok, reg = self._run(catalog, q)
            assert ok
            assert reg.get_sample_value("t_ok_n", {"tenant": TENANT}) == 1
            assert catalog.conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
            app = catalog.conn.execute("SHOW application_name").fetchone()[0]
            assert app == dm.PG_APPLICATION_NAME
        finally:
            catalog.close()

    def test_transaction_is_read_only(self, catalog_db, pg):
        pg.execute("CREATE TABLE t (a INTEGER)")
        catalog = dm._connect_postgres(dm.PgOptions())
        try:
            q = dm.Query(
                name="t_write",
                help="t",
                sql=None,
                server_sql="INSERT INTO t VALUES (1) RETURNING a AS n",
                interval_seconds=60,
                values=["n"],
            )
            ok, reg = self._run(catalog, q)
            assert not ok
            assert (
                reg.get_sample_value("ducklake_metrics_query_errors_total", {"tenant": TENANT, "query": "t_write"}) == 1
            )
            assert pg.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0
            assert catalog.conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
        finally:
            catalog.close()

    def test_statement_timeout_fails_the_query_and_the_next_one_runs(self, catalog_db):
        catalog = dm._connect_postgres(dm.PgOptions(statement_timeout_seconds=0.2))
        try:
            slow = dm.Query(
                name="t_slow",
                help="t",
                sql=None,
                server_sql="SELECT 1 AS n FROM pg_sleep(5)",
                interval_seconds=60,
                values=["n"],
            )
            ok, _ = self._run(catalog, slow)
            assert not ok
            fast = dm.Query(
                name="t_fast", help="t", sql=None, server_sql="SELECT 2 AS n", interval_seconds=60, values=["n"]
            )
            ok, reg = self._run(catalog, fast)
            assert ok and reg.get_sample_value("t_fast_n", {"tenant": TENANT}) == 2
            # The timeout was transaction-local: the session default is untouched.
            assert catalog.conn.execute("SHOW statement_timeout").fetchone()[0] != "200ms"
        finally:
            catalog.close()

    def test_lock_timeout_fails_fast_behind_an_exclusive_lock(self, catalog_db, pg):
        pg.execute("CREATE TABLE locked (a INTEGER)")
        catalog = dm._connect_postgres(dm.PgOptions(lock_timeout_seconds=0.2))
        holder = psycopg.connect(_conninfo(catalog_db))
        try:
            holder.execute("LOCK TABLE locked IN ACCESS EXCLUSIVE MODE")
            q = dm.Query(
                name="t_locked",
                help="t",
                sql=None,
                server_sql="SELECT COUNT(*) AS n FROM locked",
                interval_seconds=60,
                values=["n"],
            )
            ok, _ = self._run(catalog, q)
            assert not ok
        finally:
            holder.rollback()
            holder.close()
            catalog.close()


def _builtin(name: str) -> dm.Query:
    return next(q for q in dm.load_queries(None, set()) if q.name == name)


class TestPostgresRegexForms:
    """The two built-ins whose Postgres form changes functions (regexp_extract
    -> substring, regexp_matches -> ~), against the same value cases as the
    DuckDB-form unit tests."""

    def _run(self, pg, q):
        reg = CollectorRegistry()
        gauges = dm._build_query_gauges([q], reg)
        sm = dm._build_self_metrics(reg)
        catalog = dm._connect_postgres(dm.PgOptions())
        try:
            ok = dm._run_query(catalog, q, gauges[q.name], sm, TENANT)
        finally:
            catalog.close()
        return ok, reg

    @pytest.mark.parametrize(
        "version,expected_value,expected_suffix",
        [
            ("0.3", 0.3, ""),
            ("1.0", 1.0, ""),
            ("1.1-dev1", 1.1, "-dev1"),
            ("2.0-rc7", 2.0, "-rc7"),
            ("12", 12.0, ""),
        ],
    )
    def test_catalog_version(self, catalog_db, pg, version, expected_value, expected_suffix):
        pg.execute("CREATE TABLE ducklake_metadata (key text, value text, scope text, scope_id bigint)")
        pg.execute(
            "INSERT INTO ducklake_metadata VALUES ('version', %s, NULL, NULL), ('version', '9.9', 'table', 1)",
            (version,),
        )
        ok, reg = self._run(pg, _builtin("ducklake_catalog"))
        assert ok
        value = reg.get_sample_value("ducklake_catalog_format_version", {"tenant": TENANT, "suffix": expected_suffix})
        assert value == expected_value

    def test_catalog_junk_version_fails_loudly(self, catalog_db, pg):
        # Same contract as the DuckDB form: no leading number is an error, not a silent NULL.
        pg.execute("CREATE TABLE ducklake_metadata (key text, value text, scope text, scope_id bigint)")
        pg.execute("INSERT INTO ducklake_metadata VALUES ('version', 'totally-not-a-version', NULL, NULL)")
        ok, reg = self._run(pg, _builtin("ducklake_catalog"))
        assert not ok
        errors = reg.get_sample_value(
            "ducklake_metrics_query_errors_total", {"tenant": TENANT, "query": "ducklake_catalog"}
        )
        assert errors == 1

    def test_config_values(self, catalog_db, pg):
        pg.execute("CREATE TABLE ducklake_metadata (key text, value text, scope text, scope_id bigint)")
        pg.execute(
            "INSERT INTO ducklake_metadata VALUES "
            "('auto_compact', 'true', NULL, NULL), ('auto_compact', 'false', 'table', 7), "
            "('data_inlining_row_limit', '-2.5', 'schema', 3), ('data_inlining_row_limit', 'lots', 'table', 8), "
            "('version', '1.0', NULL, NULL)"
        )
        ok, reg = self._run(pg, _builtin("ducklake_config"))
        assert ok

        def v(key, scope, scope_id):
            return reg.get_sample_value(
                "ducklake_config_value", {"tenant": TENANT, "key": key, "scope": scope, "scope_id": scope_id}
            )

        assert v("auto_compact", "", "") == 1.0
        assert v("auto_compact", "table", "7") == 0.0
        assert v("data_inlining_row_limit", "schema", "3") == -2.5
        assert v("data_inlining_row_limit", "table", "8") is None  # not numeric: sample dropped
