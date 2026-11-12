"""Test bounded expiry against a real PostgreSQL DuckLake catalog."""

from __future__ import annotations

import contextlib
from pathlib import Path

import duckdb
import ducklake_maintenance as maintenance
import psycopg
import pytest
from psycopg import sql

from tests.integration.postgres_server import _conninfo, throwaway_database
from tests.unit.test_ducklake_pin import _VERIFIED_DUCKDB_VERSION, _VERIFIED_DUCKLAKE_SHA

pytestmark = pytest.mark.integration

_FILE_STATS = (
    "ducklake_file_column_stats",
    "ducklake_file_partition_value",
    "ducklake_file_variant_stats",
)


@contextlib.contextmanager
def _lake(db, data_path):
    conn = duckdb.connect()
    try:
        try:
            conn.execute("INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres")
        except duckdb.Error:
            pytest.skip("ducklake/postgres extensions unavailable")
        assert duckdb.__version__ == _VERIFIED_DUCKDB_VERSION
        version = conn.execute(
            "SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'"
        ).fetchone()[0]
        assert version == _VERIFIED_DUCKLAKE_SHA
        attach = f"ducklake:postgres:{_conninfo(db)}"
        conn.execute(f"ATTACH '{attach}' AS lake (DATA_PATH '{data_path}')")
        yield conn
    finally:
        conn.close()


def _snapshot(pg):
    return pg.execute("SELECT max(snapshot_id) FROM ducklake_snapshot").fetchone()[0]


def _rows(pg, table):
    return pg.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table))).fetchall()


def _catalog_contents(pg):
    tables = pg.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename").fetchall()
    return {name: sorted(_rows(pg, name), key=repr) for (name,) in tables}


def _queued_objects(pg, data_path):
    return {
        str(Path(data_path) / path) if relative else path
        for path, relative in pg.execute("SELECT path, path_is_relative FROM ducklake_files_scheduled_for_deletion")
    }


def _run_expiry(*extra):
    maintenance.main(
        [
            "expire-snapshots",
            "--days",
            "7",
            "--batch-size",
            "2",
            "--file-batch-size",
            "2",
            "--metadata-batch-size",
            "3",
            "--run-budget-s",
            "30",
            *extra,
        ]
    )


def _read_events(db, data_path, snapshot=None):
    version = f" AT (VERSION => {snapshot})" if snapshot is not None else ""
    with _lake(db, data_path) as conn:
        return conn.execute(f"SELECT part, n, payload::VARCHAR FROM lake.events{version} ORDER BY n").fetchall()


@pytest.fixture()
def expiry_catalog(catalog_db, pg, tmp_path, monkeypatch):
    """Build retained history and a dropped table with partitioned file fanout."""
    data_path = tmp_path / "data"
    monkeypatch.setenv("DUCKLAKE_DATA_PATH", str(data_path))
    monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
    with _lake(catalog_db, data_path) as conn:
        conn.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
        conn.execute(
            "CREATE TABLE lake.doomed "
            "(part INTEGER, n BIGINT, payload VARIANT, a BIGINT, b BIGINT, c BIGINT, d BIGINT, e BIGINT)"
        )
        conn.execute("ALTER TABLE lake.doomed SET PARTITIONED BY (part)")
        conn.execute(
            "INSERT INTO lake.doomed SELECT range, range, ('{\"n\":' || range || '}')::JSON::VARIANT, "
            "range, range, range, range, range FROM range(7)"
        )
        conn.execute("INSERT INTO lake.doomed VALUES (0, 100, '{\"n\":100}'::JSON::VARIANT, 1, 2, 3, 4, 5)")
        conn.execute("DELETE FROM lake.doomed WHERE n = 0")
        conn.execute("DROP TABLE lake.doomed")
        conn.execute("CREATE TABLE lake.events (part INTEGER, n BIGINT, payload VARIANT)")
        conn.execute("ALTER TABLE lake.events SET PARTITIONED BY (part)")
        conn.execute(
            "INSERT INTO lake.events SELECT range % 3, range, ('{\"n\":' || range || '}')::JSON::VARIANT FROM range(9)"
        )
        retained = _snapshot(pg)
        conn.execute("DELETE FROM lake.events WHERE part = 0")
        conn.execute("INSERT INTO lake.events VALUES (3, 100, '{\"n\":100}'::JSON::VARIANT)")
    pg.execute(
        "UPDATE ducklake_snapshot SET snapshot_time = now() - interval '30 days' WHERE snapshot_id < %s",
        (retained,),
    )
    doomed_id = pg.execute("SELECT table_id FROM ducklake_table WHERE table_name = 'doomed'").fetchone()[0]
    doomed_path = data_path / "main" / "doomed"
    pg.execute(
        "UPDATE ducklake_table SET path = %s, path_is_relative = false WHERE table_id = %s",
        (str(doomed_path) + "/", doomed_id),
    )
    first_file = pg.execute(
        "SELECT data_file_id, path FROM ducklake_data_file WHERE table_id = %s ORDER BY data_file_id LIMIT 1",
        (doomed_id,),
    ).fetchone()
    pg.execute(
        "UPDATE ducklake_data_file SET path = %s, path_is_relative = false WHERE data_file_id = %s",
        (str(doomed_path / first_file[1]), first_file[0]),
    )
    # Exercise dropped-table catalogs whose files retain open snapshot intervals.
    pg.execute("UPDATE ducklake_data_file SET end_snapshot = NULL WHERE table_id = %s", (doomed_id,))
    pg.execute("UPDATE ducklake_delete_file SET end_snapshot = NULL WHERE table_id = %s", (doomed_id,))
    # Small VARIANT files need explicit statistic rows to exercise their cascade.
    pg.execute(
        "INSERT INTO ducklake_file_variant_stats "
        "(data_file_id, table_id, column_id, variant_path, shredded_type, value_count) "
        "SELECT data_file_id, table_id, 3, '$.n', 'int64', record_count FROM ducklake_data_file"
    )
    dead_ids = {
        row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_data_file WHERE table_id = %s", (doomed_id,))
    }
    live_ids = {
        row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_data_file WHERE table_id != %s", (doomed_id,))
    }
    objects = {str(path): path.read_bytes() for path in data_path.rglob("*.parquet")}
    assert len(dead_ids) > 2
    assert objects
    return {
        "db": catalog_db,
        "data_path": data_path,
        "retained": retained,
        "head": _snapshot(pg),
        "doomed_id": doomed_id,
        "dead_ids": dead_ids,
        "live_ids": live_ids,
        "objects": objects,
    }


def test_retained_time_travel_and_file_stats_survive(expiry_catalog, pg):
    catalog = expiry_catalog
    before = _read_events(catalog["db"], catalog["data_path"], catalog["retained"])
    current = _read_events(catalog["db"], catalog["data_path"])
    stats = {table: [row for row in _rows(pg, table) if row[0] in catalog["live_ids"]] for table in _FILE_STATS}

    _run_expiry()
    _run_expiry()

    assert _read_events(catalog["db"], catalog["data_path"], catalog["retained"]) == before
    assert _read_events(catalog["db"], catalog["data_path"]) == current
    assert {row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_data_file")} == catalog["live_ids"]
    for table in _FILE_STATS:
        assert sorted(_rows(pg, table), key=repr) == sorted(stats[table], key=repr), table
    assert (
        pg.execute(
            "SELECT count(*) FROM ducklake_snapshot_changes c "
            "WHERE NOT EXISTS (SELECT 1 FROM ducklake_snapshot s WHERE s.snapshot_id = c.snapshot_id)"
        ).fetchone()[0]
        == 0
    )


def test_dropped_open_files_queue_resolved_objects_without_deleting_them(expiry_catalog, pg):
    catalog = expiry_catalog
    dead_objects = {path for path in catalog["objects"] if "/doomed/" in path}
    assert len(dead_objects) > 2

    _run_expiry()
    _run_expiry()

    assert _queued_objects(pg, catalog["data_path"]) == dead_objects
    queued_ids = [row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_files_scheduled_for_deletion")]
    assert len(queued_ids) == len(set(queued_ids))
    assert {str(path): path.read_bytes() for path in catalog["data_path"].rglob("*.parquet")} == catalog["objects"]


def test_idle_head_survives_retention(expiry_catalog, pg):
    catalog = expiry_catalog
    current = _read_events(catalog["db"], catalog["data_path"])
    pg.execute("UPDATE ducklake_snapshot SET snapshot_time = now() - interval '30 days'")

    _run_expiry()

    assert pg.execute("SELECT snapshot_id FROM ducklake_snapshot").fetchall() == [(catalog["head"],)]
    assert _read_events(catalog["db"], catalog["data_path"]) == current


@pytest.mark.parametrize("mutation", ["version", "missing_column", "extra_column", "wrong_root"])
def test_unsupported_catalog_fails_before_mutation(expiry_catalog, pg, mutation):
    if mutation == "version":
        pg.execute("UPDATE ducklake_metadata SET value = '999.0' WHERE key = 'version' AND scope IS NULL")
    elif mutation == "missing_column":
        pg.execute("ALTER TABLE ducklake_data_file DROP COLUMN path_is_relative")
    elif mutation == "extra_column":
        pg.execute("ALTER TABLE ducklake_data_file ADD COLUMN unknown_metadata text")
    else:
        pg.execute("UPDATE ducklake_metadata SET value = '/different/root/' WHERE key = 'data_path' AND scope IS NULL")
    before = _catalog_contents(pg)

    with pytest.raises((RuntimeError, ValueError, psycopg.Error)):
        _run_expiry()

    assert _catalog_contents(pg) == before


def test_dry_run_preserves_catalog_and_objects(expiry_catalog, pg):
    catalog = expiry_catalog
    before = _catalog_contents(pg)

    _run_expiry("--dry-run")

    assert _catalog_contents(pg) == before
    assert {str(path): path.read_bytes() for path in catalog["data_path"].rglob("*.parquet")} == catalog["objects"]


@pytest.mark.parametrize("mode", ["non_autocommit", "active_transaction", "repeatable_read"])
def test_borrowed_connection_contract_fails_before_catalog_mutation(expiry_catalog, pg, mode):
    before = _catalog_contents(pg)
    with psycopg.connect(_conninfo(expiry_catalog["db"]), autocommit=mode != "non_autocommit") as borrowed:
        if mode == "active_transaction":
            borrowed.execute("BEGIN")
            borrowed.execute("CREATE TEMP TABLE borrowed_marker AS SELECT 42 AS n")
        elif mode == "repeatable_read":
            borrowed.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        status = borrowed.info.transaction_status

        with pytest.raises((ValueError, RuntimeError)):
            maintenance.expire_snapshots(pg=borrowed)

        assert not borrowed.closed
        assert borrowed.info.transaction_status == status
        if mode == "active_transaction":
            assert borrowed.execute("SELECT n FROM borrowed_marker").fetchall() == [(42,)]
            borrowed.rollback()
    assert _catalog_contents(pg) == before


@pytest.mark.parametrize(
    "table,key",
    [
        ("ducklake_snapshot", "snapshot_id"),
        ("ducklake_snapshot_changes", "snapshot_id"),
        ("ducklake_data_file", "data_file_id"),
        ("ducklake_delete_file", "delete_file_id"),
        ("ducklake_schema", "schema_id"),
    ],
)
@pytest.mark.parametrize("mutation", ["missing_primary_key", "wrong_key_type"])
def test_required_key_contract_fails_before_catalog_mutation(expiry_catalog, pg, table, key, mutation):
    if mutation == "missing_primary_key":
        constraint = pg.execute(
            "SELECT conname FROM pg_constraint WHERE conrelid = %s::regclass AND contype = 'p'", (table,)
        ).fetchone()[0]
        pg.execute(
            sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(sql.Identifier(table), sql.Identifier(constraint))
        )
    else:
        pg.execute(
            sql.SQL("ALTER TABLE {} ALTER COLUMN {} TYPE integer").format(sql.Identifier(table), sql.Identifier(key))
        )
    before = _catalog_contents(pg)

    with pytest.raises(RuntimeError):
        _run_expiry()

    assert _catalog_contents(pg) == before


@pytest.mark.parametrize("mutation", ["owner", "version", "layout", "trigger", "missing_primary_key"])
def test_unsupported_progress_fails_before_snapshot_mutation(expiry_catalog, pg, mutation):
    pg.execute(
        "CREATE TABLE millpond_snapshot_expiry_progress ("
        "singleton boolean PRIMARY KEY, owner text NOT NULL, state_version integer NOT NULL, "
        "phase integer NOT NULL, cursor_id bigint NOT NULL, high_id bigint NOT NULL)"
    )
    pg.execute("INSERT INTO millpond_snapshot_expiry_progress VALUES (true, 'millpond-expire-snapshots', 1, 0, -1, -1)")
    if mutation == "owner":
        pg.execute("UPDATE millpond_snapshot_expiry_progress SET owner = 'different-tool'")
    elif mutation == "version":
        pg.execute("UPDATE millpond_snapshot_expiry_progress SET state_version = 999")
    elif mutation == "layout":
        pg.execute("ALTER TABLE millpond_snapshot_expiry_progress ADD COLUMN extra_metadata integer")
    elif mutation == "trigger":
        pg.execute(
            "CREATE FUNCTION expiry_progress_test_trigger() RETURNS trigger LANGUAGE plpgsql "
            "AS $$ BEGIN RETURN NEW; END $$"
        )
        pg.execute(
            "CREATE TRIGGER expiry_progress_test_trigger BEFORE UPDATE ON millpond_snapshot_expiry_progress "
            "FOR EACH ROW EXECUTE FUNCTION expiry_progress_test_trigger()"
        )
    else:
        pg.execute(
            "ALTER TABLE millpond_snapshot_expiry_progress DROP CONSTRAINT millpond_snapshot_expiry_progress_pkey"
        )
    before = _catalog_contents(pg)

    with pytest.raises(RuntimeError):
        _run_expiry()

    assert _catalog_contents(pg) == before


def _row_counts(pg):
    return {name: len(rows) for name, rows in _catalog_contents(pg).items()}


def test_file_fanout_and_dependent_rows_respect_transaction_budgets(expiry_catalog, pg, monkeypatch):
    original = maintenance._ExpiryRun.transaction
    observed = []

    @contextlib.contextmanager
    def observe(run):
        before = _row_counts(pg)
        with original(run):
            yield
        after = _row_counts(pg)
        changes = {table: before[table] - after.get(table, 0) for table in before}
        observed.append(changes)
        for table, removed in changes.items():
            limit = 2 if table in ("ducklake_snapshot", "ducklake_data_file", "ducklake_delete_file") else 3
            assert removed <= limit, (table, removed, limit)

    monkeypatch.setattr(maintenance._ExpiryRun, "transaction", observe)
    _run_expiry()

    assert sum(changes.get("ducklake_data_file", 0) for changes in observed) == len(expiry_catalog["dead_ids"])
    assert sum(changes.get("ducklake_file_column_stats", 0) > 0 for changes in observed) > 1
    assert sum(changes.get("ducklake_data_file", 0) > 0 for changes in observed) > 1


def test_snapshot_batch_limit_preserves_unexpired_history(expiry_catalog, pg):
    catalog = expiry_catalog
    snapshots = [row[0] for row in pg.execute("SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id")]
    before = _read_events(catalog["db"], catalog["data_path"], catalog["retained"])

    _run_expiry("--num-batches", "1")

    assert [
        row[0] for row in pg.execute("SELECT snapshot_id FROM ducklake_snapshot ORDER BY snapshot_id")
    ] == snapshots[2:]
    assert pg.execute(
        "SELECT count(*) FROM ducklake_data_file WHERE table_id = %s", (catalog["doomed_id"],)
    ).fetchone()[0] == len(catalog["dead_ids"])
    assert not _queued_objects(pg, catalog["data_path"])
    assert _read_events(catalog["db"], catalog["data_path"], catalog["retained"]) == before


@pytest.mark.parametrize("same_object", [False, True])
def test_existing_queue_identity_is_checked_after_path_resolution(expiry_catalog, pg, same_object):
    catalog = expiry_catalog
    file_id, path = pg.execute(
        "SELECT data_file_id, path FROM ducklake_data_file WHERE table_id = %s AND NOT path_is_relative LIMIT 1",
        (catalog["doomed_id"],),
    ).fetchone()
    queued_path = path if same_object else str(catalog["data_path"] / "different-object.parquet")
    pg.execute(
        "INSERT INTO ducklake_files_scheduled_for_deletion VALUES (%s, %s, false, now())",
        (file_id, queued_path),
    )

    if not same_object:
        with pytest.raises(RuntimeError):
            _run_expiry()
        assert pg.execute("SELECT 1 FROM ducklake_data_file WHERE data_file_id = %s", (file_id,)).fetchone()
        assert pg.execute(
            "SELECT path FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = %s", (file_id,)
        ).fetchall() == [(queued_path,)]
    else:
        _run_expiry()
        _run_expiry()
        queued = _queued_objects(pg, catalog["data_path"])
        assert queued == {path for path in catalog["objects"] if "/doomed/" in path}
        assert (
            pg.execute(
                "SELECT count(*) FROM ducklake_files_scheduled_for_deletion WHERE data_file_id = %s", (file_id,)
            ).fetchone()[0]
            == 1
        )


def test_conflicting_parent_paths_keep_file_metadata(expiry_catalog, pg):
    catalog = expiry_catalog
    pg.execute(
        "INSERT INTO ducklake_table "
        "SELECT table_id, table_uuid, begin_snapshot + 1, end_snapshot, schema_id, table_name, "
        "path || 'different/', path_is_relative FROM ducklake_table WHERE table_id = %s",
        (catalog["doomed_id"],),
    )

    with pytest.raises(RuntimeError, match="ambiguous"):
        _run_expiry()

    assert {
        row[0]
        for row in pg.execute(
            "SELECT data_file_id FROM ducklake_data_file WHERE table_id = %s", (catalog["doomed_id"],)
        )
    } == catalog["dead_ids"]
    assert not any("/different/" in path for path in _queued_objects(pg, catalog["data_path"]))


def test_expiry_keeps_inline_registry_and_physical_tables(expiry_catalog, pg):
    catalog = expiry_catalog
    with _lake(catalog["db"], catalog["data_path"]) as conn:
        conn.execute("CALL lake.set_option('data_inlining_row_limit', 10)")
        conn.execute("CREATE TABLE lake.inline_churn (n INTEGER)")
        conn.execute("INSERT INTO lake.inline_churn VALUES (1)")
        conn.execute("DROP TABLE lake.inline_churn")
    table_id, inline_name = pg.execute(
        "SELECT i.table_id, i.table_name FROM ducklake_inlined_data_tables i "
        "JOIN ducklake_table t USING (table_id) WHERE t.table_name = 'inline_churn'"
    ).fetchone()
    before = _rows(pg, inline_name)
    pg.execute(
        "UPDATE ducklake_snapshot SET snapshot_time = now() - interval '30 days' WHERE snapshot_id < %s",
        (_snapshot(pg),),
    )

    _run_expiry()

    assert pg.execute(
        "SELECT 1 FROM ducklake_inlined_data_tables WHERE table_id = %s AND table_name = %s",
        (table_id, inline_name),
    ).fetchone()
    assert _rows(pg, inline_name) == before
    assert pg.execute("SELECT 1 FROM ducklake_table WHERE table_id = %s", (table_id,)).fetchone()


@pytest.mark.parametrize("boundary", ["snapshots", "statistics", "files", "table"])
def test_restart_after_committed_batches_preserves_live_data(expiry_catalog, pg, monkeypatch, boundary):
    catalog = expiry_catalog
    before = _read_events(catalog["db"], catalog["data_path"], catalog["retained"])
    original = maintenance._ExpiryRun.transaction
    target = {
        "snapshots": "ducklake_snapshot",
        "statistics": "ducklake_file_column_stats",
        "files": "ducklake_data_file",
        "table": "ducklake_column",
    }[boundary]
    interrupted = False

    @contextlib.contextmanager
    def interrupt(run):
        nonlocal interrupted
        count = len(_rows(pg, target))
        with original(run):
            yield
        if not interrupted and len(_rows(pg, target)) < count:
            interrupted = True
            raise InterruptedError("simulate a process interruption after commit")

    with monkeypatch.context() as patch:
        patch.setattr(maintenance._ExpiryRun, "transaction", interrupt)
        with pytest.raises(InterruptedError):
            _run_expiry()
    assert interrupted
    assert _read_events(catalog["db"], catalog["data_path"], catalog["retained"]) == before

    _run_expiry()
    _run_expiry()

    assert _read_events(catalog["db"], catalog["data_path"], catalog["retained"]) == before
    assert not catalog["dead_ids"] & {row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_data_file")}
    assert _queued_objects(pg, catalog["data_path"]) == {path for path in catalog["objects"] if "/doomed/" in path}


def test_failure_before_file_commit_rolls_back_queue_files_and_cursor(expiry_catalog, pg, monkeypatch):
    original = maintenance._expiry_file_page
    interrupted_state = None

    def interrupt(run, relation, cursor, high, base):
        nonlocal interrupted_state
        before = _catalog_contents(pg)
        result = original(run, relation, cursor, high, base)
        if relation == "ducklake_data_file" and result[1]:
            interrupted_state = before
            run.execute("SELECT pg_sleep(0.1)")
        return result

    with monkeypatch.context() as patch:
        patch.setattr(maintenance, "_expiry_file_page", interrupt)
        with pytest.raises(psycopg.errors.QueryCanceled):
            _run_expiry("--statement-timeout-s", "0.02")

    assert interrupted_state is not None
    assert _catalog_contents(pg) == interrupted_state
    _run_expiry()
    _run_expiry()
    assert _read_events(expiry_catalog["db"], expiry_catalog["data_path"], expiry_catalog["retained"])
    assert not expiry_catalog["dead_ids"] & {
        row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_data_file")
    }


def test_resumed_data_phase_collects_newly_dead_delete_vectors(expiry_catalog, pg, monkeypatch):
    catalog = expiry_catalog
    with _lake(catalog["db"], catalog["data_path"]) as conn:
        conn.execute("DELETE FROM lake.events WHERE n = 1")
    vector_id, parent_id = pg.execute(
        "SELECT dv.delete_file_id, dv.data_file_id FROM ducklake_delete_file dv "
        "JOIN ducklake_table t USING (table_id) WHERE t.table_name = 'events'"
    ).fetchone()
    original = maintenance._ExpiryRun.transaction

    @contextlib.contextmanager
    def stop_after_vectors(run):
        with original(run):
            yield
        if pg.execute("SELECT to_regclass('millpond_snapshot_expiry_progress')").fetchone()[0]:
            phase = pg.execute("SELECT phase FROM millpond_snapshot_expiry_progress").fetchone()[0]
            if phase == maintenance._EXPIRE_PHASES.index("ducklake_data_file"):
                raise InterruptedError("stop after scanning live delete vectors")

    with monkeypatch.context() as patch:
        patch.setattr(maintenance._ExpiryRun, "transaction", stop_after_vectors)
        with pytest.raises(InterruptedError):
            _run_expiry()
    assert pg.execute("SELECT 1 FROM ducklake_delete_file WHERE delete_file_id = %s", (vector_id,)).fetchone()
    with _lake(catalog["db"], catalog["data_path"]) as conn:
        conn.execute("CREATE TABLE lake.sentinel AS SELECT 42 AS n")
        conn.execute("DROP TABLE lake.events")
    pg.execute(
        "UPDATE ducklake_snapshot SET snapshot_time = now() - interval '30 days' WHERE snapshot_id < %s",
        (_snapshot(pg),),
    )

    _run_expiry("--run-budget-s", "2")
    _run_expiry("--run-budget-s", "2")

    assert not pg.execute("SELECT 1 FROM ducklake_delete_file WHERE delete_file_id = %s", (vector_id,)).fetchone()
    assert not pg.execute("SELECT 1 FROM ducklake_data_file WHERE data_file_id = %s", (parent_id,)).fetchone()
    queued_ids = {row[0] for row in pg.execute("SELECT data_file_id FROM ducklake_files_scheduled_for_deletion")}
    assert {vector_id, parent_id} <= queued_ids
    with _lake(catalog["db"], catalog["data_path"]) as conn:
        assert conn.execute("SELECT * FROM lake.sentinel").fetchall() == [(42,)]


def test_writer_append_rename_and_drop_between_gc_commits(expiry_catalog, pg, monkeypatch):
    catalog = expiry_catalog
    original = maintenance._ExpiryRun.transaction
    written = False

    @contextlib.contextmanager
    def write_after_commit(run):
        nonlocal written
        with original(run):
            yield
        if not written and pg.execute("SELECT to_regclass('millpond_snapshot_expiry_progress')").fetchone()[0]:
            written = True
            with _lake(catalog["db"], catalog["data_path"]) as conn:
                conn.execute("ALTER TABLE lake.events RENAME TO renamed")
                conn.execute("INSERT INTO lake.renamed VALUES (4, 200, '{\"n\":200}'::JSON::VARIANT)")
                conn.execute("CREATE TABLE lake.churn AS SELECT 1 AS n")
                conn.execute("DROP TABLE lake.churn")
                conn.execute("ALTER TABLE lake.renamed RENAME TO events")

    before = _read_events(catalog["db"], catalog["data_path"], catalog["retained"])
    monkeypatch.setattr(maintenance._ExpiryRun, "transaction", write_after_commit)
    _run_expiry()

    assert written
    assert _read_events(catalog["db"], catalog["data_path"], catalog["retained"]) == before
    assert _read_events(catalog["db"], catalog["data_path"])[-1][1] == 200
    assert not any("/events/" in path for path in _queued_objects(pg, catalog["data_path"]))


@contextlib.contextmanager
def _clone_catalog(pg, server):
    """Copy metadata into an independent database while sharing immutable files."""
    tables = pg.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'").fetchall()
    with throwaway_database(server) as db, psycopg.connect(_conninfo(db), autocommit=True) as clone:
        for (table,) in tables:
            columns = pg.execute(
                "SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute "
                "WHERE attrelid = %s::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
                (table,),
            ).fetchall()
            fields = sql.SQL(", ").join(
                sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(kind)) for name, kind in columns
            )
            clone.execute(sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table), fields))
            rows = _rows(pg, table)
            if rows:
                placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in columns)
                with clone.cursor() as cursor:
                    cursor.executemany(
                        sql.SQL("INSERT INTO {} VALUES ({})").format(sql.Identifier(table), placeholders), rows
                    )
        yield db, clone


def test_matches_native_expiry_on_cloned_catalog(expiry_catalog, pg, pg_server):
    catalog = expiry_catalog
    with _clone_catalog(pg, pg_server) as (native_db, native):
        with _lake(native_db, catalog["data_path"]) as conn:
            conn.execute("CALL ducklake_expire_snapshots('lake', older_than => now() - interval '7 days')")
        _run_expiry()
        _run_expiry()

        for table in ("ducklake_snapshot", "ducklake_data_file", "ducklake_delete_file", *_FILE_STATS):
            assert sorted(_rows(pg, table), key=repr) == sorted(_rows(native, table), key=repr), table
        assert _queued_objects(pg, catalog["data_path"]) == _queued_objects(native, catalog["data_path"])
