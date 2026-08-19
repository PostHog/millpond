"""ensure-sort-keys: idempotent DuckLake SET SORTED BY maintenance.

The canonical sort keys (ducklake_maintenance.SORT_KEYS) make the extension
sort new writes per file and honor the sort in compaction merges, so parquet
row-group min/max stats become tight for the tenant column — ClickHouse's
row-group pruning then skips most of a file for team_id-filtered queries.
"""

import ducklake_maintenance
import pytest


def _sort_exprs(conn, table_name):
    return conn.execute(
        """
        SELECT e.sort_key_index, e.expression, e.sort_direction
        FROM __ducklake_metadata_lake.ducklake_sort_expression e
        JOIN __ducklake_metadata_lake.ducklake_sort_info i
          ON i.sort_id = e.sort_id AND i.table_id = e.table_id AND i.end_snapshot IS NULL
        JOIN __ducklake_metadata_lake.ducklake_table t ON t.table_id = e.table_id
        WHERE t.table_name = ?
        ORDER BY e.sort_key_index
        """,
        [table_name],
    ).fetchall()


def test_ensure_sort_keys_applies_and_is_idempotent(ducklake_conn):
    conn = ducklake_conn
    conn.execute("CREATE TABLE lake.main.t (team_id BIGINT, ts VARCHAR)")
    conn.execute("INSERT INTO lake.main.t VALUES (1, 'a'), (2, 'b')")

    sort_keys = {("main", "t"): ["team_id"]}
    applied = ducklake_maintenance.ensure_sort_keys(conn, sort_keys=sort_keys, dry_run=False)
    assert applied == [("main", "t")]
    assert _sort_exprs(conn, "t") == [(0, "team_id", "ASC")]

    # second run: nothing to change
    applied = ducklake_maintenance.ensure_sort_keys(conn, sort_keys=sort_keys, dry_run=False)
    assert applied == []
    assert _sort_exprs(conn, "t") == [(0, "team_id", "ASC")]


def test_ensure_sort_keys_updates_changed_key(ducklake_conn):
    conn = ducklake_conn
    conn.execute("CREATE TABLE lake.main.t (team_id BIGINT, ts VARCHAR)")
    conn.execute(f"ALTER TABLE lake.main.t SET SORTED BY (ts)")

    applied = ducklake_maintenance.ensure_sort_keys(conn, sort_keys={("main", "t"): ["team_id"]}, dry_run=False)
    assert applied == [("main", "t")]
    assert _sort_exprs(conn, "t") == [(0, "team_id", "ASC")]


def test_ensure_sort_keys_dry_run_changes_nothing(ducklake_conn):
    conn = ducklake_conn
    conn.execute("CREATE TABLE lake.main.t (team_id BIGINT, ts VARCHAR)")

    applied = ducklake_maintenance.ensure_sort_keys(conn, sort_keys={("main", "t"): ["team_id"]}, dry_run=True)
    assert applied == [("main", "t")]
    assert _sort_exprs(conn, "t") == []


def test_sort_keys_cover_megaduck_tables():
    """The canonical set must name (schema, table) pairs of the megaduck tenant tables."""
    expected = {
        ("main", "events"): ["team_id", "timestamp"],
        ("main", "events_nrt"): ["team_id", "timestamp"],
        ("main", "heatmap_events"): ["team_id", "timestamp"],
        ("main", "person"): ["team_id"],
        ("main", "person_distinct_id"): ["team_id"],
        ("main", "groups"): ["team_id"],
    }
    assert ducklake_maintenance.SORT_KEYS == expected
