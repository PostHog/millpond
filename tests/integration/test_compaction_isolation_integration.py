"""Catalog-wide compaction must survive one poisoned table (real DuckLake).

Builds a real local DuckLake (duckdb-file metadata + local data dir, stock
ducklake extension), two hive-partitioned tables with several small files
each, then rewrites half of one table's file paths in the metadata to a
divergent hive directory — the `ducklake_add_data_files` foreign-path mix
that aborted a production compactor with "DuckLakeCompactor: Files have
different hive partition path" (the compactor groups candidates by logical
partition_values, then asserts every file in a group shares one hive
directory string).

Asserts the per-table compact() loop:
  * still merges the healthy table's files,
  * reports (does NOT raise on) the poisoned table,
  * leaves the poisoned table's files untouched,
  * raises only when EVERY table fails (systemic).

These tests also pin the load-bearing recovery property: the DuckDB
connection survives the extension's InternalException, so the loop can keep
going (verified on duckdb 1.5.2; a future duckdb that invalidates the
instance on INTERNAL errors would fail here, loudly).

Skips when the ducklake extension can't be installed (offline CI).
"""

from __future__ import annotations

import logging

import duckdb
import ducklake_maintenance
import pytest

pytestmark = pytest.mark.integration

LAKE = ducklake_maintenance.ATTACH_NAME  # "lake" — compact() targets this name
META_SCHEMA = ducklake_maintenance.METADATA_SCHEMA


def _live_file_count(conn: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return conn.execute(
        f"SELECT COUNT(*) FROM {META_SCHEMA}.ducklake_data_file df "
        f"JOIN {META_SCHEMA}.ducklake_table t ON t.table_id = df.table_id "
        "WHERE df.end_snapshot IS NULL AND t.end_snapshot IS NULL AND t.table_name = ?",
        [table_name],
    ).fetchone()[0]


def _seed_partitioned_table(conn: duckdb.DuckDBPyConnection, table_name: str, n_files: int = 4) -> None:
    """A hive-partitioned table with n single-row parquet files (every INSERT
    is its own file; all well under the tier-1 1MiB ceiling). PARTITIONED BY
    is set BEFORE the inserts so every file carries partition values and a
    hive path — the preconditions for the compactor's hive-path check."""
    conn.execute(f"CREATE TABLE {LAKE}.main.{table_name} (y INTEGER, v VARCHAR)")
    conn.execute(f"ALTER TABLE {LAKE}.main.{table_name} SET PARTITIONED BY (y)")
    for i in range(n_files):
        conn.execute(f"INSERT INTO {LAKE}.main.{table_name} VALUES (2026, 'row{i}')")
    assert _live_file_count(conn, table_name) == n_files


def _poison_tables(conn: duckdb.DuckDBPyConnection, attach: str, table_names: tuple[str, ...]) -> None:
    """Rewrite half of each table's file paths to a foreign hive directory
    within the SAME logical partition (what add_data_files did in production —
    different prefix AND different zero-padding, so neither directory string
    is a substring of the other and the compactor's containment check fails
    regardless of which file leads the group). Direct UPDATE on the metadata
    duckdb file, with the lake detached."""
    # 1.5.2 attached the catalog as a second database named METADATA_SCHEMA.
    # 1.5.5 stores catalog tables in the lake attach itself (database_name=LAKE).
    meta_path = conn.execute(
        f"SELECT path FROM duckdb_databases() WHERE database_name = '{LAKE}'"
    ).fetchone()[0]
    conn.execute(f"DETACH {LAKE}")
    meta_conn = duckdb.connect(meta_path)
    for table_name in table_names:
        meta_conn.execute(
            "UPDATE ducklake_data_file "
            "SET path = 'backfill/' || replace(path, 'y=2026', 'y=02026') "
            "WHERE table_id = (SELECT table_id FROM ducklake_table "
            f"                  WHERE table_name = '{table_name}' AND end_snapshot IS NULL) "
            "  AND data_file_id % 2 = 0"
        )
    n_poisoned = meta_conn.execute("SELECT COUNT(*) FROM ducklake_data_file WHERE path LIKE 'backfill/%'").fetchone()[0]
    meta_conn.close()
    assert n_poisoned > 0, "poisoning must hit at least one live file"
    conn.execute(attach)


@pytest.fixture
def lake(tmp_path):
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
    except Exception:
        pytest.skip("ducklake extension unavailable (offline?)")
    meta = tmp_path / "meta.ducklake"
    data = tmp_path / "data"
    # DATA_INLINING_ROW_LIMIT 0: single-row INSERTs must land as real parquet
    # files with hive paths + partition-value rows (inlined rows produce no
    # ducklake_data_file entries and nothing for the compactor to merge).
    attach = f"ATTACH 'ducklake:{meta}' AS {LAKE} (DATA_PATH '{data}', DATA_INLINING_ROW_LIMIT 0)"
    conn.execute(attach)
    yield conn, attach
    conn.close()


def _compact_tier1(conn) -> int:
    return ducklake_maintenance.compact(
        conn,
        tier=1,
        table=None,
        dry_run=False,
        threads=2,
        memory_limit="1GB",
        max_compacted_files=100,
    )


def test_poisoned_table_does_not_abort_catalog_compaction(lake, caplog):
    conn, attach = lake
    for t in ("good", "bad"):
        _seed_partitioned_table(conn, t)
    _poison_tables(conn, attach, ("bad",))

    with caplog.at_level(logging.INFO, logger="maintenance"):
        # Must NOT raise: partial failure is reported, healthy tables proceed.
        n_failed = _compact_tier1(conn)

    # Healthy table merged 4 small files down; poisoned table untouched.
    assert n_failed == 1
    assert _live_file_count(conn, "good") < 4
    assert _live_file_count(conn, "bad") == 4
    assert "main.bad failed" in caplog.text
    assert "1/2 table(s) failed" in caplog.text
    # The poisoned table failed with THE production error, not something
    # incidental — this test reproduces the production incident end to end.
    assert "Files have different hive partition path" in caplog.text


def test_all_tables_poisoned_does_not_wedge(lake, caplog):
    """Even ALL tables failing must not raise — the poison-set attractor:
    candidate-driven enumeration converges on the failed set (healthy tables
    drain out once compacted; poisoned ones never do), so all-failed is the
    steady state under persistent poison. Raising would wedge the recipe
    chain (later tiers + cleanup-all) every tick — the incident mode this
    loop exists to prevent. Pinned against the real extension error."""
    conn, attach = lake
    for t in ("bad1", "bad2"):
        _seed_partitioned_table(conn, t)
    _poison_tables(conn, attach, ("bad1", "bad2"))

    with caplog.at_level(logging.WARNING, logger="maintenance"):
        n_failed = _compact_tier1(conn)

    assert n_failed == 2
    assert _live_file_count(conn, "bad1") == 4
    assert _live_file_count(conn, "bad2") == 4
    assert "2/2 table(s) failed" in caplog.text


def test_biggest_backlog_served_first_within_budget(lake):
    """Behavioral pin of the whole fix: with an alphabetically-first small
    table and an alphabetically-last big one, the big one must be served
    first (ORDER BY candidate count DESC flowing through the loop), and the
    budget must stop the run before the small one — no alphabetical
    starvation of the backlog."""
    conn, _attach = lake
    _seed_partitioned_table(conn, "aaa_small", n_files=2)
    _seed_partitioned_table(conn, "zzz_big", n_files=6)

    ducklake_maintenance.compact(
        conn,
        tier=1,
        table=None,
        dry_run=False,
        threads=2,
        memory_limit="1GB",
        max_compacted_files=6,
    )

    assert _live_file_count(conn, "zzz_big") < 6, "biggest backlog must be served first"
    assert _live_file_count(conn, "aaa_small") == 2, "budget must exhaust before the small table"
