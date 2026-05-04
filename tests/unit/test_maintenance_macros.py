"""Tier C: SQL macros in tools/maintenance.sql against a stub schema.

In-process DuckDB with stub catalog tables and a real local-filesystem glob.
Exercises path-normalization logic where the same physical file may be
referenced as either an absolute URI or a bucket-relative key (per quirk r1
in the followup doc) — the same area where a previous regression
(``heal-orphans`` B1 gate, false-pass on cross-table form mismatch) lived.
"""

import duckdb
import pytest

import maintenance


def _make_stub_lake(con):
    """Mirror the two DuckLake catalog tables the macros touch."""
    con.execute("CREATE SCHEMA __ducklake_metadata_lake")
    con.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion ("
        "  data_file_id BIGINT, path VARCHAR"
        ")"
    )
    con.execute("CREATE TABLE __ducklake_metadata_lake.ducklake_data_file (  data_file_id BIGINT, path VARCHAR)")


def _seed_queue(con, rows):
    con.executemany(
        "INSERT INTO __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion VALUES (?, ?)",
        rows,
    )


def _load_macros(con):
    con.execute(maintenance.MAINTENANCE_SQL_PATH.read_text())


@pytest.fixture
def lake_con():
    con = duckdb.connect()
    _make_stub_lake(con)
    yield con
    con.close()


@pytest.fixture
def data_dir(tmp_path):
    """A local-filesystem stand-in for the lake's S3 data path."""
    d = tmp_path / "lake" / "data"
    d.mkdir(parents=True)
    return d


def _touch(directory, names):
    for n in names:
        (directory / n).write_bytes(b"")


class TestCountPendingDups:
    def test_no_dups(self, lake_con):
        _seed_queue(lake_con, [(1, "a.parquet"), (2, "b.parquet")])
        _load_macros(lake_con)
        assert lake_con.execute("SELECT count_pending_dups()").fetchone()[0] == 0

    def test_one_dup(self, lake_con):
        _seed_queue(lake_con, [(1, "a.parquet"), (1, "a.parquet"), (2, "b.parquet")])
        _load_macros(lake_con)
        assert lake_con.execute("SELECT count_pending_dups()").fetchone()[0] == 1

    def test_multiple_dups_per_path(self, lake_con):
        # path 'c' has 3 entries: 2 extras. path 'a' has 2 entries: 1 extra. Total 3.
        _seed_queue(
            lake_con,
            [
                (1, "a.parquet"),
                (1, "a.parquet"),
                (2, "b.parquet"),
                (3, "c.parquet"),
                (3, "c.parquet"),
                (3, "c.parquet"),
            ],
        )
        _load_macros(lake_con)
        assert lake_con.execute("SELECT count_pending_dups()").fetchone()[0] == 3

    def test_empty_queue(self, lake_con):
        _load_macros(lake_con)
        assert lake_con.execute("SELECT count_pending_dups()").fetchone()[0] == 0


class TestFindCatalogOrphans:
    """Path-matching tolerates absolute s3:// URIs and bucket-relative keys
    in the same column (per quirk r1). Tests both forms and a mix."""

    def test_returns_empty_when_all_paths_live(self, lake_con, data_dir):
        _touch(data_dir, ["a.parquet", "b.parquet"])
        _seed_queue(lake_con, [(1, str(data_dir / "a.parquet")), (2, str(data_dir / "b.parquet"))])
        _load_macros(lake_con)
        rows = lake_con.execute("SELECT * FROM find_catalog_orphans(?)", [str(data_dir)]).fetchall()
        assert rows == []

    def test_absolute_form_orphan_detected(self, lake_con, data_dir):
        _touch(data_dir, ["live.parquet"])
        _seed_queue(
            lake_con,
            [
                (1, str(data_dir / "live.parquet")),  # absolute, exists
                (2, str(data_dir / "missing.parquet")),  # absolute, gone
            ],
        )
        _load_macros(lake_con)
        rows = lake_con.execute("SELECT * FROM find_catalog_orphans(?)", [str(data_dir)]).fetchall()
        assert rows == [(2, str(data_dir / "missing.parquet"))]

    def test_relative_form_orphan_detected(self, lake_con, data_dir):
        _touch(data_dir, ["live.parquet"])
        _seed_queue(
            lake_con,
            [
                (1, "live.parquet"),  # relative, exists
                (2, "missing.parquet"),  # relative, gone
            ],
        )
        _load_macros(lake_con)
        rows = lake_con.execute("SELECT * FROM find_catalog_orphans(?)", [str(data_dir)]).fetchall()
        assert rows == [(2, "missing.parquet")]

    def test_mixed_forms_in_same_queue(self, lake_con, data_dir):
        _touch(data_dir, ["a.parquet", "b.parquet"])
        _seed_queue(
            lake_con,
            [
                (1, str(data_dir / "a.parquet")),  # absolute, live
                (2, "b.parquet"),  # relative, live
                (3, str(data_dir / "x.parquet")),  # absolute, orphan
                (4, "y.parquet"),  # relative, orphan
            ],
        )
        _load_macros(lake_con)
        rows = lake_con.execute(
            "SELECT * FROM find_catalog_orphans(?) ORDER BY data_file_id",
            [str(data_dir)],
        ).fetchall()
        assert rows == [
            (3, str(data_dir / "x.parquet")),
            (4, "y.parquet"),
        ]

    def test_no_false_positive_for_relative_when_only_absolute_in_listing(self, lake_con, data_dir):
        # Live S3 (here: local) listing returns absolute paths; the queue stores
        # the same file as a relative key. Without the second branch of the
        # macro's join (`l.file = data_path || '/' || s.path`), this would be
        # falsely flagged as an orphan.
        _touch(data_dir, ["live.parquet"])
        _seed_queue(lake_con, [(1, "live.parquet")])
        _load_macros(lake_con)
        rows = lake_con.execute("SELECT * FROM find_catalog_orphans(?)", [str(data_dir)]).fetchall()
        assert rows == [], "relative-form path with matching live file must NOT be reported as orphan"

    def test_empty_queue_returns_empty(self, lake_con, data_dir):
        _touch(data_dir, ["live.parquet"])
        _load_macros(lake_con)
        rows = lake_con.execute("SELECT * FROM find_catalog_orphans(?)", [str(data_dir)]).fetchall()
        assert rows == []

    def test_empty_listing_makes_everything_an_orphan(self, lake_con, data_dir):
        # Empty data_dir — no parquet files exist. Every queue row is an orphan.
        _seed_queue(lake_con, [(1, "a.parquet"), (2, "b.parquet")])
        _load_macros(lake_con)
        rows = lake_con.execute(
            "SELECT * FROM find_catalog_orphans(?) ORDER BY data_file_id",
            [str(data_dir)],
        ).fetchall()
        assert rows == [(1, "a.parquet"), (2, "b.parquet")]
