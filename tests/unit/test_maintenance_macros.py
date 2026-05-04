"""Tier C: SQL macros in tools/maintenance.sql against a stub schema.

In-process DuckDB with stub catalog tables and a real local-filesystem glob.
Exercises path-normalization logic where the same physical file may be
referenced as either an absolute URI or a bucket-relative key (per quirk r1
in the followup doc) — the same area where a previous regression
(``heal-orphans`` B1 gate, false-pass on cross-table form mismatch) lived.

Also covers the heal-orphans B1/B3 safety gates against stub
ducklake_data_file / ducklake_delete_file tables, including the
end_snapshot live filter that distinguishes current rows from expired
historical entries.
"""

import re

import duckdb
import pytest

import maintenance


def _make_stub_lake(con):
    """Mirror the DuckLake catalog tables maintenance.py / maintenance.sql touch.

    Includes the ``end_snapshot`` column on data_file and delete_file even
    though the existing macro tests don't read it — heal-orphans's safety
    gates do, and keeping one stub schema across the file avoids drift.
    """
    con.execute("CREATE SCHEMA __ducklake_metadata_lake")
    con.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion ("
        "  data_file_id BIGINT, path VARCHAR"
        ")"
    )
    con.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_data_file ("
        "  data_file_id BIGINT, path VARCHAR, end_snapshot BIGINT"
        ")"
    )
    con.execute(
        "CREATE TABLE __ducklake_metadata_lake.ducklake_delete_file (  data_file_id BIGINT, end_snapshot BIGINT)"
    )


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


def _seed_data_files(con, rows):
    """rows: iterable of (data_file_id, path, end_snapshot_or_None)."""
    con.executemany(
        "INSERT INTO __ducklake_metadata_lake.ducklake_data_file VALUES (?, ?, ?)",
        rows,
    )


def _seed_delete_files(con, rows):
    """rows: iterable of (data_file_id, end_snapshot_or_None)."""
    con.executemany(
        "INSERT INTO __ducklake_metadata_lake.ducklake_delete_file VALUES (?, ?)",
        rows,
    )


class TestHealOrphansGates:
    """heal-orphans's B1 and B3 safety gates, including the end_snapshot live
    filter (the bug that an earlier review caught: gates were counting all
    rows, so historical entries blocked perfectly valid heal-orphans runs).

    Drives heal_orphans(conn, dry_run=True) end-to-end so the test exercises
    macro loading + path normalization + gate SQL in one shot. Dry-run skips
    advisory-lock acquisition (which would need a real pg ATTACH) and the
    final DELETE — ideal for unit-style coverage.
    """

    def _run(self, lake_con, data_dir, monkeypatch):
        monkeypatch.setenv("DUCKLAKE_DATA_PATH", str(data_dir))
        _load_macros(lake_con)
        # heal_orphans creates _orphans via find_catalog_orphans(?) then
        # runs B1/B3. dry_run=True returns before the DELETE.
        maintenance.heal_orphans(lake_con, dry_run=True)

    def test_b1_passes_when_only_expired_rows_match_queue(self, lake_con, data_dir, monkeypatch):
        # data_file_id=1 was once live (path 'a') but is now expired (snapshot 100).
        # data_file_id=2 is currently live with a different path ('live').
        # Queue holds 'a.parquet' (orphaned because the live data file does
        # not include it). Without the end_snapshot filter, B1 sees the
        # expired row and aborts. With the filter, it does not.
        _seed_queue(lake_con, [(1, "a.parquet")])
        _seed_data_files(
            lake_con,
            [
                (1, "a.parquet", 100),  # expired
                (2, "live.parquet", None),  # live
            ],
        )
        # data_dir empty so the queue path is genuinely orphaned.
        self._run(lake_con, data_dir, monkeypatch)  # must NOT raise

    def test_b1_aborts_when_live_row_matches_queue(self, lake_con, data_dir, monkeypatch):
        # data_file_id=1 is currently live with path 'a' AND that same path is
        # in the queue. The queue entry is NOT a real orphan; B1 must abort.
        _seed_queue(lake_con, [(1, "a.parquet")])
        _seed_data_files(lake_con, [(1, "a.parquet", None)])
        with pytest.raises(RuntimeError, match="safety gate B1 failed.*still appear as live"):
            self._run(lake_con, data_dir, monkeypatch)

    def test_b1_aborts_when_no_live_data_files_at_all(self, lake_con, data_dir, monkeypatch):
        # Vacuous-pass guard: a catalog with zero LIVE rows is suspect even
        # if there are historical rows. Bail rather than assuming the queue
        # is full of orphans on an empty live state.
        _seed_queue(lake_con, [(1, "a.parquet")])
        _seed_data_files(lake_con, [(99, "old.parquet", 50)])  # only expired rows
        with pytest.raises(RuntimeError, match="zero live rows"):
            self._run(lake_con, data_dir, monkeypatch)

    def test_b3_ignores_expired_delete_vectors(self, lake_con, data_dir, monkeypatch):
        # Queue has data_file_id=42 as orphan; ducklake_delete_file has an
        # expired delete vector against id=42. Without the end_snapshot
        # filter, B3 would abort; with it, it doesn't.
        _seed_queue(lake_con, [(42, "a.parquet")])
        _seed_data_files(lake_con, [(99, "live.parquet", None)])  # at least one live row
        _seed_delete_files(lake_con, [(42, 100)])  # expired vector against orphan id
        self._run(lake_con, data_dir, monkeypatch)  # must NOT raise

    def test_b3_aborts_on_live_delete_vector_against_orphan(self, lake_con, data_dir, monkeypatch):
        # A live (end_snapshot IS NULL) positional-delete vector pointing at
        # an "orphan" id means the file is still live for vector lookups —
        # hard abort.
        _seed_queue(lake_con, [(42, "a.parquet")])
        _seed_data_files(lake_con, [(99, "live.parquet", None)])
        _seed_delete_files(lake_con, [(42, None)])  # live vector
        with pytest.raises(RuntimeError, match="safety gate B3 failed"):
            self._run(lake_con, data_dir, monkeypatch)


class TestSchemaConsistency:
    def test_macros_match_metadata_schema_constant(self):
        """The .sql file hardcodes `__ducklake_metadata_lake` (verbatim load
        via `.read` in `just shell` precludes Python templating). If anyone
        ever changes ATTACH_NAME, this test fails loudly — the constraint
        is documented in the .sql header but the assertion is what makes it
        load-bearing."""
        sql = maintenance.MAINTENANCE_SQL_PATH.read_text()
        # Strip line comments so we don't catch the cautionary references in
        # the header.
        without_comments = "\n".join(line.split("--", 1)[0] for line in sql.splitlines())
        refs = set(re.findall(r"__ducklake_metadata_\w+", without_comments))
        unexpected = refs - {maintenance.METADATA_SCHEMA}
        assert not unexpected, (
            f"maintenance.sql references {sorted(unexpected)} but METADATA_SCHEMA "
            f"is {maintenance.METADATA_SCHEMA!r}. If you change ATTACH_NAME in "
            "maintenance.py, update the schema references in maintenance.sql to match."
        )
