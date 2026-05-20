"""Integration tests for write path and schema evolution against local DuckDB.

Uses an in-memory DuckDB database attached as 'lake' to exercise the real
ducklake.write() and schema.SchemaManager code paths without requiring
Postgres or S3.
"""

from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest

from millpond.ducklake import write
from millpond.schema import SchemaManager


@pytest.fixture()
def conn():
    """DuckDB connection with an in-memory 'lake' catalog mimicking DuckLake."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    yield c
    c.close()


@pytest.fixture()
def cache() -> set[str]:
    """Per-test caller-owned ensure cache (formerly module-level `_tables_ensured`)."""
    return set()


@pytest.mark.integration
class TestWritePath:
    def test_basic_write(self, conn, cache):
        batch = pa.table({"event": ["click", "view"], "team_id": [1, 2]})
        write(conn, "events", batch, cache)

        rows = conn.execute("SELECT event, team_id FROM lake.main.events").fetchall()
        assert set(rows) == {("click", 1), ("view", 2)}

    def test_inserted_at_column_added(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        write(conn, "events", batch, cache)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "_inserted_at" in col_names

    def test_multiple_writes_accumulate(self, conn, cache):
        batch1 = pa.table({"x": [1, 2]})
        batch2 = pa.table({"x": [3, 4]})
        write(conn, "events", batch1, cache)
        write(conn, "events", batch2, cache)

        rows = conn.execute("SELECT x FROM lake.main.events ORDER BY x").fetchall()
        assert [r[0] for r in rows] == [1, 2, 3, 4]

    def test_empty_batch_creates_table(self, conn, cache):
        batch = pa.table({"a": pa.array([], type=pa.int64())})
        write(conn, "events", batch, cache)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "a" in col_names
        assert "_inserted_at" in col_names


@pytest.mark.integration
class TestSchemaEvolution:
    def test_add_new_column(self, conn, cache):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Second write introduces a new column — full write, not just evolve
        batch2 = pa.table({"event": ["view"], "source": ["web"]})
        write(conn, "events", batch2, cache, schema_mgr)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "source" in col_names

        # Verify data landed correctly
        rows = conn.execute("SELECT event, source FROM lake.main.events ORDER BY event").fetchall()
        assert rows == [("click", None), ("view", "web")]

    def test_widen_integer_to_bigint(self, conn):
        # Create table with INTEGER column
        conn.execute("CREATE TABLE lake.main.events (x INTEGER)")
        schema_mgr = SchemaManager(conn, "events")

        # Write with BIGINT — should widen
        batch = pa.table({"x": pa.array([1], type=pa.int64())})
        schema_mgr.evolve(batch.schema)

        type_result = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events' AND column_name = 'x'"
        ).fetchone()
        assert type_result[0] == "BIGINT"

    def test_widen_float_to_double(self, conn):
        conn.execute("CREATE TABLE lake.main.events (x FLOAT)")
        schema_mgr = SchemaManager(conn, "events")

        batch = pa.table({"x": pa.array([1.0], type=pa.float64())})
        schema_mgr.evolve(batch.schema)

        type_result = conn.execute(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events' AND column_name = 'x'"
        ).fetchone()
        assert type_result[0] == "DOUBLE"

    def test_multiple_new_columns_at_once(self, conn, cache):
        batch1 = pa.table({"a": [1]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Full write with multiple new columns
        batch2 = pa.table({"a": [2], "b": ["x"], "c": [3.0]})
        write(conn, "events", batch2, cache, schema_mgr)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert {"a", "b", "c", "_inserted_at"} <= col_names

        # Verify data integrity
        rows = conn.execute("SELECT a, b, c FROM lake.main.events ORDER BY a").fetchall()
        assert rows == [(1, None, None), (2, "x", 3.0)]

    def test_schema_cached_across_writes(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        assert schema_mgr._initialized
        assert "event" in schema_mgr._known_columns

    def test_invalidate_forces_reload(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        schema_mgr.invalidate()
        assert not schema_mgr._initialized

        # Next evolve should reload
        schema_mgr.evolve(batch.schema)
        assert schema_mgr._initialized

    @patch("millpond.schema.metrics")
    def test_column_added_increments_counter(self, mock_metrics, conn, cache):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        batch2 = pa.table({"event": ["view"], "source": ["web"]})
        schema_mgr.evolve(batch2.schema)

        mock_metrics.schema_columns_added_total.inc.assert_called_once()

    @patch("millpond.schema.metrics")
    def test_type_widened_increments_counter(self, mock_metrics, conn):
        conn.execute("CREATE TABLE lake.main.events (x INTEGER)")
        schema_mgr = SchemaManager(conn, "events")

        batch = pa.table({"x": pa.array([1], type=pa.int64())})
        schema_mgr.evolve(batch.schema)

        mock_metrics.schema_columns_widened_total.inc.assert_called_once()

    @patch("millpond.schema.metrics")
    def test_no_change_no_counter(self, mock_metrics, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        # Same schema again — no evolution needed
        schema_mgr.evolve(batch.schema)

        mock_metrics.schema_columns_added_total.inc.assert_not_called()
        mock_metrics.schema_columns_widened_total.inc.assert_not_called()

    @patch("millpond.schema.metrics")
    def test_incompatible_type_change_increments_error(self, mock_metrics, conn):
        """Incompatible type change should be rejected, logged, and metricked.

        DuckLake enforces widening-only for ALTER COLUMN SET DATA TYPE, but
        plain DuckDB allows nearly anything. We simulate DuckLake's rejection
        by wrapping the connection to raise on ALTER COLUMN.
        """
        conn.execute("CREATE TABLE lake.main.events (x BIGINT)")
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()

        # Wrap the connection to reject ALTER COLUMN (simulating DuckLake)
        real_conn = schema_mgr._conn
        mock_conn = MagicMock(wraps=real_conn)
        mock_conn.execute = MagicMock(
            side_effect=lambda sql, *a, **kw: (
                (_ for _ in ()).throw(duckdb.Error("Cannot narrow BIGINT to INTEGER"))
                if "ALTER COLUMN" in sql
                else real_conn.execute(sql, *a, **kw)
            )
        )
        schema_mgr._conn = mock_conn

        # Arrow batch with narrower type
        batch = pa.table({"x": pa.array([1], type=pa.int32())})
        schema_mgr.evolve(batch.schema)

        # Should have incremented the schema error counter
        mock_metrics.errors_total.labels.assert_called_with(type="schema")
        mock_metrics.errors_total.labels(type="schema").inc.assert_called_once()
        # Column type should remain BIGINT
        assert schema_mgr._known_columns["x"] == "BIGINT"

    @patch("millpond.schema.metrics")
    def test_unsafe_field_name_skipped(self, mock_metrics, conn, cache):
        """Fields with unsafe names (SQL injection risk) should be skipped."""
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Simulate a batch with an unsafe field name
        unsafe_schema = pa.schema([pa.field("event", pa.string()), pa.field("x; DROP TABLE", pa.string())])
        schema_mgr.evolve(unsafe_schema)

        mock_metrics.records_skipped_total.labels.assert_called_with(reason="unsafe_field_name")
        # The unsafe column should not have been added
        assert "x; DROP TABLE" not in schema_mgr._known_columns
