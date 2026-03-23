"""Integration tests for write path and schema evolution against local DuckDB.

Uses an in-memory DuckDB database attached as 'lake' to exercise the real
ducklake.write() and schema.SchemaManager code paths without requiring
Postgres or S3.
"""

from unittest.mock import patch

import duckdb
import pyarrow as pa
import pytest

from millpond.ducklake import _tables_ensured, write
from millpond.schema import SchemaManager


@pytest.fixture()
def conn():
    """DuckDB connection with an in-memory 'lake' catalog mimicking DuckLake."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    yield c
    c.close()


@pytest.fixture(autouse=True)
def _clear_ensure_cache():
    """Clear the _tables_ensured cache between tests."""
    _tables_ensured.clear()
    yield
    _tables_ensured.clear()


@pytest.mark.integration
class TestWritePath:
    def test_basic_write(self, conn):
        batch = pa.table({"event": ["click", "view"], "team_id": [1, 2]})
        write(conn, "events", batch)

        rows = conn.execute("SELECT event, team_id FROM lake.main.events").fetchall()
        assert set(rows) == {("click", 1), ("view", 2)}

    def test_inserted_at_column_added(self, conn):
        batch = pa.table({"event": ["click"]})
        write(conn, "events", batch)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "_inserted_at" in col_names

    def test_multiple_writes_accumulate(self, conn):
        batch1 = pa.table({"x": [1, 2]})
        batch2 = pa.table({"x": [3, 4]})
        write(conn, "events", batch1)
        write(conn, "events", batch2)

        rows = conn.execute("SELECT x FROM lake.main.events ORDER BY x").fetchall()
        assert [r[0] for r in rows] == [1, 2, 3, 4]

    def test_empty_batch_creates_table(self, conn):
        batch = pa.table({"a": pa.array([], type=pa.int64())})
        write(conn, "events", batch)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "a" in col_names
        assert "_inserted_at" in col_names


@pytest.mark.integration
class TestSchemaEvolution:
    def test_add_new_column(self, conn):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, schema_mgr)

        # Second batch introduces a new column — verify DDL happens
        batch2 = pa.table({"event": ["view"], "source": ["web"]})
        schema_mgr.evolve(batch2.schema)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert "source" in col_names

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

    def test_multiple_new_columns_at_once(self, conn):
        batch1 = pa.table({"a": [1]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, schema_mgr)

        # Verify evolve adds multiple columns in one call
        batch2 = pa.table({"a": [2], "b": ["x"], "c": [3.0]})
        schema_mgr.evolve(batch2.schema)

        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = 'events'"
        ).fetchall()
        col_names = {row[0] for row in cols}
        assert {"a", "b", "c", "_inserted_at"} <= col_names

    def test_schema_cached_across_writes(self, conn):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, schema_mgr)

        assert schema_mgr._initialized
        assert "event" in schema_mgr._known_columns

    def test_invalidate_forces_reload(self, conn):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, schema_mgr)

        schema_mgr.invalidate()
        assert not schema_mgr._initialized

        # Next evolve should reload
        schema_mgr.evolve(batch.schema)
        assert schema_mgr._initialized

    @patch("millpond.schema.metrics")
    def test_column_added_increments_counter(self, mock_metrics, conn):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, schema_mgr)

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
    def test_no_change_no_counter(self, mock_metrics, conn):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, schema_mgr)

        # Same schema again — no evolution needed
        schema_mgr.evolve(batch.schema)

        mock_metrics.schema_columns_added_total.inc.assert_not_called()
        mock_metrics.schema_columns_widened_total.inc.assert_not_called()
