"""Integration tests for write path and schema evolution against local DuckDB.

Uses an in-memory DuckDB database attached as 'lake' to exercise the real
ducklake.write() and schema.SchemaManager code paths without requiring
Postgres or S3.
"""

from unittest.mock import MagicMock, patch

import duckdb
import orjson
import pyarrow as pa
import pytest

from millpond.arrow_converter import coerce_typed_columns, convert
from millpond.ducklake import write
from millpond.schema import SchemaManager


@pytest.fixture()
def conn():
    """DuckDB connection with an in-memory 'lake' catalog mimicking DuckLake.

    NB: plain DuckDB does NOT enforce DuckLake's widening-only ALTER rule — it
    permissively allows narrowing casts. Tests that need that semantic install
    it explicitly (see `_reject_alter_column` in the coercion tests); don't drop
    that wrapper assuming the fixture provides it."""
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


@pytest.mark.integration
class TestTimestampCoercionWritePath:
    """End-to-end: NRT JSON (string timestamps) written into a table whose
    timestamp columns are already TIMESTAMPTZ — the duckling backfill's
    `posthog.events` shape. Without coercion this is the prod wedge that
    PR #12334 hit; coercion makes the batch type match the table so no
    schema-evolution DDL is needed.
    """

    # The events table's TIMESTAMPTZ columns, per the backfill DDL.
    TS_COLS = (
        "timestamp",
        "created_at",
        "person_created_at",
        "group0_created_at",
    )
    TS_PAIRS = tuple((c, "timestamptz") for c in TS_COLS)
    WIRE = "2024-01-01 12:00:00.000000"

    def _nrt_batch(self) -> pa.Table:
        """An NRT batch the way it reaches the sink: JSON → convert() infers
        VARCHAR for the date-time strings."""
        msg = {"uuid": "u1", "event": "$pageview", "team_id": 1}
        for c in self.TS_COLS:
            msg[c] = self.WIRE
        table = convert([orjson.dumps(msg)])
        assert table is not None
        # Precondition: inference really does type these as strings.
        for c in self.TS_COLS:
            assert table.schema.field(c).type == pa.string()
        return table

    def _create_events_table(self, conn) -> None:
        cols = ", ".join(f"{c} TIMESTAMPTZ" for c in self.TS_COLS)
        # _inserted_at mirrors the backfill DDL — write()'s INSERT ... BY NAME
        # appends NOW() into it.
        conn.execute(
            f"CREATE TABLE lake.main.events "
            f"(uuid VARCHAR, event VARCHAR, team_id BIGINT, {cols}, _inserted_at TIMESTAMPTZ)"
        )

    def _reject_alter_column(self, schema_mgr) -> None:
        """Wrap the connection so ALTER COLUMN raises, simulating DuckLake's
        widening-only enforcement (plain DuckDB would permissively allow the
        narrowing and hide the bug)."""
        real_conn = schema_mgr._conn
        mock_conn = MagicMock(wraps=real_conn)
        mock_conn.execute = MagicMock(
            side_effect=lambda sql, *a, **kw: (
                (_ for _ in ()).throw(duckdb.Error("DuckLake only widens"))
                if "ALTER COLUMN" in sql
                else real_conn.execute(sql, *a, **kw)
            )
        )
        schema_mgr._conn = mock_conn
        return mock_conn

    @patch("millpond.schema.metrics")
    def test_uncoerced_string_batch_triggers_failing_alter(self, mock_metrics, conn, cache):
        """The baseline this change fixes: without coercion, evolve() attempts to
        narrow each TIMESTAMPTZ column to VARCHAR and gets rejected.

        Caveat — this asserts the *narrowing ALTER is attempted and metricked*,
        not the full prod wedge. The real stall is DuckLake-specific at INSERT
        time; plain in-memory DuckDB would permissively auto-cast the VARCHAR
        rows into the TIMESTAMPTZ column on `INSERT ... BY NAME`, so the harness
        can't reproduce the stall itself. `_reject_alter_column` supplies the
        DuckLake widening-only semantic; the failing ALTER is the observable
        signal (`errors_total{type="schema"}` bumping every flush) that the fix
        eliminates."""
        self._create_events_table(conn)
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()
        self._reject_alter_column(schema_mgr)

        schema_mgr.evolve(self._nrt_batch().schema)

        # One ALTER attempt per timestamp column, each rejected and metricked.
        assert mock_metrics.errors_total.labels(type="schema").inc.call_count == len(self.TS_COLS)

    @patch("millpond.schema.metrics")
    def test_coerced_batch_writes_with_no_schema_ddl(self, mock_metrics, conn, cache):
        """The fix: coerced batch matches the table, so evolve() issues no DDL
        and the rows land with real timestamps — even when ALTER is forbidden."""
        self._create_events_table(conn)
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()
        mock_conn = self._reject_alter_column(schema_mgr)

        batch = coerce_typed_columns(self._nrt_batch(), self.TS_PAIRS)
        # write() goes through the same (ALTER-rejecting) connection.
        write(mock_conn, "events", batch, cache, schema_mgr, schema_name="main")

        # No schema error, no widen, no add — types already matched.
        mock_metrics.errors_total.labels(type="schema").inc.assert_not_called()
        mock_metrics.schema_columns_widened_total.inc.assert_not_called()
        mock_metrics.schema_columns_added_total.inc.assert_not_called()

        # Data landed and the stored value is a real timestamp.
        row = conn.execute(
            "SELECT event, timestamp FROM lake.main.events WHERE uuid = 'u1'"
        ).fetchone()
        assert row[0] == "$pageview"
        assert str(row[1]).startswith("2024-01-01 12:00:00")

    def test_fresh_table_created_with_timestamptz(self, conn, cache):
        """When millpond owns table creation, a coerced batch yields TIMESTAMPTZ
        columns from the start (vs VARCHAR for an uncoerced string batch)."""
        batch = coerce_typed_columns(self._nrt_batch(), self.TS_PAIRS)
        write(conn, "events", batch, cache, SchemaManager(conn, "events"), schema_name="main")

        types = dict(
            conn.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_catalog = 'lake' AND table_name = 'events'"
            ).fetchall()
        )
        for c in self.TS_COLS:
            assert types[c] == "TIMESTAMP WITH TIME ZONE"

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
