"""Unit tests for the Iceberg SchemaManager (millpond/iceberg.py).

Schema evolution lives inside the iceberg module rather than the shared
schema.py — that file is DuckLake-specific. Tests here use PyIceberg's
SqlCatalog against a temp SQLite file so we exercise the real
update_schema() commit path.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from millpond import iceberg
from millpond.iceberg import SchemaManager, _arrow_to_iceberg


@pytest.fixture
def catalog(tmp_path):
    return SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )


def _initial_batch() -> pa.Table:
    return pa.table({"event": ["click"], "team_id": [1]})


def _seed_table(catalog) -> None:
    """Land an initial table via iceberg._ensure_table so SchemaManager has something to read."""
    iceberg._ensure_table(catalog, "ns", "events", _initial_batch(), {})


# ---------------------------------------------------------------------------
# _arrow_to_iceberg type mapping
# ---------------------------------------------------------------------------


class TestArrowToIceberg:
    def test_bool(self):
        assert isinstance(_arrow_to_iceberg(pa.bool_()), BooleanType)

    def test_int_widths_below_64_to_integer(self):
        for t in (pa.int8(), pa.int16(), pa.int32(), pa.uint8(), pa.uint16()):
            assert isinstance(_arrow_to_iceberg(t), IntegerType), f"{t} should map to IntegerType"

    def test_int64_and_uint32_uint64_to_long(self):
        # uint32 exceeds signed int32 range; widen to LongType to be safe.
        for t in (pa.int64(), pa.uint32(), pa.uint64()):
            assert isinstance(_arrow_to_iceberg(t), LongType), f"{t} should map to LongType"

    def test_float32_to_float_float64_to_double(self):
        assert isinstance(_arrow_to_iceberg(pa.float32()), FloatType)
        assert isinstance(_arrow_to_iceberg(pa.float64()), DoubleType)

    def test_strings_to_string(self):
        for t in (pa.string(), pa.large_string(), pa.utf8()):
            assert isinstance(_arrow_to_iceberg(t), StringType)

    def test_binary_to_binary(self):
        assert isinstance(_arrow_to_iceberg(pa.binary()), BinaryType)
        assert isinstance(_arrow_to_iceberg(pa.large_binary()), BinaryType)

    def test_date_to_date(self):
        assert isinstance(_arrow_to_iceberg(pa.date32()), DateType)

    def test_timestamp_naive_vs_tz_aware(self):
        assert isinstance(_arrow_to_iceberg(pa.timestamp("us")), TimestampType)
        assert isinstance(_arrow_to_iceberg(pa.timestamp("us", tz="UTC")), TimestamptzType)

    def test_unknown_falls_back_to_string(self):
        # duration isn't in our explicit map — fallback prevents a crash on
        # weird PyArrow types and lands the column as JSON-ish strings.
        assert isinstance(_arrow_to_iceberg(pa.duration("us")), StringType)


# ---------------------------------------------------------------------------
# SchemaManager.evolve — happy paths
# ---------------------------------------------------------------------------


class TestEvolveAddColumns:
    def test_adds_new_column(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")

        # Add a column the table doesn't have yet.
        new_schema = pa.schema([("event", pa.string()), ("team_id", pa.int64()), ("user_id", pa.string())])
        mgr.evolve(new_schema)

        table = catalog.load_table(("ns", "events"))
        names = {f.name for f in table.schema().fields}
        assert "user_id" in names

    def test_multiple_new_columns_one_transaction(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")

        new_schema = pa.schema(
            [
                ("event", pa.string()),
                ("team_id", pa.int64()),
                ("user_id", pa.string()),
                ("country", pa.string()),
                ("score", pa.float64()),
            ]
        )
        mgr.evolve(new_schema)

        table = catalog.load_table(("ns", "events"))
        names = {f.name for f in table.schema().fields}
        assert {"user_id", "country", "score"} <= names

    def test_idempotent_on_no_change(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")

        # First evolve — already-known columns; should commit nothing.
        same_schema = pa.schema([("event", pa.string()), ("team_id", pa.int64())])
        before = catalog.load_table(("ns", "events")).schema()
        mgr.evolve(same_schema)
        after = catalog.load_table(("ns", "events")).schema()
        assert before == after


class TestEvolveSkipsReserved:
    def test_skips_inserted_at_and_partition_cols(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")

        # Batch claims to have _inserted_at + partition cols (which it would,
        # because they were added by iceberg.write). evolve must not try to
        # re-add them — they're owned by iceberg.py.
        with_meta = pa.schema(
            [
                ("event", pa.string()),
                ("team_id", pa.int64()),
                ("_inserted_at", pa.timestamp("us", tz="UTC")),
                ("year", pa.int32()),
                ("month", pa.int32()),
                ("day", pa.int32()),
                ("hour", pa.int32()),
            ]
        )

        before = catalog.load_table(("ns", "events")).schema()
        mgr.evolve(with_meta)
        after = catalog.load_table(("ns", "events")).schema()
        # No DDL should have fired (table already has _inserted_at + partition
        # cols from iceberg._ensure_table; everything else in batch is known).
        assert before == after


class TestEvolveSkipsUnsafeNames:
    def test_unsafe_name_skipped(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")

        # "evil; DROP" would be an injection if the writer just interpolated.
        # PyIceberg's API doesn't interpolate (this is belt-and-suspenders),
        # but we keep the safety check at the SchemaManager layer.
        bad = pa.schema([("event", pa.string()), ("team_id", pa.int64()), ("evil; DROP", pa.string())])
        mgr.evolve(bad)

        table = catalog.load_table(("ns", "events"))
        names = {f.name for f in table.schema().fields}
        assert "evil; DROP" not in names


# ---------------------------------------------------------------------------
# SchemaManager.evolve — uninitialised paths
# ---------------------------------------------------------------------------


class TestEvolveBeforeTableExists:
    def test_returns_silently_when_table_missing(self, catalog):
        # No table seeded. evolve() should not raise — iceberg.write will
        # create the table on the next call; evolve picks it up afterward.
        mgr = SchemaManager(catalog, "ns", "events")
        schema = pa.schema([("event", pa.string()), ("team_id", pa.int64())])
        mgr.evolve(schema)  # must not raise

    def test_picks_up_table_on_next_evolve(self, catalog):
        mgr = SchemaManager(catalog, "ns", "events")

        # First evolve — table doesn't exist yet, returns silently.
        mgr.evolve(pa.schema([("event", pa.string())]))

        # iceberg.write creates the table (would happen in the real flow).
        _seed_table(catalog)

        # Second evolve adds a new column.
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64()), ("user_id", pa.string())]))
        names = {f.name for f in catalog.load_table(("ns", "events")).schema().fields}
        assert "user_id" in names


# ---------------------------------------------------------------------------
# Widening / narrowing
# ---------------------------------------------------------------------------


class TestEvolveWidens:
    def test_int32_widens_to_long(self, catalog):
        # Seed with team_id as IntegerType (int64 arrow → LongType, so seed
        # with int32 to land an IntegerType column).
        iceberg._ensure_table(
            catalog, "ns", "events", pa.table({"event": ["x"], "team_id": pa.array([1], pa.int32())}), {}
        )
        mgr = SchemaManager(catalog, "ns", "events")
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int32())]))  # initial load
        # Now arrive with int64 — should widen Integer → Long.
        committed = mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64())]))
        assert committed is True
        field = next(f for f in catalog.load_table(("ns", "events")).schema().fields if f.name == "team_id")
        assert isinstance(field.field_type, LongType)

    def test_no_change_returns_false_and_skips_commit(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")
        # Same schema — no commit, no reload signal.
        result = mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64())]))
        assert result is False


class TestEvolveRejectsNarrowing:
    def test_long_to_int_raises_and_bumps_error_counter(self, catalog):
        from unittest.mock import patch

        # Seed with team_id as int64 → LongType.
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64())]))  # initialize

        # Iceberg rejects narrowing in update_column. Attempt long → int.
        from pyiceberg.exceptions import ValidationError

        narrowing = pa.schema([("event", pa.string()), ("team_id", pa.int32())])
        # PyIceberg 0.11.x raises ValidationError on narrowing. Lock the
        # specific class so a future change in pyiceberg's exception
        # hierarchy doesn't silently still "pass" against a broad except.
        with patch("millpond.iceberg.metrics") as mock_metrics, pytest.raises(ValidationError):
            mgr.evolve(narrowing)
        # errors_total{type=schema}.inc() must have fired before the re-raise.
        mock_metrics.errors_total.labels.assert_any_call(type="schema")
        # Schema unchanged — narrowing was rejected, not silently applied.
        field = next(f for f in catalog.load_table(("ns", "events")).schema().fields if f.name == "team_id")
        assert isinstance(field.field_type, LongType)

    def test_failed_evolve_invalidates_for_next_attempt(self, catalog):
        from unittest.mock import patch

        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64())]))
        assert mgr._initialized

        from pyiceberg.exceptions import ValidationError

        narrowing = pa.schema([("event", pa.string()), ("team_id", pa.int32())])
        with patch("millpond.iceberg.metrics"), pytest.raises(ValidationError):
            mgr.evolve(narrowing)
        # After the failure, the manager must be un-initialized so the next
        # call re-loads (possibly observing another writer's evolution).
        assert mgr._initialized is False


# ---------------------------------------------------------------------------
# invalidate
# ---------------------------------------------------------------------------


class TestInvalidate:
    def test_reload_after_invalidate(self, catalog):
        _seed_table(catalog)
        mgr = SchemaManager(catalog, "ns", "events")
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64())]))  # initializes

        # Add a column via another (simulated) writer — bypassing our manager.
        table = catalog.load_table(("ns", "events"))
        with table.update_schema() as us:
            us.add_column("user_id", StringType())

        # Without invalidate, mgr's cached known-set wouldn't have user_id —
        # invalidate forces a reload.
        mgr.invalidate()
        mgr.evolve(pa.schema([("event", pa.string()), ("team_id", pa.int64()), ("user_id", pa.string())]))
        names = {f.name for f in catalog.load_table(("ns", "events")).schema().fields}
        assert "user_id" in names
