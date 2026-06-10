"""DuckLake behavior + value-fidelity locks.

Ported from the deleted cross-backend equivalence suite (see the
`final-iceberg` tag) — these are the DuckLake-side contracts that
remain load-bearing now that DuckLake is the only sink: reserved-column
collisions raise at the Sink boundary, `_inserted_at` provenance,
no derived partition-column emission, and round-trip fidelity for
pathological values/column shapes. Uses a real in-memory DuckDB store;
no mocks.
"""

from __future__ import annotations

import datetime
import time
import warnings

import duckdb
import pyarrow as pa
import pytest

from millpond import ducklake as ducklake_mod
from millpond import schema as schema_mod


class _Handle:
    """Thin write-and-read handle over a real local DuckDB 'lake'."""

    def __init__(self):
        self.conn = duckdb.connect()
        self.conn.execute("ATTACH ':memory:' AS lake")
        self.cache: set[str] = set()
        self.schema_mgr = schema_mod.SchemaManager(self.conn, "events")

    def write(self, batch: pa.Table) -> None:
        ducklake_mod.write(self.conn, "events", batch, self.cache, self.schema_mgr)

    def read(self) -> pa.Table:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return self.conn.execute("SELECT * FROM lake.main.events").fetch_arrow_table()


@pytest.fixture
def handle() -> _Handle:
    h = _Handle()
    yield h
    h.conn.close()


def _three_row_batch() -> pa.Table:
    return pa.table(
        {
            "event": pa.array(["click", "view", "scroll"], pa.string()),
            "team_id": pa.array([1, 2, 3], pa.int64()),
        }
    )


class TestReservedColumnCollision:
    @pytest.mark.parametrize("col_name", ["year", "month", "day", "hour", "_inserted_at"])
    def test_raises_early_on_reserved_column_collision(self, handle, col_name):
        """If a producer emits a column named after a backend-managed
        metadata column, the Sink raises `ValueError` at the `write()`
        boundary — before any backend-specific work — with a uniform
        error message."""
        if col_name == "_inserted_at":
            # Build the offending column with a tz-aware timestamp type
            # so the source schema is something a real producer could emit.
            offending = pa.array(
                [datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)],
                pa.timestamp("us", tz="UTC"),
            )
        else:
            offending = pa.array([1999], pa.int32())
        batch = pa.table({"event": ["x"], col_name: offending})

        with pytest.raises(ValueError, match="collide with"):
            handle.write(batch)


class TestPartitionColumnEmission:
    def test_does_not_emit_derived_partition_cols(self, handle):
        handle.write(_three_row_batch())
        names = set(handle.read().schema.names)
        # year/month/day/hour are reserved but never produced by DuckLake;
        # they must not appear unless the source batch carried them.
        assert names.isdisjoint({"year", "month", "day", "hour"})


class TestInsertedAtProvenance:
    """DuckLake stamps `_inserted_at` via DuckDB's NOW() at INSERT time —
    flush time, not event time. Lock the observed shape."""

    def test_all_rows_share_one_inserted_at_via_now(self, handle):
        # NOW() returns a single value per statement, so all 50 rows of a
        # single INSERT land with the same timestamp — that's the
        # implementation detail of NOW() vs CURRENT_TIMESTAMP /
        # TRANSACTION_TIMESTAMP. Lock the observed behaviour: same
        # statement → same timestamp.
        batch = pa.table({"event": [f"e{i}" for i in range(50)]})
        handle.write(batch)
        ts = handle.read().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 1

    def test_writes_use_distinct_timestamps_across_statements(self, handle):
        """Two separate write() calls = two separate INSERTs = two NOW()
        evaluations."""
        handle.write(pa.table({"event": ["a"]}))
        # NOW() resolution in DuckDB is microseconds; the second statement
        # is guaranteed to be later as long as the prior one took >0us.
        time.sleep(0.001)
        handle.write(pa.table({"event": ["b"]}))
        ts = handle.read().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 2

    def test_inserted_at_is_tz_aware(self, handle):
        handle.write(_three_row_batch())
        ts_field = handle.read().schema.field("_inserted_at")
        assert isinstance(ts_field.type, pa.TimestampType)
        assert ts_field.type.tz is not None, "_inserted_at must be tz-aware; downstream queries assume UTC."


class TestRoundTripFidelity:
    def test_all_null_typed_int_column_preserves_nulls(self, handle):
        batch = pa.table(
            {
                "event": pa.array(["x", "y"], pa.string()),
                "score": pa.array([None, None], pa.int64()),
            }
        )
        handle.write(batch)
        assert handle.read().column("score").to_pylist() == [None, None]

    def test_mixed_some_null_some_present(self, handle):
        batch = pa.table(
            {
                "event": pa.array(["a", None, "c"], pa.string()),
                "team_id": pa.array([1, None, 3], pa.int64()),
            }
        )
        handle.write(batch)
        out = handle.read()
        assert out.column("event").to_pylist() == ["a", None, "c"]
        assert out.column("team_id").to_pylist() == [1, None, 3]

    def test_two_writes_accumulate(self, handle):
        handle.write(_three_row_batch())
        handle.write(_three_row_batch())
        assert handle.read().num_rows == 6

    def test_post_arrow_converter_json_stringified_nested(self, handle):
        """arrow_converter JSON-stringifies nested dicts before they hit
        the sink. By the time a batch arrives, a struct column is just a
        VARCHAR. Lock that we preserve the bytes."""
        json_blob = '{"k":1,"nested":{"a":[1,2,3]}}'
        batch = pa.table({"event": ["x"], "payload": pa.array([json_blob], pa.string())})
        handle.write(batch)
        assert handle.read().column("payload").to_pylist() == [json_blob]

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "schema._arrow_type_to_duckdb keys on str(arrow_type) but "
            "pa.date32() stringifies as 'date32[day]' (not 'date32'), so "
            "DATE columns are silently mapped to VARCHAR by the schema "
            "manager. The eager CREATE TABLE path lands DATE correctly, "
            "but the subsequent evolve() call widens DATE -> VARCHAR. "
            "Real defect carried over from the equivalence suite."
        ),
    )
    def test_date_column_round_trips(self, handle):
        batch = pa.table({"d": pa.array([18262, 18263, 18264], pa.date32())})
        handle.write(batch)
        out = handle.read().column("d").to_pylist()
        assert out == [
            datetime.date(2020, 1, 1),
            datetime.date(2020, 1, 2),
            datetime.date(2020, 1, 3),
        ]


class TestPathologicalValues:
    def test_uint64_max_round_trips(self, handle):
        """DuckLake has UBIGINT, so uint64 max survives."""
        max_u64 = (1 << 64) - 1
        batch = pa.table({"big": pa.array([max_u64], pa.uint64())})
        handle.write(batch)
        assert handle.read().column("big").to_pylist() == [max_u64]

    def test_max_int64_round_trips(self, handle):
        big = (1 << 63) - 1
        small = -(1 << 63)
        batch = pa.table({"x": pa.array([big, small, 0], pa.int64())})
        handle.write(batch)
        assert handle.read().column("x").to_pylist() == [big, small, 0]

    def test_nan_and_inf_round_trip(self, handle):
        batch = pa.table(
            {
                "f": pa.array(
                    [float("inf"), float("-inf"), float("nan"), 0.0],
                    pa.float64(),
                )
            }
        )
        handle.write(batch)
        out = handle.read().column("f").to_pylist()
        # NaN doesn't equal itself; compare positionally.
        assert out[0] == float("inf")
        assert out[1] == float("-inf")
        assert out[2] != out[2]  # NaN
        assert out[3] == 0.0

    def test_empty_string_round_trips(self, handle):
        batch = pa.table({"s": pa.array(["", "x", ""], pa.string())})
        handle.write(batch)
        assert handle.read().column("s").to_pylist() == ["", "x", ""]

    def test_embedded_nul_byte_in_string(self, handle):
        # NUL inside a VARCHAR is a routine pathology when JSON came from
        # a binary-tinted upstream.
        batch = pa.table({"s": pa.array(["a\x00b", "no\x00ul"], pa.string())})
        handle.write(batch)
        assert handle.read().column("s").to_pylist() == ["a\x00b", "no\x00ul"]

    def test_unicode_value_round_trip(self, handle):
        batch = pa.table({"s": pa.array(["日本語", "café", "🦆"], pa.string())})
        handle.write(batch)
        assert handle.read().column("s").to_pylist() == ["日本語", "café", "🦆"]

    def test_very_long_string_value(self, handle):
        big = "x" * 100_000
        batch = pa.table({"s": pa.array(["small", big], pa.string())})
        handle.write(batch)
        got = handle.read().column("s").to_pylist()
        assert got[0] == "small"
        assert len(got[1]) == 100_000


class TestPathologicalColumnNames:
    def test_one_column_batch(self, handle):
        batch = pa.table({"only": pa.array([1, 2, 3], pa.int64())})
        handle.write(batch)
        assert handle.read().column("only").to_pylist() == [1, 2, 3]

    def test_many_columns_smoke(self, handle):
        cols = {f"c{i}": pa.array([i], pa.int64()) for i in range(500)}
        handle.write(pa.table(cols))
        out = handle.read()
        assert "c0" in out.schema.names
        assert "c499" in out.schema.names

    def test_long_column_name_accepted(self, handle):
        long_name = "a" * 200  # well over typical SQL identifier limits
        batch = pa.table({long_name: pa.array([1, 2], pa.int64())})
        handle.write(batch)
        assert handle.read().column(long_name).to_pylist() == [1, 2]
