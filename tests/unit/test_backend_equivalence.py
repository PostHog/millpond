"""Cross-backend behaviour + performance equivalence contract.

The two destination backends (DuckLake and Iceberg) are expected to be
swappable at deployment time. main.py only ever talks to a Sink, and a
DuckLake-shaped Sink must behave indistinguishably from an Iceberg-shaped
Sink in the dimensions that matter to a downstream consumer of the lake
(rows land, columns are addressable, types are preserved within a
documented coercion table).

The tests in this file are the forward-looking contract for that
equivalence. Where behaviour SHOULD match, the test is parametrized over
both backends via the `sink_factory` fixture. Where behaviour DIVERGES
INTENTIONALLY (per the Sink Protocol docstring or the schema/iceberg
module commentary), the divergence is named explicitly with two
separate, single-backend tests that lock the actual behaviour. A change
that silently aligns or splits the two surfaces will fail at least one
of these.

If a test reveals a genuine defect, it is marked
`@pytest.mark.xfail(strict=True, reason=...)` rather than fixed
in-place — the goal here is observation, not patching.

Categories:
  1. make_sink dispatch + Protocol cross-backend smoke
  2. Empty-batch behaviour (DIVERGENT — locked separately)
  3. _inserted_at provenance (DIVERGENT)
  4. Partition column emission (DIVERGENT)
  5. Type mapping equivalence + per-backend mapping lock
  6. Round-trip fidelity (parametrized)
  7. Pathological column names / shapes (parametrized)
  8. Pathological values (NaN/Inf/NUL/long/unicode) (parametrized)
  9. Schema evolution semantics + metric parity (parametrized + divergent)
 10. Caller-contract enforcement in main._write_with_retry
 11. Performance smoke contracts (parametrized)

Test infrastructure:
  * DuckLake uses the `ATTACH ':memory:' AS lake` pattern from
    `tests/integration/test_write_integration.py` — no Postgres required.
  * Iceberg uses pyiceberg's `SqlCatalog` against a tmp SQLite file with
    a local-filesystem warehouse — no S3, no testcontainers.

Each backend "Sink-like" handle bundles the connection/catalog, the
table name(s), the caller-owned cache, and its SchemaManager so the
parametrized tests can drive both through a uniform .write(batch)
interface without standing up the full Config-aware Sink wrappers.
"""

from __future__ import annotations

import datetime
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError

from millpond import ducklake as ducklake_mod
from millpond import iceberg as iceberg_mod
from millpond import schema as schema_mod

# ---------------------------------------------------------------------------
# Backend harnesses
# ---------------------------------------------------------------------------


@dataclass
class _BackendHandle:
    """A uniform write-and-read handle over either backend.

    Carries enough state to be both a thin Sink replacement (`.write(batch)`)
    and a tester (`.read_all()` returns an Arrow table of everything that
    landed). Both backends use real local stores; no mocks.
    """

    name: str  # "ducklake" or "iceberg"
    write_fn: Any  # callable(batch: pa.Table) -> None
    read_fn: Any  # callable() -> pa.Table (or None if table absent)
    table_exists_fn: Any  # callable() -> bool
    schema_mgr: Any  # whichever SchemaManager backs this handle
    raw: dict  # backend-specific bag for test-specific pokes


def _make_ducklake_handle(tmp_path) -> _BackendHandle:
    """In-memory DuckDB attached as `lake`, mirroring DuckLakeSink internals
    without requiring a real Postgres catalog."""
    conn = duckdb.connect()
    conn.execute("ATTACH ':memory:' AS lake")
    cache: set[str] = set()
    schema_mgr = schema_mod.SchemaManager(conn, "events")

    def write(batch: pa.Table) -> None:
        ducklake_mod.write(conn, "events", batch, cache, schema_mgr)

    def read() -> pa.Table | None:
        # Pull all source columns + _inserted_at into a single Arrow table
        # in column order. SELECT * is enough because DuckLake adds nothing
        # except _inserted_at.
        if not _table_exists():
            return None
        # fetch_arrow_table is deprecated in DuckDB 1.5 but to_arrow_table
        # isn't a method on the result cursor — pyarrow().to_table works
        # but is more roundabout. Suppress the deprecation warning locally;
        # the existing integration tests use fetch_arrow_table similarly.
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return conn.execute("SELECT * FROM lake.main.events").fetch_arrow_table()

    def _table_exists() -> bool:
        return ducklake_mod._table_exists(conn, "events")

    return _BackendHandle(
        name="ducklake",
        write_fn=write,
        read_fn=read,
        table_exists_fn=_table_exists,
        schema_mgr=schema_mgr,
        raw={"conn": conn, "cache": cache},
    )


def _make_iceberg_handle(tmp_path) -> _BackendHandle:
    """PyIceberg SqlCatalog + filesystem warehouse, mirroring IcebergSink
    internals without requiring S3 or testcontainers."""
    catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    cache: dict = {}
    schema_mgr = iceberg_mod.SchemaManager(catalog, "ns", "events")

    def write(batch: pa.Table) -> None:
        iceberg_mod.write(catalog, "ns", "events", batch, cache, schema_mgr)

    def read() -> pa.Table | None:
        try:
            return catalog.load_table(("ns", "events")).scan().to_arrow()
        except NoSuchTableError:
            return None

    def _table_exists() -> bool:
        try:
            catalog.load_table(("ns", "events"))
            return True
        except NoSuchTableError:
            return False

    return _BackendHandle(
        name="iceberg",
        write_fn=write,
        read_fn=read,
        table_exists_fn=_table_exists,
        schema_mgr=schema_mgr,
        raw={"catalog": catalog, "cache": cache},
    )


@pytest.fixture
def ducklake_handle(tmp_path) -> _BackendHandle:
    h = _make_ducklake_handle(tmp_path)
    yield h
    h.raw["conn"].close()


@pytest.fixture
def iceberg_handle(tmp_path) -> _BackendHandle:
    yield _make_iceberg_handle(tmp_path)


@pytest.fixture(params=["ducklake", "iceberg"])
def handle(request, tmp_path) -> _BackendHandle:
    """Parametrized handle — same test body runs once against each backend."""
    if request.param == "ducklake":
        h = _make_ducklake_handle(tmp_path)
        yield h
        h.raw["conn"].close()
    else:
        yield _make_iceberg_handle(tmp_path)


def _three_row_batch() -> pa.Table:
    """Canonical flat batch used by most equivalence tests."""
    return pa.table(
        {
            "event": pa.array(["click", "view", "scroll"], pa.string()),
            "team_id": pa.array([1, 2, 3], pa.int64()),
        }
    )


# ---------------------------------------------------------------------------
# 1. make_sink dispatch + Protocol cross-backend smoke
# ---------------------------------------------------------------------------


class TestCrossBackendSmoke:
    """The whole point of the Sink abstraction is that swapping
    `cfg.destination` Just Works. Drive both backends through a single
    identical batch via their .write() entry points (skipping the heavy
    Config-aware Sink wrappers — the wiring is covered by
    test_sink.py / test_sink_wrappers.py). Both must accept the batch,
    persist it, and let us read back the source rows."""

    def test_same_batch_lands_on_both_backends(self, ducklake_handle, iceberg_handle):
        batch = _three_row_batch()
        ducklake_handle.write_fn(batch)
        iceberg_handle.write_fn(batch)

        dl = ducklake_handle.read_fn()
        ic = iceberg_handle.read_fn()
        assert dl is not None
        assert ic is not None

        # Source columns and values land identically. Metadata columns
        # diverge intentionally (different sets, different timestamps); the
        # divergence is locked separately below.
        assert dl.column("event").to_pylist() == ["click", "view", "scroll"]
        assert ic.column("event").to_pylist() == ["click", "view", "scroll"]
        assert dl.column("team_id").to_pylist() == [1, 2, 3]
        assert ic.column("team_id").to_pylist() == [1, 2, 3]


# ---------------------------------------------------------------------------
# 2. Empty-batch behaviour (DIVERGENT)
# ---------------------------------------------------------------------------


class TestEmptyBatchDivergence:
    """The Sink protocol explicitly documents this divergence: DuckLake
    creates the table eagerly on any call including len==0; Iceberg short-
    circuits and does not create the table. Both are correct given that
    main.py gates on pending_records > 0. Lock both shapes; also lock that
    main.py honours the contract."""

    def test_ducklake_creates_table_on_empty_batch(self, ducklake_handle):
        empty = pa.table({"a": pa.array([], pa.int64())})
        ducklake_handle.write_fn(empty)
        # Table exists with the source col + DuckLake's _inserted_at.
        out = ducklake_handle.read_fn()
        assert out is not None
        assert out.num_rows == 0
        assert "a" in out.schema.names
        assert "_inserted_at" in out.schema.names

    def test_iceberg_skips_table_on_empty_batch(self, iceberg_handle):
        empty = pa.table({"a": pa.array([], pa.int64())})
        iceberg_handle.write_fn(empty)
        assert iceberg_handle.read_fn() is None
        assert iceberg_handle.table_exists_fn() is False

    def test_main_write_with_retry_does_not_call_write_on_empty_caller_contract(self):
        """main.py is the source of truth for the empty-batch contract.
        The flush trigger is `pending_records > 0`, so a Sink never sees
        an empty batch in steady state. Verify by source inspection that
        the gate exists — if somebody refactors that condition away, the
        divergence above becomes user-visible."""
        import inspect

        from millpond import main

        src = inspect.getsource(main.main)
        assert "pending_records > 0" in src, (
            "main.py must gate flush on pending_records > 0 — both backends "
            "rely on this to never see an empty batch in steady state."
        )


# ---------------------------------------------------------------------------
# 3. _inserted_at provenance (DIVERGENT)
# ---------------------------------------------------------------------------


class TestInsertedAtProvenance:
    """Both backends stamp _inserted_at, but with different semantics:

      * DuckLake uses DuckDB's NOW() at INSERT time — every row may receive
        a slightly different timestamp (microsecond drift across the
        partitioned-insert batches).
      * Iceberg uses Python `datetime.now()` once per batch and broadcasts
        it across every row, so a flush always lands in exactly one
        partition.

    Both encode the same intent ("flush time, not event time") but the
    actual values diverge. Lock both shapes."""

    def test_iceberg_all_rows_share_one_inserted_at(self, iceberg_handle):
        # Larger batch makes the contrast obvious.
        batch = pa.table({"event": [f"e{i}" for i in range(50)]})
        iceberg_handle.write_fn(batch)
        ts = iceberg_handle.read_fn().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 1

    def test_ducklake_all_rows_share_one_inserted_at_via_now(self, ducklake_handle):
        # In practice DuckDB's NOW() returns a single value per statement,
        # so all 50 rows of a single INSERT also land with the same
        # timestamp — but that's an implementation detail of NOW() vs
        # CURRENT_TIMESTAMP / TRANSACTION_TIMESTAMP. Lock the observed
        # behaviour: same statement → same timestamp.
        batch = pa.table({"event": [f"e{i}" for i in range(50)]})
        ducklake_handle.write_fn(batch)
        ts = ducklake_handle.read_fn().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 1

    def test_ducklake_writes_use_distinct_timestamps_across_statements(self, ducklake_handle):
        """Two separate write() calls = two separate INSERTs = two NOW()
        evaluations. The Iceberg side achieves the same property via
        per-batch datetime.now()."""
        ducklake_handle.write_fn(pa.table({"event": ["a"]}))
        # NOW() resolution in DuckDB is microseconds; the second statement
        # is guaranteed to be later as long as the prior one took >0us.
        time.sleep(0.001)
        ducklake_handle.write_fn(pa.table({"event": ["b"]}))
        ts = ducklake_handle.read_fn().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 2

    def test_iceberg_writes_use_distinct_timestamps_across_batches(self, iceberg_handle, monkeypatch):
        """iceberg._now_utc_us truncates to seconds (microsecond=0), so two
        batches within the same wall-clock second naturally share a
        timestamp. To prove the per-batch evaluation behaviour
        deterministically, monkeypatch _now_utc_us to return distinct
        values on successive calls — same shape as the production code
        path, just without the second-resolution flake risk."""
        import datetime as _dt

        ts_iter = iter(
            [
                _dt.datetime(2026, 1, 1, 12, 0, 0, tzinfo=_dt.UTC),
                _dt.datetime(2026, 1, 1, 12, 0, 1, tzinfo=_dt.UTC),
            ]
        )
        monkeypatch.setattr(iceberg_mod, "_now_utc_us", lambda: next(ts_iter))

        iceberg_handle.write_fn(pa.table({"event": ["a"]}))
        iceberg_handle.write_fn(pa.table({"event": ["b"]}))
        ts = iceberg_handle.read_fn().column("_inserted_at").to_pylist()
        assert len(set(ts)) == 2, (
            "Each batch should evaluate _now_utc_us() once — second batch "
            "must get a distinct timestamp value from the first."
        )

    def test_inserted_at_is_tz_aware_on_both_backends(self, handle):
        handle.write_fn(_three_row_batch())
        ts_field = handle.read_fn().schema.field("_inserted_at")
        assert isinstance(ts_field.type, pa.TimestampType)
        assert ts_field.type.tz is not None, (
            f"{handle.name} _inserted_at must be tz-aware; "
            "downstream queries assume UTC."
        )


# ---------------------------------------------------------------------------
# 4. Partition column emission (DIVERGENT)
# ---------------------------------------------------------------------------


class TestPartitionColumnEmission:
    """Iceberg always materialises year/month/day/hour as identity-partition
    columns; DuckLake never does (its partitioning is a caller-provided
    expression, not a column-emission contract). Lock the cardinal facts."""

    def test_iceberg_emits_four_derived_partition_cols(self, iceberg_handle):
        iceberg_handle.write_fn(_three_row_batch())
        names = set(iceberg_handle.read_fn().schema.names)
        assert {"year", "month", "day", "hour"} <= names

    def test_ducklake_does_not_emit_partition_cols(self, ducklake_handle):
        ducklake_handle.write_fn(_three_row_batch())
        names = set(ducklake_handle.read_fn().schema.names)
        # None of Iceberg's derived cols should appear in a DuckLake table
        # unless the source batch happened to carry them by the same name.
        assert names.isdisjoint({"year", "month", "day", "hour"})

    @pytest.mark.parametrize("col_name", ["year", "month", "day", "hour", "_inserted_at"])
    def test_both_backends_raise_early_on_reserved_column_collision(
        self, handle, col_name
    ):
        """Reserved-column-collision contract: if a producer emits a
        column named after a backend-managed metadata column, the Sink
        raises `ValueError` at the `write()` boundary — before any
        backend-specific work — with a uniform error message.

        The reserved set is held identical between backends (DuckLake
        reserves `year/month/day/hour` defensively even though it
        doesn't produce them itself) so a deployment-time destination
        swap doesn't suddenly start accepting or rejecting batches
        based on column-name collisions."""
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
            handle.write_fn(batch)


# ---------------------------------------------------------------------------
# 5. Type mapping equivalence + per-backend mapping lock
# ---------------------------------------------------------------------------


class TestTypeMappingMatrix:
    """Both backends accept arrow types; the SQL-side types differ, but
    the round-trip Python values should match for types in the common
    subset. The full per-backend mapping is locked module-by-module in
    test_schema.py / test_iceberg_schema.py — this class focuses on
    cross-backend equivalence and the documented widening rules."""

    # Date intentionally excluded — see test_date_column_round_trip below
    # (DuckLake date mapping is broken; see xfail).
    _COMMON_SUBSET = {
        "bool": pa.array([True, False, None], pa.bool_()),
        "int32": pa.array([1, -1, 2_147_483_647], pa.int32()),
        "int64": pa.array([1, -1, 2**62], pa.int64()),
        "float32": pa.array([1.5, -1.5, 0.0], pa.float32()),
        "float64": pa.array([1.5, -1.5, 1e300], pa.float64()),
        "string": pa.array(["a", "b", "c"], pa.string()),
    }

    def test_common_subset_round_trips_on_both_backends(self, handle):
        cols = dict(self._COMMON_SUBSET)
        batch = pa.table(cols)
        handle.write_fn(batch)
        out = handle.read_fn()

        # Equality test per column, in Python values. Numeric widening
        # (int32→long on Iceberg) doesn't change the Python int value.
        for name in cols:
            assert out.column(name).to_pylist() == cols[name].to_pylist(), (
                f"{handle.name}: column {name} did not round-trip"
            )

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "schema._arrow_type_to_duckdb keys on str(arrow_type) but "
            "pa.date32() stringifies as 'date32[day]' (not 'date32'), so "
            "DATE columns are silently mapped to VARCHAR by the schema "
            "manager. The eager CREATE TABLE path lands DATE correctly, "
            "but the subsequent evolve() call widens DATE -> VARCHAR. "
            "Real defect symmetric to the float16 issue above."
        ),
    )
    def test_date_column_round_trips_on_ducklake(self, ducklake_handle):
        batch = pa.table(
            {"d": pa.array([18262, 18263, 18264], pa.date32())}
        )
        ducklake_handle.write_fn(batch)
        out = ducklake_handle.read_fn().column("d").to_pylist()
        import datetime as _dt

        assert out == [
            _dt.date(2020, 1, 1),
            _dt.date(2020, 1, 2),
            _dt.date(2020, 1, 3),
        ]

    def test_date_column_round_trips_on_iceberg(self, iceberg_handle):
        batch = pa.table(
            {"d": pa.array([18262, 18263, 18264], pa.date32())}
        )
        iceberg_handle.write_fn(batch)
        out = iceberg_handle.read_fn().column("d").to_pylist()
        import datetime as _dt

        assert out == [
            _dt.date(2020, 1, 1),
            _dt.date(2020, 1, 2),
            _dt.date(2020, 1, 3),
        ]

    def test_ducklake_uint64_max_round_trips(self, ducklake_handle):
        """DuckLake has UBIGINT, so uint64 max survives. Iceberg has only
        signed Long → values > int63 max overflow (see next test)."""
        max_u64 = (1 << 64) - 1
        batch = pa.table({"big": pa.array([max_u64], pa.uint64())})
        ducklake_handle.write_fn(batch)
        assert ducklake_handle.read_fn().column("big").to_pylist() == [max_u64]

    def test_iceberg_uint64_above_int63_max_raises(self, iceberg_handle):
        """Locked divergence: iceberg.py docs explicitly call out the
        data-loss risk for uint64 > 2^63-1 (no UnsignedLongType in
        Iceberg). The actual write raises rather than silently
        truncating, which is the right failure mode — lock it."""
        too_big = (1 << 64) - 1
        batch = pa.table({"big": pa.array([too_big], pa.uint64())})
        # PyIceberg surfaces this as a struct.pack ValueError from
        # Arrow→Parquet's "q" (int64) format. The specific class isn't
        # the contract — the contract is "this fails loudly, not silently".
        with pytest.raises(Exception):
            iceberg_handle.write_fn(batch)

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "schema._arrow_type_to_duckdb keys on str(arrow_type) but "
            "pa.float16() stringifies as 'halffloat' (not 'float16'), so "
            "float16 falls through to VARCHAR on DuckLake. Iceberg "
            "correctly maps float16 to FloatType via is_float16(). Real "
            "divergence + real defect in the DuckLake mapping."
        ),
    )
    def test_float16_maps_consistently_on_both_backends(self):
        # The Iceberg side returns FloatType — i.e. a numeric type — so
        # the parity check is "DuckLake also returns a numeric type."
        from millpond.iceberg import _arrow_to_iceberg
        from millpond.schema import _arrow_type_to_duckdb

        dl_type = _arrow_type_to_duckdb(pa.float16())
        ic_type = _arrow_to_iceberg(pa.float16())
        # Iceberg side is numeric.
        from pyiceberg.types import FloatType

        assert isinstance(ic_type, FloatType)
        # DuckLake side should also be numeric (FLOAT or DOUBLE).
        assert dl_type in ("FLOAT", "DOUBLE"), (
            f"DuckLake mapped float16 to {dl_type!r}, expected FLOAT/DOUBLE"
        )

    def test_pa_null_columns_filtered_at_converter_so_backends_never_see_them(self):
        """`pa.null()`-typed columns are dropped by `arrow_converter.convert()`
        before any Sink sees them — that's the contract that gives the two
        backends uniform behaviour for an Arrow type Iceberg v2 rejects
        outright.

        In normal use `_build_schema` falls back to `pa.string()` for keys
        that are None in every record, so `pa.null()` shouldn't appear via
        the converter. This test pushes a record where the only key has a
        None value to exercise the all-null path end-to-end, then asserts
        the resulting table has no `pa.null()` columns. Test the contract
        at the converter, not the Sinks."""
        from millpond.arrow_converter import _drop_null_typed_columns, convert

        # End-to-end: a record where the only field is None.
        table = convert([b'{"only_null": null, "real": "x"}'])
        assert table is not None
        assert all(not pa.types.is_null(f.type) for f in table.schema), (
            "convert() must not emit pa.null() columns"
        )

        # Defensive layer: if a pa.null() column somehow appears in an
        # Arrow Table fed to _drop_null_typed_columns directly, it gets
        # dropped. (Both Sinks would otherwise diverge: DuckLake stores,
        # Iceberg raises.)
        synth = pa.table(
            {"all_null": pa.array([None, None], pa.null()), "kept": ["x", "y"]}
        )
        filtered = _drop_null_typed_columns(synth)
        assert filtered.column_names == ["kept"]


# ---------------------------------------------------------------------------
# 6. Round-trip fidelity (parametrized)
# ---------------------------------------------------------------------------


class TestRoundTripFidelity:
    """For values that should round-trip identically up to documented
    coercions, prove it does on both backends."""

    def test_all_null_typed_int_column_preserves_nulls(self, handle):
        batch = pa.table(
            {
                "event": pa.array(["x", "y"], pa.string()),
                "score": pa.array([None, None], pa.int64()),
            }
        )
        handle.write_fn(batch)
        assert handle.read_fn().column("score").to_pylist() == [None, None]

    def test_mixed_some_null_some_present(self, handle):
        batch = pa.table(
            {
                "event": pa.array(["a", None, "c"], pa.string()),
                "team_id": pa.array([1, None, 3], pa.int64()),
            }
        )
        handle.write_fn(batch)
        out = handle.read_fn()
        assert out.column("event").to_pylist() == ["a", None, "c"]
        assert out.column("team_id").to_pylist() == [1, None, 3]

    def test_two_writes_accumulate(self, handle):
        handle.write_fn(_three_row_batch())
        handle.write_fn(_three_row_batch())
        assert handle.read_fn().num_rows == 6

    def test_post_arrow_converter_json_stringified_nested(self, handle):
        """arrow_converter JSON-stringifies nested dicts before they hit
        the sink. By the time a batch arrives, a struct column is just a
        VARCHAR. Lock that we preserve the bytes."""
        json_blob = '{"k":1,"nested":{"a":[1,2,3]}}'
        batch = pa.table(
            {"event": ["x"], "payload": pa.array([json_blob], pa.string())}
        )
        handle.write_fn(batch)
        assert handle.read_fn().column("payload").to_pylist() == [json_blob]


# ---------------------------------------------------------------------------
# 7. Pathological column names / shapes (parametrized)
# ---------------------------------------------------------------------------


class TestPathologicalColumnNames:
    def test_one_column_batch(self, handle):
        batch = pa.table({"only": pa.array([1, 2, 3], pa.int64())})
        handle.write_fn(batch)
        assert handle.read_fn().column("only").to_pylist() == [1, 2, 3]

    def test_many_columns_smoke(self, handle):
        # Iceberg's create_table with 1000 columns is measurable; cap at
        # 500 for the parametrized test to keep CI total under the
        # performance budget.
        cols = {f"c{i}": pa.array([i], pa.int64()) for i in range(500)}
        handle.write_fn(pa.table(cols))
        out = handle.read_fn()
        assert "c0" in out.schema.names
        assert "c499" in out.schema.names

    def test_long_column_name_accepted(self, handle):
        long_name = "a" * 200  # well over typical SQL identifier limits
        batch = pa.table({long_name: pa.array([1, 2], pa.int64())})
        handle.write_fn(batch)
        assert handle.read_fn().column(long_name).to_pylist() == [1, 2]

    @pytest.mark.parametrize(
        "bad_name",
        [
            "événement",  # non-ASCII Unicode
            "1starts_with_digit",
            "has space",
            "has-dash",
            "drop; --",
        ],
    )
    def test_unsafe_field_names_skipped_by_schema_manager(self, handle, bad_name):
        """Both backends' SchemaManagers gate on the same regex
        ^[a-zA-Z_][a-zA-Z0-9_]*$. They should both refuse to ADD the
        column rather than risk SQL injection or pyiceberg parser
        errors. Lock the regex behaviour at the manager layer."""
        # Seed the table with a safe shape first so the schema manager
        # has something to compare against. (Iceberg's SchemaManager
        # returns early when the table doesn't exist yet — we want to
        # exercise the post-existence path.)
        handle.write_fn(_three_row_batch())

        # Now arrive with the bad name in the schema. Iceberg's
        # SchemaManager.evolve receives a pa.Schema; DuckLake's
        # signature is the same. Drive both directly to avoid the
        # iceberg.write defensive short-circuit.
        bad_schema = pa.schema(
            [
                pa.field("event", pa.string()),
                pa.field("team_id", pa.int64()),
                pa.field(bad_name, pa.string()),
            ]
        )
        handle.schema_mgr.evolve(bad_schema)

        out = handle.read_fn()
        assert bad_name not in out.schema.names, (
            f"{handle.name}: unsafe field name {bad_name!r} leaked into the table"
        )


# ---------------------------------------------------------------------------
# 8. Pathological values (parametrized)
# ---------------------------------------------------------------------------


class TestPathologicalValues:
    def test_max_int64_round_trips(self, handle):
        big = (1 << 63) - 1
        small = -(1 << 63)
        batch = pa.table({"x": pa.array([big, small, 0], pa.int64())})
        handle.write_fn(batch)
        assert handle.read_fn().column("x").to_pylist() == [big, small, 0]

    def test_nan_and_inf_round_trip(self, handle):
        batch = pa.table(
            {
                "f": pa.array(
                    [float("inf"), float("-inf"), float("nan"), 0.0],
                    pa.float64(),
                )
            }
        )
        handle.write_fn(batch)
        out = handle.read_fn().column("f").to_pylist()
        # NaN doesn't equal itself; compare positionally.
        assert out[0] == float("inf")
        assert out[1] == float("-inf")
        assert out[2] != out[2]  # NaN
        assert out[3] == 0.0

    def test_empty_string_round_trips(self, handle):
        batch = pa.table({"s": pa.array(["", "x", ""], pa.string())})
        handle.write_fn(batch)
        assert handle.read_fn().column("s").to_pylist() == ["", "x", ""]

    def test_embedded_nul_byte_in_string(self, handle):
        # NUL inside a VARCHAR is a routine pathology when JSON came from
        # a binary-tinted upstream. Both backends should preserve.
        batch = pa.table({"s": pa.array(["a\x00b", "no\x00ul"], pa.string())})
        handle.write_fn(batch)
        assert handle.read_fn().column("s").to_pylist() == ["a\x00b", "no\x00ul"]

    def test_unicode_value_round_trip(self, handle):
        batch = pa.table({"s": pa.array(["日本語", "café", "🦆"], pa.string())})
        handle.write_fn(batch)
        assert handle.read_fn().column("s").to_pylist() == ["日本語", "café", "🦆"]

    def test_very_long_string_value(self, handle):
        big = "x" * 100_000
        batch = pa.table({"s": pa.array(["small", big], pa.string())})
        handle.write_fn(batch)
        got = handle.read_fn().column("s").to_pylist()
        assert got[0] == "small"
        assert len(got[1]) == 100_000


# ---------------------------------------------------------------------------
# 9. Schema evolution semantics + metric parity (parametrized + divergent)
# ---------------------------------------------------------------------------


class TestSchemaEvolutionParity:
    """Both backends share the schema-evolution use cases (add column,
    widen, reject narrowing). The metric names emitted should match. The
    exception-handling semantics on commit failure DIVERGE — locked
    separately below."""

    def test_add_column_works_on_both(self, handle):
        handle.write_fn(_three_row_batch())
        # Second write introduces a new column.
        batch2 = pa.table(
            {
                "event": ["x"],
                "team_id": pa.array([4], pa.int64()),
                "country": ["US"],
            }
        )
        handle.write_fn(batch2)
        names = set(handle.read_fn().schema.names)
        assert "country" in names

    def test_add_column_increments_schema_columns_added(self, handle):
        handle.write_fn(_three_row_batch())
        # Patch the metrics module that *the backend imports* — DuckLake
        # imports `from millpond import metrics`, Iceberg the same.
        with patch(f"millpond.{handle.name if handle.name=='iceberg' else 'schema'}.metrics") as mock_metrics:
            handle.write_fn(
                pa.table(
                    {
                        "event": ["x"],
                        "team_id": pa.array([4], pa.int64()),
                        "country": ["US"],
                    }
                )
            )
            # Both must call schema_columns_added_total.inc() at least once.
            assert mock_metrics.schema_columns_added_total.inc.called, (
                f"{handle.name} did not increment schema_columns_added_total on column add"
            )

    def test_widen_int_increments_schema_columns_widened(self, handle):
        # Seed with int32 → IntegerType / INTEGER on each side.
        handle.write_fn(
            pa.table(
                {
                    "event": ["x"],
                    "team_id": pa.array([1], pa.int32()),
                }
            )
        )
        # Force the schema manager to know the current state. The Iceberg
        # SchemaManager re-loads on its own; the DuckLake one too.
        with patch(f"millpond.{handle.name if handle.name=='iceberg' else 'schema'}.metrics") as mock_metrics:
            handle.write_fn(
                pa.table(
                    {
                        "event": ["x"],
                        "team_id": pa.array([1], pa.int64()),
                    }
                )
            )
            assert mock_metrics.schema_columns_widened_total.inc.called, (
                f"{handle.name} did not increment schema_columns_widened_total on int32→int64"
            )

    def test_unsafe_field_name_increments_records_skipped(self, handle):
        handle.write_fn(_three_row_batch())
        target = "iceberg" if handle.name == "iceberg" else "schema"
        with patch(f"millpond.{target}.metrics") as mock_metrics:
            handle.schema_mgr.evolve(
                pa.schema(
                    [
                        pa.field("event", pa.string()),
                        pa.field("team_id", pa.int64()),
                        pa.field("évil", pa.string()),  # unsafe (non-ASCII)
                    ]
                )
            )
            mock_metrics.records_skipped_total.labels.assert_called_with(
                reason="unsafe_field_name"
            )

    def test_repeated_evolution_back_and_forth(self, handle):
        """A batch with col A, then col A+B, then col A again (no col B
        in the new arrival). The schema must still hold both columns —
        evolution is additive, dropping is not supported on either side."""
        handle.write_fn(pa.table({"event": ["a"]}))
        handle.write_fn(pa.table({"event": ["b"], "extra": ["x"]}))
        handle.write_fn(pa.table({"event": ["c"]}))
        names = set(handle.read_fn().schema.names)
        assert "extra" in names

    def test_no_change_does_not_increment_added(self, handle):
        handle.write_fn(_three_row_batch())
        target = "iceberg" if handle.name == "iceberg" else "schema"
        with patch(f"millpond.{target}.metrics") as mock_metrics:
            handle.write_fn(_three_row_batch())
            mock_metrics.schema_columns_added_total.inc.assert_not_called()


class TestSchemaEvolutionErrorDivergence:
    """DuckLake.SchemaManager swallows per-column ALTER failures
    (log + metric, then keep evolving the other columns). Iceberg's
    SchemaManager wraps all changes in one update_schema() transaction
    and RE-RAISES on commit failure (per the module docstring: "the
    write-retry loop in main.py then invalidates caches and retries").

    Both bump errors_total{type="schema"} before either reaches its
    branch. Lock the actual divergence."""

    def test_ducklake_swallows_per_column_failure(self, ducklake_handle):
        """A failed ALTER on one column doesn't stop the next ALTER.

        DuckDBPyConnection objects don't allow attribute reassignment, so
        we wrap the SchemaManager's conn reference instead — the manager
        only uses it via `._conn.execute(...)`, which is exactly the
        attribute we need to intercept."""
        # Seed.
        ducklake_handle.write_fn(_three_row_batch())
        real_conn = ducklake_handle.raw["conn"]

        def execute_side_effect(sql, *a, **kw):
            if "ADD COLUMN" in sql and "a_extra" in sql:
                raise duckdb.Error("simulated transient failure")
            return real_conn.execute(sql, *a, **kw)

        wrapped = MagicMock(wraps=real_conn)
        wrapped.execute = execute_side_effect

        # Swap in the wrapped conn just for this evolve() call.
        ducklake_handle.schema_mgr._conn = wrapped
        try:
            with patch("millpond.schema.metrics"):
                ducklake_handle.schema_mgr.evolve(
                    pa.schema(
                        [
                            pa.field("event", pa.string()),
                            pa.field("team_id", pa.int64()),
                            pa.field("a_extra", pa.string()),
                            pa.field("b_extra", pa.string()),
                        ]
                    )
                )
        finally:
            ducklake_handle.schema_mgr._conn = real_conn

        names = set(ducklake_handle.read_fn().schema.names)
        assert "b_extra" in names, (
            "DuckLake.SchemaManager.evolve must continue past a "
            "per-column failure (locks the swallow-and-continue semantic)."
        )
        assert "a_extra" not in names, (
            "The failed column should NOT have been added."
        )

    def test_iceberg_reraises_on_commit_failure(self, iceberg_handle):
        """Iceberg's evolve re-raises after invalidate+metric on
        update_schema failure; main.py's retry path is responsible for
        recovery. Simulate by patching the catalog's load_table to raise
        during the evolve transaction."""
        from unittest.mock import patch as _patch

        iceberg_handle.write_fn(_three_row_batch())
        # Pre-init the manager so evolve doesn't return early.
        iceberg_handle.schema_mgr.evolve(
            pa.schema(
                [pa.field("event", pa.string()), pa.field("team_id", pa.int64())]
            )
        )

        catalog = iceberg_handle.raw["catalog"]
        real_load = catalog.load_table

        def explode(_ident):
            raise RuntimeError("simulated catalog blip during update")

        with _patch.object(catalog, "load_table", side_effect=explode):
            with _patch("millpond.iceberg.metrics") as mock_metrics:
                with pytest.raises(Exception):
                    iceberg_handle.schema_mgr.evolve(
                        pa.schema(
                            [
                                pa.field("event", pa.string()),
                                pa.field("team_id", pa.int64()),
                                pa.field("user_id", pa.string()),
                            ]
                        )
                    )
                # errors_total{type=schema} fired before re-raise.
                mock_metrics.errors_total.labels.assert_any_call(type="schema")
        # And the manager invalidated itself for the next retry.
        assert iceberg_handle.schema_mgr._initialized is False

        # Restore real load_table so teardown can use it.
        catalog.load_table = real_load  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# 10. Caller-contract enforcement in main._write_with_retry
# ---------------------------------------------------------------------------


class TestWriteRetryHonoursSinkContract:
    """_write_with_retry must treat both Sinks identically: call
    .write(batch), on failure call .reset_caches() and retry up to
    _WRITE_MAX_RETRIES. This is parity at the orchestrator layer."""

    @pytest.mark.parametrize("backend", ["ducklake", "iceberg"])
    def test_reset_caches_called_after_failure(self, backend):
        """Mock Sink with the .write/.reset_caches/.close spec, prove
        _write_with_retry behaves identically regardless of which Sink
        implementation is plugged in."""
        from millpond.main import _write_with_retry

        sink = MagicMock(spec=["write", "reset_caches", "close"])
        sink.write.side_effect = [RuntimeError("boom"), None]
        batch = pa.table({"a": [1]})
        with patch("millpond.main.time"):
            _write_with_retry(sink, batch)
        # reset_caches was invoked between the failed and successful
        # attempts — both backends rely on this contract.
        assert sink.reset_caches.call_count == 1
        assert sink.write.call_count == 2


# ---------------------------------------------------------------------------
# 11. Performance smoke contracts (parametrized)
# ---------------------------------------------------------------------------


class TestPerformanceContracts:
    """Conservative wall-clock bounds — the goal is catching a 100x
    regression, not a 10% one. These are NOT benchmarks; they are
    "did something go horribly wrong" smoke tests. Tuned so a healthy
    laptop or CI runner finishes well under the threshold."""

    def test_first_write_to_empty_catalog_completes_under_10s(self, handle):
        """Create-table + first append is the slowest path on each
        backend (catalog round-trips, schema-sample building, partition
        spec, etc.). 10s is a generous ceiling — production typically
        sees <1s. If it ever hits the cap, something has gone wrong."""
        start = time.monotonic()
        handle.write_fn(_three_row_batch())
        elapsed = time.monotonic() - start
        assert elapsed < 10.0, (
            f"{handle.name} first write took {elapsed:.2f}s, "
            "expected <10s. Something is wrong with create-table/append."
        )

    def test_subsequent_write_completes_under_5s(self, handle):
        """Warm-path append: table exists, schema unchanged, cache hot.
        Should be much faster than first write. 5s is again a smoke
        ceiling, not a benchmark target."""
        handle.write_fn(_three_row_batch())  # warm
        start = time.monotonic()
        handle.write_fn(_three_row_batch())
        elapsed = time.monotonic() - start
        assert elapsed < 5.0, (
            f"{handle.name} warm-path write took {elapsed:.2f}s, expected <5s."
        )

    def test_100_row_batch_writes_under_5s(self, handle):
        batch = pa.table(
            {
                "event": [f"e{i}" for i in range(100)],
                "team_id": pa.array(list(range(100)), pa.int64()),
            }
        )
        start = time.monotonic()
        handle.write_fn(batch)
        elapsed = time.monotonic() - start
        assert elapsed < 5.0, (
            f"{handle.name} 100-row write took {elapsed:.2f}s, expected <5s."
        )
