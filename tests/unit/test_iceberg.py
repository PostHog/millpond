"""Unit tests for millpond/iceberg.py.

Uses PyIceberg's ``SqlCatalog`` against a temp SQLite file as a real
catalog backend so the tests exercise the actual ``create_table`` /
``load_table`` / ``Table.append`` paths rather than mocking them. The
warehouse is a temp directory on the local filesystem — no S3, no
Docker, no REST fixture.
"""

from __future__ import annotations

import datetime

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import IdentityTransform

from millpond import iceberg


@pytest.fixture
def catalog(tmp_path):
    """A SqlCatalog backed by a temp SQLite file with a temp warehouse dir."""
    cat = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path}/cat.db",
            "warehouse": f"file://{tmp_path}/warehouse",
        },
    )
    yield cat


@pytest.fixture
def cache() -> dict:
    """Per-test caller-owned ensure cache (formerly module-level `_tables_ensured`)."""
    return {}


def _sample_batch() -> pa.Table:
    """Three-row sample with a string and an int column — mimics post-JSON-conversion shape."""
    return pa.table({"event": ["click", "view", "scroll"], "team_id": [1, 2, 3]})


# ---------------------------------------------------------------------------
# _add_metadata_columns / _schema_sample
# ---------------------------------------------------------------------------


class TestAddMetadataColumns:
    def test_appends_inserted_at_and_partition_cols(self, monkeypatch):
        # Pin "now" so the derived year/month/day/hour are predictable.
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)
        out = iceberg._add_metadata_columns(_sample_batch())

        # Schema gained _inserted_at + 4 partition cols, in order (appended
        # to whatever the source schema had — slice the trailing 5 so we
        # don't depend on _sample_batch()'s column count).
        assert out.schema.names[-5:] == ["_inserted_at", "year", "month", "day", "hour"]

        # Derived values match the pinned timestamp.
        assert out.column("year").to_pylist() == [2026, 2026, 2026]
        assert out.column("month").to_pylist() == [5, 5, 5]
        assert out.column("day").to_pylist() == [13, 13, 13]
        assert out.column("hour").to_pylist() == [14, 14, 14]

    def test_partition_cols_are_int32(self):
        out = iceberg._add_metadata_columns(_sample_batch())
        for name in iceberg.PARTITION_COLS:
            assert out.schema.field(name).type == pa.int32(), (
                f"{name} must be int32 — Iceberg's IdentityTransform on int64 would still work "
                "but we lock the width so the on-disk layout doesn't depend on PyArrow's "
                "default return type of pc.year/month/day/hour."
            )

    def test_inserted_at_is_utc(self):
        out = iceberg._add_metadata_columns(_sample_batch())
        ts_type = out.schema.field("_inserted_at").type
        assert isinstance(ts_type, pa.TimestampType)
        assert ts_type.tz == "UTC"

    def test_all_rows_share_one_timestamp(self):
        # A flush must land in exactly one partition, not get split by
        # the microsecond drift of sequential per-row timestamps.
        out = iceberg._add_metadata_columns(_sample_batch())
        ts_values = out.column("_inserted_at").to_pylist()
        assert len(set(ts_values)) == 1


class TestSchemaSample:
    def test_has_zero_rows_and_correct_columns(self):
        sample = iceberg._schema_sample(_sample_batch())
        assert sample.num_rows == 0
        names = [sample.schema.field(i).name for i in range(sample.num_columns)]
        # source cols first, then metadata cols in fixed order
        assert names == ["event", "team_id", "_inserted_at", "year", "month", "day", "hour"]

    def test_metadata_col_dtypes_match_real_writes(self, monkeypatch):
        # If _schema_sample's dtypes drift from _add_metadata_columns' output,
        # create_table will declare types that don't match what Table.append
        # will try to write, and the first real write fails. Lock them.
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)
        sample = iceberg._schema_sample(_sample_batch())
        real = iceberg._add_metadata_columns(_sample_batch())
        for name in ("_inserted_at", "year", "month", "day", "hour"):
            assert sample.schema.field(name).type == real.schema.field(name).type


# ---------------------------------------------------------------------------
# _build_partition_spec
# ---------------------------------------------------------------------------


class TestBuildPartitionSpec:
    def test_four_identity_fields(self):
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
        from pyiceberg.schema import assign_fresh_schema_ids

        ice_schema = assign_fresh_schema_ids(
            _pyarrow_to_schema_without_ids(iceberg._schema_sample(_sample_batch()).schema)
        )
        spec = iceberg._build_partition_spec(ice_schema)

        assert isinstance(spec, PartitionSpec)
        assert len(spec.fields) == 4
        assert [f.name for f in spec.fields] == list(iceberg.PARTITION_COLS)
        for f in spec.fields:
            assert isinstance(f.transform, IdentityTransform)

    def test_partition_field_ids_are_distinct_from_column_ids(self):
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
        from pyiceberg.schema import assign_fresh_schema_ids

        ice_schema = assign_fresh_schema_ids(
            _pyarrow_to_schema_without_ids(iceberg._schema_sample(_sample_batch()).schema)
        )
        spec = iceberg._build_partition_spec(ice_schema)
        column_ids = {f.field_id for f in ice_schema.fields}
        partition_ids = {f.field_id for f in spec.fields}
        # Iceberg field IDs are catalog-wide unique; partition fields can't
        # share IDs with columns or we'll get conflicting metadata.
        assert column_ids.isdisjoint(partition_ids)


# ---------------------------------------------------------------------------
# _ensure_table
# ---------------------------------------------------------------------------


class TestEnsureTable:
    def test_creates_new_table_with_partition_spec(self, catalog, cache):
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        assert table.spec().fields  # has a non-trivial partition spec
        assert [f.name for f in table.spec().fields] == list(iceberg.PARTITION_COLS)

    def test_caches_subsequent_calls(self, catalog, cache):
        t1 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        t2 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        # Same Python object — cache hit, no second load_table round trip.
        assert t1 is t2

    def test_loads_existing_table(self, catalog, cache):
        # Pre-create via the real path; then a fresh cache should load,
        # not create.
        iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        cache.clear()
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        assert table.name() == ("ns", "events")

    def test_falls_back_to_load_when_create_loses_race(self, catalog, cache, monkeypatch):
        from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError

        # Pre-create via the real path so a real load_table will succeed.
        iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        cache.clear()
        # Patch load_table to raise NoSuchTableError once (forcing the
        # create path) then succeed; patch create_table to raise the
        # narrow already-exists signal we catch.
        real_load = catalog.load_table

        def raise_create(*_a, **_kw):
            raise TableAlreadyExistsError("simulated race")

        load_calls = {"n": 0}

        def load_then_succeed(identifier):
            load_calls["n"] += 1
            if load_calls["n"] == 1:
                raise NoSuchTableError("simulated initial miss")
            return real_load(identifier)

        monkeypatch.setattr(catalog, "load_table", load_then_succeed)
        monkeypatch.setattr(catalog, "create_table", raise_create)
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        assert table is not None
        assert load_calls["n"] == 2  # initial miss + fallback after create_table raised


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_appends_rows_with_metadata_cols(self, catalog, cache, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        iceberg.write(catalog, "ns", "events", _sample_batch(), cache)

        table = catalog.load_table(("ns", "events"))
        df = table.scan().to_arrow()
        # Source rows landed, plus the metadata columns we manage.
        assert df.num_rows == 3
        assert set(df.column_names) >= {"event", "team_id", "_inserted_at", "year", "month", "day", "hour"}
        assert df.column("year").to_pylist() == [2026, 2026, 2026]
        assert df.column("hour").to_pylist() == [14, 14, 14]

    def test_empty_batch_is_noop(self, catalog, cache):
        from pyiceberg.exceptions import NoSuchTableError

        empty = pa.table({"event": pa.array([], pa.string()), "team_id": pa.array([], pa.int64())})
        # Per the Sink contract, write() shouldn't be called with len==0; the
        # backend defensively skips the catalog round trip and does NOT create
        # the table. (DuckLake takes the opposite path — see Sink protocol docs.)
        iceberg.write(catalog, "ns", "events", empty, cache)
        with pytest.raises(NoSuchTableError):
            catalog.load_table(("ns", "events"))

    def test_multiple_writes_accumulate(self, catalog, cache, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        iceberg.write(catalog, "ns", "events", _sample_batch(), cache)
        iceberg.write(catalog, "ns", "events", _sample_batch(), cache)

        df = catalog.load_table(("ns", "events")).scan().to_arrow()
        assert df.num_rows == 6


# ---------------------------------------------------------------------------
# cache lifecycle
# ---------------------------------------------------------------------------


class TestRealisticPayloads:
    """Production batches are not always flat-and-tidy. Cover the shapes
    that arrow_converter actually produces:
      * nullable columns with some/all NULL values
      * mixed-width ints
      * timestamps both with and without tz
      * all-null columns (Arrow may yield `null` type when every JSON value is None)
    """

    def test_nullable_column_with_some_nulls(self, catalog, cache, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        batch = pa.table(
            {
                "event": ["click", None, "view"],
                "team_id": pa.array([1, None, 3], pa.int64()),
            }
        )
        iceberg.write(catalog, "ns", "events", batch, cache)
        df = catalog.load_table(("ns", "events")).scan().to_arrow()
        assert df.num_rows == 3
        # Null preservation: the None entries land as NULL, not coerced.
        assert df.column("event").to_pylist() == ["click", None, "view"]
        assert df.column("team_id").to_pylist() == [1, None, 3]

    def test_all_null_int_column(self, catalog, cache, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        batch = pa.table(
            {
                "event": ["x", "y"],
                "score": pa.array([None, None], pa.int64()),  # all-null, typed
            }
        )
        iceberg.write(catalog, "ns", "events", batch, cache)
        df = catalog.load_table(("ns", "events")).scan().to_arrow()
        assert df.column("score").to_pylist() == [None, None]

    def test_mixed_int_widths(self, catalog, cache, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        batch = pa.table(
            {
                "small": pa.array([1, 2], pa.int16()),
                "regular": pa.array([10, 20], pa.int32()),
                "big": pa.array([100, 200], pa.int64()),
            }
        )
        iceberg.write(catalog, "ns", "events", batch, cache)
        df = catalog.load_table(("ns", "events")).scan().to_arrow()
        # int16 and int32 widen to int32 on the Iceberg side (IntegerType);
        # int64 stays LongType. Values land unchanged.
        assert df.column("small").to_pylist() == [1, 2]
        assert df.column("regular").to_pylist() == [10, 20]
        assert df.column("big").to_pylist() == [100, 200]

    def test_timestamp_without_tz_maps_to_timestamp_type(self):
        # _arrow_to_iceberg distinguishes naive (TimestampType) from
        # tz-aware (TimestamptzType). Production payloads usually carry tz,
        # but some sources emit naive — verify both paths.
        from pyiceberg.types import TimestampType, TimestamptzType

        from millpond.iceberg import _arrow_to_iceberg

        assert isinstance(_arrow_to_iceberg(pa.timestamp("us")), TimestampType)
        assert isinstance(_arrow_to_iceberg(pa.timestamp("us", tz="UTC")), TimestamptzType)


class TestCacheLifecycle:
    def test_clearing_cache_forces_reload(self, catalog, cache):
        t1 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        cache.clear()
        t2 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache)
        # New Python object after cache reset (load_table returns a fresh
        # Table instance each call).
        assert t1 is not t2

    def test_separate_caches_do_not_interfere(self, catalog):
        # Two independent caches → two independent Table refs even for the
        # same catalog identifier. Proves caches are caller-owned.
        cache_a: dict = {}
        cache_b: dict = {}
        ta = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache_a)
        tb = iceberg._ensure_table(catalog, "ns", "events", _sample_batch(), cache_b)
        assert ta is not tb
        assert "ns.events" in cache_a
        assert "ns.events" in cache_b
