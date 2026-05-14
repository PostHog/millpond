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


@pytest.fixture(autouse=True)
def _clear_cache():
    """Each test gets a fresh module-level table cache."""
    iceberg.reset_table_cache()
    yield
    iceberg.reset_table_cache()


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

        # Schema gained _inserted_at + 4 partition cols, in order.
        added = [out.schema.field(i).name for i in range(2, 7)]
        assert added == ["_inserted_at", "year", "month", "day", "hour"]

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
    def test_creates_new_table_with_partition_spec(self, catalog):
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        assert table.spec().fields  # has a non-trivial partition spec
        assert [f.name for f in table.spec().fields] == list(iceberg.PARTITION_COLS)

    def test_caches_subsequent_calls(self, catalog):
        t1 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        t2 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        # Same Python object — cache hit, no second load_table round trip.
        assert t1 is t2

    def test_loads_existing_table(self, catalog):
        # Pre-create via PyIceberg directly to simulate "another pod made
        # it"; then _ensure_table on a fresh cache should load, not create.
        iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        iceberg.reset_table_cache()
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        assert table.name() == ("ns", "events")

    def test_falls_back_to_load_when_create_loses_race(self, catalog, monkeypatch):
        # Pre-create via the real path so load_table will succeed.
        iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        iceberg.reset_table_cache()
        # Now patch create_table to raise as if another pod beat us to it
        # between our load_table miss and our create attempt.
        real_load = catalog.load_table

        def raise_create(*_a, **_kw):
            raise RuntimeError("simulated commit conflict")

        # _ensure_table calls load_table first; force the cache-miss path
        # by patching load_table to raise once, then succeed.
        load_calls = {"n": 0}

        def load_then_succeed(identifier):
            load_calls["n"] += 1
            if load_calls["n"] == 1:
                raise RuntimeError("simulated initial miss")
            return real_load(identifier)

        monkeypatch.setattr(catalog, "load_table", load_then_succeed)
        monkeypatch.setattr(catalog, "create_table", raise_create)
        table = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        assert table is not None
        assert load_calls["n"] == 2  # initial miss + fallback after create_table raised


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


class TestWrite:
    def test_appends_rows_with_metadata_cols(self, catalog, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        iceberg.write(catalog, "ns", "events", _sample_batch())

        table = catalog.load_table(("ns", "events"))
        df = table.scan().to_arrow()
        # Source rows landed, plus the metadata columns we manage.
        assert df.num_rows == 3
        assert set(df.column_names) >= {"event", "team_id", "_inserted_at", "year", "month", "day", "hour"}
        assert df.column("year").to_pylist() == [2026, 2026, 2026]
        assert df.column("hour").to_pylist() == [14, 14, 14]

    def test_empty_batch_is_noop(self, catalog):
        empty = pa.table({"event": pa.array([], pa.string()), "team_id": pa.array([], pa.int64())})
        # Must NOT create the table for a zero-row batch — empty commit is wasted work.
        iceberg.write(catalog, "ns", "events", empty)
        # If write created the table, load_table would succeed. It shouldn't.
        with pytest.raises(Exception):
            catalog.load_table(("ns", "events"))

    def test_multiple_writes_accumulate(self, catalog, monkeypatch):
        fixed = datetime.datetime(2026, 5, 13, 14, 30, 0, tzinfo=datetime.UTC)
        monkeypatch.setattr(iceberg, "_now_utc_us", lambda: fixed)

        iceberg.write(catalog, "ns", "events", _sample_batch())
        iceberg.write(catalog, "ns", "events", _sample_batch())

        df = catalog.load_table(("ns", "events")).scan().to_arrow()
        assert df.num_rows == 6


# ---------------------------------------------------------------------------
# reset_table_cache
# ---------------------------------------------------------------------------


class TestResetTableCache:
    def test_clears_cache(self, catalog):
        t1 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        iceberg.reset_table_cache()
        t2 = iceberg._ensure_table(catalog, "ns", "events", _sample_batch())
        # New Python object after cache reset (load_table returns a fresh
        # Table instance each call).
        assert t1 is not t2
