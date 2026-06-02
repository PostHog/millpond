"""PyIceberg version-pin canary.

Both `millpond/iceberg.py` and the icebox modules depend on private
PyIceberg symbols. PyIceberg is pre-1.0 and rearranges internals between
minor versions; these tests fail loudly on any drift so we can
revalidate the affected modules deliberately rather than discovering
the breakage on first production commit.

Covered:

  * Schema field-ID assignment helpers (writer-side):
    - ``pyiceberg.io.pyarrow._pyarrow_to_schema_without_ids``
    - ``pyiceberg.schema.assign_fresh_schema_ids``

  * DataFile construction (committer-side):
    - ``DataFile.from_args(_table_format_version=..., **kwargs)``
    - DataFile's positional `_data` shape
    - ``DataFileContent``, ``FileFormat`` enums

  * Snapshot-producer / commit (committer-side):
    - ``Transaction._append_snapshot_producer(snapshot_properties=, branch=)``
    - producer ``.append_data_file`` exists

  * Single-value-serialization for bounds (committer-side):
    - ``pyiceberg.conversions.to_bytes(iceberg_type, value)``
    - ``pyiceberg.conversions.from_bytes(iceberg_type, bytes)``

  * Snapshot summary preservation:
    - ``Snapshot.summary`` accepts custom keys without filtering

When a test fails, the relevant downstream module needs revalidation:
  - Schema helpers → millpond/iceberg.py + shared/fingerprint.py
  - DataFile shape → icebox/iceberg.py
  - Producer signature → icebox/iceberg.py.commit_data_files
  - to_bytes/from_bytes → shared/bounds.py
"""

from __future__ import annotations

import inspect
from importlib.metadata import version

import pyiceberg

# Update this constant when revalidating against a new PyIceberg release.
_VERIFIED_AGAINST = "0.11.1"


def test_pyiceberg_version_is_pinned():
    installed = version("pyiceberg")
    assert installed == _VERIFIED_AGAINST, (
        f"PyIceberg {installed} installed, but millpond/iceberg.py was last "
        f"verified against {_VERIFIED_AGAINST}. Review the private-API imports "
        f"in millpond/iceberg.py and icebox/* before bumping this constant."
    )
    assert pyiceberg.__version__ == _VERIFIED_AGAINST


# ---------------------------------------------------------------------------
# Schema field-ID assignment helpers (writer-side)
# ---------------------------------------------------------------------------


def test_private_schema_id_symbols_still_importable():
    # If either import raises, the private path moved and the module
    # body in millpond/iceberg.py won't load — fail loud now instead of
    # at startup in production.
    from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
    from pyiceberg.schema import assign_fresh_schema_ids

    assert callable(_pyarrow_to_schema_without_ids)
    assert callable(assign_fresh_schema_ids)

    # Defend against a re-export shim that papers over a real internal move.
    # If pyiceberg ever moves the implementations and re-exports them from
    # the old path, the imports above succeed but `__module__` shifts —
    # catch that here so we revalidate millpond/iceberg.py against the
    # new location instead of silently coupling to a shim that may go away.
    assert _pyarrow_to_schema_without_ids.__module__ == "pyiceberg.io.pyarrow"
    assert assign_fresh_schema_ids.__module__ == "pyiceberg.schema"


# ---------------------------------------------------------------------------
# DataFile.from_args — icebox/iceberg.py's load-bearing constructor
# ---------------------------------------------------------------------------


def test_pin_canary_datafile_from_args_signature():
    """icebox/iceberg.py:build_data_file invokes
    DataFile.from_args(_table_format_version=int, **kwargs). If PyIceberg
    renames _table_format_version or changes from_args to non-keyword,
    every cycle commit silently mis-binds and writes corrupt manifests."""
    from pyiceberg.manifest import DataFile

    sig = inspect.signature(DataFile.from_args)
    params = sig.parameters
    assert "_table_format_version" in params, (
        "DataFile.from_args first parameter renamed; review "
        "icebox/iceberg.py:build_data_file"
    )
    # _table_format_version should be a positional-or-keyword with a default
    p = params["_table_format_version"]
    assert p.default == 2 or p.default == inspect.Parameter.empty, (
        f"DataFile.from_args._table_format_version default changed to "
        f"{p.default!r}; icebox passes table.metadata.format_version explicitly "
        f"but the default change may indicate a v3 transition worth tracking"
    )


def test_pin_canary_datafile_from_args_accepts_icebox_kwargs():
    """The exact kwargs build_data_file passes must construct without
    error. If PyIceberg renames any kwarg (e.g., file_size_in_bytes →
    file_size), the cycle commit will error with a kwarg name surfaced
    in the exception — but only at the first production commit."""
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.typedef import Record

    df = DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.DATA,
        file_path="s3://b/foo.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(2026, 6, 1, 14),
        record_count=10,
        file_size_in_bytes=100,
        column_sizes={1: 100},
        value_counts={1: 10},
        null_value_counts={1: 0},
        nan_value_counts={},
        lower_bounds={1: b"\x01\x00\x00\x00"},
        upper_bounds={1: b"\x0a\x00\x00\x00"},
        key_metadata=None,
        split_offsets=None,
        equality_ids=None,
        sort_order_id=None,
    )
    # Read back the exact fields the committer reads back
    assert df.file_path == "s3://b/foo.parquet"
    assert df.record_count == 10
    assert df.file_size_in_bytes == 100
    assert df.content == DataFileContent.DATA
    assert df.file_format == FileFormat.PARQUET


def test_pin_canary_datafile_enums():
    """DataFileContent.DATA + FileFormat.PARQUET must remain canonical.
    A rename would silently change manifest semantics — e.g., if
    `DATA` becomes `DATA_FILE`, the cycle commits would still type-check
    but reads might miss the files."""
    from pyiceberg.manifest import DataFileContent, FileFormat

    assert DataFileContent.DATA.name == "DATA"
    assert FileFormat.PARQUET.name == "PARQUET"


# ---------------------------------------------------------------------------
# Transaction._append_snapshot_producer — bypass for add_files cost
# ---------------------------------------------------------------------------


def test_pin_canary_transaction_append_snapshot_producer_signature():
    """icebox/iceberg.py:commit_data_files uses Transaction._append_
    snapshot_producer(snapshot_properties=, branch=) — the low-level path
    that lets us skip the footer-reading `add_files` call. If PyIceberg
    renames the params or removes the underscore-prefixed entry point,
    we revert to add_files (which is what icebox exists to avoid)."""
    from pyiceberg.table import Transaction

    assert hasattr(Transaction, "_append_snapshot_producer"), (
        "Transaction._append_snapshot_producer removed; icebox/iceberg.py"
        ".commit_data_files will not work without it"
    )
    sig = inspect.signature(Transaction._append_snapshot_producer)
    params = sig.parameters
    assert "snapshot_properties" in params, (
        "Transaction._append_snapshot_producer.snapshot_properties param "
        "renamed; icebox's cycle_id tagging will silently fail"
    )
    assert "branch" in params


def test_pin_canary_snapshot_producer_exposes_snapshot_id():
    """commit_data_files captures the new snapshot's id from the
    producer DIRECTLY (`producer.snapshot_id`), not from
    `table.current_snapshot()` post-exit. The latter would rely on
    PyIceberg refreshing the in-memory Table metadata in place — a
    behavior that varies across versions and could silently return a
    STALE pre-commit snapshot id (permanently corrupt PG cycle log).

    If `snapshot_id` ever stops being a public attribute on
    _FastAppendFiles, the entire commit path is wrong and silently
    records bad cycle metadata."""
    from pyiceberg.table.update.snapshot import _FastAppendFiles

    assert hasattr(_FastAppendFiles, "snapshot_id"), (
        "_FastAppendFiles no longer exposes snapshot_id directly; "
        "icebox/iceberg.py:commit_data_files cannot reliably record the "
        "new snapshot id without it"
    )


# ---------------------------------------------------------------------------
# Single-value-serialization conversions — load-bearing for partition pruning
# ---------------------------------------------------------------------------


def test_pin_canary_to_bytes_int_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import IntegerType

    raw = to_bytes(IntegerType(), 42)
    assert isinstance(raw, bytes)
    assert from_bytes(IntegerType(), raw) == 42


def test_pin_canary_to_bytes_long_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import LongType

    raw = to_bytes(LongType(), 9_999_999_999)
    assert from_bytes(LongType(), raw) == 9_999_999_999


def test_pin_canary_to_bytes_string_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import StringType

    raw = to_bytes(StringType(), "café")
    assert from_bytes(StringType(), raw) == "café"


def test_pin_canary_to_bytes_double_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import DoubleType

    raw = to_bytes(DoubleType(), 3.141592653589793)
    assert from_bytes(DoubleType(), raw) == 3.141592653589793


def test_pin_canary_to_bytes_boolean_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import BooleanType

    raw_true = to_bytes(BooleanType(), True)
    raw_false = to_bytes(BooleanType(), False)
    assert from_bytes(BooleanType(), raw_true) is True
    assert from_bytes(BooleanType(), raw_false) is False
    assert raw_true != raw_false


def test_pin_canary_to_bytes_binary_roundtrip():
    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import BinaryType

    payload = b"\x00\x01\xff\xfe"
    raw = to_bytes(BinaryType(), payload)
    assert from_bytes(BinaryType(), raw) == payload


def test_pin_canary_to_bytes_float_roundtrip():
    """FloatType (32-bit) is distinct from DoubleType (64-bit); writers
    that ship float32 columns hit this path. float32 precision loss
    means we use pytest.approx."""
    import pytest

    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import FloatType

    raw = to_bytes(FloatType(), 3.14)
    assert from_bytes(FloatType(), raw) == pytest.approx(3.14)


def test_pin_canary_to_bytes_date_roundtrip():
    """date partitions are real (year/month/day) — encode_bounds takes
    a date via DateType, ships it as days since epoch. A PyIceberg
    rearrangement of date encoding silently corrupts manifests for any
    date-partitioned table."""
    from datetime import date

    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import DateType

    d = date(2026, 6, 1)
    raw = to_bytes(DateType(), d)
    # from_bytes returns int days-since-epoch
    assert from_bytes(DateType(), raw) == (d - date(1970, 1, 1)).days


def test_pin_canary_to_bytes_timestamp_roundtrip():
    """TimestampType is the naive-timestamp encoding. PyIceberg returns
    micros-since-epoch on the round trip."""
    from datetime import UTC, datetime

    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import TimestampType

    dt = datetime(2026, 6, 1, 12, 34, 56, 789012)
    raw = to_bytes(TimestampType(), dt)
    expected_micros = int(
        datetime(2026, 6, 1, 12, 34, 56, 789012, tzinfo=UTC).timestamp() * 1_000_000
    )
    assert from_bytes(TimestampType(), raw) == expected_micros


def test_pin_canary_to_bytes_timestamptz_roundtrip():
    """TimestamptzType — what _inserted_at uses. Highest-cardinality
    timestamp column in PostHog data; any silent encoding drift here
    breaks query pruning across every events query."""
    from datetime import UTC, datetime

    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import TimestamptzType

    dt = datetime(2026, 6, 1, 12, 34, 56, 789012, tzinfo=UTC)
    raw = to_bytes(TimestamptzType(), dt)
    expected_micros = int(dt.timestamp() * 1_000_000)
    assert from_bytes(TimestamptzType(), raw) == expected_micros


def test_pin_canary_to_bytes_decimal_roundtrip():
    """DecimalType — eventually for money columns. PyIceberg's decimal
    encoding is precision-aware; the (precision, scale) tuple is part
    of the type, so a precision-mismatch on the round-trip catches
    encoding drift."""
    from decimal import Decimal

    from pyiceberg.conversions import from_bytes, to_bytes
    from pyiceberg.types import DecimalType

    dec_type = DecimalType(10, 2)
    raw = to_bytes(dec_type, Decimal("123.45"))
    assert from_bytes(dec_type, raw) == Decimal("123.45")


# ---------------------------------------------------------------------------
# DataFile._data positional layout — Record-style tuple under the hood
# ---------------------------------------------------------------------------


def test_pin_canary_datafile_data_shape():
    """PyIceberg 0.11.1's DataFile stores its fields in a positional
    `_data` tuple. icebox/iceberg.py uses `DataFile.from_args` which
    maps kwargs to positions internally — but if the tuple grows or
    shrinks (e.g., PyIceberg adds a column for v3 format), the bound
    `_data` shape changes. Pin the current length so any change forces
    a deliberate review of `build_data_file`."""
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
    from pyiceberg.typedef import Record

    df = DataFile.from_args(
        _table_format_version=2,
        content=DataFileContent.DATA,
        file_path="s3://b/foo.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(2026, 6, 1, 14),
        record_count=10,
        file_size_in_bytes=100,
        column_sizes={1: 100},
        value_counts={1: 10},
        null_value_counts={1: 0},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        key_metadata=None,
        split_offsets=None,
        equality_ids=None,
        sort_order_id=None,
    )
    # In 0.11.1 _data is a 16-element list. A shape change indicates
    # PyIceberg has refactored DataFile's internal storage; revisit
    # icebox/iceberg.py:build_data_file.
    assert len(df._data) == 16


def test_pin_canary_conversions_path_unchanged():
    """`from pyiceberg.conversions import to_bytes` is what
    shared/bounds.py imports. A package rename (e.g., into
    `pyiceberg.serdes`) would surface as ImportError; a same-path-rename
    surfaces as `__module__` shift."""
    from pyiceberg.conversions import from_bytes, to_bytes

    assert to_bytes.__module__ == "pyiceberg.conversions"
    assert from_bytes.__module__ == "pyiceberg.conversions"


# ---------------------------------------------------------------------------
# Snapshot.summary — preserves the cycle_id key
# ---------------------------------------------------------------------------


def test_pin_canary_snapshot_summary_preserves_custom_keys():
    """icebox tags each snapshot with `posthog.icebox.cycle_id` for
    recovery. If PyIceberg filters/transforms snapshot.summary (e.g.,
    drops keys with dots, lowercases, etc.), our recovery scan returns
    None and we silently release file claims that should have been
    finalized.

    Verifies via constructing a Snapshot directly — bypasses any
    catalog-side filtering. If PyIceberg's Snapshot model itself
    transforms keys, this catches it."""
    from pyiceberg.table.snapshots import Snapshot

    # Snapshot constructor signature varies across PyIceberg versions —
    # use kwargs that survive across 0.10/0.11.
    summary = {"operation": "append", "posthog.icebox.cycle_id": "abc-123"}
    snap = Snapshot(
        snapshot_id=1,
        sequence_number=1,
        timestamp_ms=0,
        manifest_list="",
        summary=summary,  # type: ignore[arg-type]
        schema_id=0,
    )
    assert snap.summary is not None
    # PyIceberg 0.11.1 Summary exposes custom keys via `.get(key)` —
    # but the dict-style `summary[key]` interface crashes on tuple
    # access in this version. icebox/iceberg.py:find_snapshot_for_cycle
    # uses `snap.summary.get(CYCLE_ID_SUMMARY_KEY)`, so verify that
    # exact call shape works.
    assert snap.summary.get("posthog.icebox.cycle_id") == "abc-123", (
        "Snapshot.summary stripped or transformed our cycle_id key — "
        "icebox/iceberg.py:find_snapshot_for_cycle relies on byte-identical "
        "round-trip"
    )
