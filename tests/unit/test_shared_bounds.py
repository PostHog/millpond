"""Tests for shared.bounds.encode_bounds.

This is the load-bearing piece for partition pruning correctness. Wrong
encoding here silently breaks Iceberg readers' min/max pruning — queries
return correct rows, but scan more files than necessary, or in extreme
cases mispredicate-push and skip files that match.

Round-trip tests are essential: encode → DataFile.lower_bounds →
PyIceberg manifest reader decodes → must equal original. Where direct
round-trip via DataFile is too heavy for unit tests, we test against
pyiceberg.conversions.from_bytes — the same path manifest readers use.
"""
from __future__ import annotations

import base64
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyiceberg.conversions import from_bytes
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from shared.bounds import encode_bounds


def _schema_with(*fields: NestedField) -> Schema:
    return Schema(*fields)


# ---------------------------------------------------------------------------
# Primitive numeric types — round-trip via from_bytes
# ---------------------------------------------------------------------------


def test_int_roundtrip():
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    encoded = encode_bounds({"1": 42}, schema)
    assert 1 in encoded
    assert from_bytes(IntegerType(), encoded[1]) == 42


def test_long_roundtrip():
    schema = _schema_with(
        NestedField(field_id=2, name="x", field_type=LongType(), required=True),
    )
    encoded = encode_bounds({"2": 9_999_999_999}, schema)
    assert from_bytes(LongType(), encoded[2]) == 9_999_999_999


def test_float_roundtrip():
    schema = _schema_with(
        NestedField(field_id=3, name="x", field_type=FloatType(), required=True),
    )
    encoded = encode_bounds({"3": 3.14}, schema)
    # float32 precision loss — pytest.approx handles it
    assert from_bytes(FloatType(), encoded[3]) == pytest.approx(3.14)


def test_double_roundtrip():
    schema = _schema_with(
        NestedField(field_id=4, name="x", field_type=DoubleType(), required=True),
    )
    encoded = encode_bounds({"4": 3.141592653589793}, schema)
    assert from_bytes(DoubleType(), encoded[4]) == 3.141592653589793


def test_boolean_roundtrip():
    schema = _schema_with(
        NestedField(field_id=5, name="x", field_type=BooleanType(), required=True),
    )
    encoded = encode_bounds({"5": True}, schema)
    assert from_bytes(BooleanType(), encoded[5]) is True


def test_string_roundtrip():
    schema = _schema_with(
        NestedField(field_id=6, name="x", field_type=StringType(), required=True),
    )
    encoded = encode_bounds({"6": "alpha"}, schema)
    assert from_bytes(StringType(), encoded[6]) == "alpha"


def test_string_unicode_roundtrip():
    """UTF-8 is the only correct string encoding for Iceberg."""
    schema = _schema_with(
        NestedField(field_id=7, name="x", field_type=StringType(), required=True),
    )
    encoded = encode_bounds({"7": "café 日本"}, schema)
    assert from_bytes(StringType(), encoded[7]) == "café 日本"


# ---------------------------------------------------------------------------
# Date / timestamp — wire format is ISO strings
# ---------------------------------------------------------------------------


def test_date_iso_string_roundtrip():
    """PyIceberg's from_bytes returns the raw Iceberg-spec int (days
    since epoch for date), not a Python date object. Manifest readers
    that need a date apply the conversion themselves."""
    schema = _schema_with(
        NestedField(field_id=8, name="d", field_type=DateType(), required=True),
    )
    encoded = encode_bounds({"8": "2026-06-01"}, schema)
    expected_days = (date(2026, 6, 1) - date(1970, 1, 1)).days
    assert from_bytes(DateType(), encoded[8]) == expected_days


def test_timestamptz_iso_string_roundtrip():
    """from_bytes returns int microseconds since epoch (UTC)."""
    schema = _schema_with(
        NestedField(field_id=9, name="ts", field_type=TimestamptzType(), required=True),
    )
    encoded = encode_bounds({"9": "2026-06-01T12:34:56.789012+00:00"}, schema)
    expected_micros = int(
        datetime(2026, 6, 1, 12, 34, 56, 789012, tzinfo=UTC).timestamp() * 1_000_000
    )
    assert from_bytes(TimestamptzType(), encoded[9]) == expected_micros


def test_timestamp_naive_roundtrip():
    """For naive timestamp, the wire value is treated as already-UTC
    micros; from_bytes returns the int micros."""
    schema = _schema_with(
        NestedField(field_id=10, name="ts", field_type=TimestampType(), required=True),
    )
    encoded = encode_bounds({"10": "2026-06-01T12:34:56.789012"}, schema)
    expected_micros = int(
        datetime(2026, 6, 1, 12, 34, 56, 789012, tzinfo=UTC).timestamp() * 1_000_000
    )
    assert from_bytes(TimestampType(), encoded[10]) == expected_micros


# ---------------------------------------------------------------------------
# Binary / fixed / decimal — wire format is base64 / decimal string
# ---------------------------------------------------------------------------


def test_binary_base64_roundtrip():
    schema = _schema_with(
        NestedField(field_id=11, name="b", field_type=BinaryType(), required=True),
    )
    raw = b"\x00\x01\x02\xff"
    encoded = encode_bounds({"11": base64.b64encode(raw).decode("ascii")}, schema)
    assert from_bytes(BinaryType(), encoded[11]) == raw


def test_fixed_base64_roundtrip():
    schema = _schema_with(
        NestedField(field_id=12, name="f", field_type=FixedType(4), required=True),
    )
    raw = b"\xde\xad\xbe\xef"
    encoded = encode_bounds({"12": base64.b64encode(raw).decode("ascii")}, schema)
    assert from_bytes(FixedType(4), encoded[12]) == raw


def test_decimal_string_roundtrip():
    schema = _schema_with(
        NestedField(field_id=13, name="d", field_type=DecimalType(10, 2), required=True),
    )
    encoded = encode_bounds({"13": "123.45"}, schema)
    assert from_bytes(DecimalType(10, 2), encoded[13]) == Decimal("123.45")


# ---------------------------------------------------------------------------
# Multi-field — committer iterates over all bound fields
# ---------------------------------------------------------------------------


def test_multi_field_encoding():
    """Committer encodes lower_bounds across all the writer's stats
    fields in a single call."""
    schema = _schema_with(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
        NestedField(field_id=3, name="when", field_type=TimestamptzType(), required=True),
    )
    bounds = {
        "1": 100,
        "2": "alice",
        "3": "2026-06-01T00:00:00.000000+00:00",
    }
    encoded = encode_bounds(bounds, schema)
    assert from_bytes(LongType(), encoded[1]) == 100
    assert from_bytes(StringType(), encoded[2]) == "alice"
    expected_micros = int(datetime(2026, 6, 1, tzinfo=UTC).timestamp() * 1_000_000)
    assert from_bytes(TimestamptzType(), encoded[3]) == expected_micros


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


def test_missing_field_id_in_schema_raises_keyerror():
    """Wire bound references field 99 but schema only has field 1 — a
    deploy-skew signal worth surfacing loudly."""
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    with pytest.raises(KeyError, match="99"):
        encode_bounds({"99": 42}, schema)


def test_non_numeric_int_value_raises():
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    with pytest.raises(ValueError):
        encode_bounds({"1": "not-a-number"}, schema)


def test_bool_for_int_field_raises():
    """Python bool is a subclass of int — without an explicit check this
    would silently encode True/False as 1/0 ints."""
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    with pytest.raises(ValueError):
        encode_bounds({"1": True}, schema)


def test_non_string_date_value_raises():
    schema = _schema_with(
        NestedField(field_id=1, name="d", field_type=DateType(), required=True),
    )
    with pytest.raises(ValueError):
        encode_bounds({"1": 12345}, schema)


def test_invalid_date_string_raises():
    schema = _schema_with(
        NestedField(field_id=1, name="d", field_type=DateType(), required=True),
    )
    with pytest.raises(ValueError):
        encode_bounds({"1": "06/01/2026"}, schema)


def test_invalid_base64_for_binary_raises():
    schema = _schema_with(
        NestedField(field_id=1, name="b", field_type=BinaryType(), required=True),
    )
    with pytest.raises(Exception):  # base64.binascii.Error subclasses ValueError
        encode_bounds({"1": "!!! not-base64 !!!"}, schema)


def test_invalid_decimal_string_raises():
    schema = _schema_with(
        NestedField(field_id=1, name="d", field_type=DecimalType(10, 2), required=True),
    )
    with pytest.raises(Exception):  # InvalidOperation subclasses ArithmeticError
        encode_bounds({"1": "not-a-decimal"}, schema)


def test_empty_bounds_dict_returns_empty():
    """A column-less file (impossible in practice, but defensible) yields no bounds."""
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    assert encode_bounds({}, schema) == {}


def test_result_field_ids_are_ints_not_strings():
    """DataFile.lower_bounds is typed dict[int, bytes]. Passing strings
    would break PyIceberg's manifest writer."""
    schema = _schema_with(
        NestedField(field_id=1, name="x", field_type=IntegerType(), required=True),
    )
    encoded = encode_bounds({"1": 42}, schema)
    assert all(isinstance(k, int) for k in encoded.keys())
    assert all(isinstance(v, bytes) for v in encoded.values())
