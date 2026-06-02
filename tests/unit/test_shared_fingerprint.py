"""Tests for shared.fingerprint.schema_fingerprint.

The fingerprint is the icebox's only defense against silent schema
drift between writer and committer. These tests pin the invariants:

  - Same schema (same field IDs, types, names, required-ness) ⇒ same fingerprint
  - Different schema (anywhere) ⇒ different fingerprint
  - Output shape is stable hex (64 chars, lowercase)

If any of these break, writer/committer agreement breaks.
"""
from __future__ import annotations

import re

from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from shared.fingerprint import schema_fingerprint


def _events_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=True),
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
    )


def test_fingerprint_is_hex_sha256():
    """64-char lowercase hex, deterministically derivable."""
    fp = schema_fingerprint(_events_schema())
    assert re.fullmatch(r"[0-9a-f]{64}", fp), fp


def test_same_schema_yields_same_fingerprint():
    """Two writers building the same schema must produce the same
    fingerprint — this is what makes the protocol work."""
    fp1 = schema_fingerprint(_events_schema())
    fp2 = schema_fingerprint(_events_schema())
    assert fp1 == fp2


def test_field_rename_changes_fingerprint():
    """A column rename is a schema change that requires explicit
    migration — fingerprint MUST differ."""
    fp_orig = schema_fingerprint(_events_schema())
    renamed = Schema(
        NestedField(field_id=1, name="event_name", field_type=StringType(), required=True),  # event → event_name
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
    )
    assert schema_fingerprint(renamed) != fp_orig


def test_field_type_change_changes_fingerprint():
    """int → long is a logical widening that still must require
    explicit evolution; the fingerprint catches it."""
    fp_orig = schema_fingerprint(_events_schema())
    widened = Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=True),
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=LongType(), required=True),  # int → long
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
    )
    assert schema_fingerprint(widened) != fp_orig


def test_field_id_reassignment_changes_fingerprint():
    """Same names, different IDs — Iceberg manifests would not match.
    Fingerprint correctly treats this as different."""
    fp_orig = schema_fingerprint(_events_schema())
    shifted = Schema(
        NestedField(field_id=100, name="event", field_type=StringType(), required=True),
        NestedField(field_id=101, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=102, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=103, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=104, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=105, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=106, name="hour", field_type=IntegerType(), required=True),
    )
    assert schema_fingerprint(shifted) != fp_orig


def test_field_required_flag_change_changes_fingerprint():
    """required → optional is a schema change — Iceberg nullability
    differs and queries can return different results."""
    fp_orig = schema_fingerprint(_events_schema())
    relaxed = Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=False),  # required → optional
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
    )
    assert schema_fingerprint(relaxed) != fp_orig


def test_field_addition_changes_fingerprint():
    """A new column makes a different table from the committer's POV."""
    fp_orig = schema_fingerprint(_events_schema())
    extended = Schema(
        NestedField(field_id=1, name="event", field_type=StringType(), required=True),
        NestedField(field_id=2, name="distinct_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="timestamp", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="year", field_type=IntegerType(), required=True),
        NestedField(field_id=5, name="month", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="day", field_type=IntegerType(), required=True),
        NestedField(field_id=7, name="hour", field_type=IntegerType(), required=True),
        NestedField(field_id=8, name="price", field_type=DoubleType(), required=False),
    )
    assert schema_fingerprint(extended) != fp_orig


def test_minimal_schema_produces_valid_fingerprint():
    """Edge case: single-field schema."""
    s = Schema(NestedField(field_id=1, name="id", field_type=IntegerType(), required=True))
    fp = schema_fingerprint(s)
    assert re.fullmatch(r"[0-9a-f]{64}", fp)
