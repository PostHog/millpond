"""Typed-JSON ↔ Iceberg bound bytes conversion.

The icebox wire format ships lower/upper bound values as TYPED JSON
keyed by Iceberg field ID:

  - int/long/float/double → JSON number
  - boolean → JSON bool
  - string → JSON string
  - date → JSON string "YYYY-MM-DD"
  - timestamp/timestamptz → ISO-8601 string with microsecond precision
  - binary/fixed → base64-encoded string
  - decimal → JSON string of the decimal value

The writer extracts typed values from PyArrow's stats (PyArrow's own
format is NOT Iceberg's single-value-serialization — its bytes cannot
be reused verbatim) and ships them as typed JSON. The committer alone
converts to Iceberg single-value-serialization bytes via
`pyiceberg.conversions.to_bytes(iceberg_type, python_value)`.

This split:
  - Keeps wire-format human-readable for triage.
  - Lets the committer be the single source of truth for the Iceberg
    spec (writers don't need to track PyIceberg minor-version drift
    in bound encoding).
  - Decouples writer parquet-stats extraction from Iceberg encoding.
"""
from __future__ import annotations

import base64
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pyiceberg.conversions import to_bytes
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FixedType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)


def encode_bounds(
    typed_bounds: dict[str, Any],
    schema: Schema,
) -> dict[int, bytes]:
    """Convert writer-supplied typed JSON bounds to Iceberg manifest bytes.

    Args:
        typed_bounds: Field-id-keyed (string) dict of typed JSON values
            from the writer's POST. e.g. {"1": 42, "2": "alpha"}.
        schema: The current Iceberg schema for the table — used to look
            up the field type by field ID. Must include every field ID
            referenced in typed_bounds.

    Returns:
        dict[int, bytes] suitable for DataFile.lower_bounds /
        upper_bounds. Field IDs are int (Iceberg's manifest API). Bytes
        are Iceberg single-value-serialization (little-endian ints, IEEE
        floats, UTF-8 strings, etc. per the spec).

    Raises:
        KeyError: if typed_bounds references a field ID not in `schema`.
        ValueError: if a typed JSON value cannot be coerced to the
            schema field's expected Python type. Examples: non-numeric
            string for an int field; malformed date string.
    """
    field_by_id = {f.field_id: f for f in schema.fields}
    result: dict[int, bytes] = {}
    for k, typed_value in typed_bounds.items():
        field_id = int(k)
        try:
            field = field_by_id[field_id]
        except KeyError as e:
            raise KeyError(
                f"Bound references field id {field_id} not in schema"
            ) from e
        python_value = _coerce_to_python(field.field_type, typed_value)
        result[field_id] = to_bytes(field.field_type, python_value)
    return result


def _coerce_to_python(iceberg_type: IcebergType, typed_value: Any) -> Any:
    """Coerce a typed-JSON value to the Python type that
    pyiceberg.conversions.to_bytes expects.

    For most primitives this is identity (the JSON value is already a
    Python int/float/str/bool). For date/timestamp/binary/decimal the
    wire format uses strings and we parse them here.
    """
    if isinstance(iceberg_type, (IntegerType, LongType)):
        if not isinstance(typed_value, int) or isinstance(typed_value, bool):
            raise ValueError(
                f"int/long bound must be JSON number, got {type(typed_value).__name__}"
            )
        return typed_value

    if isinstance(iceberg_type, (FloatType, DoubleType)):
        if not isinstance(typed_value, (int, float)) or isinstance(typed_value, bool):
            raise ValueError(
                f"float/double bound must be JSON number, got {type(typed_value).__name__}"
            )
        return float(typed_value)

    if isinstance(iceberg_type, BooleanType):
        if not isinstance(typed_value, bool):
            raise ValueError(
                f"bool bound must be JSON bool, got {type(typed_value).__name__}"
            )
        return typed_value

    if isinstance(iceberg_type, StringType):
        if not isinstance(typed_value, str):
            raise ValueError(
                f"string bound must be JSON string, got {type(typed_value).__name__}"
            )
        return typed_value

    if isinstance(iceberg_type, DateType):
        if not isinstance(typed_value, str):
            raise ValueError(
                f"date bound must be ISO 'YYYY-MM-DD' string, got "
                f"{type(typed_value).__name__}"
            )
        return date.fromisoformat(typed_value)

    if isinstance(iceberg_type, (TimestampType, TimestamptzType)):
        if not isinstance(typed_value, str):
            raise ValueError(
                f"timestamp bound must be ISO-8601 string, got "
                f"{type(typed_value).__name__}"
            )
        # fromisoformat handles both naive ("2024-06-01T12:34:56.789012") and
        # tz-aware ("...+00:00") forms.
        return datetime.fromisoformat(typed_value)

    if isinstance(iceberg_type, (BinaryType, FixedType)):
        if not isinstance(typed_value, str):
            raise ValueError(
                f"binary/fixed bound must be base64 string, got "
                f"{type(typed_value).__name__}"
            )
        return base64.b64decode(typed_value)

    if isinstance(iceberg_type, DecimalType):
        # Accept str or number; prefer str on the wire for exactness.
        if isinstance(typed_value, str):
            return Decimal(typed_value)
        if isinstance(typed_value, (int, float)) and not isinstance(typed_value, bool):
            return Decimal(str(typed_value))
        raise ValueError(
            f"decimal bound must be string or number, got {type(typed_value).__name__}"
        )

    raise NotImplementedError(
        f"Bound encoding not implemented for Iceberg type {type(iceberg_type).__name__}. "
        f"v1 icebox supports primitive types only — nested-struct bounds are out of scope."
    )
