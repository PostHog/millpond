import logging

import orjson
import pyarrow as pa

log = logging.getLogger(__name__)


def _normalize_numeric_types(table: pa.Table) -> pa.Table:
    """Normalize numeric columns: integers→int64, floats→float64.

    Avoids type wobble across batches (e.g. int8 vs int64) while preserving
    precision for large integers (values > 2^53 are not representable in float64).
    """
    new_columns = []
    new_fields = []
    changed = False
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_integer(field.type) and field.type != pa.int64():
            new_columns.append(col.cast(pa.int64()))
            new_fields.append(pa.field(field.name, pa.int64(), nullable=field.nullable))
            changed = True
        elif pa.types.is_floating(field.type) and field.type != pa.float64():
            new_columns.append(col.cast(pa.float64()))
            new_fields.append(pa.field(field.name, pa.float64(), nullable=field.nullable))
            changed = True
        else:
            new_columns.append(col)
            new_fields.append(field)
    if not changed:
        return table
    return pa.table(dict(zip([f.name for f in new_fields], new_columns)), schema=pa.schema(new_fields))


def _build_schema(records: list[dict]) -> pa.Schema:
    """Infer Arrow schema from the union of all keys across all records.

    pa.Table.from_pylist() only uses the first record's keys to infer the schema,
    silently dropping fields that only appear in later records. This function
    scans all records to build the complete key set and collects the first
    non-null sample for each key in a single pass.
    """
    # Single pass: collect all keys (ordered) and first non-null sample per key.
    # For nested types (dicts), prefer a sample with no null inner values to
    # avoid inferring 'null' type for struct fields.
    first_non_null: dict[str, object] = {}
    all_keys: dict[str, None] = {}  # ordered set via dict
    for record in records:
        for k, v in record.items():
            if k not in all_keys:
                all_keys[k] = None
            if v is not None:
                existing = first_non_null.get(k)
                if existing is None:
                    first_non_null[k] = v
                elif isinstance(v, dict) and isinstance(existing, dict) and None in existing.values():
                    # Replace a dict sample that has null inner values
                    if None not in v.values():
                        first_non_null[k] = v

    fields = []
    for key in all_keys:
        sample = first_non_null.get(key)
        if sample is None:
            fields.append(pa.field(key, pa.string(), nullable=True))
        else:
            inferred_type = pa.array([sample]).type
            fields.append(pa.field(key, inferred_type, nullable=True))

    return pa.schema(fields)


def convert(messages: list[bytes]) -> pa.Table | None:
    """Convert raw Kafka message values to an Arrow table.

    Parses JSON via orjson, builds a PyArrow table using the union of all keys
    across all records, and casts all numeric columns to DOUBLE to prevent
    INT64/DOUBLE type wobble across batches.

    Returns None if no valid records were parsed.
    """
    records = []
    for raw in messages:
        try:
            parsed = orjson.loads(raw)
        except orjson.JSONDecodeError:
            log.warning("Skipping malformed JSON: %s", raw[:200])
            continue
        if not isinstance(parsed, dict):
            log.warning("Skipping non-dict JSON value: %s", type(parsed).__name__)
            continue
        records.append(parsed)

    if not records:
        return None

    schema = _build_schema(records)
    table = pa.Table.from_pylist(records, schema=schema)
    table = _normalize_numeric_types(table)
    return table
