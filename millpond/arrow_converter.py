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


def _stringify_mixed_type_values(records: list[dict], schema: pa.Schema) -> list[dict]:
    """Coerce values to strings for fields where the inferred type doesn't match all values.

    JSON data from heterogeneous sources can have the same key as bool in one
    record and string in another. Rather than crash, stringify mismatched values.
    """
    # Build a map of field name -> expected Python types for the inferred Arrow type
    type_checks: dict[str, type | tuple[type, ...]] = {}
    for field in schema:
        if pa.types.is_boolean(field.type):
            type_checks[field.name] = bool
        elif pa.types.is_integer(field.type):
            type_checks[field.name] = int
        elif pa.types.is_floating(field.type):
            type_checks[field.name] = (int, float)
        elif pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
            type_checks[field.name] = str

    # Scan for conflicts
    conflicting_keys: set[str] = set()
    for record in records:
        for k, v in record.items():
            if v is not None and k in type_checks:
                if not isinstance(v, type_checks[k]):
                    conflicting_keys.add(k)

    if not conflicting_keys:
        return records

    log.info("Mixed types detected in fields %s, coercing to string", conflicting_keys)

    # Stringify conflicting fields and patch the schema later
    patched = []
    for record in records:
        new_record = dict(record)
        for k in conflicting_keys:
            if k in new_record and new_record[k] is not None:
                new_record[k] = str(new_record[k])
        patched.append(new_record)
    return patched


def _flatten_nested_to_json(records: list[dict]) -> list[dict]:
    """Serialize nested dicts and lists to JSON strings.

    PyArrow's struct inference breaks on mixed types inside nested objects
    (e.g. a field that is bool in one record and string in another within a
    nested dict). Serializing nested objects to JSON strings avoids this
    entirely — they become VARCHAR columns in DuckDB, queryable via JSON functions.
    """
    flattened = []
    for record in records:
        new = {}
        for k, v in record.items():
            if isinstance(v, (dict, list)):
                new[k] = orjson.dumps(v).decode()
            else:
                new[k] = v
        flattened.append(new)
    return flattened


def convert(messages: list[bytes]) -> pa.Table | None:
    """Convert raw Kafka message values to an Arrow table.

    Parses JSON via orjson, builds a PyArrow table using the union of all keys
    across all records, normalizes numeric types, and handles mixed-type fields
    by coercing to string. Nested dicts/lists are serialized to JSON strings.

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

    records = _flatten_nested_to_json(records)
    records = _stringify_mixed_type_values(records, _build_schema(records))
    schema = _build_schema(records)
    table = pa.Table.from_pylist(records, schema=schema)
    table = _normalize_numeric_types(table)
    return table
