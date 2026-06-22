import logging

import orjson
import pyarrow as pa
import pyarrow.compute as pc

from millpond import metrics

log = logging.getLogger(__name__)


def _drop_null_typed_columns(table: pa.Table) -> pa.Table:
    """Drop columns whose Arrow type is ``pa.null()`` before they reach a Sink.

    In normal use ``_build_schema`` falls back to ``pa.string()`` for keys
    where every record has None across the whole batch, so ``pa.null()``
    shouldn't appear via this path. This filter is defensive: if a
    ``pa.null()`` column ever slips through (e.g. via a future inference
    change), dropping it at the converter keeps the Sink contract clean —
    a column with no schema info is a column with no data, and it'll be
    re-introduced with a real type on the next batch that has a non-null
    value.
    """
    null_cols = [field.name for field in table.schema if pa.types.is_null(field.type)]
    if not null_cols:
        return table
    log.info("Dropping all-null columns with pa.null() type: %s", null_cols)
    return table.drop_columns(null_cols)


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


# Wire format the upstream ClickHouse-events producers emit for DateTime64
# columns: space-separated, six fractional digits, UTC implied, no zone suffix
# (e.g. "2024-01-01 12:00:00.000000"). See the rust kafka-deduplicator
# clickhouse_events pipeline (parser/processor) and ClickHouse's JSONEachRow
# DateTime64(6, 'UTC') rendering — both produce exactly this shape.
#
# PyArrow can't parse it via `strptime` (the `%f` directive isn't supported in
# this build) or via a direct cast to a tz-aware timestamp (that demands an
# explicit zone offset in the string). The working path is two steps: cast the
# string to a *naive* microsecond timestamp (Arrow's ISO-8601 parser accepts the
# space separator and fractional seconds), then stamp it UTC with
# `assume_timezone`. The result is `timestamp[us, tz=UTC]`, which
# schema._arrow_type_to_duckdb maps to TIMESTAMPTZ — matching the typed
# timestamp columns the duckling backfill writes.
_TIMESTAMP_UNIT = "us"
_TIMESTAMP_TZ = "UTC"


def coerce_timestamp_columns(table: pa.Table, columns: tuple[str, ...]) -> pa.Table:
    """Parse the named string columns into ``timestamp[us, tz=UTC]``.

    JSON has no native timestamp type, so ``_build_schema`` infers VARCHAR for
    any column whose values arrive as date-time strings. When millpond writes
    into a table whose column is already TIMESTAMPTZ (e.g. the duckling backfill's
    ``posthog.events``), that VARCHAR batch type can't be reconciled — DuckLake
    only widens, and TIMESTAMPTZ→VARCHAR is a narrowing — so schema evolution
    fails and flushes wedge. Pinning these columns to a real Arrow timestamp here,
    before the batch reaches the sink, makes the inferred type match the table so
    the insert is a typed append with no DDL, and makes freshly-created tables
    use TIMESTAMPTZ in the first place.

    Only columns that are (a) present in this batch and (b) string-typed are
    touched; a column already timestamp-typed, or absent from the batch, is left
    as-is so the function is safe to point at a superset of columns. Parsing is
    strict: a value that doesn't match the producer's wire format raises rather
    than being silently nulled — a format drift is a contract break that should
    surface loudly, not corrupt timestamps in place.
    """
    if not columns:
        return table

    target = set(columns)
    coerced = 0
    new_columns = []
    new_fields = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if field.name in target and (pa.types.is_string(field.type) or pa.types.is_large_string(field.type)):
            parsed = pc.assume_timezone(col.cast(pa.timestamp(_TIMESTAMP_UNIT)), _TIMESTAMP_TZ)
            new_columns.append(parsed)
            new_fields.append(pa.field(field.name, parsed.type, nullable=field.nullable))
            coerced += 1
        else:
            new_columns.append(col)
            new_fields.append(field)

    if coerced == 0:
        return table
    metrics.timestamp_columns_coerced_total.inc(coerced)
    return pa.table(dict(zip([f.name for f in new_fields], new_columns)), schema=pa.schema(new_fields))


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
    schema = _build_schema(records)
    patched = _stringify_mixed_type_values(records, schema)
    if patched is not records:
        # Mixed types were found and coerced — re-infer schema with string types
        schema = _build_schema(patched)
    table = pa.Table.from_pylist(patched, schema=schema)
    table = _normalize_numeric_types(table)
    table = _drop_null_typed_columns(table)
    return table
