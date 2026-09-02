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


def _floatify_integers_in_float_fields(records: list[dict], schema: pa.Schema) -> list[dict]:
    """Cast int values to float for fields the schema types as floating.

    JSON producers emit whole floats as integers. PyArrow accepts an int in
    a double column only when the conversion is exact; an integer above
    2**53 makes ``pa.Table.from_pylist`` raise ``ArrowInvalid`` and the
    batch crashes before any column coercion runs. Python ``float()`` is
    deliberately lossy here — these fields are floating-point measures, so
    nearest-double is the wanted semantics. Records are only copied when a
    cast is needed.
    """
    float_fields = {f.name for f in schema if pa.types.is_floating(f.type)}
    if not float_fields:
        return records

    out = []
    changed = False
    for record in records:
        needs = [k for k in float_fields if type(record.get(k)) is int]
        if not needs:
            out.append(record)
            continue
        new_record = dict(record)
        for k in needs:
            new_record[k] = float(new_record[k])
        out.append(new_record)
        changed = True
    return out if changed else records


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


# Column type-pinning. JSON has no type schema, so `_build_schema` infers a
# column's type from its values; when those values don't carry enough type
# information the inference diverges from the destination DuckLake column, and
# DuckLake's widening-only schema evolution then rejects the narrowing ALTER
# every flush (the write stalls under DuckLake at INSERT). Two cases on the
# duckling backfill's `posthog.events`:
#   - date-times arrive as strings -> inferred VARCHAR, table is TIMESTAMPTZ;
#   - `project_id` is the one numeric column the producer serializes as explicit
#     JSON null (no serde skip), so an all-null batch infers VARCHAR not BIGINT.
# `coerce_typed_columns` pins named columns to a target type before the batch
# reaches the sink so the inferred type matches the table (typed append, no DDL)
# and freshly-created tables use the right type from the start.
#
# Target type registry: name (as used in MILLPOND_TYPED_COLUMNS) -> (the Arrow
# type that maps to the intended DuckLake column type via
# schema._arrow_type_to_duckdb, used to skip already-correct columns) and a
# coercer that turns a (possibly string) column into that Arrow type.
#
# timestamptz needs special handling: PyArrow can't parse the wire format via
# `strptime` (no `%f` in this build) or a direct tz-aware cast (it demands an
# explicit zone offset). Cast to a *naive* microsecond timestamp first (Arrow's
# ISO-8601 parser accepts the space separator and 0/3/6 fractional digits the
# Node/ClickHouse producers emit — see rust `ClickHouseEvent` in
# `rust/common/types/src/event.rs`), then stamp it UTC with `assume_timezone`.
_TIMESTAMPTZ = pa.timestamp("us", tz="UTC")


def _to_timestamptz(col):
    return pc.assume_timezone(col.cast(pa.timestamp("us")), "UTC")


_COERCERS: dict[str, tuple[pa.DataType, object]] = {
    "timestamptz": (_TIMESTAMPTZ, _to_timestamptz),
    "bigint": (pa.int64(), lambda col: col.cast(pa.int64())),
    "double": (pa.float64(), lambda col: col.cast(pa.float64())),
    "boolean": (pa.bool_(), lambda col: col.cast(pa.bool_())),
    "varchar": (pa.string(), lambda col: col.cast(pa.string())),
}

# Public allowlist for config validation (single source of truth).
COERCIBLE_TYPES = frozenset(_COERCERS)


def _coerce_or_null(col, coercer, arrow_type: pa.DataType) -> tuple[object, int]:
    """Coerce ``col`` to ``arrow_type`` via ``coercer``, nulling values that don't
    convert. Returns ``(coerced_column, num_failed)``.

    The fast path is one vectorized cast. Arrow's cast is all-or-nothing — a single
    unconvertible value raises for the whole column — so on failure we fall back to
    a per-value pass that nulls only the bad values and keeps the good ones.

    Crucially the result is ALWAYS ``arrow_type``, even on total failure: a
    configured column must end up the *same* type in every batch, or
    ``pa.concat_tables()`` over the pending buffer raises ``ArrowTypeError`` at
    flush (outside the write-retry/offset path) the moment a coerced batch and a
    fallback batch are concatenated. Leaving the column as its source type would
    move the crash there instead of avoiding it. The per-value pass is a cold path
    — it runs only for a batch that actually contains an unparseable value.
    """
    try:
        return coercer(col), 0
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        src_type = col.type
        out: list = []
        failed = 0
        for v in col.to_pylist():
            if v is None:
                out.append(None)
                continue
            try:
                out.append(coercer(pa.array([v], type=src_type))[0].as_py())
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                out.append(None)
                failed += 1
        return pa.array(out, type=arrow_type), failed


def coerce_typed_columns(table: pa.Table, typed_columns: tuple[tuple[str, str], ...]) -> pa.Table:
    """Pin named columns to a target type before the batch reaches the sink.

    ``typed_columns`` is a sequence of ``(column_name, type_name)`` pairs where
    ``type_name`` is one of ``COERCIBLE_TYPES``. A column is coerced only when it
    is present in the batch and not already the target Arrow type, so the same
    map is safe to point at a superset of columns and at heterogeneous batches.

    A present, configured column is ALWAYS emitted as the target Arrow type — this
    keeps the pending buffer schema-consistent so ``pa.concat_tables()`` at flush
    never raises on a type mismatch. Coercion is non-fatal: values that can't be
    cast (a producer format/type drift) are nulled, and ``errors_total{type=
    "column_coercion"}`` is bumped so the drift is loud via metrics/alerting. It
    never raises on the consume path, where an exception would unwind past main.py's
    offset bookkeeping and risk committing past records never written.
    """
    if not typed_columns:
        return table

    targets = dict(typed_columns)  # name -> type_name
    changed = False
    new_columns = []
    new_fields = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        type_name = targets.get(field.name)
        if type_name is None:
            new_columns.append(col)
            new_fields.append(field)
            continue

        arrow_type, coercer = _COERCERS[type_name]
        if field.type == arrow_type:
            # Already the intended type (e.g. project_id arrived non-null as int64).
            new_columns.append(col)
            new_fields.append(field)
            continue

        coerced_col, failed = _coerce_or_null(col, coercer, arrow_type)
        new_columns.append(coerced_col)
        new_fields.append(pa.field(field.name, arrow_type, nullable=field.nullable))
        if failed:
            log.warning(
                "Coercion of column %s to %s nulled %d unconvertible value(s) this batch",
                field.name,
                type_name,
                failed,
            )
            metrics.errors_total.labels(type="column_coercion").inc()
        else:
            metrics.columns_coerced_total.labels(target_type=type_name).inc()
        changed = True

    if not changed:
        return table
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
    patched = _floatify_integers_in_float_fields(patched, schema)
    table = pa.Table.from_pylist(patched, schema=schema)
    table = _normalize_numeric_types(table)
    table = _drop_null_typed_columns(table)
    return table
