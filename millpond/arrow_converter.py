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
#
# uuid is the one target whose Arrow type is an EXTENSION type rather than a
# plain one. The events topic carries `uuid` and `person_id` as UUID strings
# (ClickHouse `UUID`); both destinations have a real UUID type for them, and
# `pa.uuid()` — pyarrow's canonical uuid extension type, an extension over
# `fixed_size_binary(16)` holding the 16 big-endian bytes — is the ONE wire
# form that lands correctly on both:
#   - hoglake: pyhoglake maps `fixed_size_binary(16)` to its `uuid` column type
#     and accepts `pa.uuid()` as the same thing (pyhoglake `types.py`
#     `arrow_type_to_coltype`);
#   - DuckLake: DuckDB reads an Arrow `pa.uuid()` column as native `UUID`. A
#     plain `pa.binary(16)` lands as `BLOB` there instead — silently, because
#     the bytes are identical — which is why the coercer emits the extension
#     type and adopts a bare `binary(16)` into it rather than passing it
#     through.
# The extension type also carries to parquet: pyarrow stamps
# FIXED_LEN_BYTE_ARRAY(16) + `LogicalTypeAnnotation.uuidType()` for `pa.uuid()`
# and no annotation at all for `pa.binary(16)`, and that annotation is what an
# Iceberg reader binds a uuid column through — the Trino hoglake connector
# among them. It survives all the way into the uploaded object on the hoglake
# path too, since the pyhoglake >= 1.3.0 pin: the schema `HoglakeSink._prepare`
# casts to names `pa.uuid()` for a uuid column, so this column passes through
# unchanged (tests/unit/test_hoglake.py::TestUuidColumnWireForm).
_TIMESTAMPTZ = pa.timestamp("us", tz="UTC")

#: The `uuid` target's Arrow type. Public: main.py compares filter/sort columns
#: against it, and hoglake.py recognises it when degrading against a live
#: `string` column.
UUID_TYPE = pa.uuid()
UUID_STORAGE_TYPE = pa.binary(16)

_URN_PREFIX = "urn:uuid:"


def _to_timestamptz(col):
    return pc.assume_timezone(col.cast(pa.timestamp("us")), "UTC")


def uuid_bytes(value: object) -> bytes:
    """The 16 big-endian bytes of one UUID string. Raises `ValueError` otherwise.

    The accepted shapes are ENUMERATED, deliberately narrower than
    `uuid.UUID`'s grammar:

      * canonical 8-4-4-4-12 hex, either case;
      * 32 hex digits with no separators;
      * either of those wrapped in `{...}`, or prefixed with `urn:uuid:`.

    `uuid.UUID` was the fallback here and had to go: it strips `urn:` and
    `uuid:` from ANYWHERE in the string, removes EVERY hyphen wherever it
    sits, and hands what is left to `int(text, 16)` — which itself accepts a
    `0x` prefix, a leading sign and PEP 515 underscores — so it silently
    remaps values that are not UUIDs at all into plausible-looking ones
    nobody can trace back (measured on CPython 3.13:
    `0x0123456789abcdef0123456789abcd` becomes
    `00012345-6789-abcd-ef01-23456789abcd`). A producer emitting one of those
    is a defect worth a NULL and an
    `errors_total{type="column_coercion"}`, not a made-up identifier.

    Two things this deliberately does NOT do:

      * It does not validate RFC 4122 variant/version bits. ClickHouse `UUID`
        is an opaque 128-bit value and PostHog writes v4 and v7 both; rejecting
        a "wrong" variant would drop real data.
      * The `urn:uuid:` prefix is matched case-SENSITIVELY (`URN:UUID:` is
        refused). The hex digits themselves are case-insensitive.
    """
    if not isinstance(value, str):
        raise ValueError(f"not a UUID string: {type(value).__name__}")
    text = value
    if text.startswith(_URN_PREFIX):
        text = text[len(_URN_PREFIX) :]
    elif len(text) >= 2 and text[0] == "{" and text[-1] == "}":
        text = text[1:-1]
    if len(text) == 36:
        # Hyphens at exactly the canonical offsets. An extra hyphen INSIDE a
        # group survives this check and is caught by the length test below,
        # because `replace` takes it out too and the result comes up short.
        if text[8] != "-" or text[13] != "-" or text[18] != "-" or text[23] != "-":
            raise ValueError(f"not a UUID: {value!r}")
        text = text.replace("-", "")
    if len(text) != 32:
        raise ValueError(f"not a UUID: {value!r}")
    try:
        raw = bytes.fromhex(text)
    except ValueError:
        raise ValueError(f"not a UUID: {value!r}") from None
    if len(raw) != 16:
        # `bytes.fromhex` skips ASCII whitespace, so "aabb ... ccdd" with two
        # internal spaces is 32 characters and decodes to 15 bytes. This is the
        # only guard against that silent short decode.
        raise ValueError(f"not a UUID: {value!r}")
    return raw


def uuid_text(raw: object) -> str:
    """Canonical hyphenated text for 16 big-endian UUID bytes.

    The inverse of `uuid_bytes` for the shapes that round-trip, and the
    rendering used wherever a uuid column's VALUE has to be human-readable
    again: the `filter_matched_total` label, and the restringify degradation
    in hoglake.py when the live column is `string`.
    """
    if not isinstance(raw, (bytes, bytearray)) or len(raw) != 16:
        raise ValueError(f"not 16 UUID bytes: {raw!r}")
    hexed = bytes(raw).hex()
    return f"{hexed[0:8]}-{hexed[8:12]}-{hexed[12:16]}-{hexed[16:20]}-{hexed[20:32]}"


def _to_uuid(col):
    """Coerce a column of UUID strings to `pa.uuid()`.

    Already-`pa.uuid()` is returned untouched (the registry skips that case
    before we get here; the guard keeps `_coerce_or_null`'s per-value retry
    honest). A binary column whose values are already the 16 bytes —
    `fixed_size_binary(16)`, or a variable-width `binary`/`large_binary` that
    happens to hold 16-byte values — is adopted rather than re-parsed; the
    fixed-width case is zero-copy, the variable-width one is one Arrow cast
    that raises for any other width (and `_coerce_or_null` then nulls exactly
    the wrong-width values).

    Strings are decoded value-by-value: pyarrow compute has no hex-decode
    kernel, so there is no vectorized route to the bytes. Measured at ~11.8 ms
    per 27k-row column (~0.44 us/value; ~23 ms for the events pair
    `uuid` + `person_id`) against a ~60 s flush interval — under
    `_apply_sort`'s 50-200 ms per flush and far under the flush budget at both
    the ~1.1k rec/s dev rate and 30k+ in prod. A bulk "join every value, one
    `bytes.fromhex`, slice the buffer" variant measured 7.2 ms — 18% for a
    decode that misaligns silently when two values' lengths compensate, so it
    is not worth the correctness surface. `_coerce_or_null`'s per-value
    fallback costs ~134 ms for a 27k-row batch that contains any unparseable
    value; that is the cold path, and it is a batch that is already anomalous.
    """
    if col.type == UUID_TYPE:
        return col
    if pa.types.is_fixed_size_binary(col.type) and col.type.byte_width == 16:
        storage = col.combine_chunks() if isinstance(col, pa.ChunkedArray) else col
        return pa.ExtensionArray.from_storage(UUID_TYPE, storage)
    if pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type):
        storage = col.cast(UUID_STORAGE_TYPE)
        if isinstance(storage, pa.ChunkedArray):
            storage = storage.combine_chunks()
        return pa.ExtensionArray.from_storage(UUID_TYPE, storage)
    try:
        decoded = [None if v is None else uuid_bytes(v) for v in col.to_pylist()]
    except ValueError as e:
        # The registry's contract is that a coercer signals an unconvertible
        # value with an Arrow error; `_coerce_or_null` then nulls exactly the
        # bad values via its per-value pass. `uuid_bytes` raises ValueError for
        # a bad string AND for a non-string source column, so one arm covers
        # both — translate rather than let it escape to the consume path.
        raise pa.ArrowInvalid(str(e)) from e
    return pa.ExtensionArray.from_storage(UUID_TYPE, pa.array(decoded, type=UUID_STORAGE_TYPE))


_COERCERS: dict[str, tuple[pa.DataType, object]] = {
    "timestamptz": (_TIMESTAMPTZ, _to_timestamptz),
    "bigint": (pa.int64(), lambda col: col.cast(pa.int64())),
    "double": (pa.float64(), lambda col: col.cast(pa.float64())),
    "boolean": (pa.bool_(), lambda col: col.cast(pa.bool_())),
    "varchar": (pa.string(), lambda col: col.cast(pa.string())),
    "uuid": (UUID_TYPE, _to_uuid),
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

    An extension target (``uuid``) is rebuilt through its STORAGE type. The
    per-value ``as_py()`` of an extension scalar is the logical Python object
    (``uuid.UUID``), which ``pa.array`` has no way to lay back out; the storage
    scalar is the 16 bytes, which it does.
    """
    try:
        return coercer(col), 0
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        src_type = col.type
        extension = arrow_type if isinstance(arrow_type, pa.BaseExtensionType) else None
        build_type = extension.storage_type if extension is not None else arrow_type
        out: list = []
        failed = 0
        for v in col.to_pylist():
            if v is None:
                out.append(None)
                continue
            try:
                one = coercer(pa.array([v], type=src_type))
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                out.append(None)
                failed += 1
                continue
            out.append((one.storage if extension is not None else one)[0].as_py())
        built = pa.array(out, type=build_type)
        if extension is not None:
            built = pa.ExtensionArray.from_storage(extension, built)
        return built, failed


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
