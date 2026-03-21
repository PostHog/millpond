import logging

import orjson
import pyarrow as pa

log = logging.getLogger(__name__)


def _cast_numeric_to_double(table: pa.Table) -> pa.Table:
    """Cast all integer and float columns to float64 to avoid type wobble across batches."""
    for i, field in enumerate(table.schema):
        if pa.types.is_integer(field.type) or pa.types.is_floating(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.float64()))
    return table


def _build_schema(records: list[dict]) -> pa.Schema:
    """Infer Arrow schema from the union of all keys across all records.

    pa.Table.from_pylist() only uses the first record's keys to infer the schema,
    silently dropping fields that only appear in later records. This function
    scans all records to build the complete key set, then lets PyArrow infer
    types from a single-record table per field.
    """
    all_keys: dict[str, None] = {}  # ordered set via dict
    for record in records:
        for key in record:
            if key not in all_keys:
                all_keys[key] = None

    # Build schema by inferring type from first non-null value for each key
    fields = []
    for key in all_keys:
        sample = None
        for record in records:
            if key in record and record[key] is not None:
                sample = record[key]
                break
        if sample is None:
            fields.append(pa.field(key, pa.string(), nullable=True))
        else:
            # Let PyArrow infer the type from a single-element table
            inferred = pa.Table.from_pylist([{key: sample}]).schema.field(key)
            fields.append(pa.field(key, inferred.type, nullable=True))

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
    table = _cast_numeric_to_double(table)
    return table
