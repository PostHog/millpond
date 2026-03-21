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


def convert(messages: list[bytes]) -> pa.Table | None:
    """Convert raw Kafka message values to an Arrow table.

    Parses JSON via orjson, builds a PyArrow table, and casts all numeric
    columns to DOUBLE to prevent INT64/DOUBLE type wobble across batches.

    Returns None if no valid records were parsed.
    """
    records = []
    for raw in messages:
        try:
            records.append(orjson.loads(raw))
        except orjson.JSONDecodeError:
            log.warning("Skipping malformed JSON: %s", raw[:200])

    if not records:
        return None

    table = pa.Table.from_pylist(records)
    table = _cast_numeric_to_double(table)
    return table
