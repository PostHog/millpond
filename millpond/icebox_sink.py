"""Sink that ships parquet to S3 and registers it via direct PG INSERT.

Replaces the direct PyIceberg-commit path for high-concurrency writers.
Writers compute a deterministic S3 path, write the parquet, extract
column stats from the ParquetWriter metadata, and INSERT a row into
the icebox_files table. The icebox daemon (see icebox/daemon.py) polls
that table and commits the files to Iceberg in batches, advancing
Kafka offsets on the writer's behalf.

Key responsibility distinctions vs IcebergSink:
  - No `table.append()` — the daemon does that for us.
  - No `kafka.commit()` from main.py — must be turned off when this
    sink is selected; the daemon advances offsets atomically (per
    cumulative semantics) with its Iceberg snapshot commit.
  - Schema is resolved against the daemon-side Iceberg table; locally
    we only need the field IDs + fingerprint.

This module exposes:
  - `IceboxClient` — psycopg-pool wrapper. INSERT ... ON CONFLICT
    (file_path) DO NOTHING RETURNING id, inserted_at. Replaced the
    earlier httpx-based REST client when the polling-daemon design
    landed (docs/icebox-self-healing-recovery.md).
  - `parquet_stats_from_metadata` — extract column-level stats from a
    ParquetWriter's metadata.
  - `IceboxSink` — Sink-protocol implementation. Holds the IceboxClient
    and the Iceberg schema (for field IDs + fingerprint).

The sink itself doesn't write Arrow → parquet — that's PyArrow's job;
we wire it into a BytesIO/S3 stream the same way IcebergSink does.
"""

from __future__ import annotations

import base64
import datetime
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from psycopg_pool import ConnectionPool
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.types import (
    BinaryType,
    DateType,
    FixedType,
    TimestampType,
    TimestamptzType,
)

from shared.fingerprint import schema_fingerprint
from shared.models import (
    ParquetStats,
    RegisterFileRequest,
)
from shared.paths import staged_file_path

log = logging.getLogger(__name__)


def build_s3_writer(
    *,
    access_key_id: str | None,
    secret_access_key: str | None,
    region: str | None,
    endpoint: str | None = None,
):
    """Build the production s3_writer callable for IceboxSink.

    Uses PyArrow's S3 filesystem rather than boto3 to avoid pulling
    another dep into millpond (PyArrow is already required).

    Returns a callable with signature `(s3_uri: str, data: bytes) -> None`.
    The URI must start with "s3://" and contain bucket + key.

    Args mirror the s3_* fields in millpond.config.Config.

    Idempotency:
      The icebox protocol relies on same kafka_offsets → same S3 path.
      Bytes are also stable across replay because IceboxSink.write
      takes `inserted_at` as a parameter (the caller derives it
      deterministically from message content — see _flush in
      millpond/main.py). Writer crash + replay produces an identical
      parquet at the same path; the icebox dedups via UNIQUE(file_path)
      on the POST side.

      Caveat: PyArrow's S3 `open_output_stream` uses multipart upload.
      A crash between `out.write(payload)` and `__exit__` leaves an
      abandoned multipart upload that S3 bills for indefinitely (no
      visible object). Mitigate via an S3 lifecycle rule that aborts
      multipart uploads older than N days — operational, in the
      chart's bucket terraform.
    """
    import pyarrow.fs as pafs

    fs_kwargs: dict[str, Any] = {}
    if region:
        fs_kwargs["region"] = region
    if access_key_id:
        fs_kwargs["access_key"] = access_key_id
    if secret_access_key:
        fs_kwargs["secret_key"] = secret_access_key
    if endpoint:
        fs_kwargs["endpoint_override"] = endpoint
    fs = pafs.S3FileSystem(**fs_kwargs)

    def _writer(s3_uri: str, payload: bytes) -> None:
        if not s3_uri.startswith("s3://"):
            raise ValueError(f"expected s3:// URI, got {s3_uri!r}")
        # PyArrow's S3FileSystem expects "bucket/key" (no scheme).
        path = s3_uri[len("s3://") :]
        with fs.open_output_stream(path) as out:
            out.write(payload)

    return _writer


# Status code mapping (preserved from the earlier HTTP-mode protocol
# so IceboxSink's contract with callers is unchanged):
#   - INSERT succeeded → 201
#   - ON CONFLICT (file_path) DO NOTHING with no row inserted → 409
#     (idempotent writer replay)
#
# psycopg errors propagate to main.py's _write_with_retry which retries
# on generic Exception with exponential backoff before crashing the pod.

_ICEBOX_INSERT_SQL = """
INSERT INTO icebox_files (
    file_path, writer_ordinal, kafka_offsets, partition_values,
    record_count, file_size, parquet_stats
) VALUES (
    %(file_path)s, %(writer_ordinal)s, %(kafka_offsets)s::jsonb,
    %(partition_values)s::jsonb, %(record_count)s, %(file_size)s,
    %(parquet_stats)s::jsonb
)
ON CONFLICT (file_path) DO NOTHING
RETURNING id, inserted_at
"""

_ICEBOX_LOOKUP_SQL = """
SELECT id, inserted_at FROM icebox_files WHERE file_path = %(file_path)s
"""


@dataclass
class IceboxClient:
    """psycopg-backed client that INSERTs RegisterFileRequest rows into
    icebox_files. The icebox daemon picks them up via SELECT FOR UPDATE
    SKIP LOCKED on its tick cadence.

    Attributes mirror PG connect parameters. The pool is built lazily
    in __post_init__; `close()` drains it. INSERTs run against the
    schema configured via `options=-csearch_path=<schema>` so the
    unqualified `icebox_files` reference resolves to the right
    per-deployment schema (matches icebox's own setup).

    Pool sized small (min=0, max=2) because the writer flushes at most
    ~1/min per pod; one connection at a time is enough, and lazy is
    fine — let it grow on demand.
    """

    host: str
    port: int
    database: str
    username: str
    password: str
    schema: str
    sslmode: str = "require"
    pool_min: int = 0
    pool_max: int = 2
    _pool: ConnectionPool | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        if self._pool is None:
            conninfo = psycopg.conninfo.make_conninfo(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.username,
                password=self.password,
                sslmode=self.sslmode,
                options=f"-csearch_path={self.schema}",
            )
            # `open=False` so a malformed conninfo or unreachable PG
            # fails on first use (during a write retry loop, which
            # logs it) rather than at construction (which would fail
            # the whole pod boot silently from the operator's POV).
            self._pool = ConnectionPool(
                conninfo,
                min_size=self.pool_min,
                max_size=self.pool_max,
                open=False,
            )

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()

    def register_file(self, req: RegisterFileRequest) -> tuple[dict, int]:
        """INSERT one row into icebox_files. Returns (body, status).

        Body shape matches the HTTP RegisteredFile model
        (`{row_id, queued_at}`) so callers that inspected the body
        keep working — but in practice nobody reads it; sink.write
        returns it verbatim to main.py which only checks `status in
        (201, 409)`.

        Status:
          - 201 if the INSERT inserted a new row.
          - 409 if a row with the same file_path already existed
            (idempotent replay).

        Raises:
          psycopg.Error: transport / SQL errors. Surrounding
            _write_with_retry catches and retries.
        """
        assert self._pool is not None  # set in __post_init__
        params = {
            "file_path": req.file_path,
            "writer_ordinal": req.writer_ordinal,
            "kafka_offsets": json.dumps(dict(req.kafka_offsets)),
            "partition_values": json.dumps(dict(req.partition_values)),
            "record_count": req.record_count,
            "file_size": req.file_size,
            "parquet_stats": req.parquet_stats.model_dump_json(),
        }
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_ICEBOX_INSERT_SQL, params)
                row = cur.fetchone()
            if row is not None:
                # New row inserted. Body shape matches the HTTP
                # RegisteredFile model for caller compatibility.
                return (
                    {"row_id": row[0], "queued_at": row[1].isoformat()},
                    201,
                )
            # Conflict: look up the existing row's id/timestamp.
            # Costs a second round-trip but happens only on writer
            # replay, which is rare.
            with conn.cursor() as cur:
                cur.execute(_ICEBOX_LOOKUP_SQL, {"file_path": req.file_path})
                existing = cur.fetchone()
        if existing is None:
            # Same impossibility guard as icebox/postgres_async.py:
            # ON CONFLICT DO NOTHING returning no row AND the lookup
            # finding nothing would require a concurrent DELETE, which
            # icebox never does.
            raise RuntimeError(
                f"INSERT...DO NOTHING returned no row AND lookup returned "
                f"no row for file_path={req.file_path!r}: this should be "
                f"impossible without a concurrent DELETE."
            )
        return (
            {"row_id": existing[0], "queued_at": existing[1].isoformat()},
            409,
        )


def parquet_stats_from_metadata(
    parquet_meta: pq.FileMetaData,
    *,
    iceberg_schema: IcebergSchema,
    arrow_to_iceberg_field_id: Mapping[str, int],
) -> ParquetStats:
    """Aggregate per-row-group column stats into the icebox wire format.

    Args:
        parquet_meta: from `ParquetFile(buf).metadata` after writing.
        iceberg_schema: the Iceberg schema (with assigned field IDs)
            that the parquet conforms to. We use it to choose the right
            typed-JSON encoding for each column's bound.
        arrow_to_iceberg_field_id: maps parquet column name → Iceberg
            field id (the writer holds this mapping when building the
            schema).

    Returns:
        ParquetStats with field-id-keyed dicts. Values follow the
        wire-format rules captured in ``shared/bounds.py``.
    """
    if parquet_meta.num_row_groups == 0:
        return ParquetStats(
            column_sizes={},
            value_counts={},
            null_value_counts={},
            lower_bounds={},
            upper_bounds={},
        )

    column_sizes: dict[str, int] = {}
    value_counts: dict[str, int] = {}
    null_counts: dict[str, int] = {}
    nan_counts: dict[str, int] = {}
    lower_raw: dict[str, Any] = {}
    upper_raw: dict[str, Any] = {}

    field_by_id = {f.field_id: f for f in iceberg_schema.fields}

    for rg_idx in range(parquet_meta.num_row_groups):
        rg = parquet_meta.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col = rg.column(col_idx)
            col_name = col.path_in_schema
            fid = arrow_to_iceberg_field_id.get(col_name)
            if fid is None:
                continue
            sid = str(fid)
            column_sizes[sid] = column_sizes.get(sid, 0) + col.total_compressed_size
            value_counts[sid] = value_counts.get(sid, 0) + col.num_values
            if col.statistics is not None:
                if col.statistics.has_null_count:
                    null_counts[sid] = null_counts.get(sid, 0) + col.statistics.null_count
                if col.statistics.has_min_max:
                    field = field_by_id.get(fid)
                    if field is None:
                        continue
                    lo = _wire_encode(field.field_type, col.statistics.min)
                    hi = _wire_encode(field.field_type, col.statistics.max)
                    # Aggregate min/max across row groups
                    if sid not in lower_raw or _compare(field.field_type, lo, lower_raw[sid]) < 0:
                        lower_raw[sid] = lo
                    if sid not in upper_raw or _compare(field.field_type, hi, upper_raw[sid]) > 0:
                        upper_raw[sid] = hi

    return ParquetStats(
        column_sizes=column_sizes,
        value_counts=value_counts,
        null_value_counts=null_counts,
        nan_value_counts=nan_counts,
        lower_bounds=lower_raw,
        upper_bounds=upper_raw,
    )


def _wire_encode(iceberg_type, value):
    """Convert a PyArrow stats value to the icebox wire format.

    PyArrow returns native Python objects (int, float, str, bytes,
    datetime, date). The wire format wants:
      - date → "YYYY-MM-DD"
      - timestamp → ISO-8601 with microsecond precision
      - binary/fixed → base64 string
      - others → pass-through
    """
    if value is None:
        return None
    if isinstance(iceberg_type, DateType):
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value
    if isinstance(iceberg_type, (TimestampType, TimestamptzType)):
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return value
    if isinstance(iceberg_type, (BinaryType, FixedType)):
        if isinstance(value, bytes):
            return base64.b64encode(value).decode("ascii")
        return value
    return value


def _compare(iceberg_type, a, b) -> int:
    """Comparison appropriate for aggregating min/max across row groups.
    String comparison works for wire-encoded timestamps/dates by ISO
    ordering; numeric for ints/floats; lex for strings; base64-string
    for binary (NOT semantically equivalent to byte comparison, but for
    aggregating min/max across row groups it's stable per-aggregation)."""
    if a == b:
        return 0
    return -1 if a < b else 1


# Reserved column names — kept in sync with millpond/iceberg.py via a
# runtime equivalence assertion in tests/unit/test_icebox_sink.py.
# Kept inline rather than imported to avoid pulling
# pyiceberg/aiohttp/cryptography into a DuckLake-only deployment.
_PARTITION_COLS: tuple[str, ...] = ("year", "month", "day", "hour")
_RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at", *_PARTITION_COLS})


def _add_metadata_columns(batch: pa.Table, inserted_at: datetime.datetime) -> pa.Table:
    """Append ``_inserted_at`` + the four partition columns to the batch.

    Equivalent to millpond/iceberg.py:_add_metadata_columns in SHAPE
    (column names, types, partition derivation), but the timestamp
    comes from the CALLER, NOT from wall-clock ``now()``. Determinism
    is load-bearing for the icebox sink: the S3 path embeds the
    partition tuple, and idempotent replay requires same-input-→-same-
    path. A wall-clock stamp here breaks that on any hour-boundary
    crash + replay.

    Caller convention (main.py:_flush for the icebox dispatch): pass
    the MAX Kafka message timestamp the batch contains. Replay of the
    same Kafka offsets produces the same MAX, so the partition tuple
    is invariant under replay.

    int32 cast matches the partition spec's IntegerType (PyArrow's
    pc.year/month/day/hour return int64).
    """
    if inserted_at.tzinfo is None:
        raise ValueError(f"inserted_at must be tz-aware (UTC); got naive datetime {inserted_at!r}")
    ts_type = pa.timestamp("us", tz="UTC")
    ts_array = pa.array([inserted_at] * len(batch), ts_type)
    batch = batch.append_column("_inserted_at", ts_array)
    ts = batch.column("_inserted_at")
    batch = batch.append_column("year", pc.cast(pc.year(ts), pa.int32()))
    batch = batch.append_column("month", pc.cast(pc.month(ts), pa.int32()))
    batch = batch.append_column("day", pc.cast(pc.day(ts), pa.int32()))
    batch = batch.append_column("hour", pc.cast(pc.hour(ts), pa.int32()))
    return batch


def _partition_values_from_batch(batch: pa.Table) -> dict[str, int]:
    """Read the partition tuple from a batch produced by
    _add_metadata_columns. Asserts all rows share the same tuple — the
    convention is enforced by the upstream stamp, but a defensive
    check here catches a future refactor that splits the helpers.
    """
    values: dict[str, int] = {}
    for name in _PARTITION_COLS:
        col = batch.column(name)
        head = col[0].as_py()
        # `pc.all(pc.equal(col, head))` would be more idiomatic but
        # builds a full boolean column for a check that's O(1) common-
        # case (one tuple). Comparing min/max is faster and asserts the
        # invariant equally well.
        col_min = pc.min(col).as_py()
        col_max = pc.max(col).as_py()
        if col_min != head or col_max != head:
            raise ValueError(
                f"_partition_values_from_batch: column {name!r} has multiple "
                f"values [{col_min}, {col_max}]; partition stamping must "
                f"produce one value per batch"
            )
        values[name] = head
    return values


@dataclass
class IceboxSink:
    """Sink that ships parquet files to S3 and INSERTs them into the
    icebox_files table for the icebox daemon to commit to Iceberg.

    The sink owns:
      - The Iceberg schema (for field IDs + fingerprint).
      - The IceboxClient (psycopg pool).
      - The deterministic-path constants (bucket, warehouse prefix).
      - An s3_writer callable for shipping parquet bytes to S3.

    Per-batch state (kafka_offsets) flows in via kwargs on write().

    Field-ID resolution: on first non-empty batch, derive the Iceberg
    schema from the batch's Arrow schema (after metadata columns are
    added) via the same helpers millpond/iceberg.py uses.
    """

    client: IceboxClient
    writer_ordinal: int
    bucket: str
    warehouse_prefix: str
    namespace: str
    table: str
    s3_writer: Any = None  # callable (s3_uri: str, data: bytes) -> None
    schema_version: str = "v1"
    _iceberg_schema: IcebergSchema | None = None
    _fingerprint: str | None = None
    _field_id_by_name: dict[str, int] | None = None

    def _ensure_schema(self, batch_schema: pa.Schema) -> None:
        """Resolve Iceberg schema + fingerprint on first batch. Caller
        must have already added metadata columns to the batch."""
        if self._iceberg_schema is not None:
            return
        ice_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(batch_schema))
        self._iceberg_schema = ice_schema
        self._fingerprint = schema_fingerprint(ice_schema)
        self._field_id_by_name = {f.name: f.field_id for f in ice_schema.fields}

    def write(
        self,
        batch: pa.Table,
        *,
        kafka_offsets: Mapping[str, int] | None = None,
        inserted_at: datetime.datetime | None = None,
        s3_writer: Any = None,
    ) -> tuple[dict, int]:
        """Write the batch's parquet bytes to S3 at the deterministic
        path, register with icebox, return (response_body, status_code).

        The sink internally:
          1. Adds _inserted_at + (year, month, day, hour) columns
             (stamped from the caller's `inserted_at`, NOT wall-clock).
          2. Derives partition_values from the appended columns (all
             rows share one tuple).
          3. Computes the deterministic S3 path.
          4. Writes parquet to a BytesIO buffer, extracts stats.
          5. Ships bytes to S3 via the writer callable.
          6. POSTs RegisterFileRequest to icebox.

        Args:
            batch: non-empty Arrow table WITHOUT metadata columns. The
                sink adds them — callers MUST NOT pre-stamp.
            kafka_offsets: {partition_id (str): max_offset_in_batch}.
                Required when called from production; main.py:_flush
                computes it from the consumer's per-partition offsets.
            inserted_at: UTC datetime to use for `_inserted_at` AND
                the partition tuple. Caller MUST derive this
                deterministically from message content (millpond:_flush
                uses the MAX Kafka message timestamp seen in the
                batch). Required because wall-clock now() would break
                replay determinism: the S3 path embeds the partition
                tuple, and same-offsets-→-same-path is the dedup contract.
            s3_writer: per-call override for the instance attribute.
                Used by tests; production wires it once at construction.

        Returns:
            (body, status) where status is 201 (new) or 409 (replay).

        Raises:
            ValueError: zero-row batch, or kafka_offsets / inserted_at /
                s3_writer missing.
            psycopg.Error: see IceboxClient.register_file.
        """
        if len(batch) == 0:
            raise ValueError("IceboxSink.write called with zero-row batch")
        if kafka_offsets is None:
            raise ValueError(
                "IceboxSink.write requires kafka_offsets; the icebox "
                "commits Kafka offsets atomically with the Iceberg snapshot"
            )
        if inserted_at is None:
            raise ValueError(
                "IceboxSink.write requires inserted_at (UTC datetime) "
                "derived deterministically from the batch's messages — "
                "wall-clock now() would break replay determinism"
            )
        writer = s3_writer if s3_writer is not None else self.s3_writer
        if writer is None:
            raise ValueError(
                "IceboxSink.write requires an s3_writer (either passed per-call or set on the sink at construction)"
            )

        # Defense in depth: catch operators piping a pre-stamped batch
        # to the icebox sink — would double-add and break schema/fingerprint.
        for col in _RESERVED_COLUMNS:
            if col in batch.schema.names:
                raise ValueError(
                    f"IceboxSink.write received a batch with reserved column "
                    f"{col!r} already present; metadata columns are added "
                    f"by the sink, not the caller"
                )

        batch_with_meta = _add_metadata_columns(batch, inserted_at)
        partition_values = _partition_values_from_batch(batch_with_meta)

        self._ensure_schema(batch_with_meta.schema)
        s3_uri = staged_file_path(
            bucket=self.bucket,
            warehouse_prefix=self.warehouse_prefix,
            namespace=self.namespace,
            table=self.table,
            writer_ordinal=self.writer_ordinal,
            kafka_offsets={int(k): v for k, v in kafka_offsets.items()},
            partition_values=partition_values,
        )

        # Write parquet to in-memory buffer, then ship to S3 + extract stats
        buf = pa.BufferOutputStream()
        with pq.ParquetWriter(buf, batch_with_meta.schema) as pq_writer:
            pq_writer.write_table(batch_with_meta)
        parquet_bytes = buf.getvalue().to_pybytes()

        # Extract stats BEFORE shipping — they're a function of the
        # buffer we just wrote, not of S3.
        meta = pq.ParquetFile(pa.BufferReader(parquet_bytes)).metadata
        stats = parquet_stats_from_metadata(
            meta,
            iceberg_schema=self._iceberg_schema,
            arrow_to_iceberg_field_id=self._field_id_by_name,
        )

        # Ship parquet to S3. Idempotent: same path = same bytes (replay-safe).
        writer(s3_uri, parquet_bytes)

        req = RegisterFileRequest(
            # protocol_version + expected_iceberg_namespace / _table
            # were validation handshakes against the HTTP perimeter.
            # The PG INSERT doesn't store them (icebox_files doesn't
            # have those columns); leaving them at their defaults.
            file_path=s3_uri,
            writer_ordinal=self.writer_ordinal,
            kafka_offsets=dict(kafka_offsets),
            partition_values=partition_values,
            record_count=meta.num_rows,
            file_size=len(parquet_bytes),
            schema_version=self.schema_version,
            schema_fingerprint=self._fingerprint,
            parquet_stats=stats,
        )
        return self.client.register_file(req)

    def reset_caches(self) -> None:
        """Used by main.py's _write_with_retry. For icebox: no PyIceberg
        catalog cache to invalidate, but we DO want to re-derive the
        schema on next write — in case the operator reshaped upstream."""
        self._iceberg_schema = None
        self._fingerprint = None
        self._field_id_by_name = None

    def close(self) -> None:
        self.client.close()
