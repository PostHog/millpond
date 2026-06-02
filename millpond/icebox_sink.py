"""Sink that ships parquet to S3 and registers it with the icebox service.

Replaces the direct PyIceberg-commit path for high-concurrency writers.
Writers compute a deterministic S3 path, write the parquet, extract
column stats from the ParquetWriter metadata, and POST a
RegisterFileRequest to icebox. The icebox owns Iceberg commit + Kafka
offset commit in a batched cycle. See ``icebox/README.md`` for the
service design.

Key responsibility distinctions vs IcebergSink:
  - No `table.append()` — the icebox does that for us.
  - No `kafka.commit()` from main.py — must be turned off when this
    sink is selected; the icebox advances offsets atomically with its
    Iceberg snapshot commit.
  - Schema is resolved against the icebox-side Iceberg table; locally
    we only need the field IDs + fingerprint.

This module exposes:
  - `IceboxClient` — httpx-based REST client with internal 429/503
    backoff. Bounded retry budget; after it's exhausted, raises and the
    surrounding millpond _write_with_retry loop handles pod-restart.
  - `parquet_stats_from_metadata` — extract column-level stats from a
    ParquetWriter's metadata, conform to the icebox wire format.
  - `IceboxSink` — Sink-protocol implementation. Holds the IceboxClient
    and the Iceberg schema (for field IDs + fingerprint).

The sink itself doesn't write Arrow → parquet — that's PyArrow's job;
we wire it into a BytesIO/S3 stream the same way IcebergSink does.

Integration with the main consumer loop (millpond/main.py:_flush) is
in place: when cfg.destination == "icebox", _flush passes kafka_offsets
to sink.write and SKIPS the local kafka.commit() — the icebox commits
offsets atomically with its Iceberg snapshot.
"""

from __future__ import annotations

import base64
import datetime
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
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
    PROTOCOL_VERSION,
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


class IceboxResponseError(RuntimeError):
    """Non-retryable error response from icebox (400, validation errors)."""


class IceboxBackpressureExhausted(RuntimeError):
    """Internal retry budget exhausted after sustained 429/503 from icebox.
    The surrounding _write_with_retry path takes over from here."""


@dataclass
class IceboxClient:
    """Thin httpx wrapper that POSTs RegisterFileRequest and absorbs
    backpressure responses (429/503) with bounded backoff.

    Attributes:
        base_url: e.g. "http://icebox.megaberg:8000"
        max_attempts: how many times to try (default 6). Each attempt
            backs off by Retry-After if present, else exponential.
        max_backoff_s: cap on the exponential portion of backoff.
        timeout_s: per-request HTTP timeout.
    """

    base_url: str
    max_attempts: int = 6
    max_backoff_s: float = 30.0
    timeout_s: float = 10.0
    _client: httpx.Client | None = None

    def __post_init__(self):
        if self._client is None:
            self._client = httpx.Client(timeout=self.timeout_s)

    def close(self) -> None:
        if self._client is not None:
            self._client.close()

    def register_file(self, req: RegisterFileRequest) -> tuple[dict, int]:
        """POST /v1/files with bounded backoff on 429/503.

        Returns:
            (response_body, status_code). 201/409 are treated as success.

        Raises:
            IceboxResponseError: on 400 (protocol mismatch, validation).
            IceboxBackpressureExhausted: max_attempts reached with 429/503.
            httpx.HTTPError: transport-level errors after exhausting
                retries (the surrounding _write_with_retry handles these).
        """
        url = f"{self.base_url.rstrip('/')}/v1/files"
        body = req.model_dump(mode="json")
        last_exc: BaseException | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                resp = self._client.post(url, json=body)
            except httpx.HTTPError as e:
                last_exc = e
                self._sleep_for_attempt(attempt, retry_after=None)
                continue

            if resp.status_code in (201, 409):
                return resp.json(), resp.status_code
            if resp.status_code == 400:
                raise IceboxResponseError(f"icebox rejected request as invalid: {resp.text}")
            if resp.status_code in (429, 503):
                retry_after = self._parse_retry_after(resp)
                log.warning(
                    "icebox returned %d on attempt %d/%d; sleeping %.1fs",
                    resp.status_code,
                    attempt,
                    self.max_attempts,
                    retry_after,
                )
                # Final attempt — surface the exhausted error WITHOUT
                # sleeping again
                if attempt >= self.max_attempts:
                    break
                self._sleep_for_attempt(attempt, retry_after=retry_after)
                continue
            # 5xx other than 503 — retry with backoff but no Retry-After
            if resp.status_code >= 500:
                log.warning(
                    "icebox returned %d on attempt %d/%d; backing off",
                    resp.status_code,
                    attempt,
                    self.max_attempts,
                )
                if attempt >= self.max_attempts:
                    raise IceboxBackpressureExhausted(f"icebox 5xx after {attempt} attempts: {resp.text}")
                self._sleep_for_attempt(attempt, retry_after=None)
                continue
            # Anything else (e.g., 422 from FastAPI on body validation)
            raise IceboxResponseError(f"icebox returned unexpected {resp.status_code}: {resp.text}")
        # Loop exit without success
        if last_exc is not None:
            raise last_exc
        raise IceboxBackpressureExhausted(f"icebox backpressure persisted across {self.max_attempts} attempts")

    def _parse_retry_after(self, resp: httpx.Response) -> float:
        """Respect Retry-After header; fall back to body's retry_after_s
        field, then exponential."""
        header = resp.headers.get("Retry-After")
        if header is not None:
            try:
                return float(header)
            except ValueError:
                pass
        try:
            body = resp.json()
            v = body.get("retry_after_s")
            if v is not None:
                return float(v)
        except (ValueError, AttributeError):
            pass
        return 1.0

    def _sleep_for_attempt(self, attempt: int, *, retry_after: float | None) -> None:
        """If the server gave us a Retry-After, use it (capped). Otherwise
        exponential backoff capped at max_backoff_s."""
        if retry_after is not None:
            time.sleep(min(retry_after, self.max_backoff_s))
            return
        backoff = min(2**attempt, self.max_backoff_s)
        time.sleep(backoff)


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
    """Sink that POSTs parquet files to the icebox service instead of
    committing them via PyIceberg directly.

    The sink owns:
      - The Iceberg schema (for field IDs + fingerprint).
      - The IceboxClient (HTTP).
      - The deterministic-path constants (bucket, warehouse prefix).
      - An s3_writer callable for shipping parquet bytes to S3.

    Per-batch state (kafka_offsets) flows in via kwargs on write().

    Field-ID resolution: on first non-empty batch, derive the Iceberg
    schema from the batch's Arrow schema (after metadata columns are
    added) via the same helpers millpond/iceberg.py uses. The icebox-
    side table must have the same fingerprint or the icebox will
    reject our POSTs with 400.
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
            IceboxResponseError, IceboxBackpressureExhausted: see
                IceboxClient.register_file.
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
            protocol_version=PROTOCOL_VERSION,
            # Validation-only: tell the icebox what (ns, table) this
            # writer thinks it's targeting. Icebox 400s on mismatch,
            # catching writers POSTing to the wrong icebox URL before
            # the file silently lands in the wrong Iceberg table.
            expected_iceberg_namespace=self.namespace,
            expected_iceberg_table=self.table,
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
