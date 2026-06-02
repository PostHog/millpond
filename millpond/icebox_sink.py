"""Sink that ships parquet to S3 and registers it with the icebox service.

Replaces the direct PyIceberg-commit path for high-concurrency writers.
Per ICEBOX-PLAN.md: writers compute a deterministic S3 path, write the
parquet, extract column stats from the ParquetWriter metadata, and POST
a RegisterFileRequest to icebox. The icebox owns Iceberg commit + Kafka
offset commit in a batched cycle.

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

NOTE: wiring into the main consumer loop (main.py:_flush) so kafka
offsets flow to the sink instead of being committed locally is OUT OF
SCOPE here. This module defines the sink + its tests; main.py
integration is a follow-up PR.
"""

from __future__ import annotations

import base64
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx
import pyarrow as pa
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
        wire-format rules in ICEBOX-PLAN.md "Wire format rules".
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


@dataclass
class IceboxSink:
    """Sink that POSTs parquet files to the icebox service instead of
    committing them via PyIceberg directly.

    The sink owns:
      - The Iceberg schema (for field IDs + fingerprint).
      - The IceboxClient (HTTP).
      - The deterministic-path constants (bucket, warehouse prefix).

    It does NOT own the Kafka offsets — those flow in per-batch from the
    caller. The Sink protocol's write(batch) signature is extended via a
    second kwarg `kafka_offsets`; the main.py wiring is a follow-up.

    Field-ID resolution: on first non-empty batch, derive the Iceberg
    schema from the batch's Arrow schema via the same helpers
    millpond/iceberg.py uses. The icebox-side table must have the same
    fingerprint or the icebox will reject our POSTs with 400.
    """

    client: IceboxClient
    writer_ordinal: int
    bucket: str
    warehouse_prefix: str
    namespace: str
    table: str
    schema_version: str = "v1"
    _iceberg_schema: IcebergSchema | None = None
    _fingerprint: str | None = None
    _field_id_by_name: dict[str, int] | None = None

    def _ensure_schema(self, batch_schema: pa.Schema) -> None:
        """Resolve Iceberg schema + fingerprint on first batch."""
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
        kafka_offsets: Mapping[str, int],
        partition_values: Mapping[str, int],
        s3_writer,
    ) -> tuple[dict, int]:
        """Write the batch's parquet bytes to S3 at the deterministic
        path, register with icebox, return (response_body, status_code).

        Args:
            batch: non-empty Arrow table.
            kafka_offsets: {partition_id (str): max_offset_in_batch}
            partition_values: {year/month/day/hour: int}.
            s3_writer: an injectable callable with signature
                `(s3_uri: str, data: bytes) -> None`. Injection makes
                the sink unit-testable without an S3 stub.

        Returns:
            (body, status) where status is 201 or 409.

        Raises:
            IceboxResponseError, IceboxBackpressureExhausted: see
                IceboxClient.register_file.
        """
        if len(batch) == 0:
            raise ValueError("IceboxSink.write called with zero-row batch")

        self._ensure_schema(batch.schema)
        s3_uri = staged_file_path(
            bucket=self.bucket,
            warehouse_prefix=self.warehouse_prefix,
            namespace=self.namespace,
            table=self.table,
            writer_ordinal=self.writer_ordinal,
            kafka_offsets={int(k): v for k, v in kafka_offsets.items()},
            partition_values=dict(partition_values),
        )

        # Write parquet to in-memory buffer, then ship to S3 + extract stats
        buf = pa.BufferOutputStream()
        with pq.ParquetWriter(buf, batch.schema) as writer:
            writer.write_table(batch)
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
        s3_writer(s3_uri, parquet_bytes)

        req = RegisterFileRequest(
            protocol_version=PROTOCOL_VERSION,
            file_path=s3_uri,
            writer_ordinal=self.writer_ordinal,
            kafka_offsets=dict(kafka_offsets),
            partition_values=dict(partition_values),
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
