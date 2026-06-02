"""Pydantic models shared by the icebox REST API and the millpond sink.

The sink uses these to construct request bodies; the icebox API uses
them to validate incoming requests. Single source of truth means the
sink can't drift from the API contract.

These approximately mirror the PG row shape in `icebox.files` — not
strictly the same model (the DB has additional bookkeeping columns like
staged_at, committed_at, cycle_id, iceberg_snapshot_id), but the
producer-visible fields are identical.

Wire format rules for parquet_stats are spelled out in ICEBOX-PLAN.md
"Wire format rules" and enforced inside ParquetStats — values are typed
JSON keyed by Iceberg field ID strings, NOT base64/opaque bytes. The
committer alone converts to Iceberg single-value-serialization bytes.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# Bumped on schema-incompatible POST body changes. Mismatch yields a
# loud 400 from the icebox, not silent field loss. v1 == 1.
PROTOCOL_VERSION = 1


class ParquetStats(BaseModel):
    """Column-level stats captured by the writer's ParquetWriter and
    shipped to the icebox.

    All field-id maps key by the Iceberg field ID **as a string** (JSON
    requires string keys). The committer parses int(k) before populating
    DataFile.

    lower_bounds / upper_bounds are typed JSON values, NOT base64-encoded
    bytes. The committer converts to Iceberg single-value-serialization
    bytes via pyiceberg.conversions.to_bytes(iceberg_type, value).
    PyArrow's statistics.min/max format is different and cannot be used
    verbatim.
    """

    column_sizes: dict[str, int] = Field(
        description="Iceberg field id (string) → compressed column size in bytes",
    )
    value_counts: dict[str, int] = Field(
        description="Iceberg field id (string) → total value count (including nulls)",
    )
    null_value_counts: dict[str, int] = Field(
        description="Iceberg field id (string) → null value count",
    )
    nan_value_counts: dict[str, int] = Field(
        default_factory=dict,
        description="Iceberg field id (string) → NaN value count (float/double "
        "columns only; absent for other types)",
    )
    lower_bounds: dict[str, Any] = Field(
        description="Iceberg field id (string) → TYPED JSON min value. "
        "int/long/float/double → number; string → string; date → "
        '"YYYY-MM-DD"; timestamp/timestamptz → ISO-8601 with microsecond '
        "precision; binary/fixed → base64 string; decimal → decimal "
        "string. The committer (NOT the writer) converts to Iceberg "
        "single-value-serialization bytes.",
    )
    upper_bounds: dict[str, Any] = Field(
        description="Iceberg field id (string) → TYPED JSON max value. "
        "Same format rules as lower_bounds.",
    )
    split_offsets: list[int] = Field(
        default_factory=list,
        description="Row group offsets in the parquet file. Optional.",
    )


class RegisterFileRequest(BaseModel):
    """Body of POST /v1/files. The producer (millpond) sends this after
    writing parquet to S3 to register the file for eventual Iceberg
    commit.

    Idempotency: file_path is UNIQUE in icebox.files. Re-POST with the
    same file_path returns 409 with the same RegisteredFile body —
    safe for writer crash + replay.
    """

    protocol_version: int = Field(
        default=PROTOCOL_VERSION,
        description="Wire-protocol version. Icebox rejects mismatched "
        "versions with 400 to catch deploy-skew scenarios.",
    )
    file_path: str = Field(
        description="Full s3:// URI of the parquet file. The "
        "deterministic-path scheme makes this unique per (writer, "
        "kafka_offsets) tuple, so writer replay collides on the same "
        "path and gets dedup'd by UNIQUE(file_path).",
    )
    writer_ordinal: int = Field(
        ge=0,
        description="The millpond writer's ordinal (0..N-1). Operator "
        "triage; not used for uniqueness.",
    )
    kafka_offsets: dict[str, int] = Field(
        description="Per-Kafka-partition max offset included in this "
        "file. Keys are stringified partition ids (JSON-friendly). The "
        "icebox committer commits these offsets to Kafka in the same "
        "cycle as the Iceberg commit.",
    )
    partition_values: dict[str, int] = Field(
        description="Iceberg partition values for the file (e.g. "
        '{"year": 2026, "month": 6, "day": 1, "hour": 14}). The '
        "committer uses these to construct DataFile.partition.",
    )
    record_count: int = Field(
        ge=0,
        description="Number of records in the parquet file.",
    )
    file_size: int = Field(
        ge=0,
        description="Byte size of the parquet file.",
    )
    schema_version: str = Field(
        default="v1",
        description="Producer-side free-form schema tag (e.g. 'v1'). "
        "Operator visibility; the load-bearing check is "
        "schema_fingerprint below.",
    )
    schema_fingerprint: str = Field(
        description="SHA-256 hex of the writer's Iceberg-Schema "
        "model_dump_json() output. The committer compares against the "
        "table's schema fingerprint and rejects mismatches with 400. "
        "Catches silent schema drift before parquet is registered.",
    )
    parquet_stats: ParquetStats = Field(
        description="Column-level stats captured by the writer's "
        "ParquetWriter. The committer builds DataFile records from these "
        "WITHOUT reading parquet footers, eliminating O(files) S3 GETs "
        "per cycle.",
    )


class RegisteredFile(BaseModel):
    """Body of a 201 (or 409 — same shape, idempotent replay) response."""

    row_id: int
    queued_at: datetime


class BackpressureResponse(BaseModel):
    """Body of 429 (queue depth) or 503 (committer degraded) responses.
    Clients should pause and retry after `retry_after_s` seconds."""

    reason: str
    retry_after_s: int
    queue_depth: int | None = None
    consecutive_failures: int | None = None


class StatusResponse(BaseModel):
    """Body of GET /v1/status — observability endpoint.

    `oldest_pending_age_seconds` is the wall-clock age of the oldest
    unclaimed file. Alerts on "icebox is falling behind" should key on
    this rather than absolute pending_files count, since healthy steady-
    state pending counts vary with writer throughput.
    """

    pending_files: int
    oldest_pending_age_seconds: float | None = None
    last_success_at: datetime | None = None
    last_cycle_at: datetime | None = None
    last_committer_heartbeat: datetime | None = None
    consecutive_failures: int = 0
    last_committed_iceberg_snapshot: int | None = None
