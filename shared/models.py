"""Pydantic models shared by the icebox REST API and the millpond sink.

The sink uses these to construct request bodies; the icebox API uses
them to validate incoming requests. Single source of truth means the
sink can't drift from the API contract.

These also approximately mirror the PG row shape in `icebox.files` —
not strictly the same model (the DB has additional bookkeeping columns
like staged_at, committed_at, iceberg_snapshot_id), but the producer-
visible fields are identical.
"""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RegisterFileRequest(BaseModel):
    """Body of POST /v1/files. The producer (millpond) sends this after
    writing parquet to S3 to register the file for eventual Iceberg
    commit."""

    file_path: str = Field(
        description="Full s3:// URI of the parquet file. Must be unique "
        "in icebox.files (the deterministic-path scheme guarantees this "
        "for the same set of Kafka offsets).",
    )
    writer_ordinal: int = Field(
        ge=0,
        description="The millpond writer's ordinal (0..N-1). Operator "
        "triage; not used for uniqueness.",
    )
    kafka_offsets: dict[str, int] = Field(
        description="Per-Kafka-partition max offset included in this "
        "file. Keys are stringified partition ids (Pydantic-friendly). "
        "Used by the icebox committer to commit these offsets to Kafka "
        "in the same transaction as the Iceberg commit.",
    )
    partition_values: dict[str, int] = Field(
        description="Iceberg partition values for the file (e.g. "
        '{"year": 2026, "month": 6, "day": 1, "hour": 14}). The '
        "committer uses these when calling add_files.",
    )
    record_count: int = Field(
        ge=0,
        description="Number of records in the parquet file. Operator "
        "visibility; not validated against the actual file.",
    )
    file_size: int = Field(
        ge=0,
        description="Byte size of the parquet file. Same.",
    )
    schema_version: str = Field(
        default="v1",
        description="Producer-side schema marker. v1 of the icebox API "
        "doesn't validate this; future versions may reject mismatches.",
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
    """Body of GET /v1/status — observability endpoint."""

    pending_files: int
    last_success_at: datetime | None
    last_cycle_at: datetime | None
    consecutive_failures: int
    last_committed_iceberg_snapshot: int | None
