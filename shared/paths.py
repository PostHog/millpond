"""Deterministic file-path helper for icebox-staged parquet files.

Writer crash + replay must produce the SAME S3 path for the SAME Kafka
offsets, so idempotent S3 PUT + UNIQUE(file_path) in icebox PG produces
the right semantic exactly-once behavior:

  - If writer crashes after S3 write but before icebox POST:
    replay re-writes same path (S3 no-op), POST succeeds.
  - If writer crashes after icebox POST but before Kafka commit:
    replay re-writes same path (S3 no-op), POST returns 409 (UNIQUE),
    writer re-commits Kafka offset.

The path is constructed so the kafka_offsets dict uniquely determines
it. We hash the offsets (sorted JSON) to keep the filename bounded
regardless of partition count, while leaving the writer ordinal +
partition values + arbitrary kafka hint visible for operator triage.
"""
from __future__ import annotations

import hashlib
import json


def staged_file_path(
    *,
    bucket: str,
    warehouse_prefix: str,
    namespace: str,
    table: str,
    writer_ordinal: int,
    kafka_offsets: dict[int, int],
    partition_values: dict[str, int],
) -> str:
    """Construct the deterministic S3 path for a staged parquet file.

    Args:
        bucket: e.g. "posthog-megaberg-mw-prod-us"
        warehouse_prefix: e.g. "warehouses/ingest"
        namespace: e.g. "kafka"
        table: e.g. "events"
        writer_ordinal: the millpond ordinal (0..N-1)
        kafka_offsets: {kafka_partition_id: max_offset_in_this_batch}
        partition_values: Iceberg partition values for the batch
            (e.g. {"year": 2026, "month": 6, "day": 1, "hour": 14})

    Returns:
        Full s3:// URI. Identical inputs always yield identical output.

    Example:
        s3://posthog-megaberg-mw-prod-us/warehouses/ingest/kafka/events/data/
          year=2026/month=06/day=01/hour=14/
          writer-20-3f8c9a2b1e7d4f0a.parquet
    """
    partition_path = "/".join(
        f"{k}={v:02d}" if k in ("month", "day", "hour") else f"{k}={v}"
        for k, v in sorted(partition_values.items())
    )
    fingerprint = _offsets_fingerprint(kafka_offsets)
    filename = f"writer-{writer_ordinal}-{fingerprint}.parquet"
    return (
        f"s3://{bucket}/{warehouse_prefix}/{namespace}/{table}/data/"
        f"{partition_path}/{filename}"
    )


def _offsets_fingerprint(kafka_offsets: dict[int, int]) -> str:
    """Stable 16-char hex fingerprint of the offsets dict. Same input
    map yields the same output regardless of key insertion order."""
    canonical = json.dumps(
        {str(k): v for k, v in sorted(kafka_offsets.items())},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
