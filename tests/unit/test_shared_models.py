"""Tests for the shared REST-API Pydantic models.

These are the contract between the writer sink and the icebox API.
Drift here means writers and the icebox stop agreeing on what a
'registered file' looks like — exactly the silent failure mode that
PROTOCOL_VERSION + schema_fingerprint exist to catch.
"""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.models import (
    PROTOCOL_VERSION,
    BackpressureResponse,
    ParquetStats,
    RegisteredFile,
    RegisterFileRequest,
    StatusResponse,
)


# ---------------------------------------------------------------------------
# ParquetStats
# ---------------------------------------------------------------------------


def _valid_stats_kwargs() -> dict:
    return {
        "column_sizes": {"1": 1024, "2": 2048},
        "value_counts": {"1": 1000, "2": 1000},
        "null_value_counts": {"1": 0, "2": 5},
        "lower_bounds": {"1": 1, "2": "alpha"},
        "upper_bounds": {"1": 1000, "2": "zulu"},
    }


def test_parquet_stats_accepts_minimal_required_fields():
    stats = ParquetStats(**_valid_stats_kwargs())
    assert stats.column_sizes == {"1": 1024, "2": 2048}
    assert stats.nan_value_counts == {}  # default
    assert stats.split_offsets == []  # default


def test_parquet_stats_typed_bounds_allow_heterogeneous_types():
    """Different columns have different Iceberg types; lower/upper bounds
    must support int, float, str, etc. simultaneously."""
    kwargs = _valid_stats_kwargs()
    kwargs["lower_bounds"] = {
        "1": 42,  # int
        "2": "alpha",  # str
        "3": 3.14,  # float
        "4": "2026-06-01",  # date as string
        "5": "2026-06-01T12:34:56.789012",  # timestamp as string
    }
    kwargs["upper_bounds"] = kwargs["lower_bounds"]
    # all the upper_bounds keys need column_sizes etc; broaden them
    for k in ("3", "4", "5"):
        kwargs["column_sizes"][k] = 1
        kwargs["value_counts"][k] = 1
        kwargs["null_value_counts"][k] = 0
    stats = ParquetStats(**kwargs)
    assert stats.lower_bounds["1"] == 42
    assert stats.lower_bounds["3"] == 3.14
    assert stats.lower_bounds["5"] == "2026-06-01T12:34:56.789012"


def test_parquet_stats_field_id_keys_must_be_strings():
    """JSON requires string keys. Int keys would round-trip lossy, so
    Pydantic strict-rejects them — failing at the boundary is better
    than corrupting jsonb downstream."""
    kwargs = _valid_stats_kwargs()
    kwargs["column_sizes"] = {1: 1024}
    with pytest.raises(ValidationError):
        ParquetStats(**kwargs)


def test_parquet_stats_nan_value_counts_optional():
    """nan_value_counts only meaningful for float/double; defaults to {}."""
    stats = ParquetStats(**_valid_stats_kwargs())
    assert stats.nan_value_counts == {}


def test_parquet_stats_split_offsets_list_of_ints():
    kwargs = _valid_stats_kwargs()
    kwargs["split_offsets"] = [0, 4096, 8192]
    stats = ParquetStats(**kwargs)
    assert stats.split_offsets == [0, 4096, 8192]


# ---------------------------------------------------------------------------
# RegisterFileRequest
# ---------------------------------------------------------------------------


def _valid_register_kwargs() -> dict:
    return {
        "file_path": "s3://bucket/warehouses/ingest/kafka/events/data/year=2026/month=06/day=01/hour=14/writer-0-abc123.parquet",
        "writer_ordinal": 0,
        "kafka_offsets": {"20": 12345},
        "partition_values": {"year": 2026, "month": 6, "day": 1, "hour": 14},
        "record_count": 1000,
        "file_size": 4096,
        "schema_fingerprint": "deadbeef" * 8,
        "parquet_stats": _valid_stats_kwargs(),
    }


def test_register_file_request_defaults_protocol_version():
    req = RegisterFileRequest(**_valid_register_kwargs())
    assert req.protocol_version == PROTOCOL_VERSION
    assert req.protocol_version == 1
    assert req.schema_version == "v1"


def test_register_file_request_rejects_missing_schema_fingerprint():
    """schema_fingerprint is mandatory — the committer's only defense
    against silent schema drift."""
    kwargs = _valid_register_kwargs()
    del kwargs["schema_fingerprint"]
    with pytest.raises(ValidationError):
        RegisterFileRequest(**kwargs)


def test_register_file_request_rejects_missing_parquet_stats():
    """Without stats, the committer would have to read parquet footers —
    the very cost icebox exists to avoid."""
    kwargs = _valid_register_kwargs()
    del kwargs["parquet_stats"]
    with pytest.raises(ValidationError):
        RegisterFileRequest(**kwargs)


def test_register_file_request_kafka_offsets_string_keys():
    """Per-partition offsets are jsonb-keyed; JSON requires string keys."""
    req = RegisterFileRequest(**_valid_register_kwargs())
    for k in req.kafka_offsets:
        assert isinstance(k, str)


def test_register_file_request_negative_record_count_rejected():
    kwargs = _valid_register_kwargs()
    kwargs["record_count"] = -1
    with pytest.raises(ValidationError):
        RegisterFileRequest(**kwargs)


def test_register_file_request_negative_writer_ordinal_rejected():
    kwargs = _valid_register_kwargs()
    kwargs["writer_ordinal"] = -1
    with pytest.raises(ValidationError):
        RegisterFileRequest(**kwargs)


def test_register_file_request_protocol_version_override():
    """A writer with a newer schema must be able to send a higher
    protocol_version — the icebox is what enforces compatibility."""
    kwargs = _valid_register_kwargs()
    kwargs["protocol_version"] = 99
    req = RegisterFileRequest(**kwargs)
    assert req.protocol_version == 99


def test_register_file_request_roundtrip_via_dict():
    """Sink builds a dict, sends as JSON, icebox parses back. Round-trip
    must preserve all fields."""
    req1 = RegisterFileRequest(**_valid_register_kwargs())
    req2 = RegisterFileRequest.model_validate(req1.model_dump())
    assert req1 == req2


# ---------------------------------------------------------------------------
# RegisteredFile / BackpressureResponse / StatusResponse
# ---------------------------------------------------------------------------


def test_registered_file_minimal():
    from datetime import UTC, datetime

    rf = RegisteredFile(row_id=42, queued_at=datetime.now(UTC))
    assert rf.row_id == 42


def test_backpressure_response_429_shape():
    """429 includes queue_depth; consecutive_failures absent."""
    resp = BackpressureResponse(
        reason="queue full",
        retry_after_s=5,
        queue_depth=1500,
    )
    assert resp.queue_depth == 1500
    assert resp.consecutive_failures is None


def test_backpressure_response_503_shape():
    """503 includes consecutive_failures; queue_depth absent."""
    resp = BackpressureResponse(
        reason="committer degraded",
        retry_after_s=60,
        consecutive_failures=3,
    )
    assert resp.consecutive_failures == 3
    assert resp.queue_depth is None


def test_status_response_all_optional_timestamps_default_none():
    """Fresh icebox install has no committed snapshots yet."""
    resp = StatusResponse(pending_files=0)
    assert resp.last_success_at is None
    assert resp.last_committer_heartbeat is None
    assert resp.oldest_pending_age_seconds is None
    assert resp.consecutive_failures == 0


def test_status_response_oldest_pending_age_floats():
    """oldest_pending_age_seconds is a fractional wall-clock seconds
    value — alerts will key on this rather than absolute pending count."""
    resp = StatusResponse(pending_files=10, oldest_pending_age_seconds=42.5)
    assert resp.oldest_pending_age_seconds == 42.5
