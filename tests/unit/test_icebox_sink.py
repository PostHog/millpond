"""Tests for millpond.icebox_sink — the writer-side icebox client.

Covers:
  - IceboxClient retry behavior (201/409 success, 400 raises, 429/503
    backoff, transport errors).
  - parquet_stats_from_metadata correctness against a real PyArrow-
    written parquet buffer.
  - IceboxSink end-to-end: produces correct S3 path, correct stats wire
    format, correct RegisterFileRequest body. Uses injectable
    s3_writer + a mock IceboxClient to stay in unit-test scope.
"""
from __future__ import annotations

import base64
import io
import time
from datetime import UTC, datetime
from unittest.mock import MagicMock

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from millpond.icebox_sink import (
    IceboxBackpressureExhausted,
    IceboxClient,
    IceboxResponseError,
    IceboxSink,
    _wire_encode,
    parquet_stats_from_metadata,
)
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import assign_fresh_schema_ids
from shared.models import PROTOCOL_VERSION


# ---------------------------------------------------------------------------
# IceboxClient — retry semantics
# ---------------------------------------------------------------------------


def _client_with_mock_httpx(*responses) -> IceboxClient:
    """Build an IceboxClient where the underlying httpx.Client.post is
    a MagicMock returning the given sequence of responses."""
    mock_http = MagicMock(spec=httpx.Client)
    mock_http.post.side_effect = list(responses)
    client = IceboxClient(base_url="http://icebox:8000", _client=mock_http)
    return client


def _make_response(status: int, json_body: dict | None = None, headers: dict | None = None) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status
    resp.json.return_value = json_body or {}
    resp.text = "" if not json_body else str(json_body)
    resp.headers = headers or {}
    return resp


def _valid_register_req():
    from shared.models import ParquetStats, RegisterFileRequest
    return RegisterFileRequest(
        file_path="s3://b/foo.parquet",
        writer_ordinal=0,
        kafka_offsets={"0": 100},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        record_count=10,
        file_size=100,
        schema_fingerprint="a" * 64,
        parquet_stats=ParquetStats(
            column_sizes={"1": 100}, value_counts={"1": 10},
            null_value_counts={"1": 0}, lower_bounds={"1": 1},
            upper_bounds={"1": 100},
        ),
    )


def test_register_file_201_returns_body_and_201(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)
    client = _client_with_mock_httpx(
        _make_response(201, {"row_id": 42, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert status == 201
    assert body["row_id"] == 42


def test_register_file_409_treated_as_success(monkeypatch):
    """409 means 'already registered' — same body shape as 201.
    Writer treats as success and moves on."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    client = _client_with_mock_httpx(
        _make_response(409, {"row_id": 7, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert status == 409
    assert body["row_id"] == 7


def test_register_file_400_raises_response_error(monkeypatch):
    """400 = protocol mismatch or invalid body — not retryable."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    client = _client_with_mock_httpx(
        _make_response(400, {"error": "protocol_version_mismatch"}),
    )
    with pytest.raises(IceboxResponseError):
        client.register_file(_valid_register_req())


def test_register_file_retries_on_429(monkeypatch):
    """429 → sleep Retry-After → retry."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    client = _client_with_mock_httpx(
        _make_response(429, {"retry_after_s": 2}, headers={"Retry-After": "2"}),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert status == 201
    assert sleeps == [2.0]


def test_register_file_retries_on_503(monkeypatch):
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    client = _client_with_mock_httpx(
        _make_response(503, {"retry_after_s": 5}),
        _make_response(503, {"retry_after_s": 5}),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert status == 201
    assert len(sleeps) == 2


def test_register_file_exhausts_after_max_attempts(monkeypatch):
    """Persistent 503 → IceboxBackpressureExhausted after max_attempts."""
    monkeypatch.setattr(time, "sleep", lambda s: None)
    responses = [_make_response(503, {"retry_after_s": 1}) for _ in range(6)]
    client = _client_with_mock_httpx(*responses)
    client.max_attempts = 6
    with pytest.raises(IceboxBackpressureExhausted):
        client.register_file(_valid_register_req())


def test_register_file_retries_on_5xx_other_than_503(monkeypatch):
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    client = _client_with_mock_httpx(
        _make_response(502),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert status == 201


def test_register_file_uses_header_retry_after_over_body(monkeypatch):
    """Header takes precedence — RFC-conformant behavior."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    client = _client_with_mock_httpx(
        _make_response(429, {"retry_after_s": 5}, headers={"Retry-After": "1"}),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    body, status = client.register_file(_valid_register_req())
    assert sleeps == [1.0]


def test_register_file_caps_retry_after_at_max_backoff(monkeypatch):
    """If icebox tells us to retry in an hour, we cap at max_backoff_s
    so the writer doesn't stall indefinitely."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    client = _client_with_mock_httpx(
        _make_response(429, headers={"Retry-After": "3600"}),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    )
    client.max_backoff_s = 30.0
    body, _ = client.register_file(_valid_register_req())
    assert sleeps == [30.0]


def test_register_file_retries_on_http_error(monkeypatch):
    """Transport errors (broken connection, timeout) trigger backoff."""
    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    mock_http = MagicMock(spec=httpx.Client)
    mock_http.post.side_effect = [
        httpx.ConnectError("conn refused"),
        _make_response(201, {"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}),
    ]
    client = IceboxClient(base_url="http://icebox:8000", _client=mock_http)
    body, status = client.register_file(_valid_register_req())
    assert status == 201


# ---------------------------------------------------------------------------
# parquet_stats_from_metadata
# ---------------------------------------------------------------------------


def _write_parquet(table: pa.Table) -> pq.FileMetaData:
    buf = pa.BufferOutputStream()
    with pq.ParquetWriter(buf, table.schema) as w:
        w.write_table(table)
    return pq.ParquetFile(pa.BufferReader(buf.getvalue())).metadata


def _ice_schema_and_field_map(arrow_schema: pa.Schema):
    """Mimic the field-id resolution the sink does on first batch."""
    ice = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow_schema))
    field_map = {f.name: f.field_id for f in ice.fields}
    return ice, field_map


def test_parquet_stats_int_min_max_aggregated():
    """Single row-group int column → min/max == row-group min/max,
    keyed by Iceberg field id."""
    t = pa.table({"x": pa.array([5, 1, 3, 2, 4], type=pa.int64())})
    meta = _write_parquet(t)
    ice, fm = _ice_schema_and_field_map(t.schema)
    stats = parquet_stats_from_metadata(meta, iceberg_schema=ice, arrow_to_iceberg_field_id=fm)
    assert stats.value_counts == {str(fm["x"]): 5}
    assert stats.null_value_counts == {str(fm["x"]): 0}
    assert stats.lower_bounds == {str(fm["x"]): 1}
    assert stats.upper_bounds == {str(fm["x"]): 5}


def test_parquet_stats_string_min_max():
    t = pa.table({"s": pa.array(["alpha", "delta", "bravo", "charlie"])})
    meta = _write_parquet(t)
    ice, fm = _ice_schema_and_field_map(t.schema)
    stats = parquet_stats_from_metadata(meta, iceberg_schema=ice, arrow_to_iceberg_field_id=fm)
    assert stats.lower_bounds[str(fm["s"])] == "alpha"
    assert stats.upper_bounds[str(fm["s"])] == "delta"


def test_parquet_stats_null_count_accumulated():
    t = pa.table({"x": pa.array([1, None, 3, None, 5], type=pa.int64())})
    meta = _write_parquet(t)
    ice, fm = _ice_schema_and_field_map(t.schema)
    stats = parquet_stats_from_metadata(meta, iceberg_schema=ice, arrow_to_iceberg_field_id=fm)
    assert stats.null_value_counts[str(fm["x"])] == 2


def test_parquet_stats_empty_metadata():
    """Empty parquet (zero row groups) → empty maps. Edge case — main.py
    gates on non-empty batches so this shouldn't actually happen, but
    don't crash."""
    schema = pa.schema([("x", pa.int64())])
    meta = pq.ParquetFile(
        pa.BufferReader(pa.BufferOutputStream().getvalue())
    ).metadata if False else None
    # Build a real empty-row-group metadata
    buf = pa.BufferOutputStream()
    with pq.ParquetWriter(buf, schema) as _w:
        pass  # write nothing
    meta = pq.ParquetFile(pa.BufferReader(buf.getvalue())).metadata
    ice, fm = _ice_schema_and_field_map(schema)
    stats = parquet_stats_from_metadata(meta, iceberg_schema=ice, arrow_to_iceberg_field_id=fm)
    assert stats.lower_bounds == {}
    assert stats.upper_bounds == {}


def test_parquet_stats_field_id_keys_are_strings():
    """Wire format uses string keys for JSON compat."""
    t = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
    meta = _write_parquet(t)
    ice, fm = _ice_schema_and_field_map(t.schema)
    stats = parquet_stats_from_metadata(meta, iceberg_schema=ice, arrow_to_iceberg_field_id=fm)
    assert all(isinstance(k, str) for k in stats.column_sizes)
    assert all(isinstance(k, str) for k in stats.lower_bounds)


# ---------------------------------------------------------------------------
# _wire_encode — type-aware conversion
# ---------------------------------------------------------------------------


def test_wire_encode_date_to_iso():
    from datetime import date
    from pyiceberg.types import DateType

    assert _wire_encode(DateType(), date(2026, 6, 1)) == "2026-06-01"


def test_wire_encode_timestamp_to_iso():
    from pyiceberg.types import TimestamptzType

    dt = datetime(2026, 6, 1, 12, 34, 56, 789012, tzinfo=UTC)
    assert _wire_encode(TimestamptzType(), dt) == dt.isoformat()


def test_wire_encode_binary_to_base64():
    from pyiceberg.types import BinaryType

    raw = b"\xde\xad\xbe\xef"
    assert _wire_encode(BinaryType(), raw) == base64.b64encode(raw).decode("ascii")


def test_wire_encode_none_passthrough():
    from pyiceberg.types import IntegerType

    assert _wire_encode(IntegerType(), None) is None


def test_wire_encode_int_passthrough():
    from pyiceberg.types import IntegerType

    assert _wire_encode(IntegerType(), 42) == 42


# ---------------------------------------------------------------------------
# IceboxSink — end-to-end (mocked S3 + client)
# ---------------------------------------------------------------------------


def _arrow_batch_simple() -> pa.Table:
    return pa.table({
        "event": pa.array(["a", "b", "c"], type=pa.string()),
        "x": pa.array([1, 2, 3], type=pa.int64()),
    })


def test_sink_write_produces_deterministic_path():
    """Same kafka_offsets twice → same S3 URI ⇒ idempotent replay works."""
    s3_calls = []
    sink = IceboxSink(
        client=MagicMock(register_file=MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201))),
        writer_ordinal=0,
        bucket="bucket",
        warehouse_prefix="warehouses/ingest",
        namespace="kafka",
        table="events",
    )
    batch = _arrow_batch_simple()
    sink.write(
        batch,
        kafka_offsets={"0": 100, "1": 200},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    # Re-run with the same args
    sink.write(
        batch,
        kafka_offsets={"0": 100, "1": 200},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    assert s3_calls[0] == s3_calls[1]


def test_sink_write_posts_correct_request_body():
    """The icebox sees: full RegisterFileRequest with stats, fingerprint,
    partition values, kafka_offsets."""
    register_mock = MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201))
    sink = IceboxSink(
        client=MagicMock(register_file=register_mock),
        writer_ordinal=20,
        bucket="bucket",
        warehouse_prefix="warehouses/ingest",
        namespace="kafka",
        table="events",
    )
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1000},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        s3_writer=lambda uri, data: None,
    )
    req = register_mock.call_args[0][0]
    assert req.writer_ordinal == 20
    assert req.kafka_offsets == {"0": 1000}
    assert req.partition_values == {"year": 2026, "month": 6, "day": 1, "hour": 14}
    assert req.record_count == 3
    assert req.file_size > 0
    # Fingerprint is set
    assert len(req.schema_fingerprint) == 64
    # Stats populated
    assert req.parquet_stats.value_counts  # non-empty
    assert req.parquet_stats.lower_bounds


def test_sink_write_uploads_to_s3_at_request_path():
    """The S3 PUT must hit the same path that's in the POST body —
    if these diverge, the icebox registers a path with no file."""
    s3_calls = []
    register_mock = MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201))
    sink = IceboxSink(
        client=MagicMock(register_file=register_mock),
        writer_ordinal=0,
        bucket="bucket",
        warehouse_prefix="warehouses/ingest",
        namespace="kafka",
        table="events",
    )
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1000},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        s3_writer=lambda uri, data: s3_calls.append((uri, data)),
    )
    s3_uri = s3_calls[0][0]
    req = register_mock.call_args[0][0]
    assert req.file_path == s3_uri


def test_sink_write_rejects_empty_batch():
    """main.py gates on non-empty; defensive guard catches regressions."""
    sink = IceboxSink(
        client=MagicMock(),
        writer_ordinal=0, bucket="b", warehouse_prefix="w", namespace="n", table="t",
    )
    empty = pa.table({"x": pa.array([], type=pa.int64())})
    with pytest.raises(ValueError, match="zero-row"):
        sink.write(
            empty, kafka_offsets={"0": 0},
            partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
            s3_writer=lambda uri, data: None,
        )


def test_sink_reset_caches_clears_schema():
    """After reset, next write re-derives the schema (handles upstream
    schema changes)."""
    sink = IceboxSink(
        client=MagicMock(register_file=MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201))),
        writer_ordinal=0, bucket="b", warehouse_prefix="w", namespace="n", table="t",
    )
    sink.write(
        _arrow_batch_simple(), kafka_offsets={"0": 0},
        partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
        s3_writer=lambda uri, data: None,
    )
    assert sink._iceberg_schema is not None
    sink.reset_caches()
    assert sink._iceberg_schema is None
    assert sink._fingerprint is None
    assert sink._field_id_by_name is None


def test_sink_close_propagates_to_client():
    client = MagicMock()
    sink = IceboxSink(
        client=client, writer_ordinal=0, bucket="b", warehouse_prefix="w",
        namespace="n", table="t",
    )
    sink.close()
    client.close.assert_called_once()


def test_sink_schema_fingerprint_stable_across_batches():
    """Two writes of the same schema produce the same fingerprint in
    the POST body."""
    register_mock = MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201))
    sink = IceboxSink(
        client=MagicMock(register_file=register_mock),
        writer_ordinal=0, bucket="b", warehouse_prefix="w", namespace="n", table="t",
    )
    batch = _arrow_batch_simple()
    sink.write(batch, kafka_offsets={"0": 1}, partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
               s3_writer=lambda uri, data: None)
    sink.write(batch, kafka_offsets={"0": 2}, partition_values={"year": 2026, "month": 6, "day": 1, "hour": 14},
               s3_writer=lambda uri, data: None)
    fps = [c[0][0].schema_fingerprint for c in register_mock.call_args_list]
    assert fps[0] == fps[1]
