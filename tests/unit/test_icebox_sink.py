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


def _make_sink(**overrides):
    register_mock = overrides.pop(
        "register_mock",
        MagicMock(return_value=({"row_id": 1, "queued_at": "2026-06-01T00:00:00Z"}, 201)),
    )
    defaults = {
        "client": MagicMock(register_file=register_mock),
        "writer_ordinal": 0,
        "bucket": "bucket",
        "warehouse_prefix": "warehouses/ingest",
        "namespace": "kafka",
        "table": "events",
    }
    defaults.update(overrides)
    return IceboxSink(**defaults), register_mock


# Standard inserted_at for tests — a stable, tz-aware UTC datetime.
# Real production callers (millpond/main.py:_flush) derive this from
# the MAX Kafka message timestamp in the batch.
_FIXED_INSERTED_AT = datetime(2026, 6, 1, 14, 30, 45, 0, tzinfo=UTC)


def test_sink_write_produces_deterministic_path_across_hour_boundary():
    """The load-bearing test: same kafka_offsets PLUS same inserted_at
    yields the same S3 path, regardless of wall-clock. The previous
    version of this test passed by wall-clock coincidence (both writes
    in the same wall-second). The fix: pin inserted_at explicitly so
    the test exercises the actual determinism contract."""
    s3_calls = []
    sink, _ = _make_sink()
    batch = _arrow_batch_simple()
    sink.write(
        batch,
        kafka_offsets={"0": 100, "1": 200},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    sink.write(
        batch,
        kafka_offsets={"0": 100, "1": 200},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    assert s3_calls[0] == s3_calls[1]


def test_sink_write_path_differs_when_inserted_at_crosses_hour():
    """Confirms the partition is BAKED INTO the path. Different
    inserted_at → different path (this is the desired property — same
    offsets at different times genuinely mean different partitions)."""
    s3_calls = []
    sink, _ = _make_sink()
    batch = _arrow_batch_simple()
    sink.write(
        batch,
        kafka_offsets={"0": 100},
        inserted_at=datetime(2026, 6, 1, 13, 59, 30, tzinfo=UTC),
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    sink.write(
        batch,
        kafka_offsets={"0": 100},
        inserted_at=datetime(2026, 6, 1, 14, 0, 15, tzinfo=UTC),
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    assert s3_calls[0] != s3_calls[1], (
        "different inserted_at must produce different paths "
        "(year/month/day/hour partition embedded in S3 URI)"
    )


def test_sink_write_replay_after_hour_boundary_yields_same_path():
    """The critical regression test for the PE/QE blocker: writer
    crashes at 13:59, replays at 14:00 with the same Kafka offsets;
    because main.py:_flush derives inserted_at from the MAX MESSAGE
    timestamp in the batch (NOT wall-clock), replay produces the same
    inserted_at and therefore the same S3 path → S3 dedup → icebox 409
    on POST → no duplicate registration."""
    s3_calls = []
    sink, _ = _make_sink()
    batch = _arrow_batch_simple()
    # Simulate what main.py:_flush computes: a per-batch deterministic
    # inserted_at derived from the data's max Kafka timestamp.
    batch_max_ts = datetime(2026, 6, 1, 13, 58, 0, tzinfo=UTC)

    # First attempt: wall-clock 13:59:30 (irrelevant — sink only uses inserted_at)
    sink.write(
        batch, kafka_offsets={"0": 100, "1": 200},
        inserted_at=batch_max_ts,
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    # Replay: wall-clock 14:00:15 (after hour boundary), but caller
    # recomputes the same batch_max_ts because the messages are the same
    sink.write(
        batch, kafka_offsets={"0": 100, "1": 200},
        inserted_at=batch_max_ts,
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    assert s3_calls[0] == s3_calls[1], (
        "Replay-with-same-messages MUST produce same S3 path "
        "(this is the deterministic-replay contract). If this test fails, "
        "the icebox sink is no longer idempotent under writer-crash-replay."
    )


def test_sink_write_path_includes_partition_columns():
    """The S3 path embeds year/month/day/hour derived from inserted_at."""
    s3_calls = []
    sink, _ = _make_sink()
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 100},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: s3_calls.append(uri),
    )
    uri = s3_calls[0]
    assert "year=2026" in uri
    assert "month=06" in uri
    assert "day=01" in uri
    assert "hour=14" in uri


def test_sink_write_rejects_naive_inserted_at():
    """A naive (tz-less) datetime would silently produce ambiguous
    partition values; refuse at the call site."""
    sink, _ = _make_sink()
    with pytest.raises(ValueError, match="tz-aware"):
        sink.write(
            _arrow_batch_simple(),
            kafka_offsets={"0": 1},
            inserted_at=datetime(2026, 6, 1, 14, 0),  # naive
            s3_writer=lambda uri, data: None,
        )


def test_sink_write_requires_inserted_at():
    """No inserted_at = refuse. The icebox's idempotency depends on
    caller-provided determinism here."""
    sink, _ = _make_sink()
    with pytest.raises(ValueError, match="inserted_at"):
        sink.write(
            _arrow_batch_simple(),
            kafka_offsets={"0": 1},
            s3_writer=lambda uri, data: None,
        )


def test_sink_write_posts_correct_request_body():
    """The icebox sees: full RegisterFileRequest with stats, fingerprint,
    partition values (sink-derived from inserted_at), kafka_offsets."""
    sink, register_mock = _make_sink(writer_ordinal=20)
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1000},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: None,
    )
    req = register_mock.call_args[0][0]
    assert req.writer_ordinal == 20
    assert req.kafka_offsets == {"0": 1000}
    assert req.partition_values == {"year": 2026, "month": 6, "day": 1, "hour": 14}
    assert req.record_count == 3
    assert req.file_size > 0
    assert len(req.schema_fingerprint) == 64
    assert req.parquet_stats.value_counts
    assert req.parquet_stats.lower_bounds


def test_sink_write_uploads_to_s3_at_request_path():
    """The S3 PUT must hit the same path that's in the POST body."""
    s3_calls = []
    sink, register_mock = _make_sink()
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1000},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: s3_calls.append((uri, data)),
    )
    s3_uri = s3_calls[0][0]
    req = register_mock.call_args[0][0]
    assert req.file_path == s3_uri


def test_sink_write_rejects_empty_batch():
    sink, _ = _make_sink(bucket="b", warehouse_prefix="w", namespace="n", table="t")
    empty = pa.table({"x": pa.array([], type=pa.int64())})
    with pytest.raises(ValueError, match="zero-row"):
        sink.write(
            empty, kafka_offsets={"0": 0},
            inserted_at=_FIXED_INSERTED_AT,
            s3_writer=lambda uri, data: None,
        )


def test_sink_write_requires_kafka_offsets():
    sink, _ = _make_sink()
    with pytest.raises(ValueError, match="kafka_offsets"):
        sink.write(
            _arrow_batch_simple(),
            inserted_at=_FIXED_INSERTED_AT,
            s3_writer=lambda uri, data: None,
        )


def test_sink_write_requires_s3_writer():
    sink, _ = _make_sink()
    with pytest.raises(ValueError, match="s3_writer"):
        sink.write(
            _arrow_batch_simple(),
            kafka_offsets={"0": 1},
            inserted_at=_FIXED_INSERTED_AT,
        )


def test_sink_write_uses_instance_s3_writer_when_no_per_call():
    s3_calls = []
    sink, _ = _make_sink(s3_writer=lambda uri, data: s3_calls.append(uri))
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1},
        inserted_at=_FIXED_INSERTED_AT,
    )
    assert len(s3_calls) == 1


@pytest.mark.parametrize("reserved_col", ["_inserted_at", "year", "month", "day", "hour"])
def test_sink_write_rejects_batch_with_reserved_columns(reserved_col):
    """The sink adds ALL metadata columns. A caller passing ANY of them
    pre-stamped would double-add."""
    sink, _ = _make_sink()
    pre_stamped = _arrow_batch_simple().append_column(
        reserved_col, pa.array([1, 2, 3], pa.int64())
    )
    with pytest.raises(ValueError, match=reserved_col):
        sink.write(
            pre_stamped,
            kafka_offsets={"0": 1},
            inserted_at=_FIXED_INSERTED_AT,
            s3_writer=lambda uri, data: None,
        )


def test_sink_write_adds_partition_columns():
    s3_payloads: list[bytes] = []
    sink, _ = _make_sink()
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 1},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: s3_payloads.append(data),
    )
    pf = pq.ParquetFile(pa.BufferReader(s3_payloads[0]))
    schema_names = pf.schema_arrow.names
    for col in ("_inserted_at", "year", "month", "day", "hour"):
        assert col in schema_names, f"S3-shipped parquet missing {col!r}"


def test_sink_reset_caches_clears_schema():
    sink, _ = _make_sink()
    sink.write(
        _arrow_batch_simple(),
        kafka_offsets={"0": 0},
        inserted_at=_FIXED_INSERTED_AT,
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
    sink, register_mock = _make_sink()
    batch = _arrow_batch_simple()
    sink.write(
        batch, kafka_offsets={"0": 1},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: None,
    )
    sink.write(
        batch, kafka_offsets={"0": 2},
        inserted_at=_FIXED_INSERTED_AT,
        s3_writer=lambda uri, data: None,
    )
    fps = [c[0][0].schema_fingerprint for c in register_mock.call_args_list]
    assert fps[0] == fps[1]


# ---------------------------------------------------------------------------
# Drift-detection tests vs millpond/iceberg.py (PE + QE flagged)
# ---------------------------------------------------------------------------


def test_reserved_columns_match_iceberg_sink():
    """The icebox sink keeps an inline copy of millpond/iceberg.py's
    RESERVED_COLUMNS to avoid pulling pyiceberg into a DuckLake-only
    deployment. This test pins equivalence so a future addition to
    one set without the other surfaces immediately."""
    from millpond.icebox_sink import _RESERVED_COLUMNS
    from millpond.iceberg import RESERVED_COLUMNS as ICEBERG_RESERVED
    assert _RESERVED_COLUMNS == ICEBERG_RESERVED


def test_add_metadata_columns_schema_matches_iceberg_sink():
    """The icebox sink's _add_metadata_columns must produce the same
    schema shape as millpond/iceberg.py's. Drift here would silently
    break the schema fingerprint contract between writer and icebox-
    side table — every POST would 400 with schema_mismatch.

    Pins SHAPE (column names, types) not VALUES (timestamps differ
    naturally between calls, by design)."""
    import millpond.iceberg as iceberg_mod
    import millpond.icebox_sink as icebox_sink_mod

    batch = _arrow_batch_simple()
    fixed_ts = _FIXED_INSERTED_AT

    # icebox-sink helper takes inserted_at; iceberg.py's uses now() —
    # so VALUES differ but SHAPE must match.
    icebox_out = icebox_sink_mod._add_metadata_columns(batch, fixed_ts)
    iceberg_out = iceberg_mod._add_metadata_columns(batch)

    # Same column ORDER and TYPES — the schema fingerprint depends on this.
    assert icebox_out.schema.names == iceberg_out.schema.names, (
        f"column order drift: icebox={icebox_out.schema.names} "
        f"iceberg={iceberg_out.schema.names}"
    )
    for name in icebox_out.schema.names:
        ix_type = icebox_out.schema.field(name).type
        ib_type = iceberg_out.schema.field(name).type
        assert ix_type == ib_type, (
            f"type drift on column {name!r}: icebox={ix_type} iceberg={ib_type}"
        )


def test_partition_values_from_batch_rejects_heterogeneous_partition():
    """Defensive: if a future refactor split _add_metadata_columns from
    the row-0 read and someone built a batch with mixed (y, m, d, h)
    tuples, we want to fail loudly rather than ship a row-0-only
    partition tuple that mismatches the parquet's actual data."""
    from millpond.icebox_sink import _partition_values_from_batch

    # Build a batch where the 'hour' column has two different values
    bad = pa.table({
        "x": pa.array([1, 2], pa.int64()),
        "_inserted_at": pa.array([_FIXED_INSERTED_AT, _FIXED_INSERTED_AT], pa.timestamp("us", tz="UTC")),
        "year": pa.array([2026, 2026], pa.int32()),
        "month": pa.array([6, 6], pa.int32()),
        "day": pa.array([1, 1], pa.int32()),
        "hour": pa.array([14, 15], pa.int32()),  # split across hours
    })
    with pytest.raises(ValueError, match="multiple values"):
        _partition_values_from_batch(bad)
