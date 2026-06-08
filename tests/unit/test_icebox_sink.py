"""Tests for millpond.icebox_sink — the writer-side icebox client.

Covers:
  - IceboxClient: INSERT path (201/409 success, jsonb encoding,
    impossible-state guard).
  - parquet_stats_from_metadata correctness against a real PyArrow-
    written parquet buffer.
  - IceboxSink end-to-end: produces correct S3 path, correct stats wire
    format, correct RegisterFileRequest body. Uses injectable
    s3_writer + a mock IceboxClient (PG pool) to stay in unit-test scope.
"""
from __future__ import annotations

import base64
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.schema import assign_fresh_schema_ids

from millpond.icebox_sink import (
    IceboxClient,
    IceboxSink,
    _wire_encode,
    parquet_stats_from_metadata,
)


def _valid_register_req():
    """Build a RegisterFileRequest with all required fields populated
    for IceboxClient tests."""
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


# ---------------------------------------------------------------------------
# IceboxClient — psycopg INSERT
# ---------------------------------------------------------------------------


def test_pg_client_pool_is_open_after_construction():
    """Regression: __post_init__ used to construct the pool with
    `open=False` "to fail loud on first use", but nothing else here
    ever called `.open()`, so `register_file` immediately raised
    `psycopg_pool.PoolClosed: the pool 'pool-1' is not open yet`
    against real PG.

    With min_size=0 there's no eager-connection hazard at boot, so
    letting psycopg_pool's default (open=True) hold is correct.

    `pool.closed` is True both before .open() and after .close();
    asserting it's False right after construction pins the open state.
    """
    client = IceboxClient(
        host="bogus.invalid",  # never contacted at min_size=0
        port=5432,
        database="icebox",
        username="megaberg",
        password="secret",
        schema="icebox_events",
        sslmode="require",
    )
    try:
        assert client._pool is not None
        assert client._pool.closed is False, (
            "IceboxClient._pool must be open after construction — see "
            "the PoolClosed regression in icebox_sink.py __post_init__"
        )
    finally:
        client.close()


def _pg_client_with_mock_pool():
    """Build an IceboxClient with its connection pool replaced by a
    MagicMock that surfaces the cursor's fetchone()."""
    client = IceboxClient(
        host="megaberg.example.com",
        port=5432,
        database="icebox",
        username="megaberg",
        password="secret",
        schema="icebox_events",
        sslmode="require",
    )
    pool = MagicMock()
    cursor = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__ = lambda self: cursor
    conn.cursor.return_value.__exit__ = lambda self, *a: None
    pool.connection.return_value.__enter__ = lambda self: conn
    pool.connection.return_value.__exit__ = lambda self, *a: None
    client._pool = pool
    return client, cursor


def test_pg_register_file_201_on_new_insert():
    """INSERT inserted a row → fetchone returns (id, inserted_at) →
    return ({row_id, queued_at}, 201). Status 201 matches HTTP
    'created' so IceboxSink callers don't need to special-case PG."""
    client, cur = _pg_client_with_mock_pool()
    inserted = datetime(2026, 6, 5, 12, 0, 0, tzinfo=UTC)
    cur.fetchone.return_value = (42, inserted)

    body, status = client.register_file(_valid_register_req())

    assert status == 201
    assert body["row_id"] == 42
    assert body["queued_at"] == inserted.isoformat()
    # Exactly one execute on the happy path — the INSERT.
    assert cur.execute.call_count == 1


def test_pg_register_file_409_on_conflict():
    """ON CONFLICT DO NOTHING returned no row → fall back to a
    SELECT for the existing row's id/inserted_at → return 409."""
    client, cur = _pg_client_with_mock_pool()
    existing = datetime(2026, 6, 5, 11, 0, 0, tzinfo=UTC)
    # First fetchone (INSERT...RETURNING) returns None; second
    # (lookup SELECT) returns the existing row.
    cur.fetchone.side_effect = [None, (99, existing)]

    body, status = client.register_file(_valid_register_req())

    assert status == 409
    assert body["row_id"] == 99
    assert body["queued_at"] == existing.isoformat()
    # Two executes on the replay path.
    assert cur.execute.call_count == 2


def test_pg_register_file_serializes_jsonb_params():
    """psycopg's parameter binding doesn't auto-encode dicts as jsonb
    in our version; the client must json.dumps() the dict-shaped
    columns. A regression that passes a raw dict would surface as a
    psycopg.errors.ProgrammingError at the boundary."""
    client, cur = _pg_client_with_mock_pool()
    cur.fetchone.return_value = (1, datetime.now(UTC))

    client.register_file(_valid_register_req())

    # Single execute call carries the bind dict.
    params = cur.execute.call_args.args[1]
    assert isinstance(params["kafka_offsets"], str)
    assert isinstance(params["partition_values"], str)
    assert isinstance(params["parquet_stats"], str)
    # round-trip through json to confirm it's valid JSON.
    import json as _json
    assert _json.loads(params["kafka_offsets"]) == {"0": 100}
    assert _json.loads(params["partition_values"]) == {
        "year": 2026, "month": 6, "day": 1, "hour": 14,
    }


def test_pg_register_file_carries_file_path_to_lookup_on_conflict():
    """The lookup SQL params on the 409 path must include the same
    file_path the INSERT tried — otherwise we'd be doing a useless
    full-table read."""
    client, cur = _pg_client_with_mock_pool()
    cur.fetchone.side_effect = [None, (1, datetime.now(UTC))]

    client.register_file(_valid_register_req())

    lookup_call = cur.execute.call_args_list[1]
    assert lookup_call.args[1] == {"file_path": "s3://b/foo.parquet"}


def test_pg_register_file_raises_on_impossible_state():
    """If INSERT returns no row AND the lookup also finds nothing, the
    icebox state is impossible without a concurrent DELETE (which the
    icebox daemon never issues). Raise loudly so the operator sees the
    invariant violation rather than silently passing wrong status."""
    client, cur = _pg_client_with_mock_pool()
    cur.fetchone.side_effect = [None, None]

    with pytest.raises(RuntimeError, match="this should be impossible"):
        client.register_file(_valid_register_req())


def test_pg_register_file_quotes_search_path_in_conninfo():
    """The schema is interpolated into the conninfo options string. A
    regression that drops or escapes the search_path setter would
    cause INSERTs to land in the public schema. Sanity-check the
    raw conninfo string the client builds."""
    client = IceboxClient(
        host="megaberg.example.com",
        port=5432,
        database="icebox",
        username="megaberg",
        password="secret",
        schema="icebox_events",
        sslmode="require",
    )
    # Reach into the pool's conninfo. ConnectionPool stores it as
    # `kwargs["conninfo"]` when constructed positionally; check
    # the public attribute.
    assert "search_path=icebox_events" in client._pool.conninfo


def test_pg_register_file_sql_uses_on_conflict_do_nothing():
    """Without ON CONFLICT, the writer's idempotent-replay POST would
    raise UniqueViolation instead of mapping to 409."""
    from millpond.icebox_sink import _ICEBOX_INSERT_SQL
    sql = _ICEBOX_INSERT_SQL.lower()
    assert "on conflict (file_path) do nothing" in sql
    assert "returning id, inserted_at" in sql


def test_pg_register_file_sql_targets_icebox_files():
    """The v6 daemon reads only icebox_files; INSERTing into the
    cycle-era `files` table during the rollout would silently land in
    the wrong place. Pin the target table name."""
    from millpond.icebox_sink import _ICEBOX_INSERT_SQL
    assert "insert into icebox_files" in _ICEBOX_INSERT_SQL.lower()


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
    from millpond.iceberg import RESERVED_COLUMNS as ICEBERG_RESERVED
    from millpond.icebox_sink import _RESERVED_COLUMNS
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
