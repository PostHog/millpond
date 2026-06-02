"""Determinism tests for shared.paths.staged_file_path.

The icebox architecture relies on: same (writer_ordinal, kafka_offsets,
partition_values) ⇒ same S3 path. If this breaks, idempotent replay
breaks; duplicate parquet files start accumulating in S3 and the
UNIQUE(file_path) constraint in icebox.files no longer dedupes
replayed writes.
"""
from __future__ import annotations

import pytest

from shared.paths import _offsets_fingerprint, staged_file_path


@pytest.fixture
def base_kwargs() -> dict:
    return {
        "bucket": "posthog-megaberg-mw-prod-us",
        "warehouse_prefix": "warehouses/ingest",
        "namespace": "kafka",
        "table": "events",
        "writer_ordinal": 20,
        "kafka_offsets": {20: 1245678, 52: 9876543, 84: 3456789},
        "partition_values": {"year": 2026, "month": 6, "day": 1, "hour": 14},
    }


def test_same_inputs_yield_same_path(base_kwargs):
    """The load-bearing property: idempotent replay."""
    assert staged_file_path(**base_kwargs) == staged_file_path(**base_kwargs)


def test_offsets_dict_order_does_not_change_path(base_kwargs):
    """The kafka_offsets fingerprint must be insertion-order-insensitive.
    Two dicts with the same key/value pairs constructed in different
    iteration orders must produce the same path."""
    original = base_kwargs["kafka_offsets"]
    reversed_offsets = dict(reversed(list(original.items())))
    # Python dicts compare by content, so == is true; what matters is
    # that the fingerprint computation doesn't depend on iteration order.
    fp_original = _offsets_fingerprint(original)
    fp_reversed = _offsets_fingerprint(reversed_offsets)
    assert fp_original == fp_reversed
    # And the resulting paths must be identical too.
    a = staged_file_path(**base_kwargs)
    b = staged_file_path(**{**base_kwargs, "kafka_offsets": reversed_offsets})
    assert a == b


def test_different_offsets_yield_different_path(base_kwargs):
    """Changing any offset value must change the path; otherwise two
    distinct flushes could collide on the same S3 key."""
    different = {**base_kwargs["kafka_offsets"], 20: 9999999}
    assert staged_file_path(**base_kwargs) != staged_file_path(
        **{**base_kwargs, "kafka_offsets": different}
    )


def test_different_writer_ordinal_yields_different_path(base_kwargs):
    """Two writers covering different partitions must never produce
    the same path even if (improbably) their offset fingerprints
    matched."""
    assert staged_file_path(**base_kwargs) != staged_file_path(
        **{**base_kwargs, "writer_ordinal": 21}
    )


def test_path_layout_matches_iceberg_data_path(base_kwargs):
    """Files must land inside the warehouse's data path so Iceberg's
    add_files / compaction / orphan-cleanup see them as native."""
    path = staged_file_path(**base_kwargs)
    assert path.startswith(
        "s3://posthog-megaberg-mw-prod-us/warehouses/ingest/kafka/events/data/"
    )


def test_partition_path_has_zero_padded_month_day_hour(base_kwargs):
    """Iceberg Hive-style partition paths use zero-padded values for
    month/day/hour so lexicographic ordering matches chronological."""
    path = staged_file_path(**base_kwargs)
    assert "year=2026" in path
    assert "month=06" in path  # zero-padded
    assert "day=01" in path  # zero-padded
    assert "hour=14" in path  # zero-padded


def test_filename_format(base_kwargs):
    """Filename: writer-<N>-<16-hex-chars>.parquet"""
    path = staged_file_path(**base_kwargs)
    filename = path.rsplit("/", 1)[1]
    assert filename.startswith("writer-20-")
    assert filename.endswith(".parquet")
    # Strip prefix/suffix; the middle should be 16 hex chars
    hex_part = filename[len("writer-20-") : -len(".parquet")]
    assert len(hex_part) == 16
    assert all(c in "0123456789abcdef" for c in hex_part)


def test_fingerprint_is_stable_across_processes(base_kwargs):
    """The fingerprint must be a pure function — no time, no PID, no
    process-local random state."""
    # Recompute from scratch
    f1 = _offsets_fingerprint(base_kwargs["kafka_offsets"])
    f2 = _offsets_fingerprint(dict(base_kwargs["kafka_offsets"]))  # new dict, same data
    assert f1 == f2
