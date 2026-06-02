"""Tests for icebox.kafka — offset-merge logic + admin client wiring.

The Kafka admin client side-effect path (`commit_offsets`) is mocked
since real broker calls don't belong in unit tests. Integration is
covered by the end-to-end committer test that exercises a real
WarpStream/Kafka in CI.
"""
from __future__ import annotations

from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from icebox.kafka import (
    build_admin_client,
    commit_offsets,
    merge_max_offsets,
)


# ---------------------------------------------------------------------------
# merge_max_offsets — pure function, the most-exercised path
# ---------------------------------------------------------------------------


def test_merge_max_offsets_empty():
    """No files → no offsets — vacuous cycle."""
    assert merge_max_offsets([]) == {}


def test_merge_max_offsets_single_file():
    """One file's offsets pass through, with key conversion str → int."""
    result = merge_max_offsets([{"20": 1000, "21": 2000}])
    assert result == {20: 1000, 21: 2000}


def test_merge_max_offsets_takes_max_across_files():
    """Two files for the same partition → max wins. This is the property
    that makes the merge correct: the icebox cycle covers all of these
    files, so committing max ensures the committed offset is >= every
    record included."""
    files = [
        {"20": 1000, "21": 500},
        {"20": 1500, "21": 200},
        {"20": 800, "21": 700},
    ]
    assert merge_max_offsets(files) == {20: 1500, 21: 700}


def test_merge_max_offsets_disjoint_partitions():
    """Different writers may own disjoint partitions; merge unions them."""
    files = [
        {"0": 100, "1": 200},
        {"2": 300, "3": 400},
    ]
    assert merge_max_offsets(files) == {0: 100, 1: 200, 2: 300, 3: 400}


def test_merge_max_offsets_string_keys_normalized_to_int():
    """The TopicPartition constructor takes int partitions; the wire
    format uses string keys. Conversion must happen at merge time."""
    result = merge_max_offsets([{"0": 1}])
    assert all(isinstance(k, int) for k in result.keys())


# ---------------------------------------------------------------------------
# build_admin_client — just config parsing, no real connection
# ---------------------------------------------------------------------------


def test_build_admin_client_with_empty_extra_config():
    """The default ICEBOX_KAFKA_EXTRA_CONFIG = '{}' → just bootstrap."""
    client = build_admin_client(
        bootstrap_servers="localhost:9092",
        extra_config_json="{}",
    )
    assert client is not None  # AdminClient instantiates lazily


def test_build_admin_client_passes_extra_through_to_librdkafka():
    """Real production config: librdkafka tunables like timeouts get
    forwarded. We use a tunable that doesn't require credentials so the
    unit test stays self-contained."""
    extra = '{"socket.timeout.ms":"5000","client.id":"icebox-test"}'
    client = build_admin_client(
        bootstrap_servers="localhost:9092",
        extra_config_json=extra,
    )
    assert client is not None


def test_build_admin_client_rejects_invalid_json():
    """Misformed env var fails loud — easy to introduce by accident in K8s."""
    with pytest.raises(ValueError, match="JSON object"):
        build_admin_client(
            bootstrap_servers="localhost:9092",
            extra_config_json="{not json",
        )


def test_build_admin_client_rejects_json_array():
    """A JSON array is valid JSON but not a config dict — fail loud."""
    with pytest.raises(ValueError, match="JSON object"):
        build_admin_client(
            bootstrap_servers="localhost:9092",
            extra_config_json='["a","b"]',
        )


def test_build_admin_client_empty_string_treated_as_no_extra():
    """Empty-string env vars in K8s should not crash — treat as no extra config."""
    client = build_admin_client(
        bootstrap_servers="localhost:9092",
        extra_config_json="",
    )
    assert client is not None


# ---------------------------------------------------------------------------
# commit_offsets — side-effect path, mock the admin client
# ---------------------------------------------------------------------------


def test_commit_offsets_short_circuits_on_empty_map():
    """No partitions to commit → no admin RPC. Vacuous cycle still
    finishes cleanly."""
    admin = MagicMock()
    commit_offsets(
        admin,
        group_id="grp",
        topic="events",
        max_offsets={},
    )
    admin.alter_consumer_group_offsets.assert_not_called()


def test_commit_offsets_adds_one_to_each_offset():
    """Kafka committed offset = next offset to consume. Writers send
    max_offset_seen; we MUST add 1 here, otherwise on restart the
    writer re-consumes the last record."""
    admin = MagicMock()
    fut: Future = Future()
    fut.set_result(None)
    admin.alter_consumer_group_offsets.return_value = {"grp": fut}

    commit_offsets(
        admin,
        group_id="grp",
        topic="events",
        max_offsets={0: 100, 1: 200},
    )

    call_args = admin.alter_consumer_group_offsets.call_args
    requests = call_args[0][0]
    assert len(requests) == 1
    req = requests[0]
    assert req.group_id == "grp"
    offsets_by_partition = {tp.partition: tp.offset for tp in req.topic_partitions}
    assert offsets_by_partition == {0: 101, 1: 201}


def test_commit_offsets_uses_provided_topic():
    """Same group can in theory commit on different topics; we constrain
    each icebox instance to one topic, but verify it's passed through."""
    admin = MagicMock()
    fut: Future = Future()
    fut.set_result(None)
    admin.alter_consumer_group_offsets.return_value = {"grp": fut}

    commit_offsets(
        admin,
        group_id="grp",
        topic="other-events",
        max_offsets={0: 50},
    )

    req = admin.alter_consumer_group_offsets.call_args[0][0][0]
    assert all(tp.topic == "other-events" for tp in req.topic_partitions)


def test_commit_offsets_raises_when_future_resolves_with_exception():
    """Kafka rejected the commit — committer must see this so it can
    mark the cycle failed and back off."""
    admin = MagicMock()
    fut: Future = Future()
    fut.set_exception(RuntimeError("group state mismatch"))
    admin.alter_consumer_group_offsets.return_value = {"grp": fut}

    with pytest.raises(RuntimeError, match="group state mismatch"):
        commit_offsets(
            admin,
            group_id="grp",
            topic="events",
            max_offsets={0: 100},
        )


def test_commit_offsets_passes_request_timeout_through():
    """Per-call timeout must reach the admin RPC."""
    admin = MagicMock()
    fut: Future = Future()
    fut.set_result(None)
    admin.alter_consumer_group_offsets.return_value = {"grp": fut}

    commit_offsets(
        admin,
        group_id="grp",
        topic="events",
        max_offsets={0: 100},
        request_timeout_seconds=10.0,
    )

    kwargs = admin.alter_consumer_group_offsets.call_args[1]
    assert kwargs.get("request_timeout") == 10.0


def test_commit_offsets_orders_partitions_deterministically():
    """Tests run faster when output is stable; logs/dashboards also
    benefit. Partitions are committed in ascending order."""
    admin = MagicMock()
    fut: Future = Future()
    fut.set_result(None)
    admin.alter_consumer_group_offsets.return_value = {"grp": fut}

    commit_offsets(
        admin,
        group_id="grp",
        topic="events",
        max_offsets={5: 50, 1: 10, 3: 30},
    )

    req = admin.alter_consumer_group_offsets.call_args[0][0][0]
    partitions_in_order = [tp.partition for tp in req.topic_partitions]
    assert partitions_in_order == [1, 3, 5]
