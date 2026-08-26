from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pytest
from confluent_kafka import KafkaException

from millpond.main import (
    _convert_batch,
    _flush,
    _write_with_retry,
)


def _make_sink() -> MagicMock:
    """Mock Sink — just needs `write`, `reset_caches`, and `close`."""
    return MagicMock(spec=["write", "reset_caches", "close"])


# Realistic failure modes the broad `except Exception` in _write_with_retry
# must catch. If somebody narrows that handler later, these parametrizations
# fail in CI before the regression reaches production. Includes:
#   - OSError: S3 / network
#   - duckdb.Error: DuckLake local execution
#   - KafkaException: broker disconnect during a write (rare but possible
#     if the same Kafka client surfaces an error mid-flush)
#   - RuntimeError: catch-all for backend-internal surprises
_RETRYABLE_EXCEPTIONS = [
    OSError("S3 timeout"),
    duckdb.Error("serialization conflict"),
    KafkaException("broker disconnect"),
    RuntimeError("unexpected"),
]


class TestWriteWithRetry:
    def test_succeeds_first_try(self):
        sink = _make_sink()
        table = pa.table({"a": [1]})
        _write_with_retry(sink, table)
        assert sink.write.call_count == 1
        sink.reset_caches.assert_not_called()

    @pytest.mark.parametrize("exc", _RETRYABLE_EXCEPTIONS)
    def test_retries_on_failure(self, exc):
        sink = _make_sink()
        sink.write.side_effect = [exc, None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time") as mock_time:
            _write_with_retry(sink, table)
        assert sink.write.call_count == 2
        mock_time.sleep.assert_called_once_with(1.0)

    @pytest.mark.parametrize(
        "exc_cls",
        [
            OSError,
            duckdb.Error,
            KafkaException,
            RuntimeError,
        ],
    )
    def test_raises_after_max_retries(self, exc_cls):
        sink = _make_sink()
        sink.write.side_effect = exc_cls("persistent failure")
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"), pytest.raises(exc_cls):
            _write_with_retry(sink, table)
        assert sink.write.call_count == 3

    def test_exponential_backoff(self):
        sink = _make_sink()
        sink.write.side_effect = [OSError(), OSError(), None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time") as mock_time:
            _write_with_retry(sink, table)
        calls = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert calls == [1.0, 2.0]

    @pytest.mark.parametrize("exc", _RETRYABLE_EXCEPTIONS)
    def test_resets_caches_on_retry(self, exc):
        sink = _make_sink()
        sink.write.side_effect = [exc, None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"):
            _write_with_retry(sink, table)
        sink.reset_caches.assert_called_once()


# DuckLake-contention strings the metric labeler must catch. Locking these
# in so a future "improve error messages upstream" PR doesn't silently
# break the alert-able signal.
_DUCKLAKE_CONTENTION_MESSAGES = [
    # DuckLake's own retry-budget-exhausted error (the one operators most
    # often surface via `ducklake_max_retry_count` bumps)
    "Failed to commit DuckLake transaction.\nExceeded the maximum retry count of 100 ...",
    # Underlying Postgres unique-violation bubbled up before DuckLake
    # gives up
    'duplicate key value violates unique constraint "ducklake_snapshot_pkey"',
    # Postgres serialization failure under SERIALIZABLE isolation —
    # unlikely on duckling RDS today but cheap to classify
    "ERROR: could not serialize access due to concurrent update",
]


class TestWriteWithRetryErrorLabels:
    """The metric label distinguishes catalog-contention retries from
    every other write failure. Without this split, an alert on
    `millpond_errors_total{type="write_retry"}` lumps S3 timeouts in
    with snapshot-id PK collisions and can't tell you which is which.
    """

    @pytest.mark.parametrize("msg", _DUCKLAKE_CONTENTION_MESSAGES)
    def test_contention_message_labels_as_commit_contention(self, msg):
        sink = _make_sink()
        sink.write.side_effect = [duckdb.Error(msg), None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"), patch("millpond.main.metrics") as mock_metrics:
            _write_with_retry(sink, table)
        # Exactly one increment, labeled ducklake_commit_contention.
        calls = mock_metrics.errors_total.labels.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs == {"type": "ducklake_commit_contention"}

    @pytest.mark.parametrize(
        "exc",
        [
            OSError("S3 timeout"),
            duckdb.Error("schema mismatch"),
            KafkaException("broker disconnect"),
            RuntimeError("unexpected"),
        ],
    )
    def test_non_contention_exception_labels_as_write_retry(self, exc):
        sink = _make_sink()
        sink.write.side_effect = [exc, None]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"), patch("millpond.main.metrics") as mock_metrics:
            _write_with_retry(sink, table)
        calls = mock_metrics.errors_total.labels.call_args_list
        assert len(calls) == 1
        assert calls[0].kwargs == {"type": "write_retry"}

    def test_mixed_attempts_label_per_attempt(self):
        """Each retry increments according to the exception that triggered
        it — a contention failure then an S3 failure produces one of each
        label, not "whichever exception was last."
        """
        sink = _make_sink()
        sink.write.side_effect = [
            duckdb.Error("Exceeded the maximum retry count of 100"),
            OSError("S3 connection reset"),
            None,
        ]
        table = pa.table({"a": [1]})
        with patch("millpond.main.time"), patch("millpond.main.metrics") as mock_metrics:
            _write_with_retry(sink, table)
        labels = [c.kwargs["type"] for c in mock_metrics.errors_total.labels.call_args_list]
        assert labels == ["ducklake_commit_contention", "write_retry"]


class TestFlushErrorDistinction:
    """Offset commit failures must be distinguishable from write failures in metrics and logs."""

    def _make_flush_args(self):
        sink = _make_sink()
        cfg = MagicMock()
        cfg.table_label = "test_table"
        # Pin sort_by to None so _apply_sort() short-circuits — otherwise
        # the bare MagicMock returns a MagicMock for cfg.sort_by which
        # _apply_sort treats as a configured (truthy, iterable) value and
        # pyarrow's sort_keys validation raises.
        cfg.sort_by = None
        kafka = MagicMock()
        table = pa.table({"a": [1, 2]})
        # Sinks return the count actually written; _flush feeds it to
        # records_written_total.
        sink.write.return_value = table.num_rows
        offsets = {("topic", 0): 42}
        return sink, cfg, kafka, table, offsets

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_failure_raises_after_retries(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = KafkaException("broker unavailable")

        with pytest.raises(KafkaException):
            _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)

        assert kafka.commit.call_count == 3
        # Each failed attempt increments the offset_commit error counter
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 3

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_succeeds_after_retry(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("transient"), None]

        # Should not raise — commit succeeds on second attempt
        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)
        assert kafka.commit.call_count == 2

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_commit_retry_exponential_backoff(self, mock_metrics, mock_server, mock_time):
        mock_time.monotonic.return_value = 0.0
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        kafka.commit.side_effect = [KafkaException("fail"), KafkaException("fail"), None]

        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)
        delays = [c.args[0] for c in mock_time.sleep.call_args_list]
        assert delays == [0.5, 1.0]

    @patch("millpond.main.time")
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_write_failure_does_not_increment_offset_commit_error(self, mock_metrics, mock_server, mock_time):
        sink, cfg, kafka, table, offsets = self._make_flush_args()
        sink.write.side_effect = OSError("S3 timeout")

        with pytest.raises(OSError):
            _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)

        # offset_commit error should NOT have been incremented
        commit_calls = [
            c for c in mock_metrics.errors_total.labels.call_args_list if c.kwargs.get("type") == "offset_commit"
        ]
        assert len(commit_calls) == 0

    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_successful_flush_records_write_metrics(self, mock_metrics, mock_server):
        sink, cfg, kafka, table, offsets = self._make_flush_args()

        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0, trigger="size")

        mock_metrics.records_written_total.inc.assert_called_once_with(2)
        mock_metrics.batches_flushed_total.labels.assert_called_once_with(trigger="size")
        mock_metrics.batches_flushed_total.labels.return_value.inc.assert_called_once()


class TestArrowConversionTiming:
    """Arrow conversion time should be tracked via a histogram metric."""

    @patch("millpond.main.metrics")
    @patch("millpond.main.arrow_converter")
    def test_conversion_time_observed(self, mock_converter, mock_metrics):
        """convert() duration should be observed on the histogram."""
        mock_converter.convert.return_value = pa.table({"a": [1]})

        table = _convert_batch([b'{"a": 1}'])
        assert table is not None
        mock_metrics.arrow_conversion_seconds.observe.assert_called_once()
        observed = mock_metrics.arrow_conversion_seconds.observe.call_args[0][0]
        assert isinstance(observed, float)
        assert observed >= 0

    @patch("millpond.main.metrics")
    @patch("millpond.main.arrow_converter")
    def test_conversion_time_not_observed_when_none(self, mock_converter, mock_metrics):
        """If convert() returns None, no timing should be observed."""
        mock_converter.convert.return_value = None

        table = _convert_batch([b"garbage"])
        assert table is None
        mock_metrics.arrow_conversion_seconds.observe.assert_not_called()


def _capture_skip_calls(mock_metrics):
    """Bind (reason, count) pairs from records_skipped_total inc() calls.

    The default MagicMock pattern memoizes labels(...) to a single inner
    mock, so inc() calls across distinct reason values land in a flat
    list and the reason→count binding is lost. This helper installs a
    side_effect on labels() so each call returns a fresh counter that
    appends (reason, n) into a shared log. Tests assert on the log
    directly — both ordering and pairing are observable.
    """
    skip_calls: list[tuple[str, int]] = []

    def _counter_for(reason):
        counter = MagicMock()
        counter.inc.side_effect = lambda n, r=reason: skip_calls.append((r, n))
        return counter

    mock_metrics.records_skipped_total.labels.side_effect = _counter_for
    return skip_calls


class TestCoerceColumns:
    """The main.py delegate that gates coercion on cfg.typed_columns."""

    def _cfg(self, *, typed_columns=None):
        cfg = MagicMock()
        cfg.typed_columns = typed_columns
        return cfg

    def test_no_op_when_unconfigured(self):
        from millpond.main import _coerce_columns

        table = pa.table({"timestamp": ["2024-01-01 12:00:00.000000"]})
        result = _coerce_columns(table, self._cfg(typed_columns=None))
        # Short-circuit: same object, no coercion.
        assert result is table
        assert result.schema.field("timestamp").type == pa.string()

    def test_coerces_when_configured(self):
        from millpond.main import _coerce_columns

        table = pa.table({"timestamp": ["2024-01-01 12:00:00.000000"], "project_id": pa.array([None], pa.string())})
        pairs = (("timestamp", "timestamptz"), ("project_id", "bigint"))
        result = _coerce_columns(table, self._cfg(typed_columns=pairs))
        assert result.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert result.schema.field("project_id").type == pa.int64()


class TestApplyFilter:
    """Hot-path keep-filter behaviour. Mocks the metrics module so the
    skipped-record counter calls are visible without setting up a real
    Prometheus registry. Each test constructs a Config-shaped object only
    with the fields _apply_filter reads."""

    def _cfg(self, *, keep=None):
        # values now flow as _apply_filter's third arg (the include-values
        # source's current set), not via config.
        cfg = MagicMock()
        cfg.filter_keep_field = keep
        return cfg

    def test_no_op_when_filter_unconfigured(self):
        from millpond.main import _apply_filter

        table = pa.table({"team_id": [1, 2, 3]})
        result = _apply_filter(table, self._cfg(), None)
        # Same object: no slicing or filtering — short-circuit at the top.
        assert result is table

    @patch("millpond.main.metrics")
    def test_int_allowlist_keeps_matching_rows(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [1, 2, 3, 4, 5], "event": ["a", "b", "c", "d", "e"]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (2, 4))

        assert result.num_rows == 2
        assert result.column("team_id").to_pylist() == [2, 4]
        assert result.column("event").to_pylist() == ["b", "d"]
        # 3 of 5 rows fail the allowlist; exactly one increment, bound to
        # the correct reason.
        assert skip_calls == [("filter_excluded", 3)]

    @patch("millpond.main.metrics")
    def test_string_allowlist_keeps_matching_rows(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"region": ["us-east-1", "us-west-2", "eu-central-1"]})
        result = _apply_filter(table, self._cfg(keep="region"), ("us-east-1", "eu-central-1"))

        assert result.column("region").to_pylist() == ["us-east-1", "eu-central-1"]
        assert skip_calls == [("filter_excluded", 1)]

    @patch("millpond.main.metrics")
    def test_matched_counts_per_value(self, mock_metrics):
        # filter_matched_total gets one increment per DISTINCT kept value
        # with the per-value row count — the stacked-proportion panel's
        # data. Excluded values must not appear.
        from millpond.main import _apply_filter

        match_calls: list[tuple[str, int]] = []

        def _counter_for(value):
            counter = MagicMock()
            counter.inc.side_effect = lambda n, v=value: match_calls.append((v, n))
            return counter

        mock_metrics.filter_matched_total.labels.side_effect = _counter_for
        table = pa.table({"team_id": [2, 2, 2, 47074, 47074, 999]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (2, 47074))

        assert result.num_rows == 5
        assert sorted(match_calls) == [("2", 3), ("47074", 2)]

    @patch("millpond.main.metrics")
    def test_matched_counts_skipped_on_empty_result(self, mock_metrics):
        # Nothing kept -> no filter_matched_total activity at all.
        from millpond.main import _apply_filter

        table = pa.table({"team_id": [7, 8, 9]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (2,))
        assert result.num_rows == 0
        mock_metrics.filter_matched_total.labels.assert_not_called()

    @patch("millpond.main.metrics")
    def test_int_values_coerce_to_string_column(self, mock_metrics):
        # When the column is string-typed and values parsed as int, the
        # values get cast to their canonical string form ("2") and matched
        # against the column. JSON ints sometimes deserialise as Arrow
        # strings; this is the supported path. Leading-zero strings like
        # "02" deliberately do NOT match a configured value of 2 — strict
        # string equality after coercion. See
        # `test_leading_zero_strings_do_not_match_int_values` for the
        # adjacent contract.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": ["1", "2", "3"]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (2,))

        assert result.num_rows == 1
        assert result.column("team_id").to_pylist() == ["2"]
        # Two excluded rows ("1", "3"); pin the (reason, count) binding.
        assert skip_calls == [("filter_excluded", 2)]

    @patch("millpond.main.metrics")
    def test_leading_zero_strings_do_not_match_int_values(self, mock_metrics):
        # Documents the strict-string-equality semantic. An operator who
        # wants to match leading-zero IDs must configure them as strings
        # (`MILLPOND_FILTER_VALUES=02`).
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": ["02", "2", "003"]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (2,))

        # Only the unambiguous "2" matches; the leading-zero strings don't.
        assert result.column("team_id").to_pylist() == ["2"]
        assert skip_calls == [("filter_excluded", 2)]

    @patch("millpond.main.metrics")
    def test_missing_field_drops_whole_batch(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"event": ["a", "b", "c"]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        # Column not in schema → whole batch lands in filter_field_missing.
        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_null_values_counted_as_field_missing(self, mock_metrics):
        # Distinguishing the two skip-reason buckets is the whole point of
        # the dual-counter design: missing/null is anomalous, excluded is
        # expected steady-state behaviour. The assertion pins the bucket
        # *and* the count, not just the sorted set of counts.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([1, None, 2, None, 3], type=pa.int64())})
        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 2
        assert result.column("team_id").to_pylist() == [1, 2]
        # 2 nulls → field_missing; 1 row (team_id=3) → excluded.
        assert sorted(skip_calls) == [("filter_excluded", 1), ("filter_field_missing", 2)]

    @patch("millpond.main.metrics")
    def test_all_rows_kept_emits_no_skip_metric(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [1, 2, 1]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 3
        assert skip_calls == []

    @patch("millpond.main.metrics")
    def test_all_rows_dropped_returns_empty_table_with_same_schema(self, mock_metrics):
        from millpond.main import _apply_filter

        _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [9, 10, 11], "event": ["a", "b", "c"]})
        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 0
        # Schema is preserved — important so a downstream concat doesn't trip
        # on a column mismatch when a batch happens to filter to empty.
        assert result.schema == table.schema

    @patch("millpond.main.metrics")
    def test_empty_input_batch_is_no_op(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([], type=pa.int64())})
        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 0
        assert skip_calls == []

    # --- Schema variance / cast failure paths --------------------------------

    @patch("millpond.main.metrics")
    def test_multi_chunk_column_is_handled(self, mock_metrics):
        # Defensive against the case where a column ends up multi-chunk
        # (a fresh `_convert_batch` output is single-chunk, but anything
        # that goes through ChunkedArray-producing arrow surgery later
        # could pass one in). The compute kernels we use must work on
        # multi-chunk inputs and `column.null_count` must aggregate
        # across chunks correctly.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        team_ids = pa.chunked_array([[1, 2], [3, 4, 5], [2]])
        events = pa.chunked_array([["a", "b"], ["c", "d", "e"], ["f"]])
        table = pa.table({"team_id": team_ids, "event": events})

        result = _apply_filter(table, self._cfg(keep="team_id"), (2,))

        # Two rows match (the two 2s spread across chunks 0 and 2).
        assert result.column("team_id").to_pylist() == [2, 2]
        assert result.column("event").to_pylist() == ["b", "f"]
        assert skip_calls == [("filter_excluded", 4)]

    # --- Unsupported column types (must skip, not silently match) ----------
    #
    # The filter restricts itself to integer and string columns. Everything
    # else lands the batch in `filter_field_missing` so the operator sees
    # a clear signal in the skip-reason metric rather than a quiet,
    # semantically-wrong match. Each of the tests below pins one specific
    # column type the explicit allowlist rejects — bool, float, timestamp,
    # struct, list — plus the cast-overflow case for in-range types.

    @patch("millpond.main.metrics")
    def test_bool_column_rejected_as_unsupported(self, mock_metrics):
        # PyArrow happily casts ints to bool (0→False, non-zero→True),
        # which would otherwise mean `MILLPOND_FILTER_VALUES=2` keeps
        # every `True` row regardless of the configured value. The
        # column-type allowlist explicitly rejects bool to prevent that.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"flag": pa.array([True, False, True, False], type=pa.bool_())})

        result = _apply_filter(table, self._cfg(keep="flag"), (1,))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 4)]

    @patch("millpond.main.metrics")
    def test_float_column_rejected_as_unsupported(self, mock_metrics):
        # Without the allowlist, `(2,)` would cast to `2.0` and silently
        # match floating rows that happen to equal 2.0. We don't want
        # equality-on-float-columns semantics to be a hidden feature.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"score": pa.array([1.0, 2.0, 3.0], type=pa.float64())})

        result = _apply_filter(table, self._cfg(keep="score"), (2,))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_timestamp_column_rejected_as_unsupported(self, mock_metrics):
        # Without the allowlist, ints cast to timestamp as
        # microseconds-since-epoch — a wildly surprising match. Reject.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        ts_col = pa.array([1700000000_000000, 1700000001_000000], type=pa.timestamp("us"))
        table = pa.table({"ts": ts_col})

        result = _apply_filter(table, self._cfg(keep="ts"), (1700000000_000000,))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 2)]

    @patch("millpond.main.metrics")
    def test_struct_column_rejected_as_unsupported(self, mock_metrics):
        # Struct columns hit the same allowlist rejection — critical
        # because a struct-cast was the previous crash hazard, and the
        # column-type check fires *before* the cast attempt.
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        struct_col = pa.array(
            [{"a": 1}, {"a": 2}, {"a": 3}],
            type=pa.struct([pa.field("a", pa.int64())]),
        )
        table = pa.table({"team_id": struct_col})

        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_list_column_rejected_as_unsupported(self, mock_metrics):
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        list_col = pa.array([[1, 2], [3], [4, 5, 6]], type=pa.list_(pa.int64()))
        table = pa.table({"team_id": list_col})

        result = _apply_filter(table, self._cfg(keep="team_id"), (1, 2))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_int_values_overflowing_int32_column_skip_batch(self, mock_metrics):
        # Integer column type passes the allowlist; the cast itself
        # raises on width overflow under `safe=True`, and that lands in
        # the cast-failure branch (still `filter_field_missing`, but a
        # different code path from the unsupported-type case above).
        from millpond.main import _apply_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([1, 2, 3], type=pa.int32())})
        # 2**40 is well outside int32 range.
        result = _apply_filter(table, self._cfg(keep="team_id"), (2**40,))

        assert result.num_rows == 0
        assert skip_calls == [("filter_field_missing", 3)]


def _capture_sort_skip_calls(mock_metrics):
    """Bind (reason, count) pairs from sort_skipped_total inc() calls.

    Mirrors `_capture_skip_calls` but for the sort-skip metric so the
    sort tests don't have to do `labels.return_value.inc.call_args_list`
    introspection and risk the same MagicMock memoisation pitfall the
    filter tests hit.
    """
    sort_calls: list[tuple[str, int]] = []

    def _counter_for(reason):
        counter = MagicMock()
        counter.inc.side_effect = lambda n, r=reason: sort_calls.append((r, n))
        return counter

    mock_metrics.sort_skipped_total.labels.side_effect = _counter_for
    return sort_calls


class TestApplyDropFilter:
    """Drop-direction (denylist) filter. Composes AFTER the keep-filter;
    fail-open on anything unevaluable (the opposite of keep — see the
    docstring on _apply_drop_filter for why)."""

    def _cfg(self, *, drop=None, drop_values=None):
        cfg = MagicMock()
        cfg.filter_drop_field = drop
        cfg.filter_drop_values = drop_values
        return cfg

    def test_no_op_when_unconfigured(self):
        from millpond.main import _apply_drop_filter

        table = pa.table({"team_id": [1, 2, 3]})
        result = _apply_drop_filter(table, self._cfg())
        assert result is table

    @patch("millpond.main.metrics")
    def test_int_denylist_drops_matching_rows(self, mock_metrics):
        from millpond.main import _apply_drop_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [1, 47074, 3, 47074, 5], "event": ["a", "b", "c", "d", "e"]})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=(47074,)))

        assert result.column("team_id").to_pylist() == [1, 3, 5]
        assert result.column("event").to_pylist() == ["a", "c", "e"]
        assert skip_calls == [("filter_dropped", 2)]

    @patch("millpond.main.metrics")
    def test_string_values_cast_to_int_column(self, mock_metrics):
        # Operator wrote MILLPOND_FILTER_DROP_VALUES with a stray
        # non-numeric token → whole tuple parses as strings; safe cast to
        # the int64 column must still match.
        from millpond.main import _apply_drop_filter

        _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [1, 47074, 3]})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=("47074",)))
        assert result.column("team_id").to_pylist() == [1, 3]

    @patch("millpond.main.metrics")
    def test_missing_field_fails_open(self, mock_metrics):
        from millpond.main import _apply_drop_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"other": [1, 2, 3]})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=(47074,)))
        # Batch unchanged — a blacklist that can't evaluate must not drop.
        assert result is table
        assert skip_calls == []

    @patch("millpond.main.metrics")
    def test_unsupported_column_type_fails_open(self, mock_metrics):
        from millpond.main import _apply_drop_filter

        _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([1.5, 2.5], pa.float64())})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=(47074,)))
        assert result is table

    @patch("millpond.main.metrics")
    def test_uncastable_values_fail_open(self, mock_metrics):
        from millpond.main import _apply_drop_filter

        _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [1, 2]})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=("not-a-number",)))
        assert result is table

    @patch("millpond.main.metrics")
    def test_null_field_values_are_kept(self, mock_metrics):
        from millpond.main import _apply_drop_filter

        skip_calls = _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([1, None, 47074], pa.int64())})
        result = _apply_drop_filter(table, self._cfg(drop="team_id", drop_values=(47074,)))
        # Null can't match a blacklist → kept; only the real match drops.
        assert result.column("team_id").to_pylist() == [1, None]
        assert skip_calls == [("filter_dropped", 1)]

    @patch("millpond.main.metrics")
    def test_composes_with_keep_filter(self, mock_metrics):
        # The production shape: CP include set says {2, 47074, 50689},
        # blacklist says drop 47074 → survivors are keep ∩ ¬drop.
        from millpond.main import _apply_drop_filter, _apply_filter

        _capture_skip_calls(mock_metrics)
        table = pa.table({"team_id": [2, 47074, 50689, 99999]})
        keep_cfg = MagicMock()
        keep_cfg.filter_keep_field = "team_id"
        kept = _apply_filter(table, keep_cfg, (2, 47074, 50689))
        result = _apply_drop_filter(kept, self._cfg(drop="team_id", drop_values=(47074,)))
        assert result.column("team_id").to_pylist() == [2, 50689]


class TestApplySort:
    """Hot-path pre-write sort behaviour.

    `_apply_sort` runs inside `_flush()` between consolidate and write;
    the unit tests here drive it directly with handcrafted tables.
    `cfg` is a MagicMock with `sort_by` (and any other field we read)
    pinned explicitly — see filter-test rationale for why the bare
    MagicMock pattern is a trap.
    """

    def _cfg(self, sort_by=None):
        cfg = MagicMock()
        cfg.sort_by = sort_by
        return cfg

    def _clear_warned(self):
        # Module-level dedup set leaks between tests if not reset.
        import millpond.main as _main

        _main._sort_missing_fields_warned.clear()

    def test_no_op_when_unconfigured(self):
        from millpond.main import _apply_sort

        table = pa.table({"team_id": [3, 1, 2]})
        result = _apply_sort(table, self._cfg(sort_by=None))
        # Same object: short-circuit at top.
        assert result is table

    @patch("millpond.main.metrics")
    def test_single_field_ascending(self, mock_metrics):
        from millpond.main import _apply_sort

        sort_calls = _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"team_id": [3, 1, 2], "event": ["c", "a", "b"]})
        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))

        assert result.column("team_id").to_pylist() == [1, 2, 3]
        # event column must reorder consistently — full-row reordering.
        assert result.column("event").to_pylist() == ["a", "b", "c"]
        assert sort_calls == []

    @patch("millpond.main.metrics")
    def test_multi_field_sort_left_to_right(self, mock_metrics):
        # First field is primary key, second is secondary, etc. The
        # multi-field test pins this ordering: equal team_id rows are
        # sub-sorted by timestamp.
        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        table = pa.table(
            {
                "team_id": [2, 1, 2, 1],
                "timestamp": [200, 100, 100, 200],
                "event": ["d", "b", "c", "a"],
            }
        )
        result = _apply_sort(table, self._cfg(sort_by=("team_id", "timestamp")))

        assert result.column("team_id").to_pylist() == [1, 1, 2, 2]
        assert result.column("timestamp").to_pylist() == [100, 200, 100, 200]
        # "b" had (team_id=1, timestamp=100) → first row in sort order; etc.
        assert result.column("event").to_pylist() == ["b", "a", "c", "d"]

    @patch("millpond.main.metrics")
    def test_sort_is_stable(self, mock_metrics):
        # PyArrow's sort_indices is stable. Equal keys preserve input order;
        # this lets operators reason about secondary attributes without
        # naming them in the sort key.
        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        # Two rows share team_id=1; input order: (event=alpha) before (event=beta).
        # After ascending sort by team_id, alpha must still precede beta.
        table = pa.table({"team_id": [1, 2, 1], "event": ["alpha", "x", "beta"]})
        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))

        assert result.column("team_id").to_pylist() == [1, 1, 2]
        assert result.column("event").to_pylist() == ["alpha", "beta", "x"]

    @patch("millpond.main.metrics")
    def test_nulls_placed_at_end(self, mock_metrics):
        # Default null_placement="at_end" — null rows still ride through
        # the sink, just sorted to the tail. They are NOT counted as
        # sort-skipped (the sort applied; some rows just happened to be
        # null in the key).
        from millpond.main import _apply_sort

        sort_calls = _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([3, None, 1, None, 2], type=pa.int64())})
        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))

        assert result.column("team_id").to_pylist() == [1, 2, 3, None, None]
        assert sort_calls == []

    @patch("millpond.main.metrics")
    def test_missing_field_skips_sort_and_increments_metric(self, mock_metrics):
        # Critical contract: a missing sort field MUST NOT drop records.
        # The data still rides through to the sink in its existing
        # (unsorted) order; only the sort step was skipped.
        self._clear_warned()
        from millpond.main import _apply_sort

        sort_calls = _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"event": ["c", "a", "b"]})
        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))

        # Order unchanged — proves the records weren't filtered or sorted
        # by a different key when the configured key was unavailable.
        assert result.column("event").to_pylist() == ["c", "a", "b"]
        assert sort_calls == [("field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_partial_missing_fields_skip_sort(self, mock_metrics):
        # Multi-field sort: if ANY configured field is absent, the whole
        # sort is skipped (rather than partially sorting on the available
        # fields, which would silently differ from the operator's intent).
        self._clear_warned()
        from millpond.main import _apply_sort

        sort_calls = _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"team_id": [2, 1, 3]})  # has team_id, missing timestamp
        result = _apply_sort(table, self._cfg(sort_by=("team_id", "timestamp")))

        assert result.column("team_id").to_pylist() == [2, 1, 3]
        assert sort_calls == [("field_missing", 3)]

    @patch("millpond.main.metrics")
    def test_missing_field_warning_logged_once_per_pattern(self, mock_metrics, caplog):
        # The metric increments every flush; the *log* is once per
        # missing-fields pattern. This is what keeps a misconfigured
        # high-volume topic from spamming logs.
        self._clear_warned()
        import logging

        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"event": ["a"]})
        cfg = self._cfg(sort_by=("team_id",))

        with caplog.at_level(logging.WARNING, logger="millpond.main"):
            for _ in range(5):
                _apply_sort(table, cfg)

        warnings_for_pattern = [
            r for r in caplog.records if "Sort field(s) missing" in r.message and "team_id" in r.message
        ]
        assert len(warnings_for_pattern) == 1

    @patch("millpond.main.metrics")
    def test_distinct_missing_patterns_each_log_once(self, mock_metrics, caplog):
        # If a deployment somehow sees two different missing-fields
        # patterns (e.g. schema drift), each gets its own warning so the
        # operator sees both signals.
        self._clear_warned()
        import logging

        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        cfg_a = self._cfg(sort_by=("team_id",))
        cfg_b = self._cfg(sort_by=("distinct_id",))
        table = pa.table({"event": ["a"]})

        with caplog.at_level(logging.WARNING, logger="millpond.main"):
            _apply_sort(table, cfg_a)
            _apply_sort(table, cfg_a)  # already-warned pattern
            _apply_sort(table, cfg_b)
            _apply_sort(table, cfg_b)  # already-warned pattern

        warnings = [r for r in caplog.records if "Sort field(s) missing" in r.message]
        # One per distinct pattern.
        assert len(warnings) == 2

    @patch("millpond.main.metrics")
    def test_empty_input_batch_is_no_op(self, mock_metrics):
        # Edge case: a fully-filtered-out batch reaches the sort step
        # with zero rows. sort_indices on empty returns empty; take()
        # returns an empty table with the same schema. No metric.
        from millpond.main import _apply_sort

        sort_calls = _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"team_id": pa.array([], type=pa.int64())})
        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))

        assert result.num_rows == 0
        assert sort_calls == []

    @patch("millpond.main.metrics")
    def test_multi_chunk_column_sorted_correctly(self, mock_metrics):
        # Defensive: sort_indices must aggregate across chunks correctly
        # (it does in PyArrow). Pin this so a future regression doesn't
        # ship a per-chunk sort that looks correct only on single-chunk
        # tables.
        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        team_ids = pa.chunked_array([[3, 1], [4, 1], [2]])
        events = pa.chunked_array([["a", "b"], ["c", "d"], ["e"]])
        table = pa.table({"team_id": team_ids, "event": events})

        result = _apply_sort(table, self._cfg(sort_by=("team_id",)))
        assert result.column("team_id").to_pylist() == [1, 1, 2, 3, 4]
        # Stable sort preserves the two team_id=1 rows' relative order.
        assert result.column("event").to_pylist() == ["b", "d", "e", "a", "c"]

    @patch("millpond.main.metrics")
    def test_sorts_by_string_column(self, mock_metrics):
        # Sort keys aren't limited to integers — string columns sort
        # lexically, also ascending.
        from millpond.main import _apply_sort

        _capture_sort_skip_calls(mock_metrics)
        table = pa.table({"region": ["us-west-2", "eu-central-1", "us-east-1"]})
        result = _apply_sort(table, self._cfg(sort_by=("region",)))

        assert result.column("region").to_pylist() == ["eu-central-1", "us-east-1", "us-west-2"]


class TestFlushCommit:
    @patch("millpond.main.server")
    @patch("millpond.main.metrics")
    def test_flush_commits_offsets_synchronously(self, mock_metrics, mock_server):
        sink = _make_sink()
        cfg = MagicMock()
        cfg.table_label = "test_table"
        cfg.sort_by = None
        kafka = MagicMock()
        table = pa.table({"a": [1, 2]})
        offsets = {("events", 0): 100}
        _flush(sink, cfg, kafka, table, 100, 2, offsets, 1.0)
        kafka.commit.assert_called_once()
        # Committed offset is next-to-fetch (max consumed + 1).
        committed = kafka.commit.call_args.kwargs["offsets"]
        assert [(tp.partition, tp.offset) for tp in committed] == [(0, 101)]


class TestConsumeTimeout:
    """_consume_timeout must never exceed the liveness poll-age budget.

    server.health marks the process dead when no poll lands for 300s
    (max_poll_age_s), and record_poll only runs after consume() returns.
    Before the cap, FLUSH_INTERVAL_MS=600000 on a quiet topic meant a
    600s consume block -> liveness SIGKILL loop across the fleet.
    """

    def test_large_interval_capped_at_max_block(self):
        from millpond.main import _CONSUME_MAX_BLOCK_S, _consume_timeout

        assert _consume_timeout(600.0) == _CONSUME_MAX_BLOCK_S

    def test_cap_stays_under_liveness_budget(self):
        from millpond.main import _CONSUME_MAX_BLOCK_S
        from millpond.server import _HealthState

        assert _CONSUME_MAX_BLOCK_S < _HealthState().max_poll_age_s

    def test_short_remaining_passes_through(self):
        from millpond.main import _consume_timeout

        assert _consume_timeout(30.0) == 30.0

    def test_elapsed_interval_floors_at_poll_minimum(self):
        from millpond.main import _consume_timeout

        assert _consume_timeout(-5.0) == 0.1
        assert _consume_timeout(0.0) == 0.1
