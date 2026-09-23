import io
import uuid as uuid_mod
from datetime import UTC, datetime
from unittest.mock import patch

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from millpond.arrow_converter import (
    _drop_null_typed_columns,
    _to_uuid,
    coerce_typed_columns,
    convert,
    uuid_bytes,
    uuid_text,
)


class TestConvert:
    def test_basic(self):
        messages = [
            orjson.dumps({"name": "alice", "age": 30}),
            orjson.dumps({"name": "bob", "age": 25}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert table.column("name").to_pylist() == ["alice", "bob"]

    def test_numeric_type_normalization(self):
        messages = [orjson.dumps({"x": 42, "y": 3.14})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("x").type == pa.int64()
        assert table.schema.field("y").type == pa.float64()

    def test_integer_normalized_to_int64(self):
        messages = [orjson.dumps({"count": 100})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("count").type == pa.int64()

    def test_heterogeneous_schemas(self):
        # Field "b" only appears in the second record — must still be included
        messages = [
            orjson.dumps({"a": 1}),
            orjson.dumps({"a": 2, "b": "new_field"}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert "a" in table.schema.names
        assert "b" in table.schema.names
        assert table.column("b").to_pylist() == [None, "new_field"]

    def test_field_only_in_first_record(self):
        messages = [
            orjson.dumps({"a": 1, "b": "only_here"}),
            orjson.dumps({"a": 2}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert "b" in table.schema.names
        assert table.column("b").to_pylist() == ["only_here", None]

    def test_malformed_json_skipped(self):
        messages = [
            orjson.dumps({"good": 1}),
            b"not json{{{",
            orjson.dumps({"good": 2}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2

    def test_all_malformed_returns_none(self):
        messages = [b"bad1", b"bad2"]
        table = convert(messages)
        assert table is None

    def test_empty_returns_none(self):
        table = convert([])
        assert table is None

    def test_nested_objects_serialized_as_json(self):
        messages = [orjson.dumps({"meta": {"key": "value"}, "tags": [1, 2, 3]})]
        table = convert(messages)
        assert table is not None
        assert len(table) == 1
        # Nested objects are serialized to JSON strings
        assert table.schema.field("meta").type == pa.string()
        assert table.schema.field("tags").type == pa.string()
        assert table.column("meta").to_pylist() == ['{"key":"value"}']
        assert table.column("tags").to_pylist() == ["[1,2,3]"]

    def test_null_values(self):
        messages = [orjson.dumps({"a": 1, "b": None})]
        table = convert(messages)
        assert table is not None
        assert table.column("b").to_pylist() == [None]

    def test_non_dict_json_skipped(self):
        messages = [
            orjson.dumps({"good": 1}),
            orjson.dumps("just a string"),
            orjson.dumps([1, 2, 3]),
            orjson.dumps(42),
            orjson.dumps({"also_good": 2}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2

    def test_boolean_not_cast(self):
        messages = [orjson.dumps({"flag": True})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("flag").type == pa.bool_()

    def test_nested_struct_with_null_inner_field(self):
        """Nested dicts with null inner values are serialized to JSON strings."""
        messages = [
            orjson.dumps({"props": {"referrer": None, "width": 1920}}),
            orjson.dumps({"props": {"referrer": "google", "width": 1440}}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        # Nested dicts become JSON strings, avoiding struct type inference issues
        assert table.schema.field("props").type == pa.string()

    def test_mixed_type_bool_and_string(self):
        """Same field is bool in one record and string in another."""
        messages = [
            orjson.dumps({"flag_response": True}),
            orjson.dumps({"flag_response": "variant-a"}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert table.schema.field("flag_response").type == pa.string()
        assert table.column("flag_response").to_pylist() == ["True", "variant-a"]

    def test_mixed_type_int_and_string(self):
        """Same field is int in one record and string in another."""
        messages = [
            orjson.dumps({"employee_count": 50}),
            orjson.dumps({"employee_count": "51-200"}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert table.schema.field("employee_count").type == pa.string()

    def test_large_int_in_float_field_converts_lossily(self):
        """An int above 2^53 in a float-inferred field must not crash the batch.

        The session features topic emits float aggregates as integers when
        whole; one record carried an integer above 2^53, and PyArrow refuses
        the inexact int-to-double conversion in from_pylist. The converter
        pre-casts ints to float when the inferred field type is floating —
        nearest-double is the wanted semantics for a floating measure.
        """
        huge = 130184854372975800  # > 2^53, not exactly representable
        messages = [
            orjson.dumps({"mouse_sum_x": 1.5}),
            orjson.dumps({"mouse_sum_x": huge}),
        ]
        table = convert(messages)
        assert table is not None
        assert len(table) == 2
        assert pa.types.is_floating(table.schema.field("mouse_sum_x").type)
        assert table.column("mouse_sum_x").to_pylist() == [1.5, float(huge)]

    def test_whole_int_in_float_field_stays_float(self):
        """Whole-number ints in a float-inferred field become floats, not strings."""
        messages = [
            orjson.dumps({"velocity": 0.25}),
            orjson.dumps({"velocity": 3}),
        ]
        table = convert(messages)
        assert table is not None
        assert pa.types.is_floating(table.schema.field("velocity").type)
        assert table.column("velocity").to_pylist() == [0.25, 3.0]

    def test_large_integer_precision_preserved(self):
        """Integers > 2^53 must not lose precision via float64 cast."""
        large_id = 2**53 + 1  # 9007199254740993 — not representable in float64
        messages = [orjson.dumps({"id": large_id})]
        table = convert(messages)
        assert table is not None
        assert table.column("id").to_pylist() == [large_id]
        assert table.schema.field("id").type == pa.int64()

    def test_integers_cast_to_int64(self):
        """Pure integer columns should be int64, not float64."""
        messages = [orjson.dumps({"count": 42})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("count").type == pa.int64()

    def test_floats_cast_to_float64(self):
        """Float columns should remain float64."""
        messages = [orjson.dumps({"price": 3.14})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("price").type == pa.float64()

    def test_all_null_field_does_not_produce_pa_null_column(self):
        # In normal use _build_schema falls back to pa.string() for keys
        # that are None in every record — so convert() should never emit
        # a pa.null() column. Lock that.
        table = convert([orjson.dumps({"x": None, "y": "data"})])
        assert table is not None
        assert all(not pa.types.is_null(f.type) for f in table.schema)

    def test_cross_batch_concat_with_promote(self):
        """Tables from separate convert() calls may have different schemas.
        pa.concat_tables must use promote_options to handle this."""
        batch1 = convert([orjson.dumps({"a": 1})])
        batch2 = convert([orjson.dumps({"a": 2, "b": "new"})])
        assert batch1 is not None and batch2 is not None
        # Without promote_options="default", this would raise ArrowInvalid
        merged = pa.concat_tables([batch1, batch2], promote_options="default")
        assert len(merged) == 2
        assert "b" in merged.schema.names
        assert merged.column("b").to_pylist() == [None, "new"]


class TestDropNullTypedColumns:
    """Defensive filter against pa.null() columns slipping through to a Sink.

    A column with no schema info is a column with no data; dropping at
    the converter keeps the Sink contract clean and lets the column come
    back with a real type on the next non-null batch.
    """

    def test_drops_pa_null_column(self):
        table = pa.table({"a": pa.array([None, None], pa.null()), "b": ["x", "y"]})
        out = _drop_null_typed_columns(table)
        assert out.column_names == ["b"]

    def test_passthrough_when_no_null_columns(self):
        table = pa.table({"a": [1, 2], "b": ["x", "y"]})
        out = _drop_null_typed_columns(table)
        # No re-allocation when there's nothing to do.
        assert out is table

    def test_drops_only_null_typed_columns_not_columns_with_nulls(self):
        # A regular string column with all-None values is NOT pa.null() —
        # it's a string column with nulls. Must not be dropped.
        table = pa.table(
            {
                "actually_null_type": pa.array([None], pa.null()),
                "string_with_nulls": pa.array([None], pa.string()),
            }
        )
        out = _drop_null_typed_columns(table)
        assert out.column_names == ["string_with_nulls"]


class TestCoerceTypedColumns:
    # The wire format ClickHouse-events producers emit for DateTime columns:
    # space-separated, UTC implied, variable fractional precision.
    WIRE = "2024-01-01 12:00:00.000000"

    def test_parses_wire_format_to_timestamptz(self):
        table = pa.table({"timestamp": [self.WIRE], "team_id": [1]})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert out.column("timestamp").to_pylist() == [datetime(2024, 1, 1, 12, 0, tzinfo=UTC)]
        # Untargeted columns are untouched.
        assert out.schema.field("team_id").type == pa.int64()

    def test_coerces_multiple_columns(self):
        cols = ("timestamp", "created_at", "group0_created_at")
        table = pa.table({c: [self.WIRE] for c in cols})
        out = coerce_typed_columns(table, tuple((c, "timestamptz") for c in cols))
        for c in cols:
            assert out.schema.field(c).type == pa.timestamp("us", tz="UTC")

    def test_all_null_string_project_id_coerces_to_bigint(self):
        # project_id serializes as explicit JSON null (no serde skip), so an
        # all-null batch infers VARCHAR; bigint coercion realigns it to the
        # events table's project_id BIGINT.
        table = pa.table({"project_id": pa.array([None, None], pa.string())})
        out = coerce_typed_columns(table, (("project_id", "bigint"),))
        assert out.schema.field("project_id").type == pa.int64()
        assert out.column("project_id").to_pylist() == [None, None]

    def test_already_target_int64_left_alone(self):
        # When project_id has values it already infers int64 — no rebuild.
        table = pa.table({"project_id": pa.array([1, 2], pa.int64())})
        out = coerce_typed_columns(table, (("project_id", "bigint"),))
        assert out is table

    def test_nulls_pass_through(self):
        # Nullable timestamps (e.g. person_created_at) arrive as JSON null.
        table = pa.table({"person_created_at": pa.array([self.WIRE, None], pa.string())})
        out = coerce_typed_columns(table, (("person_created_at", "timestamptz"),))
        assert out.column("person_created_at").to_pylist() == [
            datetime(2024, 1, 1, 12, 0, tzinfo=UTC),
            None,
        ]

    def test_missing_column_is_noop(self):
        # A configured column absent from this batch is skipped, not an error,
        # so the same map can be pointed at heterogeneous batches.
        table = pa.table({"team_id": [1]})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out is table

    def test_already_timestamp_typed_left_alone(self):
        ts = pa.array([datetime(2024, 1, 1, tzinfo=UTC)], pa.timestamp("us", tz="UTC"))
        table = pa.table({"timestamp": ts})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out is table

    def test_empty_map_is_noop(self):
        table = pa.table({"timestamp": [self.WIRE]})
        out = coerce_typed_columns(table, ())
        assert out is table

    def test_varying_fractional_precision_parses(self):
        # The real producer emits 0, 3, or 6 fractional digits depending on the
        # column/source; all must parse to the same Arrow type.
        cols = {
            "timestamp": "2024-01-01 12:00:00.123",  # Node: 3 digits
            "person_created_at": "2024-01-01 12:00:00",  # Node: 0 digits
            "created_at": "2024-01-01 12:00:00.123456",  # 6 digits
        }
        table = pa.table({k: [v] for k, v in cols.items()})
        out = coerce_typed_columns(table, tuple((c, "timestamptz") for c in cols))
        for c in cols:
            assert out.schema.field(c).type == pa.timestamp("us", tz="UTC")

    @patch("millpond.arrow_converter.metrics")
    def test_unparseable_value_is_non_fatal_and_typed(self, mock_metrics):
        # A format drift must NOT raise on the consume path (that risks committing
        # offsets past unwritten records). The bad value is nulled but the column
        # is STILL the target type — keeping the pending buffer schema-consistent
        # so pa.concat_tables at flush can't raise — and the error metric fires.
        table = pa.table({"timestamp": ["not-a-timestamp"], "team_id": [1]})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert out.column("timestamp").to_pylist() == [None]
        mock_metrics.errors_total.labels.assert_called_with(type="column_coercion")
        mock_metrics.errors_total.labels(type="column_coercion").inc.assert_called_once()
        mock_metrics.columns_coerced_total.labels.assert_not_called()

    def test_bad_value_nulled_good_values_preserved(self):
        # Within one column, only the unconvertible value is nulled; good values
        # survive and the column is the target type.
        table = pa.table({"timestamp": [self.WIRE, "garbage", None]})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert out.column("timestamp").to_pylist() == [datetime(2024, 1, 1, 12, 0, tzinfo=UTC), None, None]

    def test_failed_batch_stays_concatenable_with_good_batch(self):
        # Regression for the cross-batch concat crash: a batch whose coercion
        # failed must still concat with a fully-coerced batch (both end up the
        # target type), because the consume loop buffers batches and flushes them
        # via pa.concat_tables(..., promote_options="default").
        good = coerce_typed_columns(pa.table({"timestamp": [self.WIRE]}), (("timestamp", "timestamptz"),))
        bad = coerce_typed_columns(pa.table({"timestamp": ["garbage"]}), (("timestamp", "timestamptz"),))
        merged = pa.concat_tables([good, bad], promote_options="default")  # must not raise
        assert merged.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert merged.column("timestamp").to_pylist() == [datetime(2024, 1, 1, 12, 0, tzinfo=UTC), None]

    @patch("millpond.arrow_converter.metrics")
    def test_partial_failure_coerces_good_columns(self, mock_metrics):
        # One column has a bad value (nulled, error metric); the other coerces
        # cleanly (per-type success metric). Both end up the target type.
        table = pa.table({"timestamp": [self.WIRE], "created_at": ["garbage"]})
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"), ("created_at", "timestamptz")))
        assert out.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")
        assert out.schema.field("created_at").type == pa.timestamp("us", tz="UTC")
        assert out.column("created_at").to_pylist() == [None]
        mock_metrics.columns_coerced_total.labels.assert_called_once_with(target_type="timestamptz")
        mock_metrics.errors_total.labels(type="column_coercion").inc.assert_called_once()

    def test_roundtrips_through_convert(self):
        # The realistic path: JSON → convert() (infers VARCHAR) → coerce.
        messages = [orjson.dumps({"timestamp": self.WIRE, "event": "$pageview"})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("timestamp").type == pa.string()
        out = coerce_typed_columns(table, (("timestamp", "timestamptz"),))
        assert out.schema.field("timestamp").type == pa.timestamp("us", tz="UTC")


class TestCoerceUuidColumns:
    """`uuid:uuid` pins — the events topic carries `uuid`/`person_id` as UUID
    strings against hoglake `uuid` / DuckLake `UUID` columns."""

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    # `pa.uuid()` scalars come back as `uuid.UUID`; the 16 big-endian bytes
    # are what the extension actually stores.
    VALUE = uuid_mod.UUID(CANONICAL)
    BYTES = VALUE.bytes

    def test_canonical_string_coerces_to_uuid(self):
        table = pa.table({"uuid": [self.CANONICAL], "event": ["$pageview"]})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [self.VALUE]
        assert out.column("uuid").combine_chunks().storage.to_pylist() == [self.BYTES]
        # Untargeted columns are untouched.
        assert out.schema.field("event").type == pa.string()

    def test_unhyphenated_and_alternate_forms_parse(self):
        # The full accepted grammar, which is ENUMERATED rather than
        # delegated: canonical 8-4-4-4-12, 32 bare hex, either wrapped in
        # braces or prefixed `urn:uuid:`, hex in either case. Deliberately
        # NARROWER than `uuid.UUID`'s — see TestUuidBytesGrammar for the
        # stdlib-accepted shapes this refuses and why.
        forms = [
            self.CANONICAL.replace("-", ""),
            "{" + self.CANONICAL + "}",
            "urn:uuid:" + self.CANONICAL,
            self.CANONICAL.upper(),
        ]
        table = pa.table({"uuid": forms})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [self.VALUE] * len(forms)

    @patch("millpond.arrow_converter.metrics")
    def test_invalid_value_is_nulled_and_metricked(self, mock_metrics):
        table = pa.table({"uuid": ["not-a-uuid"]})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [None]
        # assert_called_once_WITH: a bare MagicMock memoizes labels() to one
        # child whatever kwargs it gets, so `labels(type=...).inc` asserts
        # nothing about the label.
        mock_metrics.errors_total.labels.assert_called_once_with(type="column_coercion")
        # A column with ANY failed value is not also counted as cleanly coerced.
        mock_metrics.columns_coerced_total.labels.assert_not_called()

    @patch("millpond.arrow_converter.metrics")
    def test_mixed_good_and_bad_keeps_good_values(self, mock_metrics):
        # Only the unconvertible values are nulled; the good ones survive.
        table = pa.table({"uuid": [self.CANONICAL, "zzzz", None]})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.column("uuid").to_pylist() == [self.VALUE, None, None]
        mock_metrics.errors_total.labels.assert_called_once_with(type="column_coercion")
        mock_metrics.columns_coerced_total.labels.assert_not_called()

    def test_nulls_pass_through(self):
        table = pa.table({"person_id": pa.array([self.CANONICAL, None], pa.string())})
        out = coerce_typed_columns(table, (("person_id", "uuid"),))
        assert out.column("person_id").to_pylist() == [self.VALUE, None]

    def test_all_null_string_column_coerces_to_uuid(self):
        # Mirrors project_id:bigint — an all-null batch infers VARCHAR and must
        # still land as the target type so the destination column is right from
        # the first flush.
        table = pa.table({"person_id": pa.array([None, None], pa.string())})
        out = coerce_typed_columns(table, (("person_id", "uuid"),))
        assert out.schema.field("person_id").type == pa.uuid()
        assert out.column("person_id").to_pylist() == [None, None]

    def test_already_uuid_typed_left_alone(self):
        storage = pa.array([self.BYTES], pa.binary(16))
        table = pa.table({"uuid": pa.ExtensionArray.from_storage(pa.uuid(), storage)})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out is table

    def test_fixed_size_binary_16_is_adopted_not_left_as_blob(self):
        # A plain binary(16) column already holds the right bytes but is BLOB
        # to DuckDB and carries no parquet UUID annotation, so it is wrapped
        # (zero-copy) rather than passed through.
        table = pa.table({"uuid": pa.array([self.BYTES], pa.binary(16))})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [self.VALUE]

    def test_failed_and_clean_batches_concat(self):
        # Type-consistency: a batch that fell back to the per-value path must
        # still concat with a fully-coerced one at flush.
        good = coerce_typed_columns(pa.table({"uuid": [self.CANONICAL]}), (("uuid", "uuid"),))
        bad = coerce_typed_columns(pa.table({"uuid": ["garbage"]}), (("uuid", "uuid"),))
        merged = pa.concat_tables([good, bad])
        assert merged.column("uuid").to_pylist() == [self.VALUE, None]

    @patch("millpond.arrow_converter.metrics")
    def test_roundtrips_through_convert(self, mock_metrics):
        other = "5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d"
        messages = [orjson.dumps({"uuid": self.CANONICAL, "person_id": other, "event": "$pageview"})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("uuid").type == pa.string()
        out = coerce_typed_columns(table, (("uuid", "uuid"), ("person_id", "uuid")))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.schema.field("person_id").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [self.VALUE]
        assert out.column("person_id").to_pylist() == [uuid_mod.UUID(other)]
        assert out.column("event").to_pylist() == ["$pageview"]
        assert mock_metrics.errors_total.labels.call_count == 0

    def test_parquet_write_carries_the_uuid_logical_annotation(self):
        # The coerced column writes as FIXED_LEN_BYTE_ARRAY(16) WITH
        # LogicalTypeAnnotation.uuidType() — that annotation is what the Trino
        # hoglake connector reads a uuid column back through. A plain binary(16)
        # column writes the same physical bytes with no annotation. This is the
        # column's own property; that it survives into the object the hoglake
        # sink uploads is pinned separately, in
        # tests/unit/test_hoglake.py::TestUuidColumnWireForm.
        out = coerce_typed_columns(pa.table({"uuid": [self.CANONICAL]}), (("uuid", "uuid"),))
        buf = io.BytesIO()
        pq.write_table(out, buf)
        buf.seek(0)
        pf = pq.ParquetFile(buf)
        column = pf.schema.column(0)
        assert column.physical_type == "FIXED_LEN_BYTE_ARRAY"
        assert column.length == 16
        assert str(column.logical_type) == "UUID"


class TestUuidBytesGrammar:
    """`uuid_bytes` on its own — the accepted shapes are enumerated, not
    delegated to `uuid.UUID`, whose grammar remaps things that are not UUIDs."""

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    BYTES = uuid_mod.UUID(CANONICAL).bytes

    @pytest.mark.parametrize(
        "text",
        [
            CANONICAL,
            CANONICAL.upper(),
            CANONICAL.replace("-", ""),
            CANONICAL.replace("-", "").upper(),
            "{" + CANONICAL + "}",
            "urn:uuid:" + CANONICAL,
        ],
    )
    def test_accepted_shapes(self, text):
        assert uuid_bytes(text) == self.BYTES

    @pytest.mark.parametrize(
        ("text", "why"),
        [
            # `uuid.UUID` strips `urn:`/`uuid:` from ANYWHERE and removes EVERY
            # hyphen, so all three of these parse there and silently become a
            # UUID the producer never sent.
            ("0123-4567-89ab-cdef-0123-4567-89ab-cdef", "hyphens outside the canonical offsets"),
            ("urn:0123456789abcdef0123456789abcdef", "bare urn: prefix"),
            ("0123456789abcdefuuid:0123456789abcdef", "uuid: buried mid-value"),
            # Everything `int(text, 16)` accepts and a UUID does not. All
            # three are 32 characters, so the stdlib's length check passes and
            # it produces a different, plausible-looking UUID (verified on
            # CPython 3.13: "0x0123456789abcdef0123456789abcd" ->
            # 00012345-6789-abcd-ef01-23456789abcd).
            ("0x0123456789abcdef0123456789abcd", "radix prefix"),
            ("1_3456789abcdef0123456789abcdef0", "PEP 515 underscore"),
            ("+0123456789abcdef0123456789abcde", "leading sign"),
            # QE c2: the length guard is the ONLY thing standing between a
            # whitespace-bearing 32-character string and a silent 15-byte
            # decode, because `bytes.fromhex` skips ASCII whitespace.
            ("0123456789abcdef  0123456789abcd", "32 chars with two internal spaces"),
            ("0123456789abcdef0123456789abcd", "30 hex digits"),
            ("0123456789abcdef0123456789abcdeff", "33 hex digits"),
            ("URN:UUID:" + CANONICAL, "urn prefix is case-sensitive"),
            ("", "empty"),
            ("not-a-uuid", "not hex at all"),
            ("018f3c7e6b2a-7c3d-9e4f-5a6b7c8d9e0f", "canonical length, wrong hyphen offsets"),
            # One case per canonical hyphen offset, each 36 characters long and
            # each tripping ONLY that offset's check — so deleting any single
            # clause of the four fails exactly one of these.
            ("012345678-9ab-cdef-0123-456789abcdef", "offset 8 only"),
            ("0-123456-789abcdef-0123-456789abcdef", "offset 13 only"),
            ("0-123456-789a-bcdef0123-456789abcdef", "offset 18 only"),
            ("01234567-89ab-cdef-0123456789-abcdef", "offset 23 only"),
        ],
    )
    def test_refused_shapes(self, text, why):
        with pytest.raises(ValueError):
            uuid_bytes(text)

    @pytest.mark.parametrize("value", [None, 5, b"0123456789abcdef", 1.5, ["x"]])
    def test_non_strings_refused(self, value):
        with pytest.raises(ValueError):
            uuid_bytes(value)

    def test_rfc_variant_and_version_bits_are_not_validated(self):
        # ClickHouse UUID is an opaque 128-bit value and PostHog writes v4 and
        # v7 both; rejecting an "invalid" variant would drop real data.
        assert uuid_bytes("00000000-0000-0000-0000-000000000000") == b"\x00" * 16
        assert uuid_bytes("ffffffff-ffff-ffff-ffff-ffffffffffff") == b"\xff" * 16

    def test_uuid_text_is_the_inverse(self):
        assert uuid_text(self.BYTES) == self.CANONICAL
        assert uuid_bytes(uuid_text(self.BYTES)) == self.BYTES

    @pytest.mark.parametrize("raw", [b"short", b"", "text", None, b"0" * 17])
    def test_uuid_text_refuses_non_16_byte_input(self, raw):
        with pytest.raises(ValueError):
            uuid_text(raw)


class TestToUuidDirectly:
    """`_to_uuid` itself. The tests above go through `coerce_typed_columns`,
    which rebuilds the table from the declared field type — so a coercer that
    returned bare `binary(16)` would still produce a `pa.uuid()` column there
    and the bug would only surface at the sink."""

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    BYTES = uuid_mod.UUID(CANONICAL).bytes

    def test_returns_the_extension_type_not_the_storage(self):
        out = _to_uuid(pa.array([self.CANONICAL], pa.string()))
        assert out.type == pa.uuid()
        assert out.storage.type == pa.binary(16)
        assert out.storage.to_pylist() == [self.BYTES]

    def test_already_extension_typed_is_returned_as_is(self):
        arr = pa.ExtensionArray.from_storage(pa.uuid(), pa.array([self.BYTES], pa.binary(16)))
        assert _to_uuid(arr) is arr

    def test_chunked_input_is_accepted(self):
        chunked = pa.chunked_array([pa.array([self.CANONICAL], pa.string()), pa.array([None], pa.string())])
        out = _to_uuid(chunked)
        assert out.type == pa.uuid()
        assert out.storage.to_pylist() == [self.BYTES, None]

    def test_large_string_source_parses(self):
        out = _to_uuid(pa.array([self.CANONICAL], pa.large_string()))
        assert out.type == pa.uuid()
        assert out.storage.to_pylist() == [self.BYTES]

    def test_variable_width_binary_of_16_bytes_is_adopted(self):
        out = _to_uuid(pa.array([self.BYTES], pa.binary()))
        assert out.type == pa.uuid()
        assert out.storage.to_pylist() == [self.BYTES]

    @pytest.mark.parametrize(
        "array",
        [
            pa.array([1, 2], pa.int64()),
            pa.array([1.5], pa.float64()),
            pa.array([True], pa.bool_()),
            pa.array([b"01234567"], pa.binary(8)),
            pa.array([b"short"], pa.binary()),
        ],
    )
    def test_unusable_source_types_raise_arrow_invalid(self, array):
        # ArrowInvalid specifically: that is what `_coerce_or_null` catches to
        # run its per-value null-out pass.
        with pytest.raises(pa.ArrowInvalid):
            _to_uuid(array)


class TestCoerceUuidFromNonStringColumns:
    """Whole-table behaviour for source columns that are not UUID text."""

    CANONICAL = "018f3c7e-6b2a-7c3d-9e4f-5a6b7c8d9e0f"
    BYTES = uuid_mod.UUID(CANONICAL).bytes

    @patch("millpond.arrow_converter.metrics")
    def test_int64_column_nulls_every_row(self, mock_metrics):
        table = pa.table({"uuid": pa.array([1, 2], pa.int64())})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [None, None]
        mock_metrics.errors_total.labels.assert_called_once_with(type="column_coercion")

    @patch("millpond.arrow_converter.metrics")
    def test_binary_8_column_nulls_every_row(self, mock_metrics):
        table = pa.table({"uuid": pa.array([b"01234567"], pa.binary(8))})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [None]
        mock_metrics.errors_total.labels.assert_called_once_with(type="column_coercion")

    @patch("millpond.arrow_converter.metrics")
    def test_large_string_column_parses(self, mock_metrics):
        table = pa.table({"uuid": pa.array([self.CANONICAL], pa.large_string())})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.schema.field("uuid").type == pa.uuid()
        assert out.column("uuid").to_pylist() == [uuid_mod.UUID(self.CANONICAL)]
        mock_metrics.columns_coerced_total.labels.assert_called_once_with(target_type="uuid")
        mock_metrics.errors_total.labels.assert_not_called()

    @patch("millpond.arrow_converter.metrics")
    def test_variable_binary_mixed_widths_nulls_only_the_wrong_ones(self, mock_metrics):
        table = pa.table({"uuid": pa.array([self.BYTES, b"short"], pa.binary())})
        out = coerce_typed_columns(table, (("uuid", "uuid"),))
        assert out.column("uuid").to_pylist() == [uuid_mod.UUID(self.CANONICAL), None]
        mock_metrics.errors_total.labels.assert_called_once_with(type="column_coercion")
