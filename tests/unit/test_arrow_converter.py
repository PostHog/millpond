import orjson
import pyarrow as pa

from millpond.arrow_converter import _drop_null_typed_columns, convert


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
        table = pa.table(
            {"a": pa.array([None, None], pa.null()), "b": ["x", "y"]}
        )
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
