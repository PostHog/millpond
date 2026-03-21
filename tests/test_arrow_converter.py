import orjson
import pyarrow as pa

from millpond.arrow_converter import convert


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

    def test_numeric_cast_to_double(self):
        messages = [orjson.dumps({"x": 42, "y": 3.14})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("x").type == pa.float64()
        assert table.schema.field("y").type == pa.float64()

    def test_integer_only_still_double(self):
        messages = [orjson.dumps({"count": 100})]
        table = convert(messages)
        assert table is not None
        assert table.schema.field("count").type == pa.float64()

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

    def test_nested_objects_preserved(self):
        messages = [orjson.dumps({"meta": {"key": "value"}, "tags": [1, 2, 3]})]
        table = convert(messages)
        assert table is not None
        assert len(table) == 1

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
