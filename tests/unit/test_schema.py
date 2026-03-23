import pyarrow as pa

from millpond.schema import _arrow_type_to_duckdb, _normalize_duckdb_type


class TestNormalizeDuckdbType:
    def test_timestamp_with_time_zone(self):
        assert _normalize_duckdb_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMPTZ"

    def test_passthrough(self):
        for t in ["BIGINT", "DOUBLE", "VARCHAR", "BOOLEAN", "FLOAT", "INTEGER", "TIMESTAMPTZ", "JSON"]:
            assert _normalize_duckdb_type(t) == t


class TestArrowTypeToDuckdb:
    def test_integer_types(self):
        assert _arrow_type_to_duckdb(pa.int8()) == "TINYINT"
        assert _arrow_type_to_duckdb(pa.int16()) == "SMALLINT"
        assert _arrow_type_to_duckdb(pa.int32()) == "INTEGER"
        assert _arrow_type_to_duckdb(pa.int64()) == "BIGINT"

    def test_unsigned_types(self):
        assert _arrow_type_to_duckdb(pa.uint8()) == "UTINYINT"
        assert _arrow_type_to_duckdb(pa.uint16()) == "USMALLINT"
        assert _arrow_type_to_duckdb(pa.uint32()) == "UINTEGER"
        assert _arrow_type_to_duckdb(pa.uint64()) == "UBIGINT"

    def test_float_types(self):
        assert _arrow_type_to_duckdb(pa.float32()) == "FLOAT"
        assert _arrow_type_to_duckdb(pa.float64()) == "DOUBLE"

    def test_string_types(self):
        assert _arrow_type_to_duckdb(pa.string()) == "VARCHAR"
        assert _arrow_type_to_duckdb(pa.large_string()) == "VARCHAR"
        assert _arrow_type_to_duckdb(pa.utf8()) == "VARCHAR"

    def test_bool(self):
        assert _arrow_type_to_duckdb(pa.bool_()) == "BOOLEAN"

    def test_timestamp(self):
        assert _arrow_type_to_duckdb(pa.timestamp("us")) == "TIMESTAMP"
        assert _arrow_type_to_duckdb(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_struct_to_json(self):
        struct_type = pa.struct([("x", pa.int32()), ("y", pa.string())])
        assert _arrow_type_to_duckdb(struct_type) == "JSON"

    def test_list_to_json(self):
        assert _arrow_type_to_duckdb(pa.list_(pa.int32())) == "JSON"

    def test_unknown_falls_back_to_varchar(self):
        assert _arrow_type_to_duckdb(pa.duration("us")) == "VARCHAR"
