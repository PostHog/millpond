"""Tests for the Sink Protocol and `make_sink` factory.

The seam was removed with the iceberg/icebox backends (tag
`final-iceberg`) and recovered for the hoglake destination. `make_sink`
is the only glue between cfg and the backend modules: if somebody adds a
destination string to `Config.destination` without extending the factory
dispatch, this is where the gap surfaces. Likewise if a Sink class drops
a required method, the conformance assertions catch it before main.py
does at runtime.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from millpond import sink as sink_mod


def _cfg(destination: str) -> MagicMock:
    cfg = MagicMock()
    cfg.destination = destination
    # Minimal DuckLake field set DuckLakeSink.__init__ guards on.
    cfg.ducklake_schema = "main"
    cfg.ducklake_table = "events"
    cfg.ducklake_connection = ":memory:"
    cfg.ducklake_data_path = "s3://bucket/data"
    cfg.rds_host = "localhost"
    cfg.rds_port = "5432"
    cfg.rds_database = "ducklake"
    cfg.rds_username = "ducklake"
    cfg.rds_password = "secret"
    cfg.partition_by = None
    cfg.variant_columns = None
    return cfg


class TestMakeSinkDispatch:
    def test_returns_ducklake_sink_for_ducklake(self):
        # Stub the heavy `connect()` so we don't need a real DuckDB+Postgres.
        with patch("millpond.ducklake.connect") as mock_connect, patch("millpond.ducklake.schema.SchemaManager"):
            mock_connect.return_value = MagicMock()
            from millpond.ducklake import DuckLakeSink

            sink = sink_mod.make_sink(_cfg("ducklake"))
            assert isinstance(sink, DuckLakeSink)

    def test_returns_hoglake_sink_for_hoglake(self):
        cfg = _cfg("hoglake")
        cfg.hoglake_url = "http://localhost:28080"
        cfg.hoglake_catalog = "millpond"
        cfg.hoglake_namespace = "analytics"
        cfg.hoglake_table = "events"
        cfg.hoglake_s3_access_key = "ak"
        cfg.hoglake_s3_secret_key = "sk"
        with patch("millpond.hoglake.HoglakeClient"):
            from millpond.hoglake import HoglakeSink

            sink = sink_mod.make_sink(cfg)
            assert isinstance(sink, HoglakeSink)

    def test_unknown_destination_raises(self):
        with pytest.raises(ValueError, match="Unknown destination"):
            sink_mod.make_sink(_cfg("snowflake"))


class TestSinkProtocolConformance:
    """Each Sink class must expose `write`, `reset_caches`, `close` as callables.

    A duck-typed Protocol doesn't enforce this at type-check time; this test
    is the runtime backstop against an accidental rename.
    """

    @pytest.mark.parametrize(
        "class_path",
        [
            "millpond.ducklake.DuckLakeSink",
            "millpond.hoglake.HoglakeSink",
        ],
    )
    def test_required_methods_exist(self, class_path):
        module_name, class_name = class_path.rsplit(".", 1)
        module = __import__(module_name, fromlist=[class_name])
        cls = getattr(module, class_name)
        for method in ("write", "reset_caches", "close"):
            assert hasattr(cls, method), f"{class_path} missing {method!r}"
            assert callable(getattr(cls, method)), f"{class_path}.{method} is not callable"


class TestCheckReservedCollision:
    def test_no_collision_passes(self):
        schema = pa.schema([("uuid", pa.string()), ("event", pa.string())])
        sink_mod.check_reserved_collision(schema, {"_inserted_at"}, "DuckLake")

    def test_collision_raises_with_backend_name(self):
        schema = pa.schema([("_inserted_at", pa.string()), ("event", pa.string())])
        with pytest.raises(ValueError, match=r"DuckLake-reserved metadata column names"):
            sink_mod.check_reserved_collision(schema, {"_inserted_at"}, "DuckLake")

    def test_collision_lists_all_colliding_columns_sorted(self):
        schema = pa.schema([("year", pa.int64()), ("_inserted_at", pa.string())])
        with pytest.raises(ValueError, match=r"\['_inserted_at', 'year'\]"):
            sink_mod.check_reserved_collision(schema, {"_inserted_at", "year", "month"}, "Hoglake")

    def test_backend_name_appears_in_message(self):
        schema = pa.schema([("_inserted_at", pa.string())])
        with pytest.raises(ValueError, match="Hoglake-reserved"):
            sink_mod.check_reserved_collision(schema, {"_inserted_at"}, "Hoglake")


class TestSafeIdentifier:
    """SAFE_IDENTIFIER lives on the seam; schema.py re-exports it so the
    existing importers (config.py, evolve) keep working."""

    def test_shared_instance_with_schema_module(self):
        from millpond import schema

        assert schema.SAFE_IDENTIFIER is sink_mod.SAFE_IDENTIFIER

    @pytest.mark.parametrize("name", ["event", "_inserted_at", "a1", "A_b_2"])
    def test_accepts_safe(self, name):
        assert sink_mod.SAFE_IDENTIFIER.match(name)

    @pytest.mark.parametrize("name", ["1abc", "a-b", 'a"b', "a b", "", "a.b"])
    def test_rejects_unsafe(self, name):
        assert not sink_mod.SAFE_IDENTIFIER.match(name)


class TestLazyImport:
    """make_sink must not import the unused backend at module import time.

    pyhoglake pulls httpx; ducklake pulls duckdb. A deployment running one
    destination shouldn't pay the other's import cost, and (more
    importantly) a broken optional backend module must not take down the
    other destination's pods at import time.
    """

    def test_backend_imports_are_lazy(self):
        import inspect
        import re

        src = inspect.getsource(sink_mod.make_sink)
        assert re.search(r"^[ \t]+from millpond\.ducklake import", src, re.M), (
            "make_sink must import ducklake lazily (inside the function), not at module top"
        )
        assert re.search(r"^[ \t]+from millpond\.hoglake import", src, re.M), (
            "make_sink must import hoglake lazily — httpx/pyhoglake shouldn't load for DuckLake-only deployments"
        )


class TestDuckLakeWriteDelegation:
    """DuckLakeSink.write must keep returning the written-record count —
    main.py feeds it to records_written_total."""

    def test_write_returns_int(self):
        with patch("millpond.ducklake.connect") as mock_connect, patch("millpond.ducklake.schema.SchemaManager"):
            mock_connect.return_value = MagicMock()
            from millpond.ducklake import DuckLakeSink

            s = DuckLakeSink(_cfg("ducklake"))
            with patch("millpond.ducklake.write", return_value=7) as mock_write:
                out = s.write(pa.table({"a": [1]}))
            assert out == 7
            assert mock_write.call_count == 1
