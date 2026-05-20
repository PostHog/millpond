"""Tests for the Sink Protocol and `make_sink` factory.

`make_sink` is the only piece of glue between cfg and the backend modules.
If somebody adds a third destination string to `Config.destination` without
extending the factory dispatch, this is where the gap surfaces. Likewise
if either Sink class drops a required method, the conformance assertions
here catch it before main.py does at runtime.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from millpond import sink as sink_mod


def _cfg(destination: str) -> MagicMock:
    cfg = MagicMock()
    cfg.destination = destination
    # Provide the minimal field set each sink reads in __init__.
    cfg.ducklake_table = "events"
    cfg.partition_by = None
    cfg.iceberg_catalog_uri = "http://catalog:8181"
    cfg.iceberg_warehouse = "s3://warehouse/"
    cfg.iceberg_namespace = "millpond"
    cfg.iceberg_table = "events"
    cfg.iceberg_table_location = None
    cfg.iceberg_catalog_token = None
    cfg.s3_access_key_id = "akid"
    cfg.s3_secret_access_key = "secret"
    cfg.s3_region = "us-east-1"
    cfg.s3_endpoint = None
    return cfg


class TestMakeSinkDispatch:
    def test_returns_ducklake_sink_for_ducklake(self):
        # Stub out the heavy `connect()` so we don't need a real DuckDB+Postgres.
        with patch("millpond.ducklake.connect") as mock_connect, patch(
            "millpond.ducklake.schema.SchemaManager"
        ):
            mock_connect.return_value = MagicMock()
            from millpond.ducklake import DuckLakeSink

            sink = sink_mod.make_sink(_cfg("ducklake"))
            assert isinstance(sink, DuckLakeSink)

    def test_returns_iceberg_sink_for_iceberg(self):
        with patch("millpond.iceberg.connect") as mock_connect, patch(
            "millpond.iceberg.SchemaManager"
        ):
            mock_connect.return_value = MagicMock()
            from millpond.iceberg import IcebergSink

            sink = sink_mod.make_sink(_cfg("iceberg"))
            assert isinstance(sink, IcebergSink)

    def test_unknown_destination_raises(self):
        with pytest.raises(ValueError, match="Unknown destination"):
            sink_mod.make_sink(_cfg("snowflake"))


class TestSinkProtocolConformance:
    """Each Sink class must expose `write`, `reset_caches`, `close` as callables.

    A duck-typed Protocol doesn't enforce this at type-check time; this test
    is the runtime backstop against an accidental rename.
    """

    @pytest.mark.parametrize("class_path", ["millpond.ducklake.DuckLakeSink", "millpond.iceberg.IcebergSink"])
    def test_required_methods_exist(self, class_path):
        module_name, class_name = class_path.rsplit(".", 1)
        module = __import__(module_name, fromlist=[class_name])
        cls = getattr(module, class_name)
        for method in ("write", "reset_caches", "close"):
            assert hasattr(cls, method), f"{class_path} missing {method!r}"
            assert callable(getattr(cls, method)), f"{class_path}.{method} is not callable"


class TestLazyImport:
    """make_sink must not import the unused backend.

    pyiceberg pulls cryptography/aiohttp/etc. transitively; importing it on
    a DuckLake-only deployment is ~150ms of cold-start cost for nothing.
    """

    def test_ducklake_dispatch_does_not_import_iceberg_module(self):
        # We can't easily un-import a module that's already in sys.modules
        # from earlier tests in this run. Instead, prove the function body
        # uses a lazy `from ... import` by source inspection — cheap and
        # robust to other tests having pre-loaded iceberg.
        import inspect
        import re

        src = inspect.getsource(sink_mod.make_sink)
        # Imports must be inside the function (line-anchored, indented),
        # not at module top, otherwise the lazy guarantee is gone.
        assert re.search(r"^[ \t]+from millpond\.iceberg import", src, re.M), (
            "make_sink must import iceberg lazily (inside the function), not at module top"
        )
        assert re.search(r"^[ \t]+from millpond\.ducklake import", src, re.M), (
            "make_sink must import ducklake lazily (inside the function), not at module top"
        )
