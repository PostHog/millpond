"""Tests for the DuckLakeSink and IcebergSink wrapper classes.

The class wrappers are thin — constructor wiring, three delegate methods.
The module-level helper functions are exercised separately by
`test_ducklake.py` and `test_iceberg.py`. These tests cover the class
behaviour that those don't touch:

  * `__init__` validates required cfg fields and constructs an internal
    `SchemaManager` against the right namespace/table.
  * `reset_caches` clears the instance cache AND invalidates the schema
    manager — both delegates, not just one.
  * `close` releases the connection (DuckLake) or no-ops (Iceberg).
  * The `cfg.X is None` guards raise `RuntimeError` (not bare `assert`,
    which `python -O` strips).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from millpond.ducklake import DuckLakeSink
from millpond.iceberg import IcebergSink


def _ducklake_cfg(**overrides) -> MagicMock:
    cfg = MagicMock()
    cfg.destination = "ducklake"
    cfg.ducklake_table = "events"
    cfg.ducklake_connection = ":memory:"
    cfg.ducklake_data_path = "s3://bucket/data"
    cfg.rds_host = "localhost"
    cfg.rds_port = "5432"
    cfg.rds_database = "ducklake"
    cfg.rds_username = "ducklake"
    cfg.rds_password = "pass"
    cfg.partition_by = None
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def _iceberg_cfg(**overrides) -> MagicMock:
    cfg = MagicMock()
    cfg.destination = "iceberg"
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
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


class TestDuckLakeSinkInit:
    def test_constructs_schema_manager_with_right_table(self):
        with patch("millpond.ducklake.connect") as mock_connect, patch(
            "millpond.ducklake.schema.SchemaManager"
        ) as mock_sm:
            mock_connect.return_value = MagicMock(name="conn")
            sink = DuckLakeSink(_ducklake_cfg())
            mock_sm.assert_called_once_with(mock_connect.return_value, "events")
            assert sink._table_name == "events"
            assert sink._partition_by is None
            assert sink._tables_ensured == set()

    @pytest.mark.parametrize(
        "missing_field",
        [
            "ducklake_table",
            "ducklake_connection",
            "ducklake_data_path",
            "rds_host",
            "rds_port",
            "rds_database",
            "rds_username",
            "rds_password",
        ],
    )
    def test_missing_required_field_raises_runtimeerror(self, missing_field):
        # Each field connect() touches must be guarded. Without the guard,
        # a missing rds_* field would surface as a cryptic libpq
        # "host=None" or similar deep in the stack. Symmetric with the
        # IcebergSink test below.
        cfg = _ducklake_cfg(**{missing_field: None})
        with pytest.raises(RuntimeError, match=missing_field):
            DuckLakeSink(cfg)

    def test_runtimeerror_survives_python_optimize(self):
        # `assert` would silently pass under `python -O`. Confirm the guard
        # is an explicit raise by checking the class isn't using `assert`.
        import inspect

        src = inspect.getsource(DuckLakeSink.__init__)
        assert "raise RuntimeError" in src
        assert "assert cfg.ducklake_table" not in src


class TestDuckLakeSinkResetCaches:
    def test_clears_tables_ensured_and_invalidates_schema_mgr(self):
        with patch("millpond.ducklake.connect") as mock_connect, patch(
            "millpond.ducklake.schema.SchemaManager"
        ) as mock_sm:
            mock_connect.return_value = MagicMock(name="conn")
            sink = DuckLakeSink(_ducklake_cfg())
            sink._tables_ensured.add("events")
            sink.reset_caches()
            assert sink._tables_ensured == set()
            mock_sm.return_value.invalidate.assert_called_once()


class TestDuckLakeSinkClose:
    def test_closes_connection(self):
        with patch("millpond.ducklake.connect") as mock_connect, patch("millpond.ducklake.schema.SchemaManager"):
            mock_conn = MagicMock(name="conn")
            mock_connect.return_value = mock_conn
            sink = DuckLakeSink(_ducklake_cfg())
            sink.close()
            mock_conn.close.assert_called_once()


class TestIcebergSinkInit:
    def test_constructs_schema_manager_with_namespace_and_table(self):
        with patch("millpond.iceberg.connect") as mock_connect, patch(
            "millpond.iceberg.SchemaManager"
        ) as mock_sm:
            mock_connect.return_value = MagicMock(name="catalog")
            sink = IcebergSink(_iceberg_cfg())
            mock_sm.assert_called_once_with(mock_connect.return_value, "millpond", "events")
            assert sink._namespace == "millpond"
            assert sink._table_name == "events"
            assert sink._tables_ensured == {}

    def test_passes_optional_token_and_endpoint(self):
        with patch("millpond.iceberg.connect") as mock_connect, patch("millpond.iceberg.SchemaManager"):
            mock_connect.return_value = MagicMock()
            IcebergSink(_iceberg_cfg(iceberg_catalog_token="bearer-xyz", s3_endpoint="http://minio:9000"))
            kwargs = mock_connect.call_args.kwargs
            assert kwargs["catalog_token"] == "bearer-xyz"
            assert kwargs["s3_endpoint"] == "http://minio:9000"

    @pytest.mark.parametrize(
        "missing_field",
        [
            "iceberg_catalog_uri",
            "iceberg_warehouse",
            "iceberg_namespace",
            "iceberg_table",
            "s3_access_key_id",
            "s3_secret_access_key",
            "s3_region",
        ],
    )
    def test_missing_required_field_raises_runtimeerror(self, missing_field):
        cfg = _iceberg_cfg(**{missing_field: None})
        with pytest.raises(RuntimeError, match=missing_field):
            IcebergSink(cfg)

    def test_runtimeerror_survives_python_optimize(self):
        import inspect

        src = inspect.getsource(IcebergSink.__init__)
        assert "raise RuntimeError" in src
        assert "assert cfg.iceberg_catalog_uri" not in src


class TestIcebergSinkResetCaches:
    def test_clears_tables_ensured_and_invalidates_schema_mgr(self):
        with patch("millpond.iceberg.connect") as mock_connect, patch(
            "millpond.iceberg.SchemaManager"
        ) as mock_sm:
            mock_connect.return_value = MagicMock(name="catalog")
            sink = IcebergSink(_iceberg_cfg())
            sink._tables_ensured["millpond.events"] = MagicMock()
            sink.reset_caches()
            assert sink._tables_ensured == {}
            mock_sm.return_value.invalidate.assert_called_once()


class TestIcebergSinkClose:
    def test_close_is_a_noop(self):
        # PyIceberg REST catalog has no persistent connection. close()
        # should not raise even though there's nothing to release.
        with patch("millpond.iceberg.connect") as mock_connect, patch("millpond.iceberg.SchemaManager"):
            mock_connect.return_value = MagicMock(name="catalog")
            sink = IcebergSink(_iceberg_cfg())
            sink.close()  # must not raise
