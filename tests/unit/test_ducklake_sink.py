"""Tests for the DuckLakeSink wrapper class.

The class wrapper is thin — constructor wiring, three delegate methods.
The module-level helper functions are exercised separately by
`test_ducklake.py`. These tests cover the class behaviour that those
don't touch:

  * `__init__` validates required cfg fields and constructs an internal
    `SchemaManager` against the right table.
  * `reset_caches` clears the instance cache AND invalidates the schema
    manager — both delegates, not just one.
  * `close` releases the connection.
  * The `cfg.X is None` guards raise `RuntimeError` (not bare `assert`,
    which `python -O` strips).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from millpond.ducklake import DuckLakeSink


def _ducklake_cfg(**overrides) -> MagicMock:
    cfg = MagicMock()
    cfg.ducklake_schema = "main"
    cfg.ducklake_table = "events"
    cfg.ducklake_connection = ":memory:"
    cfg.ducklake_data_path = "s3://bucket/data"
    cfg.rds_host = "localhost"
    cfg.rds_port = "5432"
    cfg.rds_database = "ducklake"
    cfg.rds_username = "ducklake"
    cfg.rds_password = "pass"
    cfg.partition_by = None
    cfg.variant_columns = None
    cfg.variant_key_prefix = None
    cfg.variant_keys = None
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


class TestDuckLakeSinkInit:
    def test_constructs_schema_manager_with_right_table(self):
        with (
            patch("millpond.ducklake.connect") as mock_connect,
            patch("millpond.ducklake.schema.SchemaManager") as mock_sm,
        ):
            mock_connect.return_value = MagicMock(name="conn")
            sink = DuckLakeSink(_ducklake_cfg())
            mock_sm.assert_called_once_with(mock_connect.return_value, "events", "main")
            assert sink._schema_name == "main"
            assert sink._table_name == "events"
            assert sink._partition_by is None
            assert sink._tables_ensured == set()

    @pytest.mark.parametrize(
        "missing_field",
        [
            "ducklake_schema",
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
        # "host=None" or similar deep in the stack.
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
        with (
            patch("millpond.ducklake.connect") as mock_connect,
            patch("millpond.ducklake.schema.SchemaManager") as mock_sm,
        ):
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
