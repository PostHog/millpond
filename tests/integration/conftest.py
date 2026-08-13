"""Shared integration fixtures.

The `conn` fixtures inside individual test modules attach a plain in-memory
DuckDB as `lake`, which never writes Parquet. That is fine for SQL-shape
assertions but structurally cannot exercise the physical write — VARIANT
shredding, data inlining, file layout — so bugs living there (the 2026-08-12
dual-write incident) pass a green suite. Use these fixtures for anything that
depends on what actually lands on disk.
"""

from __future__ import annotations

import duckdb
import pytest


def _attach_ducklake(tmp_path, *, inline_rows: int | None):
    """A real local DuckLake catalog. inline_rows=0 forces Parquet for every write."""
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL ducklake; LOAD ducklake;")
    except Exception:
        pytest.skip("ducklake extension unavailable (offline?)")
    opts = f", DATA_INLINING_ROW_LIMIT {inline_rows}" if inline_rows is not None else ""
    conn.execute(f"ATTACH 'ducklake:{tmp_path}/meta.ducklake' AS lake (DATA_PATH '{tmp_path}/data'{opts})")
    return conn


@pytest.fixture()
def ducklake_conn(tmp_path):
    """Real DuckLake with inlining disabled: every write becomes a shredded Parquet file.

    This is the path that broke production — VARIANT shredding converts to
    typed columns and rejects values the cast accepted.
    """
    conn = _attach_ducklake(tmp_path, inline_rows=0)
    yield conn
    conn.close()


@pytest.fixture()
def ducklake_conn_inlining(tmp_path):
    """Real DuckLake with default data inlining: small writes land in the catalog.

    The mirror hazard of `ducklake_conn`: an unshreddable value here does NOT
    fail the INSERT, it commits into catalog state and detonates later, when
    `ducklake_flush_inlined_data` materializes it. No write-time retry can
    reach that, which is why the guard has to run per row.
    """
    conn = _attach_ducklake(tmp_path, inline_rows=None)
    yield conn
    conn.close()
