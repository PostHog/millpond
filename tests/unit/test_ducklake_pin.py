"""DuckLake extension version-pin canary.

`millpond/ducklake.py` exercises specific DuckDB+DuckLake behaviour:
the multi-pod CREATE/ALTER race patterns, `INSERT INTO ... BY NAME
SELECT * FROM _arrow_batch`, `ALTER TABLE SET PARTITIONED BY`,
`ADD COLUMN IF NOT EXISTS` idempotency, and the `ALTER COLUMN SET DATA TYPE`
widening contract enforced in `schema.SchemaManager`. The DuckLake 1.x
line is pre-stable; even patch releases can move observable behaviour
(notably catalog metadata schema and the widening rules).

DuckDB exposes the DuckLake extension only as a single git-SHA build
on the core extension repo — `INSTALL ducklake VERSION '...'` returns
404 because the repo doesn't carry versioned ducklake builds. So this
canary asserts:

  1. The DuckDB Python wheel is pinned to the expected version, AND
  2. The DuckLake extension loaded under that DuckDB reports the build
     SHA we last validated against.

If either fails, treat that as a signal to revisit `millpond/ducklake.py`
and `millpond/schema.py` against the new ducklake release — either bump
the constants below after revalidating, or hold the pin until the code
is updated.

Run cost: ~1s on first invocation (the underlying `INSTALL ducklake`
network fetch); free on subsequent runs (extension is cached in the
runner's `~/.duckdb/extensions/`).
"""

from __future__ import annotations

from importlib.metadata import version

import duckdb

# Update both constants together when revalidating against a new release.
# The DuckDB version is exact; the DuckLake SHA is whatever the core
# extension repo currently serves for that DuckDB version's platform.
_VERIFIED_DUCKDB_VERSION = "1.5.2"
_VERIFIED_DUCKLAKE_SHA = "415a9ebd"


def test_duckdb_version_is_pinned():
    installed = version("duckdb")
    assert installed == _VERIFIED_DUCKDB_VERSION, (
        f"DuckDB {installed} installed, but millpond/ducklake.py was last "
        f"verified against {_VERIFIED_DUCKDB_VERSION}. Review the DuckLake "
        f"behaviour assumptions in ducklake.py and schema.py before bumping "
        f"this constant."
    )


def test_ducklake_extension_sha_is_pinned():
    # Use a fresh in-memory connection so the test doesn't depend on any
    # session-level state. INSTALL is idempotent and cached locally.
    conn = duckdb.connect()
    conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    row = conn.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name = 'ducklake'").fetchone()
    assert row is not None, "DuckLake extension did not appear in duckdb_extensions()"
    observed_sha = row[0]
    assert observed_sha == _VERIFIED_DUCKLAKE_SHA, (
        f"DuckLake extension SHA {observed_sha!r} from DuckDB {duckdb.__version__}, "
        f"but millpond was last validated against {_VERIFIED_DUCKLAKE_SHA!r}. "
        f"The DuckLake 1.x line is pre-stable and even SHA-level drift can "
        f"change observable behaviour — revalidate ducklake.py + schema.py "
        f"against the new release before bumping this constant."
    )
