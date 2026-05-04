-- DuckLake catalog maintenance recipes.
--
-- Loaded at session start by `tools/maintenance.py` and by the `shell` recipe
-- in `tools/justfile`, so every macro defined here is callable from an
-- interactive `just shell` and from any maintenance subcommand.
--
-- Conventions
-- -----------
-- * Schema name. DuckLake stores its catalog tables in
--   `__ducklake_metadata_<attach_name>`. We attach as `lake`, so every macro
--   below references `__ducklake_metadata_lake.<table>` directly. If you
--   change the ATTACH alias in `maintenance.py` (`ATTACH_NAME`), update the
--   schema references here too.
--
-- * No `LEFT ANTI JOIN`. DuckDB 1.4 does not support that syntax. Use
--   `LEFT JOIN ... WHERE rhs IS NULL` or `NOT EXISTS (...)` instead.
--
-- * No Postgres `ctid` from duckdb-side SQL. The duckdb postgres extension
--   does not expose Postgres system columns to duckdb-side queries. Anything
--   that touches `ctid` must run via `postgres_execute` / `postgres_query`
--   against the `pg (TYPE postgres)` ATTACH — see `dedup_scheduled_deletions`
--   in `maintenance.py` for the working pattern.
--
-- * Advisory lock. Maintenance jobs that mutate the catalog acquire
--   `pg_try_advisory_lock(hashtext('millpond-ducklake-maintenance')::bigint)`
--   on the `pg` ATTACH for the duration of the session. The lock is held by
--   the `pg` connection, not the `lake` connection that DuckLake itself uses
--   internally, so it provides mutual exclusion between maintenance
--   invocations — not catalog-write atomicity against arbitrary writers.

-- Number of duplicate rows in the pending-deletion queue.
-- A non-zero value will self-poison the next `cleanup-all` per DuckLake bug c5.
CREATE OR REPLACE TEMP MACRO count_pending_dups() AS (
  SELECT COUNT(*) - COUNT(DISTINCT path)
  FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion
);
