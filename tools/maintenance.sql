-- DuckLake catalog maintenance recipes.
--
-- Loaded at session start by `tools/maintenance.py` and by the `shell` recipe
-- in `tools/justfile`, so every macro defined here is callable from an
-- interactive `just shell` and from any maintenance subcommand.
--
-- This file is rendered with Python str.format() at load time: the
-- placeholder `{{schema}}` (the DuckLake metadata schema, derived from
-- `ATTACH_NAME`) is substituted before the SQL is executed. Any literal
-- `{{` / `}}` you add must be doubled (`{{{{` / `}}}}`) to survive that
-- pass. We deliberately do NOT template the data path: a literal
-- `glob('s3://...')` inside a CREATE MACRO body is evaluated eagerly by
-- DuckDB 1.4, which would S3-LIST the lake on every connect(). Macros
-- that need a path take it as a parameter instead.
--
-- Conventions
-- -----------
-- * Schema name. DuckLake stores its catalog tables in
--   `__ducklake_metadata_<attach_name>`. Use the `{{schema}}` placeholder
--   so the file stays in sync with maintenance.py's `ATTACH_NAME` constant.
--
-- * No `LEFT ANTI JOIN`. DuckDB 1.4 does not support that syntax. Use
--   `LEFT JOIN ... WHERE rhs IS NULL` or `NOT EXISTS (...)` instead.
--
-- * No Postgres `ctid` from duckdb-side SQL. The duckdb postgres extension
--   does not expose Postgres system columns to duckdb-side queries. Anything
--   that touches `ctid` must run via `postgres_execute` / `postgres_query`
--   against the `pg (TYPE postgres)` ATTACH — see `dedup_deletions` in
--   `maintenance.py` for the working pattern.
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
  FROM {schema}.ducklake_files_scheduled_for_deletion
);

-- Catalog rows in ducklake_files_scheduled_for_deletion whose S3 key no
-- longer exists. These rows poison the next `cleanup-all` because the S3
-- DELETE returns NoSuchKey and the whole transaction rolls back (DuckLake
-- bug c1). Each invocation does an S3 LIST under `data_path` — cheap on
-- small lakes, expensive on large ones; cache via a TEMP TABLE if you call
-- it more than once in the same session.
--
-- Pass the lake's bucket-relative root as `data_path` (e.g.
-- `'s3://bucket/lake/data'`); maintenance.py's `find-orphans` subcommand
-- supplies it from `DUCKLAKE_DATA_PATH` for you.
--
-- Path-matching tolerates both storage forms (per quirk r1):
--   * absolute s3:// URI: matches `s.path = l.file` directly
--   * bucket-relative key: matches `l.file = data_path || '/' || s.path`
CREATE OR REPLACE TEMP MACRO find_catalog_orphans(data_path) AS TABLE (
  WITH live AS (
    SELECT file FROM glob(data_path || '/**/*.parquet')
  )
  SELECT s.data_file_id, s.path
  FROM {schema}.ducklake_files_scheduled_for_deletion s
  LEFT JOIN live l
    ON  s.path = l.file
     OR l.file = data_path || '/' || s.path
  WHERE l.file IS NULL
);
