-- DuckLake catalog maintenance recipes.
--
-- Loaded at session start by `tools/ducklake_maintenance.py` (via `conn.execute`) and
-- by the `shell` recipe in `tools/justfile` (via the duckdb CLI's `.read`
-- meta-command). Both paths execute the file verbatim, so the file itself
-- must be valid SQL — no templating, no placeholders.
--
-- Conventions
-- -----------
-- * Schema name. DuckLake stores its catalog tables in
--   `__ducklake_metadata_<attach_name>`. We attach as `lake` everywhere
--   (the `ATTACH_NAME` constant in `ducklake_maintenance.py`), so this file
--   references `__ducklake_metadata_lake.<table>` directly. If you ever
--   change the ATTACH alias, the references here must change with it.
--
-- * No `LEFT ANTI JOIN`. DuckDB 1.4 does not support that syntax. Use
--   `LEFT JOIN ... WHERE rhs IS NULL` or `NOT EXISTS (...)` instead.
--
-- * No Postgres `ctid` from duckdb-side SQL. The duckdb postgres extension
--   does not expose Postgres system columns to duckdb-side queries. Anything
--   that touches `ctid` must run via `postgres_execute` / `postgres_query`
--   against the `pg (TYPE postgres)` ATTACH — see `dedup_deletions` in
--   `ducklake_maintenance.py` for the working pattern.
--
-- * No literal `glob('s3://...')` inside `CREATE MACRO` bodies. DuckDB 1.4
--   evaluates a literal glob eagerly at macro creation time, which would
--   S3-LIST the lake on every connect (even for subcommands that don't
--   care). Macros that need an S3 path take it as a parameter — see
--   `find_catalog_orphans` below.
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

-- Catalog rows in ducklake_files_scheduled_for_deletion whose S3 key no
-- longer exists. These rows poison the next `cleanup-all` because the S3
-- DELETE returns NoSuchKey and the whole transaction rolls back (DuckLake
-- bug c1). Each invocation does an S3 LIST under `data_path` — cheap on
-- small lakes, expensive on large ones; cache via a TEMP TABLE if you call
-- it more than once in the same session.
--
-- Pass the lake's bucket-relative root as `data_path` (e.g.
-- `'s3://bucket/lake/data'` or `'s3://bucket/lake/data/'`);
-- ducklake_maintenance.py's `find-orphans` subcommand supplies it from
-- `DUCKLAKE_DATA_PATH` for you. We `rtrim(data_path, '/')` everywhere it
-- appears so an operator-configured trailing slash doesn't produce
-- `.../data//file.parquet` from the relative-form join (which would
-- never match an absolute live row and would misclassify a still-live
-- file as an orphan).
--
-- Path-matching tolerates both storage forms (per quirk r1):
--   * absolute s3:// URI: matches `s.path = l.file` directly
--   * bucket-relative key: matches `l.file = rtrim(data_path,'/')||'/'||s.path`
CREATE OR REPLACE TEMP MACRO find_catalog_orphans(data_path) AS TABLE (
  WITH live AS (
    SELECT file FROM glob(rtrim(data_path, '/') || '/**/*.parquet')
  )
  SELECT s.data_file_id, s.path
  FROM __ducklake_metadata_lake.ducklake_files_scheduled_for_deletion s
  LEFT JOIN live l
    ON  s.path = l.file
     OR l.file = rtrim(data_path, '/') || '/' || s.path
  WHERE l.file IS NULL
);
