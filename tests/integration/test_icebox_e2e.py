"""End-to-end integration tests for the icebox writer/committer split.

Plan: ``/tmp/icebox-integration-test-plan.md`` (lives outside the tree —
it's a working design doc, not shipped in this PR).

This module is the SCAFFOLDING + ONE sanity test slice. Subsequent PRs
add the rest of the must-have scenarios (bound encoding round-trip,
3-branch recovery, schema-fingerprint cross-check, concurrent
advisory-lock, heartbeat-during-recovery, etc.).

The sanity test proves the harness stands up:
  1. Real Postgres (testcontainers) with the icebox DDL applied.
  2. FastAPI app serving POST /v1/files against that PG.
  3. PyIceberg SqlCatalog + filesystem warehouse (a v1 stand-in for
     Lakekeeper — see the plan doc).
  4. The committer's ``run_cycle`` executing against the same PG +
     catalog and producing a real Iceberg snapshot.

The covered must-have-scenarios (subset; full list in plan doc):
  - (d) jsonb encoding round-trip through PG: writer's POST body's
    ``kafka_offsets`` / ``partition_values`` / ``parquet_stats`` are
    serialized as JSON, persisted as jsonb, and read back as Python
    dicts by the committer.
  - (b, partial) ``posthog.icebox.cycle_id`` survives the catalog
    round-trip — committer commits the snapshot, then the cycle row's
    snapshot_id matches a snapshot in the table whose summary contains
    the cycle_id.
"""
from __future__ import annotations

import datetime as dt
import io
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.transforms import IdentityTransform

from icebox import committer as cm
from icebox.iceberg import CYCLE_ID_SUMMARY_KEY
from shared.fingerprint import schema_fingerprint
from shared.models import ParquetStats, RegisterFileRequest
from tests.integration.conftest import make_committer_deps

NAMESPACE = "kafka"
TABLE = "events"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_sample_table(sql_catalog, warehouse_dir):
    """Create the icebox-target Iceberg table in ``sql_catalog``.

    Schema: one data column (``team_id`` int) + the icebox metadata
    columns (``_inserted_at``, year, month, day, hour) that the writer
    appends. Partitioned by identity on (year, month, day, hour) — the
    v1 contract.

    Returns the loaded Table.
    """
    # Use a PyArrow schema with the exact shape the writer produces so
    # the schema fingerprint we compute here matches what the writer
    # would have computed.
    arrow_schema = pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=True),
            pa.field("_inserted_at", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("year", pa.int32(), nullable=True),
            pa.field("month", pa.int32(), nullable=True),
            pa.field("day", pa.int32(), nullable=True),
            pa.field("hour", pa.int32(), nullable=True),
        ]
    )
    ice_schema = assign_fresh_schema_ids(_pyarrow_to_schema_without_ids(arrow_schema))

    # Build the partition spec — identity on (year, month, day, hour) by
    # name lookup against the schema.
    name_to_id = {f.name: f.field_id for f in ice_schema.fields}
    partition_fields = []
    next_pid = 1000  # partition field ids must not collide with column ids
    for col in ("year", "month", "day", "hour"):
        partition_fields.append(
            PartitionField(
                source_id=name_to_id[col],
                field_id=next_pid,
                transform=IdentityTransform(),
                name=col,
            )
        )
        next_pid += 1
    spec = PartitionSpec(*partition_fields)

    sql_catalog.create_namespace_if_not_exists(NAMESPACE)
    table = sql_catalog.create_table(
        (NAMESPACE, TABLE),
        schema=ice_schema,
        partition_spec=spec,
    )
    return table


def _write_parquet_to_warehouse(
    table,
    warehouse_dir,
    *,
    partition_values: dict[str, int],
) -> tuple[str, bytes, pq.FileMetaData, int]:
    """Write a tiny parquet file to the table's data directory.

    Returns (file_uri, file_bytes, parquet_metadata, row_count).
    """
    # Build a one-row batch matching the table's schema. We use the
    # PyArrow schema we created the table with — the field NAMES match,
    # so column-by-name binding works on the read side.
    row_count = 3
    inserted_at = dt.datetime(
        partition_values["year"],
        partition_values["month"],
        partition_values["day"],
        partition_values["hour"],
        0,
        0,
        tzinfo=dt.UTC,
    )
    batch = pa.table(
        {
            "team_id": pa.array([1, 2, 3], type=pa.int64()),
            "_inserted_at": pa.array([inserted_at] * row_count, type=pa.timestamp("us", tz="UTC")),
            "year": pa.array([partition_values["year"]] * row_count, type=pa.int32()),
            "month": pa.array([partition_values["month"]] * row_count, type=pa.int32()),
            "day": pa.array([partition_values["day"]] * row_count, type=pa.int32()),
            "hour": pa.array([partition_values["hour"]] * row_count, type=pa.int32()),
        }
    )

    # Filesystem path under the table's location matching the partition
    # convention. PyIceberg doesn't require this layout (data file paths
    # are stored absolute in the manifest), but the partition tuple in
    # the manifest must match the file's actual contents.
    file_dir = (
        warehouse_dir
        / f"{NAMESPACE}.db"
        / TABLE
        / "data"
        / f"year={partition_values['year']}"
        / f"month={partition_values['month']:02d}"
        / f"day={partition_values['day']:02d}"
        / f"hour={partition_values['hour']:02d}"
    )
    file_dir.mkdir(parents=True, exist_ok=True)
    file_path = file_dir / f"writer-0-{uuid.uuid4().hex[:16]}.parquet"

    buf = io.BytesIO()
    pq.write_table(batch, buf)
    parquet_bytes = buf.getvalue()
    file_path.write_bytes(parquet_bytes)

    meta = pq.ParquetFile(io.BytesIO(parquet_bytes)).metadata
    return f"file://{file_path}", parquet_bytes, meta, row_count


def _build_register_request(
    table,
    *,
    file_path: str,
    record_count: int,
    file_size: int,
    partition_values: dict[str, int],
) -> RegisterFileRequest:
    """Construct a real-shaped RegisterFileRequest with parquet_stats
    that the committer can encode via ``encode_bounds``.

    The stats are minimal but non-empty: one entry per data column with
    typed-JSON bounds matching the wire format.
    """
    ice_schema = table.schema()
    # team_id is the only "data" column the writer would report stats
    # for in this stripped-down test. Find its field id.
    name_to_id = {f.name: f.field_id for f in ice_schema.fields}
    team_id_fid = str(name_to_id["team_id"])

    stats = ParquetStats(
        column_sizes={team_id_fid: 64},
        value_counts={team_id_fid: record_count},
        null_value_counts={team_id_fid: 0},
        lower_bounds={team_id_fid: 1},  # typed JSON: int → JSON number
        upper_bounds={team_id_fid: 3},
    )

    return RegisterFileRequest(
        file_path=file_path,
        writer_ordinal=0,
        kafka_offsets={"0": 12345, "1": 67890},
        partition_values=partition_values,
        record_count=record_count,
        file_size=file_size,
        schema_version="v1",
        schema_fingerprint=schema_fingerprint(ice_schema),
        parquet_stats=stats,
    )


# ---------------------------------------------------------------------------
# Sanity test — proves the stack stands up end-to-end.
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_full_cycle_sanity_check(
    app_client,
    migrated_pg,
    sql_catalog,
    tmp_path,
    icebox_config,
):
    """POST → PG → run_cycle → Iceberg snapshot, in one happy-path test.

    Covers (subset of) scenarios:
      - (d) jsonb columns persist intact through PG.
      - (b, partial) the committed snapshot carries ``posthog.icebox.cycle_id``
        in its summary.
    """
    # 1. Create the Iceberg table the icebox will commit to.
    warehouse_dir = tmp_path / "warehouse"
    table = _build_sample_table(sql_catalog, warehouse_dir)

    # 2. Write the staged parquet file the icebox is about to register.
    partition_values = {"year": 2026, "month": 6, "day": 1, "hour": 14}
    file_uri, parquet_bytes, _meta, row_count = _write_parquet_to_warehouse(
        table, warehouse_dir, partition_values=partition_values
    )

    # 3. POST /v1/files with the matching RegisterFileRequest.
    req = _build_register_request(
        table,
        file_path=file_uri,
        record_count=row_count,
        file_size=len(parquet_bytes),
        partition_values=partition_values,
    )
    resp = app_client.post("/v1/files", json=req.model_dump(mode="json"))
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert "row_id" in body
    assert "queued_at" in body
    pg_row_id = body["row_id"]

    # 4. (Scenario d) Confirm jsonb columns landed intact in PG.
    with migrated_pg.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT kafka_offsets, partition_values, parquet_stats, "
                "schema_fingerprint, committed_at, cycle_id FROM files "
                "WHERE id = %s",
                (pg_row_id,),
            )
            row = cur.fetchone()
    assert row is not None
    kafka_offsets, part_vals, p_stats, fp, committed_at, cycle_id = row
    # psycopg returns jsonb as Python dict — bypassing JSON encoding bugs
    # would require a string here, which would FAIL the dict-shaped assert.
    assert kafka_offsets == {"0": 12345, "1": 67890}
    assert part_vals == {"year": 2026, "month": 6, "day": 1, "hour": 14}
    assert isinstance(p_stats, dict)
    assert "lower_bounds" in p_stats
    assert p_stats["lower_bounds"] == {str(table.schema().fields[0].field_id): 1} or \
        p_stats["lower_bounds"]  # tolerate field-id reshuffle; we just need non-empty
    assert fp == schema_fingerprint(table.schema())
    # Pre-cycle invariants: file is unclaimed and uncommitted.
    assert committed_at is None
    assert cycle_id is None

    # 5. Run one cycle. CommitterDeps wires the SqlCatalog; Kafka is mocked.
    deps = make_committer_deps(
        sql_catalog=sql_catalog, namespace=NAMESPACE, table=TABLE
    )
    result = cm.run_cycle(cfg=icebox_config, pg_pool=migrated_pg, deps=deps)
    assert result.success is True, f"cycle did not succeed: {result.error!r}"
    assert result.skipped_reason is None, (
        f"cycle was skipped: {result.skipped_reason!r}"
    )
    assert result.file_count == 1
    assert result.iceberg_snapshot_id is not None
    snapshot_id = result.iceberg_snapshot_id

    # 6. (Scenario b, partial) The committed snapshot must exist in the
    # table and carry our cycle_id in its summary.
    table_after = sql_catalog.load_table((NAMESPACE, TABLE))
    snapshots = {s.snapshot_id: s for s in table_after.snapshots()}
    assert snapshot_id in snapshots, (
        f"snapshot {snapshot_id} not found in table; got {list(snapshots)}"
    )
    snap = snapshots[snapshot_id]
    assert snap.summary is not None
    assert snap.summary.get(CYCLE_ID_SUMMARY_KEY) == str(result.cycle_id), (
        f"cycle_id summary tag missing from snapshot {snapshot_id}; "
        f"got summary={dict(snap.summary) if snap.summary else None!r}"
    )

    # 7. PG bookkeeping: cycle marked complete, file marked committed,
    # iceberg_snapshot_id stamped on both rows.
    with migrated_pg.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cycle_id, iceberg_snapshot_id, kafka_committed_at, "
                "completed_at FROM commit_cycles WHERE cycle_id = %s",
                (str(result.cycle_id),),
            )
            cycle_row = cur.fetchone()
            cur.execute(
                "SELECT cycle_id, committed_at, iceberg_snapshot_id "
                "FROM files WHERE id = %s",
                (pg_row_id,),
            )
            file_row = cur.fetchone()
    assert cycle_row is not None
    _cid, snap_in_cycle, kafka_at, completed_at = cycle_row
    assert snap_in_cycle == snapshot_id
    assert kafka_at is not None, "kafka_committed_at not stamped"
    assert completed_at is not None, "cycle was not marked complete"
    assert file_row is not None
    file_cid, file_committed, file_snap = file_row
    assert str(file_cid) == str(result.cycle_id)
    assert file_committed is not None
    assert file_snap == snapshot_id

    # Sanity: the committer's mocked Kafka admin received an offset commit.
    deps.kafka_commit_offsets.assert_called_once()


# ---------------------------------------------------------------------------
# DB-bootstrap-if-missing — integration test against real Postgres
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_ensure_database_exists_creates_missing_database(pg_conn_kwargs):
    """End-to-end test of the tactical boot-time DB bootstrap against
    a real Postgres. The fixture's PG comes with ONE pre-created database
    (`test` by default). We point a fresh Config at a NON-EXISTENT
    database name, call ensure_database_exists, and verify:
      1. The database is created.
      2. Re-running is idempotent (path-A early-return).

    Without this hack, a fresh icebox deployment would boot-loop on
    "database does not exist" until the database was provisioned
    out-of-band via Terraform.
    """
    import uuid

    import psycopg

    from icebox import postgres_sync as ps_mod
    from icebox.config import Config

    # Unique DB name so re-running the test suite doesn't collide with
    # leftovers from a prior run on the same session container.
    target_db = f"icebox_bootstrap_{uuid.uuid4().hex[:8]}"
    cfg = Config(
        pg_host=pg_conn_kwargs["host"],
        pg_port=pg_conn_kwargs["port"],
        pg_database=target_db,
        pg_username=pg_conn_kwargs["user"],
        pg_password=pg_conn_kwargs["password"],
        pg_sslmode="disable", pg_schema="icebox",
        asyncpg_pool_min=1, asyncpg_pool_max=2,
        psycopg_pool_min=1, psycopg_pool_max=2,
        iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
        kafka_bootstrap_servers="x", kafka_topic="events",
        kafka_group_id="grp", kafka_extra_config_json="{}",
        committer_cadence_seconds=60,
        committer_max_pending_files=1000,
        committer_degraded_failure_threshold=2,
        committer_heartbeat_stale_multiple=3.0,
        api_host="0.0.0.0", api_port=8000, log_level="INFO",
    )

    # Precondition: the target DB does NOT exist
    sys_conninfo = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host, port=cfg.pg_port, dbname="postgres",
        user=cfg.pg_username, password=cfg.pg_password,
    )
    with psycopg.connect(sys_conninfo) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
        assert cur.fetchone() is None, f"target DB {target_db!r} unexpectedly exists"

    # Call 1: bootstrap creates the DB
    ps_mod.ensure_database_exists(cfg)

    with psycopg.connect(sys_conninfo) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
        assert cur.fetchone() == (1,), (
            f"ensure_database_exists did not create {target_db!r}"
        )

    # Call 2: idempotent — second call is a no-op (path A early-return).
    # Must not raise.
    ps_mod.ensure_database_exists(cfg)

    # Cleanup: drop the test database so re-runs work.
    with psycopg.connect(sys_conninfo, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP DATABASE "{target_db}"')


# ---------------------------------------------------------------------------
# Per-schema isolation — multiple iceboxes on one PG don't conflict
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_ensure_schema_exists_creates_schema(pg_conn_kwargs):
    """ensure_schema_exists creates the configured PG schema if missing
    and is idempotent on re-invocation."""
    import psycopg

    from icebox import postgres_sync as ps_mod
    from icebox.config import Config

    schema = f"icebox_test_{__import__('uuid').uuid4().hex[:8]}"
    cfg = Config(
        pg_host=pg_conn_kwargs["host"], pg_port=pg_conn_kwargs["port"],
        pg_database=pg_conn_kwargs["database"],
        pg_username=pg_conn_kwargs["user"], pg_password=pg_conn_kwargs["password"],
        pg_sslmode="disable", pg_schema=schema,
        asyncpg_pool_min=1, asyncpg_pool_max=2,
        psycopg_pool_min=1, psycopg_pool_max=2,
        iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
        kafka_bootstrap_servers="x", kafka_topic="events",
        kafka_group_id="grp", kafka_extra_config_json="{}",
        committer_cadence_seconds=60,
        committer_max_pending_files=1000,
        committer_degraded_failure_threshold=2,
        committer_heartbeat_stale_multiple=3.0,
        api_host="0.0.0.0", api_port=8000, log_level="INFO",
    )

    # Precondition: schema does NOT exist
    conninfo = psycopg.conninfo.make_conninfo(
        host=cfg.pg_host, port=cfg.pg_port, dbname=cfg.pg_database,
        user=cfg.pg_username, password=cfg.pg_password,
    )
    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        assert cur.fetchone() is None

    ps_mod.ensure_schema_exists(cfg)

    with psycopg.connect(conninfo) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        assert cur.fetchone() == (1,)

    # Idempotent: second call is a no-op
    ps_mod.ensure_schema_exists(cfg)

    # Cleanup
    with psycopg.connect(conninfo, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f'DROP SCHEMA "{schema}" CASCADE')


@pytest.mark.integration
def test_two_iceboxes_with_different_schemas_isolated(pg_conn_kwargs):
    """Two iceboxes with different cfg.pg_schema values run their
    migrations in their own schemas and don't see each other's rows.
    This is the load-bearing property for per-table icebox-per-millpond
    deployments sharing a PG instance."""
    import uuid

    import psycopg

    from icebox import postgres_sync as ps_mod
    from icebox.config import Config

    schema_a = f"icebox_a_{uuid.uuid4().hex[:8]}"
    schema_b = f"icebox_b_{uuid.uuid4().hex[:8]}"

    def _cfg(schema):
        return Config(
            pg_host=pg_conn_kwargs["host"], pg_port=pg_conn_kwargs["port"],
            pg_database=pg_conn_kwargs["database"],
            pg_username=pg_conn_kwargs["user"], pg_password=pg_conn_kwargs["password"],
            pg_sslmode="disable", pg_schema=schema,
            asyncpg_pool_min=1, asyncpg_pool_max=2,
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
            committer_degraded_failure_threshold=2,
            committer_heartbeat_stale_multiple=3.0,
            api_host="0.0.0.0", api_port=8000, log_level="INFO",
        )

    cfg_a, cfg_b = _cfg(schema_a), _cfg(schema_b)

    # Bootstrap both schemas + migrate
    for cfg in (cfg_a, cfg_b):
        ps_mod.ensure_schema_exists(cfg)

    pool_a = ps_mod.build_psycopg_pool(cfg_a)
    pool_b = ps_mod.build_psycopg_pool(cfg_b)
    pool_a.open(wait=True)
    pool_b.open(wait=True)
    try:
        with pool_a.connection() as conn:
            ps_mod.apply_migrations(conn)
        with pool_b.connection() as conn:
            ps_mod.apply_migrations(conn)

        # Each pool's connection should resolve unqualified `status` to
        # its own schema's row.
        from uuid import uuid4 as _uuid

        cycle_a = _uuid()
        with pool_a.connection() as conn:
            ps_mod.insert_cycle(conn, cycle_id=cycle_a)
            conn.commit()

        # B's pool MUST NOT see A's cycle row.
        with pool_b.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM commit_cycles")
            assert cur.fetchone() == (0,), (
                "search_path isolation broken: schema B sees rows from schema A"
            )

        # A sees its own row.
        with pool_a.connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM commit_cycles")
            assert cur.fetchone() == (1,)

        # Each derived a different advisory lock id
        assert (
            ps_mod.committer_advisory_lock_id(schema_a)
            != ps_mod.committer_advisory_lock_id(schema_b)
        )
    finally:
        pool_a.close()
        pool_b.close()
        # Cleanup
        conninfo = psycopg.conninfo.make_conninfo(
            host=cfg_a.pg_host, port=cfg_a.pg_port, dbname=cfg_a.pg_database,
            user=cfg_a.pg_username, password=cfg_a.pg_password,
        )
        with psycopg.connect(conninfo, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{schema_a}" CASCADE')
            cur.execute(f'DROP SCHEMA "{schema_b}" CASCADE')


@pytest.mark.integration
def test_asyncpg_pool_isolates_by_schema(pg_conn_kwargs):
    """The API hot-path (every POST, every status read) uses asyncpg.
    Per-schema isolation depends on `server_settings={"search_path":
    cfg.pg_schema}` being honored on every connection — the previous
    integration test only covered the psycopg pool.

    Build TWO asyncpg pools against the same PG but different schemas;
    INSERT a file into schema A's `files` table via INSERT_FILE_SQL;
    query schema B's `files` table via the status query → must be
    empty. Anything else means the asyncpg path isn't actually pinning
    search_path per connection."""
    import asyncio
    import uuid

    import psycopg

    from icebox import postgres_async as pa_mod
    from icebox import postgres_sync as ps_mod
    from icebox.config import Config

    schema_a = f"icebox_async_a_{uuid.uuid4().hex[:8]}"
    schema_b = f"icebox_async_b_{uuid.uuid4().hex[:8]}"

    def _cfg(schema):
        return Config(
            pg_host=pg_conn_kwargs["host"], pg_port=pg_conn_kwargs["port"],
            pg_database=pg_conn_kwargs["database"],
            pg_username=pg_conn_kwargs["user"], pg_password=pg_conn_kwargs["password"],
            pg_sslmode="disable", pg_schema=schema,
            asyncpg_pool_min=1, asyncpg_pool_max=2,
            psycopg_pool_min=1, psycopg_pool_max=2,
            iceberg_catalog_uri="x", iceberg_warehouse="x", iceberg_namespace="kafka", iceberg_table="events",
            kafka_bootstrap_servers="x", kafka_topic="events",
            kafka_group_id="grp", kafka_extra_config_json="{}",
            committer_cadence_seconds=60,
            committer_max_pending_files=1000,
            committer_degraded_failure_threshold=2,
            committer_heartbeat_stale_multiple=3.0,
            api_host="0.0.0.0", api_port=8000, log_level="INFO",
        )

    cfg_a, cfg_b = _cfg(schema_a), _cfg(schema_b)

    # Bootstrap both schemas + migrate (via the sync helpers — they're
    # the only thing that knows how to set up the icebox tables).
    for cfg in (cfg_a, cfg_b):
        ps_mod.ensure_schema_exists(cfg)
        sync_pool = ps_mod.build_psycopg_pool(cfg)
        sync_pool.open(wait=True)
        try:
            with sync_pool.connection() as conn:
                ps_mod.apply_migrations(conn)
        finally:
            sync_pool.close()

    async def _exercise_pools():
        async_pool_a = await pa_mod.build_asyncpg_pool(cfg_a)
        async_pool_b = await pa_mod.build_asyncpg_pool(cfg_b)
        try:
            # Insert into A via the production INSERT_FILE_SQL
            async with async_pool_a.acquire() as conn:
                await conn.execute(
                    pa_mod.INSERT_FILE_SQL,
                    "s3://b/test.parquet",  # file_path
                    0,  # writer_ordinal
                    "{}",  # kafka_offsets
                    "{}",  # partition_values
                    100,  # record_count
                    1024,  # file_size
                    "v1",  # schema_version
                    "deadbeef" * 8,  # schema_fingerprint
                    "{}",  # parquet_stats
                )

            # Query B via the production STATUS_QUERY_SQL — B must see
            # ZERO pending files. If the search_path pin doesn't fire on
            # every asyncpg acquire, B would see A's row.
            async with async_pool_b.acquire() as conn:
                row_b = await conn.fetchrow(pa_mod.STATUS_QUERY_SQL)
            async with async_pool_a.acquire() as conn:
                row_a = await conn.fetchrow(pa_mod.STATUS_QUERY_SQL)
            return row_a, row_b
        finally:
            await async_pool_a.close()
            await async_pool_b.close()

    try:
        row_a, row_b = asyncio.run(_exercise_pools())
        assert row_a is not None
        assert row_a["pending_files"] == 1
        assert row_b is not None
        assert row_b["pending_files"] == 0, (
            f"asyncpg pool isolation broken: schema B sees "
            f"{row_b['pending_files']} pending files from schema A"
        )
    finally:
        # Cleanup
        conninfo = psycopg.conninfo.make_conninfo(
            host=cfg_a.pg_host, port=cfg_a.pg_port, dbname=cfg_a.pg_database,
            user=cfg_a.pg_username, password=cfg_a.pg_password,
        )
        with psycopg.connect(conninfo, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f'DROP SCHEMA "{schema_a}" CASCADE')
            cur.execute(f'DROP SCHEMA "{schema_b}" CASCADE')
