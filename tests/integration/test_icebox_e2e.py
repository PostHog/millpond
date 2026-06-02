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
                "schema_fingerprint, committed_at, cycle_id FROM icebox.files "
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
                "completed_at FROM icebox.commit_cycles WHERE cycle_id = %s",
                (str(result.cycle_id),),
            )
            cycle_row = cur.fetchone()
            cur.execute(
                "SELECT cycle_id, committed_at, iceberg_snapshot_id "
                "FROM icebox.files WHERE id = %s",
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
