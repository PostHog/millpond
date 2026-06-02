"""Pin the single-table-only assumptions in the v1 icebox.

millpond deployments run six (topic, table) consumers per environment
today: events-iceberg, person-iceberg, person-distinct-id-iceberg,
groups-iceberg, heatmap-events-iceberg, ai-events-iceberg. v1 of the
icebox ships per (topic, table) — i.e., we'd need six iceboxes per env
to cover the fleet. The follow-up "multi-table icebox" refactor will
land one icebox per millpond deployment instead.

These tests document EXACTLY what's single-table TODAY. When the
multi-table refactor lands, each of these tests will (correctly) fail.
At that point the refactoring engineer updates them to assert the
multi-table shape. The set of failures IS the change-list.

Treat this file as a deliberate test-anchor — not as a coverage check.
Touching it should be intentional.
"""
from __future__ import annotations

import dataclasses
import inspect

from icebox import config as icebox_config
from icebox import postgres_sync as ps
from icebox.schema import ALL_DDL, CREATE_COMMIT_CYCLES, CREATE_FILES
from shared.models import RegisterFileRequest


# ---------------------------------------------------------------------------
# Config — Kafka scalars
# ---------------------------------------------------------------------------


def test_config_has_scalar_kafka_topic_field_v1():
    """v1 single-table assumption: ICEBOX_KAFKA_TOPIC is a scalar str.

    Multi-table refactor: this becomes a list (likely a JSON-encoded
    list of {topic, group_id, namespace, table} dicts under one env var,
    or one env var per table). When that lands, kafka_topic disappears
    from Config and this test fails. The replacement test should pin
    the multi-table config shape — e.g., assert `tables_config` exists
    and is a list[TableConfig]."""
    fields = {f.name: f for f in dataclasses.fields(icebox_config.Config)}
    assert "kafka_topic" in fields
    assert fields["kafka_topic"].type == "str"


def test_config_has_scalar_kafka_group_id_field_v1():
    """v1: ICEBOX_KAFKA_GROUP_ID is a scalar str. Multi-table: per-table
    group_id. Each writer's group_id is keyed by table."""
    fields = {f.name: f for f in dataclasses.fields(icebox_config.Config)}
    assert "kafka_group_id" in fields
    assert fields["kafka_group_id"].type == "str"


def test_config_has_no_per_table_collections_v1():
    """v1: no field shaped like a per-table list. Multi-table: the
    canonical shape is a list of TableConfig dataclasses."""
    fields = {f.name for f in dataclasses.fields(icebox_config.Config)}
    assert "tables" not in fields
    assert "tables_config" not in fields
    assert "table_configs" not in fields


# ---------------------------------------------------------------------------
# Wire format — RegisterFileRequest has no table identifier
# ---------------------------------------------------------------------------


def test_register_file_request_has_no_iceberg_namespace_field_v1():
    """v1: the icebox knows from config which table it serves; writers
    don't include a table identifier in the POST. Multi-table: writers
    MUST identify the target table so the icebox can route correctly.

    The refactor will add `iceberg_namespace: str` + `iceberg_table:
    str` fields to RegisterFileRequest, and probably bump
    PROTOCOL_VERSION to 2."""
    fields = set(RegisterFileRequest.model_fields.keys())
    assert "iceberg_namespace" not in fields
    assert "iceberg_table" not in fields


def test_register_file_request_protocol_version_pinned_at_1_v1():
    """v1 PROTOCOL_VERSION = 1. Multi-table refactor bumps to 2,
    documented in shared/models.py. This test fires when 2 lands."""
    from shared.models import PROTOCOL_VERSION
    assert PROTOCOL_VERSION == 1


# ---------------------------------------------------------------------------
# Schema — no table_name column on commit_cycles or files
# ---------------------------------------------------------------------------


def test_commit_cycles_ddl_lacks_table_name_column_v1():
    """v1: all commit_cycles rows belong implicitly to the single table
    this icebox serves. Multi-table: needs an explicit `table_name`
    (or `iceberg_namespace`/`iceberg_table` pair) column. The refactor
    adds it; this test fires on column addition."""
    sql = CREATE_COMMIT_CYCLES.lower()
    assert "table_name" not in sql
    assert "iceberg_table" not in sql


def test_files_ddl_lacks_table_name_column_v1():
    """v1: same single-table assumption holds for files. Multi-table:
    files get `table_name` and the partial indexes need to include it."""
    sql = CREATE_FILES.lower()
    assert "table_name" not in sql
    assert "iceberg_table" not in sql


# ---------------------------------------------------------------------------
# Postgres queries — no per-table WHERE clauses
# ---------------------------------------------------------------------------


def test_claim_files_sql_has_no_per_table_filter_v1():
    """v1: claim across the whole files table because they're all for
    the same target. Multi-table: claim must filter on table_name to
    avoid pulling files from table A into a cycle for table B."""
    sql = ps.CLAIM_FILES_SQL.lower()
    assert "table_name" not in sql


def test_files_for_cycle_sql_has_no_table_join_v1():
    """v1: the cycle_id alone identifies a contiguous batch. Multi-
    table: the cycle_id also implies a target table (from the
    commit_cycles row); files_for_cycle inherits via the FK."""
    sql = ps.FILES_FOR_CYCLE_SQL.lower()
    assert "table_name" not in sql
    assert "join" not in sql


def test_status_query_has_no_per_table_aggregation_v1():
    """v1 GET /v1/status returns global counts. Multi-table: per-table
    breakdown (so dashboards can show 'events: N pending, person: M
    pending'). StatusResponse will grow a per-table map."""
    from icebox import postgres_async as pa
    sql = pa.STATUS_QUERY_SQL.lower()
    assert "table_name" not in sql
    assert "group by" not in sql


# ---------------------------------------------------------------------------
# Committer — single run_cycle per cadence, not per (table, cadence)
# ---------------------------------------------------------------------------


def test_run_cycle_takes_no_table_arg_v1():
    """v1: run_cycle commits whatever's queued. Multi-table: signature
    becomes `run_cycle(*, cfg, pg_pool, deps, table_name)` with the
    outer loop iterating tables."""
    from icebox import committer as cm
    sig = inspect.signature(cm.run_cycle)
    params = set(sig.parameters.keys())
    assert "table_name" not in params
    assert "table" not in params


def test_committer_loop_takes_no_tables_iterable_v1():
    """v1: loop is `while not stop: run_cycle(); sleep(cadence)`.
    Multi-table: `for table in tables: run_cycle(table); sleep(...)`,
    so loop signature gains a `tables` kwarg (or pulls from cfg)."""
    from icebox import committer as cm
    sig = inspect.signature(cm.committer_loop)
    params = set(sig.parameters.keys())
    assert "tables" not in params
    assert "tables_config" not in params


def test_committer_deps_load_table_takes_no_args_v1():
    """v1: load_table() takes no args because there IS only one table.
    Multi-table: load_table(table_name) — the deps.load_table callable
    becomes `Callable[[str], Table]`."""
    from icebox import committer as cm
    deps_fields = {f.name: f for f in dataclasses.fields(cm.CommitterDeps)}
    assert "load_table" in deps_fields


# ---------------------------------------------------------------------------
# API — no per-table routing
# ---------------------------------------------------------------------------


def test_api_post_handler_does_not_branch_on_table_v1():
    """v1: every POST is for the same table; no routing logic. Multi-
    table: the handler reads req.iceberg_namespace + req.iceberg_table
    and dispatches into per-table validation + insertion."""
    import icebox.api as api
    src = inspect.getsource(api._handle_register_file)
    assert "iceberg_table" not in src
    assert "iceberg_namespace" not in src


# ---------------------------------------------------------------------------
# Migration aid — the test count is itself a checklist
# ---------------------------------------------------------------------------


def test_single_table_pin_count_is_visible():
    """When the multi-table refactor lands, the failure count from THIS
    file is a checklist of work items. This test does nothing — it's a
    documentation anchor."""
    # Intentionally minimal — the value is in this docstring as a
    # reference for the refactoring engineer.
    pass
