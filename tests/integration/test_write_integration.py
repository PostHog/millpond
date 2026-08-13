"""Integration tests for write path and schema evolution against local DuckDB.

Uses an in-memory DuckDB database attached as 'lake' to exercise the real
ducklake.write() and schema.SchemaManager code paths without requiring
Postgres or S3.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import duckdb
import orjson
import pyarrow as pa
import pytest

from millpond import metrics
from millpond.arrow_converter import coerce_typed_columns, convert
from millpond.ducklake import write
from millpond.schema import SchemaManager, variant_column_name


@pytest.fixture()
def conn():
    """DuckDB connection with an in-memory 'lake' catalog mimicking DuckLake.

    NB: plain DuckDB does NOT enforce DuckLake's widening-only ALTER rule — it
    permissively allows narrowing casts. Tests that need that semantic install
    it explicitly (see `_reject_alter_column` in the coercion tests); don't drop
    that wrapper assuming the fixture provides it."""
    c = duckdb.connect()
    c.execute("ATTACH ':memory:' AS lake")
    yield c
    c.close()


@pytest.fixture()
def cache() -> set[str]:
    """Per-test caller-owned ensure cache (formerly module-level `_tables_ensured`)."""
    return set()


def _table_columns(conn, table: str = "events") -> dict[str, str]:
    """column_name → data_type for a lake table (catalog-stored casing).

    Single home for the information_schema assertion query so a change to how
    columns are read (schema filter, type rename) lands in one place.
    """
    return {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog = 'lake' AND table_name = ?",
            [table],
        ).fetchall()
    }


@pytest.mark.integration
class TestWritePath:
    def test_basic_write(self, conn, cache):
        batch = pa.table({"event": ["click", "view"], "team_id": [1, 2]})
        write(conn, "events", batch, cache)

        rows = conn.execute("SELECT event, team_id FROM lake.main.events").fetchall()
        assert set(rows) == {("click", 1), ("view", 2)}

    def test_inserted_at_column_added(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        write(conn, "events", batch, cache)

        col_names = _table_columns(conn)
        assert "_inserted_at" in col_names

    def test_multiple_writes_accumulate(self, conn, cache):
        batch1 = pa.table({"x": [1, 2]})
        batch2 = pa.table({"x": [3, 4]})
        write(conn, "events", batch1, cache)
        write(conn, "events", batch2, cache)

        rows = conn.execute("SELECT x FROM lake.main.events ORDER BY x").fetchall()
        assert [r[0] for r in rows] == [1, 2, 3, 4]

    def test_empty_batch_creates_table(self, conn, cache):
        batch = pa.table({"a": pa.array([], type=pa.int64())})
        write(conn, "events", batch, cache)

        col_names = _table_columns(conn)
        assert "a" in col_names
        assert "_inserted_at" in col_names


@pytest.mark.integration
class TestVariantDualWrite:
    """Dual-write JSON/VARCHAR source columns as VARIANT companions.

    Keeps the original string column and adds `{name}_variant` via
    try_cast(try_cast(col AS JSON) AS VARIANT). Uses SchemaManager so
    ADD COLUMN + type-cache stay coherent across flushes.
    """

    def test_dual_writes_properties_variant(self, conn, cache):
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "event": ["$pageview", "click"],
                "properties": [
                    '{"$browser": "Chrome", "width": 1920}',
                    '{"$browser": "Firefox", "width": 1440}',
                ],
            }
        )
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        cols = _table_columns(conn)
        assert cols["properties"] in ("VARCHAR", "JSON")  # source stays text-ish
        assert cols[variant_column_name("properties")] == "VARIANT"

        rows = conn.execute(
            "SELECT event, properties, "
            "variant_typeof(properties_variant), "
            'properties_variant."$browser", '
            "properties_variant.width "
            "FROM lake.main.events ORDER BY event"
        ).fetchall()
        assert len(rows) == 2
        by_event = {r[0]: r for r in rows}
        # Original string preserved on the source column
        assert '"Chrome"' in by_event["$pageview"][1]
        assert '"Firefox"' in by_event["click"][1]
        # VARIANT is a parsed object (not VARCHAR holding JSON text)
        assert by_event["$pageview"][2].startswith("OBJECT")
        assert by_event["$pageview"][3] == "Chrome"
        assert by_event["$pageview"][4] == 1920
        assert by_event["click"][3] == "Firefox"
        assert by_event["click"][4] == 1440

    def test_original_string_column_unchanged(self, conn, cache):
        schema_mgr = SchemaManager(conn, "events")
        raw = '{"a": 1, "b": "two"}'
        batch = pa.table({"properties": [raw]})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        (stored,) = conn.execute("SELECT properties FROM lake.main.events").fetchone()
        assert stored == raw

    def test_malformed_json_nulls_variant_keeps_string(self, conn, cache):
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "id": [1, 2, 3],
                "properties": ['{"ok": true}', "not json{{{", '{"ok": false}'],
            }
        )
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        rows = conn.execute(
            "SELECT id, properties, properties_variant IS NULL AS vnull FROM lake.main.events ORDER BY id"
        ).fetchall()
        assert rows[0] == (1, '{"ok": true}', False)
        assert rows[1][0] == 2
        assert rows[1][1] == "not json{{{"
        assert rows[1][2] is True  # variant nulled
        assert rows[2] == (3, '{"ok": false}', False)

    # The ONLY window DuckDB accepts into a VARIANT but cannot shred is
    # (INT64_MAX, UINT64_MAX]. Verified against a real catalog: ns timestamps,
    # snowflake ids, INT64_MAX itself, values above UINT64_MAX (kept as a
    # VARIANT string) and negatives all shred fine. Getting this boundary wrong
    # in either direction is the whole hazard, so the cases below pin it.
    _POISON_JSON = '{"n": 9223372036854775999}'  # INT64_MAX < n <= UINT64_MAX

    def test_unshreddable_integer_is_coerced_keeping_the_rest_of_the_row(self, ducklake_conn, cache):
        """A JSON integer above INT64_MAX must not wedge the partition.

        2026-08-12 incident: try_cast accepts the value (it IS a valid UINT64
        variant), but DuckDB shreds VARIANT into typed Parquet columns on
        write and overflows converting to INT64, failing the whole INSERT
        forever since offsets never advance. The value is rewritten as a JSON
        string for the companion; every other field of that row survives.
        """
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        poison_row = '{"n": 9223372036854775999, "browser": "Chrome", "w": 1920}'
        batch = pa.table({"id": [1, 2], "properties": [poison_row, '{"ok": true, "w": 1440}']})
        written = write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        assert written == 2
        # The source column keeps the original bytes — it is authoritative.
        assert conn.execute("SELECT properties FROM lake.main.events WHERE id = 1").fetchone()[0] == poison_row
        # The companion survives with the unshreddable value typed as a string
        # and every sibling field intact.
        n, browser, w = conn.execute(
            "SELECT properties_variant.n, properties_variant.browser, properties_variant.w "
            "FROM lake.main.events WHERE id = 1"
        ).fetchone()
        assert n == "9223372036854775999"
        assert browser == "Chrome"
        assert w == 1920
        assert conn.execute("SELECT properties_variant.w FROM lake.main.events WHERE id = 2").fetchone()[0] == 1440

    def test_shreddable_values_are_left_alone(self, ducklake_conn, cache):
        """Values that shred fine must keep their numeric VARIANT type.

        Nanosecond timestamps and snowflake ids are 19 digits and ubiquitous;
        an earlier digit-count heuristic nulled their companions wholesale.
        """
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "properties": [
                    '{"v": 1723526400000000000}',  # ns timestamp
                    '{"v": 1234567890123456789}',  # snowflake id
                    '{"v": 9223372036854775807}',  # INT64_MAX exactly
                    '{"v": 123456789012345678901234567890}',  # > UINT64_MAX
                    '{"v": -9223372036854775809}',  # below INT64_MIN
                ],
            }
        )
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        rows = conn.execute(
            "SELECT id, properties_variant IS NULL, variant_typeof(properties_variant.v) "
            "FROM lake.main.events ORDER BY id"
        ).fetchall()
        assert [r[1] for r in rows] == [False] * 5  # no companion lost
        # The three inside INT64's range keep numeric typing — a digit-count
        # heuristic would have stringified or nulled these.
        assert [r[2] for r in rows[:3]] == ["INT64"] * 3, rows
        # Rows 4-5 read back as VARCHAR because that is how DuckDB itself
        # represents out-of-INT64-range literals in a VARIANT; the sanitizer
        # leaves them alone (they shred fine), and the digits survive.
        assert (
            conn.execute("SELECT properties_variant.v FROM lake.main.events WHERE id = 5").fetchone()[0]
            == "-9223372036854775809"
        )

    def test_digits_inside_a_string_value_are_untouched(self, ducklake_conn, cache):
        """A long digit run inside a JSON string is not a number — leave it be."""
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        raw = '{"session": "12345678901234567890"}'
        write(
            conn,
            "events",
            pa.table({"id": [1], "properties": [raw]}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        stored, session = conn.execute(
            'SELECT properties, properties_variant."session" FROM lake.main.events'
        ).fetchone()
        assert stored == raw
        assert session == "12345678901234567890"

    def test_bare_top_level_number_is_handled(self, ducklake_conn_inlining, cache):
        """A source whose whole document is a number still has to be sanitized.

        This one is only observable through the inlining path: the INSERT
        succeeds, the value commits into catalog state, and
        ducklake_flush_inlined_data fails forever afterwards — no write-time
        retry can reach it.
        """
        conn = ducklake_conn_inlining
        schema_mgr = SchemaManager(conn, "events")
        write(
            conn,
            "events",
            pa.table({"id": [1], "properties": ["9223372036854775999"]}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        conn.execute("CALL ducklake_flush_inlined_data('lake')")
        assert conn.execute("SELECT properties_variant FROM lake.main.events").fetchone()[0] == "9223372036854775999"

    def test_non_string_variant_source_does_not_crash(self, ducklake_conn, cache):
        """A variant source column inferred as a scalar type must still write.

        arrow_converter types each key from its first non-null sample, so an
        all-numeric batch for the configured source yields int64 — which
        `try_cast(col AS JSON)` handles fine. A guard that assumed VARCHAR
        turned that into a BinderException and a crash loop.
        """
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"id": [1], "properties": pa.array([12345], type=pa.int64())})
        written = write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))
        assert written == 1
        assert conn.execute("SELECT properties_variant FROM lake.main.events").fetchone()[0] == 12345

    def test_unshreddable_integer_never_reaches_inlined_catalog_state(self, ducklake_conn_inlining, cache):
        """The inlining path commits without shredding — poison must not get in.

        With data inlining a small write does NOT fail; the value lands in
        catalog state and detonates later, when ducklake_flush_inlined_data
        materializes it. No write-time retry can reach that, so the row guard
        (not a fallback) is what has to prevent it.
        """
        conn = ducklake_conn_inlining
        schema_mgr = SchemaManager(conn, "events")
        write(
            conn,
            "events",
            pa.table({"id": [1], "properties": [self._POISON_JSON]}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        # The value is sanitized before it reaches the companion, so
        # materializing inlined data (which unguarded raises "INT128 ... out of
        # range" forever after) succeeds.
        conn.execute("CALL ducklake_flush_inlined_data('lake')")
        assert conn.execute("SELECT properties_variant.n FROM lake.main.events").fetchone()[0] == "9223372036854775999"

    def test_clean_batch_after_poison_still_dual_writes(self, ducklake_conn, cache):
        """Sanitizing is per row and stateless — later clean batches are untouched."""
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        write(
            conn,
            "events",
            pa.table({"id": [1], "properties": [self._POISON_JSON]}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        write(
            conn,
            "events",
            pa.table({"id": [2], "properties": ['{"ok": true}']}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        rows = conn.execute(
            "SELECT id, properties_variant IS NULL, variant_typeof(properties_variant.n) "
            "FROM lake.main.events ORDER BY id"
        ).fetchall()
        assert [r[1] for r in rows] == [False, False]  # both keep a companion
        assert rows[0][2] == "VARCHAR"  # the poison value, coerced to a string

    def test_second_source_keeps_its_companion(self, ducklake_conn, cache):
        """Poison in one source must not disturb a healthy second source."""
        conn = ducklake_conn
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "id": [1],
                "properties": [self._POISON_JSON],
                "person_properties": ['{"plan": "ent"}'],
            }
        )
        write(
            conn,
            "events",
            batch,
            cache,
            schema_mgr,
            variant_columns=("properties", "person_properties"),
        )
        poisoned, healthy = conn.execute(
            "SELECT properties_variant.n, person_properties_variant.plan FROM lake.main.events"
        ).fetchone()
        assert poisoned == "9223372036854775999"  # coerced, not lost
        assert healthy == "ent"  # untouched

    def test_retryable_insert_failure_is_not_absorbed(self, conn, cache):
        """Only the unshreddable-value signature may trigger the fallback.

        Commit contention and IO failures must reach main.py's _write_with_retry,
        which classifies them, backs off, and calls reset_caches(). Absorbing
        them would permanently null companions for reasons unrelated to poison
        data and blind the contention alert.
        """
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"id": [1], "properties": ['{"ok": true}']})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        class _FailVariantInsertOnly:
            """duckdb connections reject attribute patching; wrap instead."""

            def __init__(self, real, message):
                self._real = real
                self._message = message
                self.insert_attempts = 0

            def execute(self, sql, *args, **kwargs):
                if sql.lstrip().upper().startswith("INSERT"):
                    self.insert_attempts += 1
                    # Only the dual-write projection fails; a string-only
                    # retry would succeed, so a fallback would be observable.
                    if "try_cast" in sql:
                        raise duckdb.Error(self._message)
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        contention = _FailVariantInsertOnly(conn, "TransactionContext Error: could not serialize access")
        with pytest.raises(duckdb.Error, match="could not serialize access"):
            write(contention, "events", batch, cache, schema_mgr, variant_columns=("properties",))
        assert contention.insert_attempts == 1  # no string-only retry was attempted

    def test_unshreddable_error_still_falls_back(self, conn, cache):
        """The backstop stays available for value shapes the row guard misses."""
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"id": [1], "properties": ['{"ok": true}']})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        class _FailVariantInsertOnly:
            def __init__(self, real):
                self._real = real
                self.insert_attempts = 0

            def execute(self, sql, *args, **kwargs):
                if sql.lstrip().upper().startswith("INSERT"):
                    self.insert_attempts += 1
                    if "try_cast" in sql:
                        raise duckdb.Error(
                            "Invalid Input Error: Type UINT64 with value 1 can't be cast "
                            "because the value is out of range for the destination type INT64"
                        )
                return self._real.execute(sql, *args, **kwargs)

            def __getattr__(self, name):
                return getattr(self._real, name)

        wrapped = _FailVariantInsertOnly(conn)
        with patch.object(metrics, "variant_write_fallback_total") as fallback_metric:
            written = write(wrapped, "events", batch, cache, schema_mgr, variant_columns=("properties",))
        assert written == 1
        assert wrapped.insert_attempts == 2  # dual-write attempt, then string-only
        # The only "sanitizer missed something" signal operators have.
        fallback_metric.inc.assert_called_once()
        nulls = conn.execute("SELECT count(*) FROM lake.main.events WHERE properties_variant IS NULL").fetchone()[0]
        assert nulls == 1

    def test_absent_source_column_skips_dual_write(self, conn, cache):
        """Configured source missing from this batch → no companion projected.

        Column may still exist from a prior batch; rows just get NULL there.
        """
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"event": ["x"]})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        cols = _table_columns(conn)
        # properties absent from batch → properties_variant never ADDed
        assert "properties_variant" not in cols
        assert "event" in cols

    def test_existing_table_gains_variant_column_on_evolve(self, conn, cache):
        schema_mgr = SchemaManager(conn, "events")
        # First write without dual-write — table has properties VARCHAR only
        write(
            conn,
            "events",
            pa.table({"properties": ['{"a": 1}']}),
            cache,
            schema_mgr,
        )
        assert "properties_variant" not in schema_mgr._known_columns

        # Second write enables dual-write — ADD COLUMN + populate
        write(
            conn,
            "events",
            pa.table({"properties": ['{"a": 2}']}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        assert schema_mgr._known_columns.get("properties_variant") == "VARIANT"

        rows = conn.execute(
            "SELECT properties, properties_variant FROM lake.main.events ORDER BY properties"
        ).fetchall()
        assert len(rows) == 2
        # First row pre-dates the VARIANT column → NULL companion
        assert rows[0][1] is None
        assert rows[1][1] is not None

    def test_source_companion_collision_nonfatal_still_dual_writes(self, conn, cache):
        """Poison payload with both properties and properties_variant must not crash-loop.

        Companion is stripped; source dual-writes into a real VARIANT column.
        """
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "properties": ['{"a": 1}'],
                "properties_variant": ["already here"],
            }
        )
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        cols = _table_columns(conn)
        assert cols["properties_variant"] == "VARIANT"
        (props, vtype) = conn.execute(
            "SELECT properties, variant_typeof(properties_variant) FROM lake.main.events"
        ).fetchone()
        assert props == '{"a": 1}'
        assert vtype.startswith("OBJECT")

    def test_orphan_companion_payload_does_not_create_varchar_column(self, conn, cache):
        """properties_variant alone in a batch must not evolve() as VARCHAR."""
        schema_mgr = SchemaManager(conn, "events")
        write(
            conn,
            "events",
            pa.table({"event": ["x"], "properties_variant": ["poison"]}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        cols = _table_columns(conn)
        assert "properties_variant" not in cols
        assert "event" in cols

        # Later real dual-write still works and creates VARIANT, not VARCHAR.
        write(
            conn,
            "events",
            pa.table({"event": ["y"], "properties": ['{"a": 1}']}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        assert _table_columns(conn)["properties_variant"] == "VARIANT"

    def test_wrong_typed_companion_degrades_to_string_only(self, conn, cache):
        """Pre-existing non-VARIANT companion: string still writes, no crash loop."""
        conn.execute(
            "CREATE TABLE lake.main.events (properties VARCHAR, properties_variant VARCHAR, _inserted_at TIMESTAMP)"
        )
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()
        assert schema_mgr._known_columns.get("properties_variant") == "VARCHAR"

        batch = pa.table({"properties": ['{"a": 1}']})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        # Source landed; companion left alone (NULL for this insert — not projected).
        (props, companion) = conn.execute("SELECT properties, properties_variant FROM lake.main.events").fetchone()
        assert props == '{"a": 1}'
        assert companion is None
        # Cache still reflects VARCHAR — not poisoned to VARIANT.
        assert schema_mgr._known_columns.get("properties_variant") == "VARCHAR"

    def test_add_if_not_exists_noop_wrong_type_degrades(self, conn, cache):
        """ADD IF NOT EXISTS no-op on wrong type: no cache poison, string-only write."""
        conn.execute(
            "CREATE TABLE lake.main.events (properties VARCHAR, properties_variant VARCHAR, _inserted_at TIMESTAMP)"
        )
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._known_columns = {"properties": "VARCHAR", "_inserted_at": "TIMESTAMP"}
        schema_mgr._initialized = True

        batch = pa.table({"properties": ['{"a": 1}']})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        assert schema_mgr._known_columns.get("properties_variant") == "VARCHAR"
        (props,) = conn.execute("SELECT properties FROM lake.main.events").fetchone()
        assert props == '{"a": 1}'

    def test_variant_columns_requires_schema_manager(self, conn, cache):
        batch = pa.table({"properties": ['{"a": 1}']})
        with pytest.raises(ValueError, match="requires a SchemaManager"):
            write(conn, "events", batch, cache, schema_mgr=None, variant_columns=("properties",))

    def test_all_companion_poison_batch_skipped_nonfatally(self, conn, cache):
        """A batch whose every column is a companion collision must not crash-loop.

        After the drop the batch has zero columns; SELECT * over a zero-column
        relation errors, so write() must skip the flush instead of raising.
        """
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"properties_variant": ["poison", "poison2"]})
        written = write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        assert written == 0  # skipped records must not count as written
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'lake'"
        ).fetchall()
        assert tables == []  # nothing created, nothing written, no exception

    def test_genuine_zero_column_batch_still_fails_loudly(self, conn, cache):
        """A batch that arrives with zero columns did not collide — no silent skip.

        The companion-collision skip is gated on the drop actually emptying the
        batch; an upstream bug producing a 0-column batch must keep surfacing
        as a write failure, not vanish under a misleading skip reason.
        """
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"a": [1, 2]}).drop_columns(["a"])
        assert batch.num_columns == 0 and batch.num_rows == 2
        with pytest.raises(duckdb.Error):
            write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

    def test_unconfigured_writer_cannot_poison_live_variant_column(self, conn, cache):
        """Mixed fleet: a writer without variant_columns must not write into a
        live VARIANT companion (implicit VARCHAR→VARIANT cast would store a
        string-wrapped variant invisible to `companion.field` queries)."""
        schema_mgr = SchemaManager(conn, "events")
        # Configured writer creates the VARIANT companion.
        write(
            conn,
            "events",
            pa.table({"properties": ['{"a": 1}']}),
            cache,
            schema_mgr,
            variant_columns=("properties",),
        )
        assert _table_columns(conn)["properties_variant"] == "VARIANT"

        # Unconfigured writer (fresh SchemaManager, no variant_columns) gets a
        # poison payload carrying the companion key.
        other_mgr = SchemaManager(conn, "events")
        written = write(
            conn,
            "events",
            pa.table({"properties": ['{"a": 2}'], "properties_variant": ["poison"]}),
            cache,
            other_mgr,
        )
        assert written == 1
        rows = conn.execute(
            "SELECT properties, variant_typeof(properties_variant) FROM lake.main.events "
            "WHERE properties_variant IS NOT NULL ORDER BY properties"
        ).fetchall()
        # Both rows hold parsed objects — no string-wrapped variant landed.
        assert [r[1].startswith("OBJECT") for r in rows] == [True]
        # The unconfigured writer's row has a NULL companion (dropped field,
        # no dual-write config), not the poison payload.
        (companion,) = conn.execute(
            "SELECT properties_variant IS NULL FROM lake.main.events WHERE properties = '{\"a\": 2}'"
        ).fetchone()
        assert companion is True

    def test_write_returns_record_count(self, conn, cache):
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"properties": ['{"a": 1}', '{"b": 2}']})
        assert write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",)) == 2

    def test_case_variant_companion_key_dropped(self, conn, cache):
        """PROPERTIES_VARIANT lands in the same catalog column — must be stripped."""
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table(
            {
                "properties": ['{"a": 1}'],
                "PROPERTIES_VARIANT": ["poison"],
            }
        )
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        cols = _table_columns(conn)
        assert cols["properties_variant"] == "VARIANT"
        (props, vtype) = conn.execute(
            "SELECT properties, variant_typeof(properties_variant) FROM lake.main.events"
        ).fetchone()
        assert props == '{"a": 1}'
        assert vtype.startswith("OBJECT")

    def test_case_variant_source_key_still_dual_writes(self, conn, cache):
        """A Properties batch key feeds the configured properties column case-insensitively."""
        schema_mgr = SchemaManager(conn, "events")
        batch = pa.table({"Properties": ['{"a": 1}']})
        write(conn, "events", batch, cache, schema_mgr, variant_columns=("properties",))

        cols = {k.lower(): v for k, v in _table_columns(conn).items()}
        assert cols["properties_variant"] == "VARIANT"
        (vtype,) = conn.execute("SELECT variant_typeof(properties_variant) FROM lake.main.events").fetchone()
        assert vtype.startswith("OBJECT")


@pytest.mark.integration
class TestSchemaEvolution:
    def test_add_new_column(self, conn, cache):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Second write introduces a new column — full write, not just evolve
        batch2 = pa.table({"event": ["view"], "source": ["web"]})
        write(conn, "events", batch2, cache, schema_mgr)

        col_names = _table_columns(conn)
        assert "source" in col_names

        # Verify data landed correctly
        rows = conn.execute("SELECT event, source FROM lake.main.events ORDER BY event").fetchall()
        assert rows == [("click", None), ("view", "web")]

    def test_widen_integer_to_bigint(self, conn):
        # Create table with INTEGER column
        conn.execute("CREATE TABLE lake.main.events (x INTEGER)")
        schema_mgr = SchemaManager(conn, "events")

        # Write with BIGINT — should widen
        batch = pa.table({"x": pa.array([1], type=pa.int64())})
        schema_mgr.evolve(batch.schema)

        assert _table_columns(conn)["x"] == "BIGINT"

    def test_widen_float_to_double(self, conn):
        conn.execute("CREATE TABLE lake.main.events (x FLOAT)")
        schema_mgr = SchemaManager(conn, "events")

        batch = pa.table({"x": pa.array([1.0], type=pa.float64())})
        schema_mgr.evolve(batch.schema)

        assert _table_columns(conn)["x"] == "DOUBLE"

    def test_multiple_new_columns_at_once(self, conn, cache):
        batch1 = pa.table({"a": [1]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Full write with multiple new columns
        batch2 = pa.table({"a": [2], "b": ["x"], "c": [3.0]})
        write(conn, "events", batch2, cache, schema_mgr)

        col_names = _table_columns(conn)
        assert {"a", "b", "c", "_inserted_at"} <= set(col_names)

        # Verify data integrity
        rows = conn.execute("SELECT a, b, c FROM lake.main.events ORDER BY a").fetchall()
        assert rows == [(1, None, None), (2, "x", 3.0)]

    def test_schema_cached_across_writes(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        assert schema_mgr._initialized
        assert "event" in schema_mgr._known_columns

    def test_invalidate_forces_reload(self, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        schema_mgr.invalidate()
        assert not schema_mgr._initialized

        # Next evolve should reload
        schema_mgr.evolve(batch.schema)
        assert schema_mgr._initialized

    @patch("millpond.schema.metrics")
    def test_column_added_increments_counter(self, mock_metrics, conn, cache):
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        batch2 = pa.table({"event": ["view"], "source": ["web"]})
        schema_mgr.evolve(batch2.schema)

        mock_metrics.schema_columns_added_total.inc.assert_called_once()

    @patch("millpond.schema.metrics")
    def test_type_widened_increments_counter(self, mock_metrics, conn):
        conn.execute("CREATE TABLE lake.main.events (x INTEGER)")
        schema_mgr = SchemaManager(conn, "events")

        batch = pa.table({"x": pa.array([1], type=pa.int64())})
        schema_mgr.evolve(batch.schema)

        mock_metrics.schema_columns_widened_total.inc.assert_called_once()

    @patch("millpond.schema.metrics")
    def test_no_change_no_counter(self, mock_metrics, conn, cache):
        batch = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch, cache, schema_mgr)

        # Same schema again — no evolution needed
        schema_mgr.evolve(batch.schema)

        mock_metrics.schema_columns_added_total.inc.assert_not_called()
        mock_metrics.schema_columns_widened_total.inc.assert_not_called()

    @patch("millpond.schema.metrics")
    def test_incompatible_type_change_increments_error(self, mock_metrics, conn):
        """Incompatible type change should be rejected, logged, and metricked.

        DuckLake enforces widening-only for ALTER COLUMN SET DATA TYPE, but
        plain DuckDB allows nearly anything. We simulate DuckLake's rejection
        by wrapping the connection to raise on ALTER COLUMN.
        """
        conn.execute("CREATE TABLE lake.main.events (x BIGINT)")
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()

        # Wrap the connection to reject ALTER COLUMN (simulating DuckLake)
        real_conn = schema_mgr._conn
        mock_conn = MagicMock(wraps=real_conn)
        mock_conn.execute = MagicMock(
            side_effect=lambda sql, *a, **kw: (
                (_ for _ in ()).throw(duckdb.Error("Cannot narrow BIGINT to INTEGER"))
                if "ALTER COLUMN" in sql
                else real_conn.execute(sql, *a, **kw)
            )
        )
        schema_mgr._conn = mock_conn

        # Arrow batch with narrower type
        batch = pa.table({"x": pa.array([1], type=pa.int32())})
        schema_mgr.evolve(batch.schema)

        # Should have incremented the schema error counter
        mock_metrics.errors_total.labels.assert_called_with(type="schema")
        mock_metrics.errors_total.labels(type="schema").inc.assert_called_once()
        # Column type should remain BIGINT
        assert schema_mgr._known_columns["x"] == "BIGINT"


@pytest.mark.integration
class TestTimestampCoercionWritePath:
    """End-to-end: NRT JSON (string timestamps) written into a table whose
    timestamp columns are already TIMESTAMPTZ — the duckling backfill's
    `posthog.events` shape. Without coercion this is the prod wedge that
    PR #12334 hit; coercion makes the batch type match the table so no
    schema-evolution DDL is needed.
    """

    # The events table's TIMESTAMPTZ columns, per the backfill DDL.
    TS_COLS = (
        "timestamp",
        "created_at",
        "person_created_at",
        "group0_created_at",
    )
    TS_PAIRS = tuple((c, "timestamptz") for c in TS_COLS)
    WIRE = "2024-01-01 12:00:00.000000"

    def _nrt_batch(self) -> pa.Table:
        """An NRT batch the way it reaches the sink: JSON → convert() infers
        VARCHAR for the date-time strings."""
        msg = {"uuid": "u1", "event": "$pageview", "team_id": 1}
        for c in self.TS_COLS:
            msg[c] = self.WIRE
        table = convert([orjson.dumps(msg)])
        assert table is not None
        # Precondition: inference really does type these as strings.
        for c in self.TS_COLS:
            assert table.schema.field(c).type == pa.string()
        return table

    def _create_events_table(self, conn) -> None:
        cols = ", ".join(f"{c} TIMESTAMPTZ" for c in self.TS_COLS)
        # _inserted_at mirrors the backfill DDL — write()'s INSERT ... BY NAME
        # appends NOW() into it.
        conn.execute(
            f"CREATE TABLE lake.main.events "
            f"(uuid VARCHAR, event VARCHAR, team_id BIGINT, {cols}, _inserted_at TIMESTAMPTZ)"
        )

    def _reject_alter_column(self, schema_mgr) -> None:
        """Wrap the connection so ALTER COLUMN raises, simulating DuckLake's
        widening-only enforcement (plain DuckDB would permissively allow the
        narrowing and hide the bug)."""
        real_conn = schema_mgr._conn
        mock_conn = MagicMock(wraps=real_conn)
        mock_conn.execute = MagicMock(
            side_effect=lambda sql, *a, **kw: (
                (_ for _ in ()).throw(duckdb.Error("DuckLake only widens"))
                if "ALTER COLUMN" in sql
                else real_conn.execute(sql, *a, **kw)
            )
        )
        schema_mgr._conn = mock_conn
        return mock_conn

    @patch("millpond.schema.metrics")
    def test_uncoerced_string_batch_triggers_failing_alter(self, mock_metrics, conn, cache):
        """The baseline this change fixes: without coercion, evolve() attempts to
        narrow each TIMESTAMPTZ column to VARCHAR and gets rejected.

        Caveat — this asserts the *narrowing ALTER is attempted and metricked*,
        not the full prod wedge. The real stall is DuckLake-specific at INSERT
        time; plain in-memory DuckDB would permissively auto-cast the VARCHAR
        rows into the TIMESTAMPTZ column on `INSERT ... BY NAME`, so the harness
        can't reproduce the stall itself. `_reject_alter_column` supplies the
        DuckLake widening-only semantic; the failing ALTER is the observable
        signal (`errors_total{type="schema"}` bumping every flush) that the fix
        eliminates."""
        self._create_events_table(conn)
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()
        self._reject_alter_column(schema_mgr)

        schema_mgr.evolve(self._nrt_batch().schema)

        # One ALTER attempt per timestamp column, each rejected and metricked.
        assert mock_metrics.errors_total.labels(type="schema").inc.call_count == len(self.TS_COLS)

    @patch("millpond.schema.metrics")
    def test_coerced_batch_writes_with_no_schema_ddl(self, mock_metrics, conn, cache):
        """The fix: coerced batch matches the table, so evolve() issues no DDL
        and the rows land with real timestamps — even when ALTER is forbidden."""
        self._create_events_table(conn)
        schema_mgr = SchemaManager(conn, "events")
        schema_mgr._load_table_schema()
        mock_conn = self._reject_alter_column(schema_mgr)

        batch = coerce_typed_columns(self._nrt_batch(), self.TS_PAIRS)
        # write() goes through the same (ALTER-rejecting) connection.
        write(mock_conn, "events", batch, cache, schema_mgr, schema_name="main")

        # No schema error, no widen, no add — types already matched.
        mock_metrics.errors_total.labels(type="schema").inc.assert_not_called()
        mock_metrics.schema_columns_widened_total.inc.assert_not_called()
        mock_metrics.schema_columns_added_total.inc.assert_not_called()

        # Data landed and the stored value is the right instant. DuckDB renders
        # TIMESTAMPTZ in the session timezone, so compare the instant (aware
        # equality is tz-independent) rather than its string form.
        row = conn.execute("SELECT event, timestamp FROM lake.main.events WHERE uuid = 'u1'").fetchone()
        assert row[0] == "$pageview"
        assert row[1] == datetime(2024, 1, 1, 12, 0, tzinfo=UTC)

    def test_fresh_table_created_with_timestamptz(self, conn, cache):
        """When millpond owns table creation, a coerced batch yields TIMESTAMPTZ
        columns from the start (vs VARCHAR for an uncoerced string batch)."""
        batch = coerce_typed_columns(self._nrt_batch(), self.TS_PAIRS)
        write(conn, "events", batch, cache, SchemaManager(conn, "events"), schema_name="main")

        types = _table_columns(conn)
        for c in self.TS_COLS:
            assert types[c] == "TIMESTAMP WITH TIME ZONE"

    @patch("millpond.schema.metrics")
    def test_unsafe_field_name_skipped(self, mock_metrics, conn, cache):
        """Fields with unsafe names (SQL injection risk) should be skipped."""
        batch1 = pa.table({"event": ["click"]})
        schema_mgr = SchemaManager(conn, "events")
        write(conn, "events", batch1, cache, schema_mgr)

        # Simulate a batch with an unsafe field name
        unsafe_schema = pa.schema([pa.field("event", pa.string()), pa.field("x; DROP TABLE", pa.string())])
        schema_mgr.evolve(unsafe_schema)

        mock_metrics.records_skipped_total.labels.assert_called_with(reason="unsafe_field_name")
        # The unsafe column should not have been added
        assert "x; DROP TABLE" not in schema_mgr._known_columns
