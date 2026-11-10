"""The whales-shaped VARIANT table, as the (out-of-scope) setup script
creates it, for the hoglake integration and e2e suites.

D1: the table keeps the source names — `properties`, `person_properties`,
`group0_properties` .. `group4_properties` are `variant` columns, not
`_sv` companions beside strings. D2: the table is PRE-CREATED with its
shredding declarations, `identity(team_id), month(timestamp)` and a
`timestamp` sort; millpond resolves it and never declares a variant column.
"""

from __future__ import annotations

import pyarrow as pa

# The `properties_sv` declaration of pyhoglake's
# tests/vectors/variant_layout_vectors.json (the layout vector the server's
# compaction and pyhoglake's writer are both pinned to), inlined: millpond
# does not depend on the hoglake checkout. Sixteen string leaves, the
# first cut of the whales table's `properties`.
PROPERTIES_SV = {
    "type": "object",
    "fields": [
        {"name": name, "type": "string"}
        for name in (
            "$session_id",
            "$window_id",
            "$group_0",
            "$group_1",
            "$group_2",
            "$group_3",
            "$group_4",
            "$current_url",
            "$pathname",
            "$host",
            "$browser",
            "$os",
            "$device_type",
            "$lib",
            "$referring_domain",
            "$device_id",
        )
    ],
}
PERSON_DECL = {"type": "object", "fields": [{"name": "email", "type": "string"}, {"name": "age", "type": "int64"}]}
GROUP_DECL = {"type": "object", "fields": [{"name": "name", "type": "string"}]}
VARIANT_NAMES = ("properties", "person_properties", *(f"group{i}_properties" for i in range(5)))
TS_TYPE = pa.timestamp("us", tz="UTC")


def table_schema() -> pa.Schema:
    """The whales table as the setup script creates it: the source names
    kept, every JSON column a declared `variant`, `timestamp` typed, and
    `_inserted_at` present so millpond never alters it."""
    from pyhoglake import variant

    return pa.schema(
        [
            pa.field("uuid", pa.string()),
            pa.field("event", pa.string()),
            pa.field("team_id", pa.int64()),
            pa.field("timestamp", TS_TYPE),
            variant.variant_field("properties", PROPERTIES_SV),
            variant.variant_field("person_properties", PERSON_DECL),
            *(variant.variant_field(f"group{i}_properties", GROUP_DECL) for i in range(5)),
            pa.field("_inserted_at", TS_TYPE),
        ]
    )


def precreate(client, catalog: str, namespace: str, table: str, *, data_path: str | None = None):
    """What the setup script does, with pyhoglake: the table with its
    declarations, `identity(team_id), month(timestamp)` and a `timestamp`
    sort, in one create. The catalog and namespace are created when
    missing (the catalog only with `data_path`), and a table that already
    exists (a rerun after a killed run left the stack up) is returned as
    it is rather than refused."""
    from pyhoglake import AlreadyExistsError, NotFoundError, ops

    try:
        cat = client.catalog(catalog)
    except NotFoundError:
        if data_path is None:
            raise
        cat = client.create_catalog(catalog, data_path, author="setup", message="variant suite")
    try:
        ns = cat.create_namespace(namespace, author="setup", message="variant suite")
    except AlreadyExistsError:
        ns = cat.namespace(namespace)
    try:
        return ns.create_table(
            table,
            table_schema(),
            partition_fields=[
                ops.NamedPartitionField("team_id", "identity"),
                ops.NamedPartitionField("timestamp", "month"),
            ],
            sort_fields=[ops.NamedSortField("timestamp")],
            author="setup",
            message="variant suite: pre-create the whales-shaped table",
        )
    except AlreadyExistsError:
        return ns.table(table)
