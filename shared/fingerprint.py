"""Schema fingerprint — canonical form for the icebox protocol.

Both the writer and the icebox committer compute a fingerprint over the
*Iceberg* Schema (with field IDs assigned). The writer sends the
fingerprint in the POST body; the committer compares against the
fingerprint of `table.schema()`. Mismatches are rejected with 400.

Why this catches real bugs:
  - A writer building an Iceberg schema from a renamed PyArrow column
    produces a different model_dump_json — the rename trickles all the
    way down without the icebox needing to inspect field-by-field.
  - A writer running on an old image with a stale schema vs. an icebox
    that already migrated the table — fingerprint mismatch fires
    instead of silent column-shape drift into Iceberg.

What this does NOT catch:
  - Logical type equivalence ("int → long" widening); these need
    explicit schema evolution via the (future) icebox schema-evolution
    path. v1 treats any divergence as breaking.

Canonical form: SHA-256 of `Schema.model_dump_json()`. This is byte-
stable per PyIceberg version because Pydantic's model_dump_json is
deterministic for a given model instance (sorted field IDs come from
the Schema construction, not the serialization). Two writers building
the same Iceberg schema will produce the same fingerprint.

Cross-version pin: if PyIceberg changes its model_dump_json output
shape, fingerprints invalidate. That's caught by the pin canary test
in tests/unit/test_pyiceberg_pin.py and gates the version bump.
"""
from __future__ import annotations

import hashlib

from pyiceberg.schema import Schema


def schema_fingerprint(schema: Schema) -> str:
    """Return the canonical SHA-256 hex fingerprint of an Iceberg Schema.

    Args:
        schema: A pyiceberg.schema.Schema with field IDs assigned.

    Returns:
        Lowercase hex SHA-256 digest, 64 chars.
    """
    canonical = schema.model_dump_json()
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
