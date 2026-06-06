"""PG identifier validation shared by icebox + millpond writer.

Both sides interpolate `ICEBOX_PG_SCHEMA` into the libpq conninfo
`options=-csearch_path=<schema>` parameter (PG protocol doesn't allow
parameterized session options), so we validate strictly at config-load
time to make injection structurally impossible. A schema value
containing whitespace or a stray `-c` could redirect writes to a
different schema; a value containing newlines could break the
conninfo parser.

Lives in `shared/` rather than either side because writer + icebox
must agree on the validation rule exactly. If they diverge, the
chart's millpond.iceboxPgSchema helper would render a value that one
side accepts and the other rejects, and we'd find out at boot.
"""
from __future__ import annotations

import re


# Restricted to LOWERCASE only because PG case-folds unquoted
# identifiers to lowercase. Allowing uppercase in the regex would
# silently break operator intent: ICEBOX_PG_SCHEMA=MyIcebox creates
# `myicebox`, not `MyIcebox`, and any external tooling that expects
# the literal name has to mirror PG's folding rules. Easier to reject
# the case at config-load.
SAFE_PG_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")


# Schema names that are syntactically valid but semantically wrong:
# PG reserves the `pg_*` prefix and a handful of well-known names.
# - `pg_*` names: PG refuses CREATE SCHEMA with SQLSTATE 42939.
# - `information_schema`, `public`, `pg_catalog`, `pg_toast`,
#   `pg_temp`: would either fail or succeed-but-commingle, which is
#   worse than failing fast.
RESERVED_SCHEMA_NAMES = frozenset({
    "public",
    "information_schema",
    "pg_catalog",
    "pg_toast",
    "pg_temp",
})

# PG SQL reserved words (subset). Even if quoted these would work, but
# the conninfo interpolation can't quote them safely without rewriting
# the whole pipeline. Reject the common ones at config load. Not
# exhaustive — covers the cases an operator might plausibly type.
RESERVED_SQL_KEYWORDS = frozenset({
    "select", "from", "where", "table", "schema", "database",
    "create", "drop", "alter", "insert", "update", "delete",
    "commit", "rollback", "begin", "end", "union", "join",
    "on", "as", "is", "in", "and", "or", "not", "null", "true", "false",
    "primary", "foreign", "key", "index", "constraint", "default",
    "user", "group", "order", "by", "having", "limit", "offset",
    "with", "values", "returning",
})


def validate_pg_schema(value: str, env_var_name: str) -> None:
    """Reject ICEBOX_PG_SCHEMA values that aren't safe to interpolate
    into `options=-csearch_path=<value>`. Raises RuntimeError with an
    operator-actionable message; returns None on success.

    `env_var_name` is the source env var (e.g. `"ICEBOX_PG_SCHEMA"`) and
    is used only to build the error message — the caller passes its own
    name so the error points at the right variable on each side.
    """
    if not SAFE_PG_IDENTIFIER.match(value):
        # Three classes of failure surface here:
        # 1. Empty, illegal chars (dashes, dots, spaces, unicode, SQL
        #    injection attempts) — none of these are legal PG identifiers.
        # 2. Uppercase letters — disallowed because PG case-folds at
        #    connection-startup time.
        # 3. Names longer than 63 bytes — PG's NAMEDATALEN limit.
        raise RuntimeError(
            f"{env_var_name} {value!r} is not a valid PG identifier "
            f"(must match {SAFE_PG_IDENTIFIER.pattern}; lowercase only)"
        )
    if value.startswith("pg_"):
        # PG reserves the `pg_*` prefix; CREATE SCHEMA refuses these
        # with SQLSTATE 42939. Catch at config load with a clear
        # message rather than letting it surface as a cryptic boot
        # failure 30 seconds later.
        raise RuntimeError(
            f"{env_var_name} {value!r} starts with 'pg_' which is "
            f"reserved by Postgres for system schemas"
        )
    if value in RESERVED_SCHEMA_NAMES:
        raise RuntimeError(
            f"{env_var_name} {value!r} is a PG-reserved schema name "
            f"(would either fail to create or commingle with system or "
            f"shared state). Pick a different name like 'icebox_<table>'."
        )
    if value in RESERVED_SQL_KEYWORDS:
        raise RuntimeError(
            f"{env_var_name} {value!r} is a SQL reserved word. It "
            f"would require quoting in every reference, which the "
            f"conninfo `options=-csearch_path=` interpolation can't "
            f"do reliably. Pick a different name."
        )
