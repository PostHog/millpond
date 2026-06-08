"""Unit tests for shared.pg_identifier.

These hit the validator directly. End-to-end exercise via the loader
paths lives in tests/unit/test_icebox_config.py + test_config.py — the
two configs that consume this module must keep their integrations
green even as the validator evolves.
"""
from __future__ import annotations

import pytest

from shared.pg_identifier import (
    RESERVED_SCHEMA_NAMES,
    RESERVED_SQL_KEYWORDS,
    SAFE_PG_IDENTIFIER,
    validate_pg_schema,
)


@pytest.mark.parametrize("good", [
    "icebox",
    "icebox_events",
    "icebox_person_distinct_id",
    "_underscore_start",
    "a",
    "a" * 63,  # exactly NAMEDATALEN limit
])
def test_accepts_safe_identifiers(good):
    validate_pg_schema(good, "ICEBOX_PG_SCHEMA")


@pytest.mark.parametrize("bad", [
    "",                       # empty
    "1starts_with_digit",     # leading digit
    "has space",              # whitespace
    "has\tab",                # tab
    "has\nnewline",           # newline — would break conninfo
    "has;semicolon",
    "has-hyphen",
    "has.dot",
    "has/slash",
    "Uppercase",
    "snake_Case",
    "a" * 64,                 # one over NAMEDATALEN
    "unicode_é",
])
def test_rejects_unsafe_identifiers(bad):
    with pytest.raises(RuntimeError, match="ICEBOX_PG_SCHEMA"):
        validate_pg_schema(bad, "ICEBOX_PG_SCHEMA")


@pytest.mark.parametrize("reserved", sorted(RESERVED_SCHEMA_NAMES))
def test_rejects_reserved_schema_names(reserved):
    with pytest.raises(RuntimeError, match="reserved"):
        validate_pg_schema(reserved, "ICEBOX_PG_SCHEMA")


def test_rejects_pg_prefix():
    """The `pg_*` prefix is reserved by Postgres — CREATE SCHEMA refuses
    these with SQLSTATE 42939. Catch at config-load with a clear message."""
    with pytest.raises(RuntimeError, match="reserved"):
        validate_pg_schema("pg_custom_namespace", "ICEBOX_PG_SCHEMA")


@pytest.mark.parametrize("keyword", sorted(RESERVED_SQL_KEYWORDS))
def test_rejects_sql_reserved_words(keyword):
    with pytest.raises(RuntimeError, match="reserved"):
        validate_pg_schema(keyword, "ICEBOX_PG_SCHEMA")


def test_env_var_name_appears_in_error():
    """Caller passes its own env-var name so the error points at the
    right variable on each side (writer vs. icebox)."""
    with pytest.raises(RuntimeError, match="MY_OWN_VAR"):
        validate_pg_schema("bad name", "MY_OWN_VAR")


def test_regex_pattern_is_anchored():
    """Defense in depth: the regex must anchor both ends, else a value
    like `'icebox; DROP TABLE'` could match the leading `icebox` and
    pass."""
    assert SAFE_PG_IDENTIFIER.pattern.startswith("^")
    assert SAFE_PG_IDENTIFIER.pattern.endswith("$")
    assert SAFE_PG_IDENTIFIER.match("icebox; DROP TABLE x") is None
