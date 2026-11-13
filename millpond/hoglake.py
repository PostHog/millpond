"""Hoglake backend: write Arrow batches to a hoglake catalog via pyhoglake.

The hoglake control plane is a *service* (Kotlin/Ktor + Postgres): clients
write parquet to object storage themselves and register the files via
footer-shipping commits — the server never opens data files on the write
path. pyhoglake owns that writer path (field-id-stamped parquet encoded
straight from Arrow to a buffer, footer stat extraction, concurrent
single-request uploads, one-commit registration, partitioned fanout
appends, and the `read_snapshot` basis the server's conflict check
answers "did DDL touch this table since my read" against);
this module owns everything millpond-shaped around it:

* startup catalog resolution (in `__init__`, so a bad URL or an absent
  catalog fails before the pod claims readiness) and first-write
  bootstrap of namespace/table, concurrent-creation tolerant at every
  level;
* the partition spec and sort order: declared from millpond's config at
  table creation, VERIFIED afterwards, and reconciled against the live
  table on every later resolve — a config that disagrees with the table
  it writes to stops the pod rather than silently writing under a layout
  nobody declared;
* the `_inserted_at` metadata column (stamped once per flush — unlike
  DuckLake's SQL `NOW()`, every row in a flush carries the same value);
* schema evolution mirroring schema.SchemaManager's semantics: new
  batch columns become `add_column` alters, `int -> long` /
  `float -> double` mismatches become `promote_column` alters, failures
  are logged + metricked + degraded per column, never fatal to the flush;
* batch/table alignment: pyhoglake's `_align_table` REFUSES both extra
  and missing columns, so columns the table doesn't have (failed adds)
  are dropped and table columns the batch lacks are null-filled before
  `append()` (the null-fill mirrors DuckLake's `INSERT BY NAME`).

Events land as TEXT — `properties` and friends stay JSON strings, exactly
as the arrow_converter produces them — UNLESS the destination declares a
column `variant`. Then the same-named string column of the batch is fed
to pyhoglake as `pa.json_()` text (a zero-copy retype, no parse here) and
`prepare_append_tables` encodes it into the column's declared shredded
layout, one group at a time, before the first upload. millpond never
creates or alters a variant column: the table is pre-created with its
declarations, and the sink only writes into what it finds (see
`_prepare`, `_null_fill_missing`, `_observe_variant_rows`).

At-least-once: `write()` raises on any failure; main.py's retry loop
backs off, calls `reset_caches()` (which drops the resolved
catalog/namespace/table handles so the next attempt re-resolves), and
Kafka offsets commit only after `write()` returns. A commit refused by
the server (409) may orphan an uploaded parquet file — that is cleanup's
problem by hoglake design, never the catalog's, and never a duplicate
row.

What the idempotency key buys, precisely, because the difference
matters at 3am:

* IN PROCESS it is exact. The uploaded registration is held in memory
  for the life of the flush, so a retry after a lost response is the
  same request byte for byte, and the server answers it from its
  receipt. A commit whose response is lost publishes its rows exactly
  once. This holds without the key being derived from anything at all —
  a random key per flush would do — but a derived one costs nothing and
  is what makes the next paragraph possible.
* ACROSS A RESTART it is opportunistic. Nothing is held: the rebuilt
  flush stamps a fresh `_inserted_at` and uploads under fresh object
  names, so the replayed request is never byte-identical, and the most
  the key can do is recognize a repeated BOUNDARY (same table
  incarnation, same complete offset range per partition) and decline to
  publish over it a second time. The boundary is NOT reproducible in
  general — the size trigger accumulates per poll batch, the time
  trigger is wall-clock, the filter's allowlist is mutable, and every
  partition in the flush has to coincide — so the pipeline is
  at-least-once across process boundaries, with the duplicate suppressed
  in the case where the boundary does repeat.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from collections.abc import Generator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
import pyarrow as pa
import pyarrow.compute as pc
from pyhoglake import (
    AlreadyExistsError,
    AlterOp,
    Column,
    CommitConflictError,
    DdlSinceReadSnapshotError,
    ExpiredError,
    HoglakeClient,
    HoglakeError,
    IdlessFilesPresentError,
    IncarnationChangedError,
    MalformedResponseError,
    NotFoundError,
    ObjectPrefixViolationError,
    ReadSnapshotExpiredError,
    S3Config,
    UnsupportedTypeError,
    ValidationError,
    VariantEncodingError,
    VariantReport,
    object_prefix,
    ops,
    transforms,
)
from pyhoglake.auth import BearerToken, ClientCredentials
from pyhoglake.types import arrow_type_to_coltype, column_to_arrow_field, columns_to_arrow_schema

from millpond import arrow_converter, metrics
from millpond.config import _LIVENESS_BUDGET_S, Config, python_variant_codec_refusal, variant_codec_backend
from millpond.config import hoglake_flush_deadline as config_hoglake_flush_deadline
from millpond.sink import SAFE_IDENTIFIER, check_reserved_collision

log = logging.getLogger(__name__)

# Deliberately NARROWER than ducklake.RESERVED_COLUMNS, which also holds
# `year/month/day/hour`. Those names are load-bearing for DuckLake's
# Hive-style partitioning, which materializes a derived column per
# partition key. Hoglake partitions by Iceberg-semantics transforms
# recorded in the catalog: `month(_inserted_at)` produces a partition
# VALUE on the data file, never a `month` column. So for this backend
# those four are ordinary payload keys and reserving them would be
# fatal for no reason — `check_reserved_collision` raises, main.py's
# retry loop cannot make a ValueError go away, and one producer key
# named `month` wedges the partition forever. (The contrast that makes
# it indefensible: a WORSE-formed key, `month-of-year`, is merely
# dropped.) The asymmetry is safe for a deployment-time destination
# switch because it only runs one way: hoglake accepts every batch
# DuckLake accepted, plus four names DuckLake refused. See sink.py.
RESERVED_COLUMNS: frozenset[str] = frozenset({"_inserted_at"})

# How the object-store credentials were resolved — logged once at
# startup and named in the probe's refusal, because the two shapes have
# completely different remedies: a Secret versus an IAM role and bucket
# policy. The values are prose, not identifiers; they appear verbatim in
# an operator-facing message.
_STATIC_KEYS = "static HOGLAKE_S3_* keys"
_DEFAULT_CHAIN = "the AWS default credential chain (IRSA in Kubernetes)"

# The startup probe's marker object, relative to the catalog data path.
# FIXED, so every boot overwrites the same key: at most one object per
# data path, and it is never registered in a table, so hoglake leaves it
# alone (cleanup drains only server-queued paths; verify is
# metadata-only).
_PROBE_MARKER = "_millpond/probe"


def _probe_marker_uri(data_path) -> str:
    """The marker's full s3:// URI. The data path comes off the catalog
    row as the operator typed it, so the trailing slash is normalised
    here rather than trusted — otherwise the same catalog gets two
    markers depending on how it was created."""
    return f"{str(data_path).rstrip('/')}/{_PROBE_MARKER}"


def _probe_hint(sdk_error: str, credential_source: str) -> str:
    """The one sentence an operator needs after a failed probe.

    The AWS SDK's text is precise and unhelpful in equal measure: the
    failures that actually happen say nothing about the setting that
    causes them, so each names its own.
    """
    if "NoSuchBucket" in sdk_error or "NO_SUCH_BUCKET" in sdk_error:
        return (
            "The bucket in that path does not exist, or this identity cannot see it. The data path "
            "is the one frozen into the catalog row when the catalog was created — HOGLAKE_DATA_PATH "
            "is read only at creation, so changing it now moves nothing; for an existing catalog the "
            "fix is the bucket (create it, or grant this identity access to it). hoglake has no "
            "delete-catalog route, so a catalog created against a typo needs a new catalog name."
        )
    if "AuthorizationHeaderMalformed" in sdk_error or "PermanentRedirect" in sdk_error:
        return (
            "That is a region mismatch: the request was signed for one region and the bucket lives "
            "in another. Set HOGLAKE_S3_REGION to the bucket's region (under the default credential "
            "chain the SDK otherwise takes AWS_REGION, which the IRSA webhook injects)."
        )
    if "INVALID_ACCESS_KEY_ID" in sdk_error or "SIGNATURE_DOES_NOT_MATCH" in sdk_error:
        if credential_source == _STATIC_KEYS:
            return (
                "S3 rejected the key id or the signature, so the static HOGLAKE_S3_* keys are wrong "
                "(stale Secret, wrong account, truncated value). This pair of errors is only "
                "reachable with static keys set — unset both to use the default credential chain."
            )
        return (
            "S3 rejected the key id or the signature of a credential the default chain resolved — "
            "something in the pod's environment is supplying stale explicit credentials (AWS_* env "
            "vars, a mounted profile) ahead of the web-identity token."
        )
    return (
        "The write path needs s3:PutObject under this data path (plus s3:AbortMultipartUpload to "
        "clean up an interrupted upload) and nothing else — check the bucket policy and the "
        "identity it grants them to."
    )


_INSERTED_AT = "_inserted_at"
_INSERTED_AT_TYPE = pa.timestamp("us", tz="UTC")

# The server reserves the `_hog` column-name prefix for its internals
# (`_hog_row_id` is compaction's row-id carrier); DDL and appends 422 on
# it, and pyhoglake fast-fails it client-side. A payload key with that
# prefix must be dropped, not sent — otherwise one poison producer key
# wedges the partition forever (append fails, offsets never advance).
_HOG_RESERVED_PREFIX = "_hog"

# The server's column-name rule is `^[A-Za-z_][A-Za-z0-9_-]{0,127}$` —
# 128 characters, total. millpond's shared SAFE_IDENTIFIER has no length
# bound (DuckDB has no such limit), so the cap is applied here, on the
# hoglake side only. Without it the two paths disagree: in steady state
# an over-long name degrades (its `add_column` 422s and the column is
# dropped for the flush), but BOOTSTRAP ships the whole schema in a
# single `create_table` — one 129-character key in the first batch 422s
# the create, so the table is never made, offsets never advance, and the
# pod crash-loops on the same batch forever.
_MAX_COLUMN_NAME_LEN = 128

# Columns already warned about for a uuid/string rewrite below, keyed by
# (column, live type, batch type), so a permanent config/table mismatch logs
# once per column per direction per pod lifetime rather than once per flush.
# The metric is the always-on signal.
_uuid_rewrite_warned: set[tuple[str, str, str]] = set()

# Widenings mirroring SchemaManager's ALTER COLUMN path, restricted to
# the promotions the hoglake server accepts (its matrix follows
# DuckLake's documented table): int -> long, float -> double. The
# narrower int8/int16 rungs are NOT promoted — millpond never creates
# them, and a foreign table's narrow column failing the append-side cast
# is the loud outcome we want.
_PROMOTIONS: dict[tuple[str, str], str] = {
    ("int", "long"): "long",
    ("float", "double"): "double",
}

# (live catalog type, batch's mapped type) pairs whose batch column is
# REWRITTEN before the append, because neither a promotion nor the append-side
# cast can resolve them — both directions of the uuid/string mismatch, and
# nothing else. See `_rewrite_column` for why these two and not the general
# case.
_UUID_REWRITES: frozenset[tuple[str, str]] = frozenset({("string", "uuid"), ("uuid", "string")})

# The two values pyhoglake's `variant_on_invalid` takes; config.load()
# validates MILLPOND_HOGLAKE_VARIANT_ON_INVALID against the same pair.
_VARIANT_ON_INVALID: frozenset[str] = frozenset({"null", "raise"})


def _variant_backend_label() -> str:
    """Which codec pyhoglake encodes variant rows with, for this process:
    `native`, `python` or `unknown`. `config.variant_codec_backend`, which
    load() uses for the startup refusal; one reader, so the label on the
    encode histogram and the refusal cannot disagree."""
    return variant_codec_backend()


# The label vocabularies pyhoglake counts the Rust codec's fallbacks and
# `auto`'s routes to Python under (`pyhoglake._variant_backend`, module
# docstring), the same tuples millrace exports at 0 from its first scrape.
# Pre-created at 0 here too so `increase()` counts a pod's first.
_CODEC_ENCODE_REASONS = ("mismatch", "type", "panic", "shadow_mismatch")
_CODEC_DECODE_REASONS = ("mismatch", "panic")
_CODEC_ROUTED = (
    *(("encode", why) for why in ("unavailable", "plan", "not_utf8")),
    *(("decode", why) for why in ("unavailable", "storage")),
)
# What `PYHOGLAKE_VARIANT_CODEC=native` raises when the Rust codec is not
# importable or does not take the call: a bare `RuntimeError(f"{ENV}=native,
# but {why}")` out of `prepare_append_tables`. Matched on its text because
# that is all pyhoglake gives it; a typed exception would be better and is
# pyhoglake's to add.
_NATIVE_CODEC_REFUSAL_PREFIX = "PYHOGLAKE_VARIANT_CODEC=native, but "

# `json_text_columns`' read-only resolve of the destination before a
# conversion ("the peek"). It runs on the consume thread, so it is bounded
# in two regimes:
#
# * RESOLVING the table handle, once per process (and again only after
#   the table was found missing): `Namespace.table()` is `GET
#   /tables/{name}` WITHOUT `totals=false`, i.e. the server's totals scan
#   over every live file row — seconds on a big table, which is why it
#   runs under the write path's request timeout (as `_ensure_table`'s
#   own resolve does), clamped to `_PEEK_RESOLVE_TIMEOUT_S`, and never
#   under the short one below: a 5 s cap on that scan would fail every
#   pod's first peek on a healthy catalog. `_PEEK_ATTEMPTS` tries with a
#   `_PEEK_BACKOFF_S` ladder when nothing has ever been read in this
#   process; worst case against a hung catalog at pod start is by
#   construction under `_PEEK_BUDGET_SHARE` of the liveness budget.
# * REFRESHING a held handle: one `info(totals=False)` GET, no scan, under
#   `_PEEK_TIMEOUT_S` (the peek client's timeout is lowered once the
#   handle is held), ONE attempt, and after a failure not again for
#   `_PEEK_RETRY_AFTER_S` (the known answer is served meanwhile), so a
#   long outage costs the consume thread at most one short request a
#   minute.
_PEEK_ATTEMPTS = 2
_PEEK_BACKOFF_S = 1.0
_PEEK_TIMEOUT_S = 5.0
_PEEK_RETRY_AFTER_S = 60.0
# The resolve's per-request timeout: the write timeout, CLAMPED so the
# never-read worst case (`_PEEK_ATTEMPTS` x `_PEEK_RESOLVE_REQUESTS`
# requests, each hung to its timeout, plus the backoff) stays inside
# `_PEEK_BUDGET_SHARE` of the liveness budget. config allows a write
# timeout of several hundred seconds with HOGLAKE_MAX_RETRY_COUNT=1, and
# the peek runs on the consume thread before the first poll is recorded.
# At the defaults: (0.6 x 480 - 1) / (2 x 3) = 47.8 s, so the default 45 s
# write timeout is not clamped.
_PEEK_RESOLVE_REQUESTS = 3  # list_namespaces, the table GET, info
_PEEK_BUDGET_SHARE = 0.6
_PEEK_RESOLVE_TIMEOUT_S = (
    _LIVENESS_BUDGET_S * _PEEK_BUDGET_SHARE - sum(_PEEK_BACKOFF_S * (i + 1) for i in range(_PEEK_ATTEMPTS - 1))
) / (_PEEK_ATTEMPTS * _PEEK_RESOLVE_REQUESTS)
# A failed peek with a last-known answer logs at most this often; the
# count is on `millpond_errors_total{type="hoglake_shape_peek"}`.
_PEEK_LOG_INTERVAL_S = 60.0


def _is_native_codec_refusal(exc: BaseException) -> bool:
    """A `RuntimeError` from pyhoglake's codec switch saying the Rust codec
    was required and is not there (or does not take this plan).
    PERMANENT: the wheel does not appear by waiting, and the plan is the
    table's declaration. `is_retryable` would otherwise read a bare
    RuntimeError as "unknown, assume transient" and burn the ladder."""
    return (
        type(exc) is RuntimeError  # not a HoglakeSinkError, which subclasses it with its own verdict
        and str(exc).startswith(_NATIVE_CODEC_REFUSAL_PREFIX)
    )


class _VariantCodecCounts:
    """Mirror pyhoglake's process-wide codec counters onto millpond's
    labelled Counters, by delta.

    pyhoglake counts, per process and under its own lock, the calls
    `auto` routed to Python (`ROUTED`, by (op, why)), the Rust results
    Python redid (`ENCODE_FALLBACKS`, `DECODE_FALLBACKS`, by reason) and
    what the shadow compared (`SHADOWED`: calls, rows). millrace exports
    them straight from a custom collector at scrape time; millpond's
    metrics are prometheus_client Counters behind the common
    (pipeline, broker_source) labels, which cannot be set, only
    incremented — so this keeps the last snapshot it mirrored and books
    the difference. Synced after every prepare, success or failure (the
    counters only move inside encode calls, and every encode call is
    inside a prepare), and once at construction so the label sets exist
    at 0 from the first scrape.

    Best effort throughout: `_variant_backend` is private to pyhoglake,
    and a pyhoglake that renames a counter must cost a metric, never a
    flush.
    """

    def __init__(self) -> None:
        self._seen: dict[tuple[str, tuple], int] = {}

    def prime(self) -> None:
        for reason in _CODEC_ENCODE_REASONS:
            metrics.variant_codec_fallback_total.labels(reason=reason).inc(0)
        for reason in _CODEC_DECODE_REASONS:
            metrics.variant_codec_decode_fallback_total.labels(reason=reason).inc(0)
        for op, reason in _CODEC_ROUTED:
            metrics.variant_codec_routed_to_python_total.labels(op=op, reason=reason).inc(0)
        metrics.variant_shadow_calls_total.inc(0)
        metrics.variant_shadow_rows_total.inc(0)
        self.sync()

    def sync(self) -> None:
        try:
            from pyhoglake import _variant_backend as backend

            with backend._COUNTING:
                encode = dict(backend.ENCODE_FALLBACKS)
                decode = dict(backend.DECODE_FALLBACKS)
                routed = dict(backend.ROUTED)
                shadowed = dict(backend.SHADOWED)
        except Exception:  # noqa: BLE001 - a metric, never a flush
            log.debug("Could not read pyhoglake's variant codec counters; the codec metrics stay where they are")
            return
        for reason, total in encode.items():
            self._book("encode", (reason,), total, metrics.variant_codec_fallback_total.labels(reason=reason))
        for reason, total in decode.items():
            self._book("decode", (reason,), total, metrics.variant_codec_decode_fallback_total.labels(reason=reason))
        for (op, reason), total in routed.items():
            child = metrics.variant_codec_routed_to_python_total.labels(op=op, reason=reason)
            self._book("routed", (op, reason), total, child)
        self._book("shadow", ("calls",), shadowed.get("calls", 0), metrics.variant_shadow_calls_total)
        self._book("shadow", ("rows",), shadowed.get("rows", 0), metrics.variant_shadow_rows_total)

    def _book(self, family: str, key: tuple, total: int, child) -> None:
        delta = total - self._seen.get((family, key), 0)
        if delta > 0:
            child.inc(delta)
        self._seen[family, key] = total


# 4xx codes that mean "not now" rather than "not ever". Everything else
# in the 4xx range is a request the server will refuse identically
# forever. 429 is generic rate limiting; 408 is the server giving up on
# a slow request. Hoglake's own admission backpressure is a 503, which
# the 5xx rule below already covers.
_RETRYABLE_CLIENT_STATUS: frozenset[int] = frozenset({408, 429})

# Statuses whose `Retry-After` header is worth honoring: the server is
# saying "not now, try again in N seconds". 503 is hoglake's commit
# admission backpressure (`commit_queue_timeout`, `Retry-After: 1`).
_RETRY_AFTER_STATUS: frozenset[int] = frozenset({429, 503})

# Base backoff for this backend's retry ladder, matching main.py's
# DuckLake default. The doubling and its ceiling live in main.py; only
# the attempt count is configurable, because that is the part DuckLake's
# inner loop was silently providing.
_WRITE_BASE_DELAY_S = 1.0

# Namespace for the UUIDv5 commit idempotency keys. A fixed, private
# namespace means the key for a given (table, offset range) is stable
# across pods, restarts and releases — which is the only reason a retry
# from a NEW process can still be recognized as the same publication.
# Never change it: doing so re-randomizes every in-flight flush's
# identity and reopens the duplicate window for exactly one restart.
_IDEMPOTENCY_NAMESPACE = uuid.UUID("6f1b6d2e-4c5a-5f3e-9b7a-2d8c1e0a4f77")

# The server's 422 when a key is replayed with a payload that is not the
# one the receipt was written for: "idempotency_key reused with a
# different request". Matched as a substring because it is a message,
# not a code — but the condition it reports is unambiguous, and the
# alternative (treating it as a generic 422) crash-loops the pod forever
# on a flush whose rows are already in the lake.
_REUSED_KEY_MARKER = "idempotency_key reused"

# The refusals pyhoglake raises when a PREPARED file's columns do not
# match the destination's, quoted from its source so a re-align is
# attempted for the cases a re-align can actually fix:
#   * pyhoglake/client.py:2126 (`_encode_group`, 1.3.7) — the strict
#     schema/field-id comparison on the ordinary (non-variant) prepare
#     path, which is the one millpond takes. It runs on the ENCODED
#     footer, not on the Arrow schema handed in, which is what makes it
#     the check a stale cached shape trips: see `_table_info`;
#   * pyhoglake/parquet_schema.py:197 — the same refusal on the variant
#     validation path.
# Deliberately NOT here: "prepared file partition arity differs from
# destination" (a spec change, which re-aligning columns cannot fix) and
# "data is missing table columns", which only `_align_table` raises —
# and this sink stopped calling `Table.append` when it moved to prepared
# commits, so matching it was matching a string that can no longer
# reach us.
_ALIGNMENT_REFUSALS: tuple[str, ...] = (
    "prepared Parquet schema/field IDs differ from destination",
    "prepared Parquet columns differ from destination",
)

# The server's typed "the destination moved under your basis" refusals —
# the three pyhoglake flags `re_prepare` rather than `retryable`. Every
# one of them is a commit the server judged and refused atomically with
# zero rows written, and every one of them is cleared by a flush REBUILT
# against a fresh read rather than by resending this payload: a prepared
# request's `read_snapshot` is frozen, the expiry floor only moves
# forward, and an alter does not un-happen.
#
# BOTH are subclasses, and the ARM ORDER in `_commit_prepared` is what
# keeps each on the right path — not, as an earlier version of this
# comment claimed, a difference in whether the payload is replayed. It
# never is: `_is_answered_refusal` discards the payload for ANY 409, a
# plain `commit_conflict` included, and that is deliberate (see
# `_commit_prepared`). What the ordering actually buys:
#
#   * `DdlSinceReadSnapshotError` subclasses `CommitConflictError`, so
#     without the typed arm first it would be discarded and then
#     classified RETRYABLE by `is_retryable`'s conflict arm — correct by
#     accident, since the rebuild does clear it, but it would reach
#     main.py as "the catalog is contended" and be counted as OCC
#     contention rather than as a layout race.
#   * `ReadSnapshotExpiredError` subclasses `ExpiredError`, so without
#     the typed arm it would be discarded and then classified PERMANENT
#     by `is_retryable`'s `ExpiredError` arm — main.py re-raises
#     immediately and the pod crashes on a flush a rebuild publishes
#     cleanly. That one is not correct by accident, which is why the
#     order is load-bearing rather than tidy.
_DESTINATION_MOVED: tuple[type[HoglakeError], ...] = (
    DdlSinceReadSnapshotError,
    IncarnationChangedError,
    ReadSnapshotExpiredError,
)

# `records_skipped_total{reason=}` for a row whose object-prefix value
# names no `<field>=<value>/` prefix (D3): null, negative, not the
# canonical decimal, out of the type's range — exactly what
# `object_prefix.parse_value` rejects, which is the predicate pyhoglake
# refuses a group on (a `ValidationError` before any upload, permanent
# under `is_retryable`), so a batch that kept such a row would wedge its
# partition on it. The group is skipped after the fanout and its rows
# counted here instead. See `_skip_unplaceable_groups`.
_UNPLACEABLE_REASON = "unplaceable_object_prefix_value"

# How many distinct unplaceable values one warning line names.
_UNPLACEABLE_VALUES_LOGGED = 10

# `hoglake_orphaned_files_total{reason=}` for a payload the server refused
# as 422 `object_prefix_violation`: files not placed where the table's
# object-prefix layout admits them. See `_object_prefix_refused`.
_OBJECT_PREFIX_REFUSED_REASON = "object_prefix_violation"


# What the sink assumes the catalog's snapshot retention is when it
# cannot read one, mirroring pyhoglake's own
# `_ASSUMED_RETENTION_SECONDS`. Mirrored rather than imported because it
# is a private module constant; the VALUE matters less than the two
# sides agreeing, and `_live_info_ttl_s` explains why.
_ASSUMED_RETENTION_S = 1800.0

# How long the offsets line of a commit message may be, in bytes. The
# server stores `message` as unbounded text, so this bound is millpond's
# own: a snapshot row whose message is larger than the rest of the
# snapshot serves nobody.
#
# Sized so that ONE pod owning an entire 512-partition topic still
# writes every range — 512 ranges of the events topic measure about
# 10.5 KiB, and a shrunk fleet or a single-replica deployment really does
# produce that. The production shape (512 partitions over 16 pods) is
# about 32 ranges. 16 KiB holds about 780, so truncation is now reserved
# for a partition count no millpond topic has.
_MESSAGE_OFFSETS_LIMIT = 16384

# What the message calls each flush trigger. `metrics.batches_flushed_total`
# labels the interval trigger `time`, and that label value stays as it
# is because dashboards and alerts already read it; the message says
# `interval`, which is what the setting is called
# (MILLPOND_FLUSH_INTERVAL_MS). The two vocabularies are mapped here, in
# one place, rather than by renaming a live metric label. Anything not
# in this table is `unknown` — a caller that supplies no trigger, and a
# future trigger nobody taught this table about, both name themselves
# honestly instead of claiming one of the three.
_MESSAGE_TRIGGERS: dict[str | None, str] = {
    "size": "size",
    "time": "interval",
    "interval": "interval",
    "final": "final",
}
_UNKNOWN_TRIGGER = "unknown"


class TokenFile(httpx.Auth):
    """A fixed token read from a file, re-read when the file changes.

    The shape a mounted Kubernetes Secret takes: the kubelet rewrites the
    file on rotation (a new mtime), and a 401 — the server no longer
    accepting what was sent — is the other signal that the file may have
    moved on, so that re-reads once and retries. pyhoglake's BearerToken
    is the inline form of the same thing; this is the one that survives a
    rotation without a restart. The token is never logged.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.Lock()
        self._token: str | None = None
        self._mtime_ns: int | None = None

    def _read(self, *, force: bool = False) -> str:
        with self._lock:
            try:
                mtime_ns = os.stat(self._path).st_mtime_ns
            except OSError as e:
                if self._token is not None:
                    # A rotation in progress can briefly leave no file
                    # (the kubelet swaps a symlink); keep what we have.
                    return self._token
                raise HoglakeSinkError(f"hoglake token file {self._path!r} cannot be read: {e}") from e
            if force or self._token is None or mtime_ns != self._mtime_ns:
                with open(self._path, encoding="utf-8") as f:
                    token = f.read().strip()
                if not token:
                    if self._token is not None:
                        return self._token
                    raise HoglakeSinkError(f"hoglake token file {self._path!r} is empty")
                if self._token is not None and token != self._token:
                    log.info("hoglake credential file %s changed; reloaded", self._path)
                self._token, self._mtime_ns = token, mtime_ns
            return self._token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        token = self._read()
        request.headers["Authorization"] = f"Bearer {token}"
        response = yield request
        if response.status_code != 401:
            return
        fresh = self._read(force=True)
        if fresh == token:
            return
        request.headers["Authorization"] = f"Bearer {fresh}"
        yield request


def make_catalog_auth(cfg: Config) -> httpx.Auth | None:
    """The credential `HoglakeClient` attaches to every catalog request,
    from the HOGLAKE_AUTH_* config (validated in config._hoglake_auth):
    pyhoglake's ClientCredentials, its BearerToken, a TokenFile, or None
    for a server that does not ask (HOGLAKE_AUTH_MODE=off or warn)."""
    if cfg.hoglake_auth_token_url is not None:
        return ClientCredentials(
            cfg.hoglake_auth_token_url,
            cfg.hoglake_auth_client_id,
            cfg.hoglake_auth_client_secret,
            audience=cfg.hoglake_auth_audience or "hoglake",
        )
    if cfg.hoglake_auth_token is not None:
        return BearerToken(cfg.hoglake_auth_token)
    if cfg.hoglake_auth_token_file is not None:
        return TokenFile(cfg.hoglake_auth_token_file)
    return None


class HoglakeSinkError(RuntimeError):
    """A refusal raised by THIS module, not by the server or the client.

    Spec reconciliation, the partition-column checks, the declaration
    post-condition and the prepared-payload invariants are millpond's own
    safety stops: they are decisions about the CONFIG and the code, and
    no amount of waiting changes either. They were previously plain
    `RuntimeError`s, which `is_retryable` classed as "unknown, assume
    transient" — so the loudest stops in the sink were also the slowest,
    burning the whole retry ladder before the operator saw the message.

    Subclasses `RuntimeError` so callers that catch the historical type
    (and the tests that assert on it) keep working. `retryable=True` is
    the one exception, for a stop a REBUILT flush really can clear — the
    partition spec changing under an already-prepared payload.
    """

    def __init__(self, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.retryable = retryable


def is_retryable(exc: BaseException) -> bool:
    """Classify a write-path failure: is a fresh attempt (after
    reset_caches) worth anything?

    WIRED IN, not advisory: `HoglakeSink.is_retryable` exposes this to
    `main._write_with_retry`, which skips the remaining budget and
    re-raises immediately when it answers False. The alternative —
    letting the loop burn its whole ladder on a 422 that cannot become
    valid by waiting — delays the crash the operator needs to see and
    buries the real error under repeated identical failures.

    Retryable — transient by nature:
      * `CommitConflictError` (OCC 409; the server says retry on a fresh
        baseline, and `retryable=True` on the class agrees)
      * `NotFoundError` (table/namespace dropped mid-run; the next
        attempt re-ensures it)
      * `IncarnationChangedError` — BOTH shapes. The server raises it as
        a 409 carrying the recreation marker; the client's own pre-flight
        re-resolve raises it with no status at all. Either way the table
        was dropped and recreated under the same name, reset_caches
        re-resolves the live incarnation, and millpond's contract is
        "write to the name" — so adopting the new incarnation on the NEXT
        attempt is correct, and the refused commit registered zero rows.
      * transport errors / timeouts (httpx), OSError (pyarrow S3 upload)
      * any HoglakeError with a 5xx status (503 `commit_queue_timeout` is
        the server's explicit "the catalog is convoyed, come back" —
        retrying the identical commit is what its own docs ask for)
      * 408 / 429 — the two 4xx codes that mean "not now"
      * a HoglakeError with NO status: the request never reached the
        server, so nothing about it has been judged
      * anything unknown (assume transient — crashing the pod after the
        retry budget is the loud fallback either way)

    Not retryable — the request itself is wrong and will stay wrong:
      * `HoglakeSinkError` — THIS sink's own safety stops (a spec that
        disagrees with config, a partition column that is not in the
        table, a declaration the server did not apply). They are
        statements about the config or the code, and a retry cannot
        change either. The ones flagged `retryable=True` are the
        exception: `_destination_moved` raises one for each of the
        server's three re-prepare refusals, and a REBUILT flush
        genuinely clears all three.
      * `ValueError` / `KeyError` / `TypeError` — millpond's own
        validation (`check_reserved_collision`) and the pyarrow
        misuse that a schema race can produce. Same argument: waiting
        does not make a colliding column name stop colliding.
      * `ValidationError` (422), `UnsupportedTypeError`,
        `AlreadyExistsError` (409 on a create), `ExpiredError` (410 — a
        read_snapshot below the catalog's expiry floor; only a fresh plan
        fixes that), `MalformedResponseError` (wire-contract violation),
        and any other 4xx. `ObjectPrefixViolationError` (422
        `object_prefix_violation`, a `ValidationError`) is permanent HERE
        too, deliberately: raised raw it means pyhoglake's own pre-upload
        placement check refused the batch (a null or non-canonical prefix
        value the sink did not drop), which a retry replays. The commit-
        time shape — the server refusing files a stale layout placed
        under the table root — is converted by `_commit_prepared` into a
        `HoglakeSinkError(retryable=True)` exactly once per flush
        (`_object_prefix_refused`), because a rebuild against a fresh
        read is what the server's `retry: re-prepare` asks for; the
        second refusal of the same flush is permanent.

    THE THREE TYPED COMMIT REFUSALS DO NOT REACH HERE, and that is why
    their classification below reads oddly: `DdlSinceReadSnapshotError`
    (a `CommitConflictError` subclass, so the retryable arm would claim
    it) and `ReadSnapshotExpiredError` (an `ExpiredError`, so the
    permanent arm would) are both converted by `_commit_prepared` into
    `HoglakeSinkError(retryable=True)` — one verdict, carried on the
    exception itself, for the one recovery all three have. Reading the
    flags here instead would say "retryable" for a payload whose basis is
    frozen, which is a livelock, and "permanent" for a flush a rebuild
    publishes cleanly, which is a crash. `IncarnationChangedError` keeps
    its own retryable verdict for the PRE-UPLOAD shape, which is still
    raised straight out of the client's pre-flight.
    """
    # This sink's own refusals, first: they carry their own verdict.
    if isinstance(exc, HoglakeSinkError):
        return exc.retryable
    # Retryable subclasses first — both are 409s, and the generic 4xx
    # rule below would otherwise swallow them.
    if isinstance(exc, CommitConflictError | IncarnationChangedError):
        return True
    if isinstance(
        exc,
        MalformedResponseError | ValidationError | UnsupportedTypeError | AlreadyExistsError | ExpiredError,
    ):
        return False
    if isinstance(exc, HoglakeError):
        status = exc.status_code
        if status is None:
            # Never reached the server (connection setup, body parse,
            # client-side guard): nothing was judged, so retry.
            return True
        if status in _RETRYABLE_CLIENT_STATUS:
            return True
        if 400 <= status < 500:
            # 404 is the exception: a dropped table is re-ensured on retry.
            return isinstance(exc, NotFoundError)
        return True
    if isinstance(exc, httpx.HTTPError | OSError):
        return True
    # pyarrow, explicitly and BEFORE the ValueError/TypeError arm below.
    # `ArrowInvalid` is a `ValueError` and `ArrowTypeError` a `TypeError`, so
    # two of the three were already permanent — by inheritance, which is not a
    # thing to depend on — while `ArrowNotImplementedError` is a
    # `NotImplementedError` and fell through to the transient default. All
    # three say the same thing: the kernel refused THESE bytes, and the retry
    # replays the identical batch through the identical cast. Waiting cannot
    # change the answer; it only delays the crash the operator needs.
    if isinstance(exc, pa.ArrowInvalid | pa.ArrowTypeError | pa.ArrowNotImplementedError):
        return False
    # OSError is checked above (pyarrow's S3 upload raises it), so these
    # are the in-process ones: a bad value, a missing key, a wrong type.
    # Nothing about waiting fixes any of them.
    if isinstance(exc, ValueError | KeyError | TypeError):
        return False
    return True


def _error_text(exc: HoglakeError) -> str:
    """Everything the server said, lower-cased.

    Both halves, deliberately. hoglake's error body is
    `{error, detail}` and `_raise` maps them to `.message` and
    `.detail` — so for a 422 the message is the CODE ("validation") and
    the sentence is in the detail. A matcher that reads only `.message`
    matches nothing on a real response, and its unit test only passes if
    the fixture has the two fields the wrong way round.
    """
    return f"{exc.message} {exc.detail or ''}".lower()


def _is_answered_refusal(exc: BaseException) -> bool:
    """Did the SERVER judge this request and refuse it?

    A commit is one transaction, so a 409 or a 422 carried back in a
    response means the server wrote nothing and will write nothing for
    any identical resend. That is the rule for whether a prepared payload
    is still worth holding: hold for transport-uncertain (no status, a
    timeout, a reset) and for 5xx, drop for an answered 4xx refusal.

    ALL THREE codes reach here. pyhoglake maps every wire 422 to
    `ValidationError` and every wire 409 to a conflict class, and
    `_commit_prepared` catches their common base in one arm so this
    function is the only place the rule is written down.

    410 joined them with the typed refusals: a commit's 410 is its own
    `read_snapshot` below the catalog's expiry floor
    (`ReadSnapshotExpiredError`), which the server answered by refusing
    to evaluate a conflict window it cannot see — one judged
    transaction, zero rows. It is only ever reached from the commit's
    except arm, where a 410 can mean nothing else. The typed arm above
    it discards the payload itself, so this is the backstop rather than
    the mechanism; it is here because a rule stated for two of three
    answered refusals is a rule nobody can rely on.
    """
    return isinstance(exc, HoglakeError) and exc.status_code in (409, 410, 422)


# How many orphan uris one warning line will carry. The fanout is one
# file per observed partition tuple, so a `team_id`-partitioned flush can
# orphan hundreds at once and the whole list would be the log line. The
# cap is a LOG concern only — the metric books every object either way —
# and the line says how many it left out so nobody reads a truncated list
# as the complete one.
_ORPHAN_URIS_LOGGED = 20


def _count_orphans(count: int, why: str, uris: Sequence[str] = (), *, reason: str) -> None:
    """Record parquet objects uploaded to the lake that no commit
    references.

    `reason` is the metric's `reason` label and `why` the operator-facing
    sentence. Two arguments rather than one because they answer to
    different readers: the label has to be a small fixed vocabulary a
    dashboard can group by, and the sentence has to name this table,
    this fanout width and this status code. Keyword-only so a new call
    site cannot acquire a reason by argument position.

    Nothing on the server side reclaims a client's uploads, so this
    counter is the whole observability story for them; an uncounted
    orphan path is storage nobody can find. Every path that can leave one
    routes through here.

    `uris` name the objects, and naming them is the point. The obvious
    alternative — "sweep the `{idempotency_key}/` prefix" — is WRONG and
    was shipped on this branch: pyhoglake names objects
    `{uuid4}-{index}.parquet` under that prefix, so a retry under the
    SAME key writes fresh names beside the old ones. An operator who
    sweeps the prefix after a later attempt succeeded deletes live,
    committed files. The uris are the only safe unit of cleanup.
    """
    if count <= 0:
        return
    if uris:
        listed = list(uris[:_ORPHAN_URIS_LOGGED])
        omitted = len(uris) - len(listed)
        log.warning(
            "Orphaned %d uploaded parquet file(s) in the lake: %s. Delete these objects by name, "
            "never the prefix they share (a retry under the same idempotency key writes new names "
            "beside them): %s%s",
            count,
            why,
            ", ".join(listed),
            f" — and {omitted} more not listed here" if omitted else "",
        )
    else:
        log.warning("Orphaned %d uploaded parquet file(s) in the lake: %s", count, why)
    metrics.hoglake_orphaned_files_total.labels(reason=reason).inc(count)


def _observe_fanout(files: Sequence[dict]) -> None:
    """Record the flush's object fanout: how many objects it registered,
    and how many rows went into each.

    Called with the REGISTERED file list, from the one place that knows
    the commit landed — the same line that books
    `hoglake_files_written_total`, and for the same reason. A flush that
    raises in prepare or is refused at commit registered nothing, so
    observing it would describe compaction debt the lake does not carry.
    `flush_files` therefore has exactly the failure behaviour the other
    `millpond_flush_*` histograms have: main.py observes those after the
    write returns, never on the way out of a failure.

    The two series answer different halves of one question. The partition
    fanout writes one object per distinct partition tuple, so a flush
    whose rows reach far enough back in time pays an object per old
    partition no matter how few rows that partition brought:
    `flush_files` is how many pieces, `flush_file_rows` is how small the
    pieces are, and only the second can say "most of this flush's objects
    hold tens of rows."

    One observation per object, which is a few hundred per flush at
    production fanout widths — a bucket search each, against a parquet
    encode and an upload per object. Nothing cheaper answers what SHARE
    of the objects are tiny, which is the question.

    `record_count` is a required field of the registration pyhoglake
    built from each object's own parquet footer (the server's tail read
    depends on it), so it is always there and always exact. Read with a
    default anyway: this runs after the rows are published, and a
    histogram is not worth raising a KeyError over a flush that
    succeeded.
    """
    metrics.flush_files.observe(len(files))
    for registration in files:
        rows = registration.get("record_count")
        if rows is not None:
            metrics.flush_file_rows.observe(rows)


def _is_alignment_refusal(exc: ValidationError) -> bool:
    """Is this a "the prepared file's columns are not the destination's"
    refusal, i.e. one a refresh-and-null-fill can actually clear?

    Only pyhoglake's two column-comparison messages qualify. This used to
    take a `KeyError` as well, for the alignment's `select` reporting a
    name the batch lacks — but `_prepare` now null-fills against
    `info.columns` and then selects names from that same object, so the
    select can only ever narrow and no KeyError can come out of it. See
    `_prepare`.

    Narrowed to `ValidationError` in the signature rather than re-checked
    in the body: the sole call site is an `except ValidationError` arm,
    so an isinstance guard here was a branch no input could take — and a
    guard that cannot fail is a guard nobody can test, which is how it
    came to advertise a `BaseException` it never received.
    """
    message = str(exc)
    return any(marker in message for marker in _ALIGNMENT_REFUSALS)


def _json_text_column(column: pa.ChunkedArray) -> pa.ChunkedArray:
    """A batch column headed for a live `variant` column, as JSON text
    pyhoglake's encoder can take: `pa.string()` with SQL NULL where the
    source had nothing to say.

    * Text (string / large_string): returned AS IS — same object, so the
      caller can tell nothing changed — unless it holds an empty string,
      in which case every "" becomes SQL NULL (one vectorised pass).
      "" is not JSON and would land as an `invalid_json` null; a
      producer that writes "" for "nothing" meant SQL NULL, and the
      count on `variant_invalid_total` would be a false alarm.
    * Integers and booleans: pyarrow's cast to string, which is
      vectorised and already JSON (`5`, `true`).
    * Floats and everything else (struct, list, map, timestamp, decimal,
      binary): `arrow_converter.json_text` per row. Floats because
      pyarrow's cast renders `2.0` as `2`, an INTEGER to the encoder,
      which a declared `double` leaf then cannot take (fallback) — orjson
      keeps the point; NaN and the infinities are written as the JSON
      literals pyhoglake refuses (`non_finite_number`, counted) rather
      than orjson's `null`. The rest because `cast(pa.string())` on a
      struct or list raises `ArrowNotImplementedError`, which
      `is_retryable` reads as transient — so one producer sending an
      object where the converter expected text crash-looped the partition
      on the same batch. Per-row Python, but only on a batch shaped this
      way, which the arrow_converter does not produce for a column the
      sink has named (`json_text_columns`): it serializes those itself.
    """
    kind = column.type
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        if not pc.any(pc.equal(column, "")).as_py():
            return column
        return pa.chunked_array([_null_empty_strings(chunk) for chunk in column.chunks], kind)
    if pa.types.is_integer(kind) or pa.types.is_boolean(kind):
        return column.cast(pa.string())
    texts = [None if value is None else arrow_converter.json_text(value) for value in column.to_pylist()]
    return pa.chunked_array([pa.array(texts, pa.string())])


def _null_empty_strings(chunk: pa.Array) -> pa.Array:
    """`chunk` (string / large_string) with every "" made null, WITHOUT
    copying its text: the same offsets and data buffers under a new
    validity bitmap. `pc.if_else` would rebuild the whole column — for a
    9.6 KB-a-row `properties` that is the flush's size again, over one
    producer's empty strings — while the bitmap is a bit a row.

    The bitmap is computed over the slice as pyarrow sees it and then
    padded by the chunk's offset, because `from_buffers` reads every
    buffer, validity included, at that same offset."""
    keep = pc.and_kleene(chunk.is_valid(), pc.invert(pc.fill_null(pc.equal(chunk, ""), False)))
    if chunk.offset:
        keep = pa.concat_arrays([pa.array([False] * chunk.offset, pa.bool_()), keep])
    validity = keep.buffers()[1]
    _old_validity, offsets, data = chunk.buffers()
    return pa.Array.from_buffers(chunk.type, len(chunk), [validity, offsets, data], offset=chunk.offset)


def table_schema_for_batch(batch_schema: pa.Schema) -> pa.Schema:
    """The schema a fresh events table is created with: the batch's own
    columns (as the arrow_converter produced them — TEXT for
    properties/JSON, int64/float64/bool/string/timestamptz otherwise)
    plus the `_inserted_at` timestamptz metadata column."""
    if _INSERTED_AT in batch_schema.names:
        return batch_schema
    return batch_schema.append(pa.field(_INSERTED_AT, _INSERTED_AT_TYPE, nullable=True))


def _drop_unwritable_columns(batch: pa.Table) -> pa.Table:
    """Drop columns whose names the hoglake DDL surface would refuse.

    Mirrors SchemaManager.evolve's unsafe-field-name skip (same metric
    reason, one bump per column per flush): records still land, minus
    the field. Three gates:
      * SAFE_IDENTIFIER — millpond's own generated-DDL posture;
      * the server's 128-character column-name cap, which
        SAFE_IDENTIFIER does not carry;
      * the server-reserved `_hog` prefix — a 422 at append time would
        otherwise wedge the partition on one poison key.
    """
    drop = []
    for name in batch.schema.names:
        if name.lower().startswith(_HOG_RESERVED_PREFIX):
            log.warning("Skipping hoglake-reserved field name: %r (the _hog prefix is server-reserved)", name)
        elif not SAFE_IDENTIFIER.match(name):
            log.warning("Skipping unsafe field name: %r", name)
        elif len(name) > _MAX_COLUMN_NAME_LEN:
            log.warning(
                "Skipping over-long field name (%d chars, hoglake allows %d): %r",
                len(name),
                _MAX_COLUMN_NAME_LEN,
                name,
            )
        else:
            continue
        metrics.records_skipped_total.labels(reason="unsafe_field_name").inc()
        drop.append(name)
    return batch.drop_columns(drop) if drop else batch


def _partition_groups(data: pa.Table, info) -> list[tuple[tuple[str | None, ...] | None, pa.Table]]:
    """Split an aligned batch by partition tuple under the table's live
    spec: one (wire-string tuple, sub-table) per distinct tuple.

    The prepared-commit path puts row-to-partition correctness on the
    caller — the server validates the STRUCTURE of what it is told (spec
    arity, key indexes) but never opens a data file at commit time, so a
    wrong value here mis-prunes reads of that file forever. The transform
    math is therefore pyhoglake's (`transforms.transform_strings`,
    Iceberg semantics, arrow-native where it can be); only the grouping
    is ours.

    Groups come out ordered by first occurrence in the batch, so file
    registration — and the server's rows-then-offset row-id assignment —
    follows input order, which is also the order MILLPOND_SORT_BY put the
    rows in. A null source value forms its own group, per Iceberg.
    """
    spec = info.partition_spec
    if spec is None or not spec.fields:
        return [(None, data)]
    columns = {c.field_id: c for c in info.columns}
    key_names = [f"__millpond_pk_{i}" for i in range(len(spec.fields))]
    key_arrays = []
    for field in spec.fields:
        column = columns.get(field.source_field_id)
        if column is None:
            raise HoglakeSinkError(
                f"partition spec of {info.namespace}.{info.name} references field_id "
                f"{field.source_field_id}, which is not a live column"
            )
        key_arrays.append(
            transforms.transform_strings(
                field.transform,
                field.transform_param,
                transforms.partition_source_array(data, [column]),
                column.type,
                column.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__millpond_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = keyed.group_by(key_names).aggregate([("__millpond_row", "min")]).sort_by("__millpond_row_min")
    out: list[tuple[tuple[str | None, ...] | None, pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_column = keyed.column(name)
            field_mask = pc.is_null(key_column) if value is None else pc.fill_null(pc.equal(key_column, value), False)
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


def _skip_unplaceable_groups(
    groups: list[tuple[tuple[str | None, ...] | None, pa.Table]], info
) -> tuple[list[tuple[tuple[str | None, ...] | None, pa.Table]], dict[str | None, int]]:
    """D3: on an object-prefix table, the groups whose prefix value can be
    placed, and `{value: rows}` for the ones that cannot.

    A group's value is its partition tuple's entry at
    `object_prefix.key_index` — the identity transform's wire string of
    the source column — and it names a prefix exactly when
    `object_prefix.parse_value(value, column.type)` says so: a canonical
    decimal (no sign, no leading zero) in the type's non-negative range.
    That is the predicate pyhoglake's own placement refuses on, used here
    so the two cannot disagree; a group it rejects would otherwise be a
    `ValidationError` before any upload, permanent under `is_retryable`,
    and the batch would wedge its partition on the row forever. A null
    (Iceberg's own null partition, the shape the events_json topic really
    carries) and a negative `team_id` (a producer bug) both fail it.

    Decided per GROUP, after the fanout, not per row before it: the
    fanout already split the batch by this value, so the verdict is one
    string parse per group and no row is copied, where a row-level
    filter on a 2 GiB whale flush rebuilt the whole batch to drop a
    handful of rows. The caller books the skipped rows once, when the
    flush is published (`_book_skipped`), under
    `records_skipped_total{reason="unplaceable_object_prefix_value"}`.

    The layout (a live integer column, a single identity field on it) was
    verified at adopt by `_check_object_prefix`, against the same
    `TableInfo`, so the lookups here cannot miss.
    """
    field = info.object_prefix_field
    if field is None:
        return groups, {}
    column = next(c for c in info.columns if c.name == field)
    index = object_prefix.key_index(info.partition_spec.fields, column.field_id)
    kept: list[tuple[tuple[str | None, ...] | None, pa.Table]] = []
    skipped: dict[str | None, int] = {}
    for values, part in groups:
        raw = values[index] if values is not None else None
        if object_prefix.parse_value(raw, column.type) is None:
            skipped[raw] = skipped.get(raw, 0) + part.num_rows
        else:
            kept.append((values, part))
    return kept, skipped


def _grouping_schema(target: pa.Schema, info) -> pa.Schema:
    """`target` with the object-prefix field made nullable, when the table
    has one and the column is NOT NULL; otherwise `target` itself.

    The strict schema is what pyhoglake must see — the exact catalog
    schema, field ids included — but it cannot be what the batch is CAST
    to before the fanout: pyarrow refuses to cast a column holding nulls
    to a non-nullable field (`ValueError: Casting field ... with null
    values to non-nullable`), so on a NOT NULL prefix column one null
    `team_id` — or a batch that lacks the column, which
    `_null_fill_missing` fills with nulls — failed the whole flush before
    `_skip_unplaceable_groups` could skip the one group, and the
    partition stuck on it with every valid row. So the batch is aligned
    to this schema (same types, same field ids, the one field nullable),
    grouped and filtered, and each RETAINED group is cast to `target`:
    none of them holds a null in that field (a null is unplaceable), so
    the cast only relabels nullability and shares the buffers. A nullable
    prefix column, and a table without the option, get `target` back and
    the single cast they always had.
    """
    field = info.object_prefix_field
    if field is None or field not in target.names:
        return target
    index = target.get_field_index(field)
    column = target.field(index)
    if column.nullable:
        return target
    return target.set(index, column.with_nullable(True))


def _partition_tuples(spec) -> tuple[tuple[int, str, int | None], ...]:
    """A live PartitionSpec as comparable (source_field_id, transform,
    param) triples. An absent spec and an empty one are the same thing —
    the server retires a spec by setting an empty field list."""
    if spec is None or not spec.fields:
        return ()
    return tuple((f.source_field_id, f.transform, f.transform_param) for f in spec.fields)


def _sort_tuples(spec) -> tuple[tuple[int, str, str], ...]:
    """A live SortSpec as comparable (source_field_id, direction,
    null_order) triples."""
    if spec is None or not spec.fields:
        return ()
    return tuple((f.source_field_id, f.direction, f.null_order) for f in spec.fields)


def _spec_identity(info) -> tuple:
    """A TableInfo's LAYOUT as comparable tuples: (partition, sort).

    The unit `_reconcile_specs`' verdict is recorded against and
    `_table_info` compares an adopted shape to. Columns are deliberately
    not in it — a concurrent `add_column` changes the shape without
    changing the layout config has an opinion about, and treating that as
    a reconciliation-worthy change would run the config check on every
    schema-evolution flush for no reason.
    """
    return (_partition_tuples(info.partition_spec), _sort_tuples(info.sort_spec))


def _describe_partition(spec, live_columns) -> str:
    """A live spec in HOGLAKE_PARTITION_BY's own grammar, so the operator
    can paste the fix straight into the values file."""
    names = {col.field_id: name for name, col in live_columns.items()}
    return ", ".join(
        f"{f.transform}({names.get(f.source_field_id, f'field_id={f.source_field_id}')}"
        f"{', ' + str(f.transform_param) if f.transform_param is not None else ''})"
        for f in (spec.fields if spec is not None else ())
    )


@dataclass(frozen=True)
class _FlushFacts:
    """What the commit message needs that the prepared payload does not
    already carry.

    Grouped rather than passed as three more `_prepare` keywords because
    they are one thing — the flush as main.py saw it, before this module
    dropped columns, stamped `_inserted_at` and fanned the rows out over
    partitions. `_prepare` supplies the rest (rows, files, partition
    tuples) from what it actually wrote.
    """

    kafka_offsets: tuple[tuple[str, int, int, int], ...] = ()
    arrow_bytes: int = 0
    trigger: str | None = None


def _message_token(value) -> str:
    """One `key=value` value, guaranteed to hold no space.

    The summary line is a fixed sequence of `key=value` pairs, so a
    space inside a value moves every pair after it for anything that
    reads the line by splitting on whitespace. Only the version can
    carry one — `MILLPOND_SERVICE_VERSION` takes any string an operator
    sets, an image digest included — so only it is passed through here.
    An empty value becomes `unknown`, because `millpond=` at the end of
    a line reads as a defect in the writer rather than as a version
    nobody set.
    """
    text = "".join("_" if character.isspace() else character for character in str(value))
    return text or "unknown"


def _offsets_line(topic: str, ranges: list[tuple[int, int, int]], limit: int) -> str:
    """One topic's partition ranges: `offsets <topic> p<n>:<first>-<last> ... (<count>)`.

    The topic is named ONCE and every range carries its partition, so
    the line stays readable at 32 ranges and two partitions can never be
    read as having swapped ranges. The trailing count is what an
    operator checks against the pod's partition assignment: a flush that
    names fewer partitions than the pod owns is a flush some partition
    delivered nothing to.

    Over `limit` bytes the line keeps the ranges it can and ends with
    `... (+<k> more)` in place of the count, so the two forms cannot be
    confused and the dropped ranges are still counted. The kept ones are
    the first by partition number, which is arbitrary but stable — a
    truncated line is a prompt to go and read the Kafka position
    directly, not a substitute for it. The topic name and the marker are
    the floor: a limit too small for those two is answered with those
    two, because a trimmed topic name is a different topic.
    """
    head = f"offsets {topic}"
    parts = [f"p{partition}:{first}-{last}" for partition, first, last in ranges]
    whole = " ".join([head, *parts]) + f" ({len(parts)})"
    if len(whole.encode()) <= limit:
        return whole
    # Greedy prefix, and the emitted line needs no correction after it:
    # each range is admitted against the marker that would follow it if
    # it were the LAST one kept, which is exactly the marker the emitted
    # line carries. So the last admitted range leaves the line at or
    # under the limit by the same arithmetic that admitted it, digit
    # rollover in the count included. (The floor case — no range fits at
    # all — emits the head and the marker and may exceed a limit too
    # small for those two, which is the documented behaviour above.)
    kept = 0
    size = len(head.encode())
    for index, part in enumerate(parts):
        grown = size + 1 + len(part.encode())
        marker = f" ... (+{len(parts) - index - 1} more)"
        if grown + len(marker.encode()) > limit:
            break
        size = grown
        kept = index + 1
    return " ".join([head, *parts[:kept]]) + f" ... (+{len(parts) - kept} more)"


def format_commit_message(
    *,
    records: int,
    files: int,
    partitions: int,
    arrow_bytes: int,
    trigger: str | None,
    version: str,
    table_uuid: str | None,
    codec: str,
    stats: str,
    kafka_offsets: Sequence[tuple[str, int, int, int]] = (),
    limit: int = _MESSAGE_OFFSETS_LIMIT,
) -> str:
    """The text of a snapshot's `message`: what the flush was, then where
    it came from in Kafka.

    The offset ranges are the point. A hoglake snapshot records its
    author (`millpond/<table>/<ordinal>`) and its files, and nothing
    else ties it to a position in the topic — so without them no one can
    answer "is offset X in the lake", audit a replay after a crash, or
    reconcile a gap, other than by reading parquet. The summary line is
    the cheap half: it costs one line and answers the questions that
    otherwise need the file list (how much landed, how wide the fanout
    was, why the flush happened, which build wrote it).

    Line 1 is a fixed sequence of `key=value` pairs, in a fixed order,
    with no space inside any value — it is meant to be read by a person
    first and to survive `awk` second. Line 2 (and further lines, one
    per topic, if a flush ever spans more than one) lists every
    partition range, sorted by partition number.

    `partitions` is how many partition tuples the fanout produced, which
    is 0 on an unpartitioned table and equal to `files` on a partitioned
    one — the two keys answer different questions (how many objects to
    compact later, how wide the flush spread), and they only agree
    because the fanout writes one file per tuple.

    `table_uuid` is the destination INCARNATION, and it is here for the
    same reason it is in the idempotency key: a drop and recreate under
    one name makes two tables that receipts do not span, and without it
    a range published into each produces two byte-identical messages
    with nothing in the catalog to tell them apart.

    `codec` is the parquet codec every object of the flush was encoded
    with (`codec_token`), so a codec rollout or rollback can be audited
    from the catalog's snapshot log without reading a footer.

    `stats` is how the flush's files registered their column stats
    (`stats_token`): `deferred` (left to the hydrator, files `pending`)
    or `shipped` (inline in the commit). Same reason as `codec`.

    DETERMINISTIC for a given flush: the same rows, files, trigger,
    incarnation and offsets produce the same text, on a retry in this
    process and on a rebuild in a new one. Nothing compares messages —
    the server's receipt is keyed on the idempotency key alone — but two
    snapshots that describe one flush differently is a question an
    operator reconciling a gap should never have to ask.
    """
    summary = (
        f"records={records} files={files} partitions={partitions} arrow_bytes={arrow_bytes} "
        f"codec={codec} stats={stats} trigger={_MESSAGE_TRIGGERS.get(trigger, _UNKNOWN_TRIGGER)} "
        f"millpond={_message_token(version)} "
        f"table={_message_token(table_uuid or 'unknown')}"
    )
    by_topic: dict[str, list[tuple[int, int, int]]] = {}
    for topic, partition, first, last in kafka_offsets or ():
        by_topic.setdefault(topic, []).append((partition, first, last))
    return "\n".join([summary, *(_offsets_line(topic, sorted(by_topic[topic]), limit) for topic in sorted(by_topic))])


def codec_token(compression: str | None, level: int | None) -> str:
    """`zstd-3`, `snappy`, `none`: the codec as one space-free token, for
    the commit message and the startup log."""
    name = compression or "default"
    return name if level is None else f"{name}-{level}"


def stats_token(deferred_stats: bool | None) -> str:
    """`deferred` or `shipped`: how a flush's files registered their
    column stats, for the commit message and the startup log."""
    return "deferred" if deferred_stats else "shipped"


class IdlessFilesLiveError(HoglakeSinkError):
    """An `add_column` the server refused 409 `idless_files_present`.

    The server refuses `add_column` / `rename_column` while the table
    holds a live file PROVEN to lack parquet field ids: a column that
    binds by name could adopt another column's values. Since hoglake
    1.4.6 (a SERVER version; no client floor checks it) pending or
    failed hydration does not count, so deferred stats never trigger it;
    every pyhoglake writer stamps field ids. Seeing it means some other
    writer registered an id-less file, or the server predates 1.4.6.
    Retrying does not clear it (pyhoglake's `IdlessFilesPresentError` is
    `retryable=False`) until the file is compacted or rewritten, or its
    table retired.

    It is NOT a reason to drop the column from the batch. Dropping it and
    publishing the rest would commit the Kafka offsets over rows whose
    value for that column was never written: silent, permanent loss of
    one column for every record in the flush. So the sink fails the
    FLUSH instead, permanently (`is_retryable` is False, so
    `main._write_with_retry` re-raises on the first attempt and main()
    skips the `final` flush of the same batch): offsets not committed,
    the pod crashes, and every restart is refused the same way until the
    file is dealt with. Loud and lossless.

    `errors_total_type` labels it in `millpond_errors_total`, read
    duck-typed by main.py.
    """

    errors_total_type = "hoglake_idless_files_present"

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=False)


class HoglakeSink:
    """The hoglake sink: owns the pyhoglake client, the resolved
    catalog/namespace/table handles, and the live-schema cache.

    main.py only calls `write()`, `reset_caches()`, and `close()`.
    `write()` must not be called with a zero-row batch (main.py gates on
    `pending_records > 0`); `reset_caches()` is invoked only by main.py's
    write-retry loop after a failure; `close()` is called exactly once at
    pod shutdown.
    """

    def __init__(self, cfg: Config):
        # Explicit guards rather than assert: `python -O` strips asserts
        # and would surface as a cryptic httpx/pyarrow failure instead of
        # a clear startup error. config.load() enforces these already;
        # this protects callers that bypass it.
        for name in (
            "hoglake_url",
            "hoglake_catalog",
            "hoglake_namespace",
            "hoglake_table",
        ):
            if getattr(cfg, name) is None:
                raise HoglakeSinkError(f"HoglakeSink requires cfg.{name}; config.load() should have enforced this")
        # The S3 keys are optional, but only TOGETHER. Both set is the
        # static shape (MinIO in local dev and CI); both None lets
        # pyhoglake hand pyarrow no keys at all, so the AWS SDK's
        # default credential chain resolves them — in Kubernetes, the
        # ServiceAccount's web-identity token (IRSA). Half a pair is
        # refused here rather than left to pyarrow, which does reject it
        # (`ValueError: ... both access_key and secret_key must be
        # provided`) but only when pyhoglake first builds the
        # filesystem, and in a message that names neither config field.
        if (cfg.hoglake_s3_access_key is None) != (cfg.hoglake_s3_secret_key is None):
            raise HoglakeSinkError(
                "HoglakeSink requires cfg.hoglake_s3_access_key and cfg.hoglake_s3_secret_key together "
                "(both for static keys, neither for the AWS default credential chain); config.load() "
                "should have enforced this"
            )
        self._cfg = cfg
        self._credential_source = _STATIC_KEYS if cfg.hoglake_s3_access_key is not None else _DEFAULT_CHAIN
        # ONE credential object for both clients: a ClientCredentials
        # caches its token, so sharing it means one mint per lifetime,
        # not one per client. Built before either client exists.
        self._auth = make_catalog_auth(cfg)
        log.info("hoglake catalog auth source: %s", cfg.hoglake_auth_source)
        # Otherwise pyhoglake's hardcoded 30s applies to every request
        # including the commit, and an operator with a convoyed catalog
        # has no knob at all.
        self._client = self._make_client(cfg.hoglake_request_timeout_s)
        # Retry-After, captured off the raw response: pyhoglake maps
        # status codes to exception classes and discards headers, so the
        # server's own backoff advice (503 `commit_queue_timeout` answers
        # with `Retry-After: 1`) would be lost. Installed as an httpx
        # event hook — a private attribute of the client, so the whole
        # thing is best-effort: if pyhoglake restructures its transport
        # we silently fall back to the exponential curve rather than
        # failing to construct a sink.
        self._retry_after: float | None = None
        try:
            self._client._http.event_hooks["response"].append(self._note_response)
        except Exception:  # noqa: BLE001 - optional enhancement, never fatal
            log.debug("Could not install the Retry-After hook; backoff falls back to the exponential curve")
        # Author recorded on every snapshot this pod makes — the pipeline
        # identity plus the pod ordinal, for multi-writer forensics. One
        # name for the data commits AND the DDL (catalog, namespace and
        # table creation, spec declaration, schema evolution), so "which
        # pod created this table" and "which pod wrote this file" are the
        # same question against the snapshot listing.
        self._author = f"millpond/{cfg.table_label}/{cfg.ordinal}"
        # The running version, named in every commit message and read
        # ONCE. `cfg.service_version` already holds it (the package
        # version, or whatever MILLPOND_SERVICE_VERSION overrides it
        # with — an image digest, typically), so the message and the
        # OTLP `service.version` resource attribute cannot disagree
        # about which build wrote a snapshot. Reading it per flush would
        # put a metadata lookup in the write path for a value that
        # cannot change while the process lives.
        self._version = _message_token(cfg.service_version or "unknown")
        self._codec = codec_token(cfg.hoglake_compression, cfg.hoglake_compression_level)
        log.info("hoglake parquet codec: %s", self._codec)
        self._stats = stats_token(cfg.hoglake_deferred_stats)
        log.info(
            "hoglake column stats: %s (HOGLAKE_DEFERRED_STATS=%s)",
            self._stats,
            "true" if cfg.hoglake_deferred_stats else "false",
        )
        # What a flush does with JSON a live variant column cannot encode.
        # Guarded like the fields above: pyhoglake refuses anything but
        # the two words, permanently, on the first flush — and a
        # hand-built Config that skipped config.load() would otherwise
        # start clean and crash there.
        if cfg.hoglake_variant_on_invalid not in _VARIANT_ON_INVALID:
            raise HoglakeSinkError(
                f"HoglakeSink requires cfg.hoglake_variant_on_invalid in {sorted(_VARIANT_ON_INVALID)}, got "
                f"{cfg.hoglake_variant_on_invalid!r}; config.load() should have enforced this"
            )
        self._variant_on_invalid: str = cfg.hoglake_variant_on_invalid
        # Which codec pyhoglake will encode variant rows with, read once:
        # it is decided by pyhoglake's own environment
        # (PYHOGLAKE_VARIANT_CODEC) at import, so it cannot change while
        # the process lives. It labels the encode-time histogram and the
        # codec-info gauge — and when it says `python` it refuses, at
        # resolve, a destination WITH a variant column
        # (`_check_variant_columns`) unless
        # MILLPOND_ALLOW_PYTHON_VARIANT_CODEC says otherwise: the Python
        # encoder is 6-10x the Rust codec's CPU per whale row, and a pod
        # running it looks healthy in every way but its throughput. A
        # string-only table never encodes, so it is not refused.
        self._variant_backend = _variant_backend_label()
        log.info(
            "hoglake variant columns: invalid JSON -> %s (MILLPOND_HOGLAKE_VARIANT_ON_INVALID); codec: %s "
            "(PYHOGLAKE_VARIANT_CODEC; MILLPOND_ALLOW_PYTHON_VARIANT_CODEC=%s)",
            self._variant_on_invalid,
            self._variant_backend,
            "true" if cfg.hoglake_allow_python_variant_codec else "false",
        )
        metrics.variant_codec_info.labels(backend=self._variant_backend).set(1)
        # pyhoglake's per-process codec counters, mirrored after every
        # prepare; primed now so every label set is on the first scrape.
        self._codec_counts = _VariantCodecCounts()
        self._codec_counts.prime()
        # The peek's own client — the same catalog and credentials, built
        # by the same helper — because `json_text_columns` runs on the
        # consume thread before every conversion and its REFRESHES must
        # not spend the write path's 45 s per request there. It starts at
        # the write timeout, for the one slow resolve (the totals scan in
        # `Namespace.table()`), and is lowered to `_PEEK_TIMEOUT_S` once
        # the table handle is held; see `_peek_live_shape`. The catalog
        # and table handles are resolved lazily and kept.
        self._peek_resolve_timeout_s = min(cfg.hoglake_request_timeout_s, _PEEK_RESOLVE_TIMEOUT_S)
        self._peek_client = self._make_client(self._peek_resolve_timeout_s)
        self._peek_catalog = None
        self._peek_table = None
        self._peek_timeout_warned = False
        # When a peek with a known answer last failed: no read for
        # `_PEEK_RETRY_AFTER_S` after it (cleared when any shape is
        # adopted), so an outage costs one short request a minute.
        self._peek_failed_at: float | None = None
        # The variant column names of the LAST SHAPE THIS PROCESS READ, or
        # None when it never read one. Set by `_adopt_columns`, never
        # cleared — `reset_caches()` drops the shape, not this — so a peek
        # that fails during a catalog outage has a safe answer: a variant
        # declaration is immutable, and the only thing a stale answer can
        # miss is a variant column added since, which the TTL window
        # already accepts. See `json_text_columns`.
        self._known_json_text_columns: frozenset[str] | None = None
        # When the peek last found the table missing (negative cache, on
        # the TTL clock), and when it last warned about a failed peek.
        self._peek_missing_at: float | None = None
        self._peek_warned_at: float | None = None
        # Wall seconds the most recent prepare took (encode + upload,
        # whether it returned or raised), for `write_flush_deadline`: the
        # next attempt of a rebuilt flush redoes that work, so its
        # reservation is derived from it rather than from a constant that
        # cannot know the batch. None until the first prepare.
        self._last_prepare_s: float | None = None
        # The `VariantReport` of the prepared payload, booked on the row
        # metrics when the payload is PUBLISHED (not when it is prepared:
        # a rebuild re-encodes the same rows), together with the aligned
        # batch's variant columns. Cleared with the payload.
        self._prepared_variant: VariantReport | None = None
        # `{value: rows}` of the prepared payload's unplaceable groups
        # (D3, `_skip_unplaceable_groups`), booked on
        # `records_skipped_total` and logged when the payload is
        # PUBLISHED, for the same reason as the variant report: a rebuild
        # re-skips the same rows. Cleared with the payload.
        self._prepared_skipped: dict[str | None, int] | None = None
        # Resolved lazily on first write; reset_caches() drops them so the
        # retry path re-resolves (another pod may have created/altered the
        # table, or it may have been dropped+recreated).
        self._table = None
        # The incarnation `self._table` was resolved AND reconciled as.
        # Held separately because the pyhoglake handle's own
        # `table_uuid` is not a pin: `Table.info()` adopts whatever the
        # name resolves to now, so any refresh silently rebases it onto a
        # recreated table. Every guard in the flush — the idempotency
        # key, the client pre-flight, the server's `expected_table_uuid`
        # — is named from THIS value.
        self._table_uuid: str | None = None
        self._live_columns: dict[str, Column] = {}
        # The last `TableInfo` read for the destination — the columns AND
        # the specs the flush path builds against. Held here rather than
        # re-read per flush: see `_table_info` for why that is safe and
        # `_destination_moved` for what drops it.
        #
        # pyhoglake keeps its own copy (`Table._info`, paired with the
        # snapshot it was read at in `Table._cache`) and that is the one
        # the commit's `read_snapshot` comes from, but it exposes only
        # `Table.columns` publicly — not the partition spec, which the
        # fanout needs. So this is millpond's copy of the same read, not
        # a second source of truth: both are seeded from a `table.info()`
        # or a `table.alter()` on the same handle, and the server's
        # conflict check on `read_snapshot` is what makes a disagreement
        # between them a refusal rather than a wrong file.
        self._live_info = None
        # When `_live_info` was read, on the MONOTONIC clock, and the
        # clock itself — injected so a test can drive the TTL without
        # sleeping out a catalog retention period. `_live_info_ttl_s` is
        # the whole argument for why there is a TTL at all.
        self._live_info_read_at: float | None = None
        self._monotonic = time.monotonic
        # The (partition, sort) spec tuples `_reconcile_specs` last
        # validated against config. See `_table_info`: a shape adopted by
        # schema evolution or the alignment self-heal is FRESH but has
        # not been reconciled, and without this the fresh-path early
        # return skipped the config check for as long as evolution kept
        # re-stamping the clock.
        self._reconciled_specs: tuple | None = None
        # The in-flight flush's uploaded-and-not-yet-published commit
        # request, held IN MEMORY for the lifetime of the flush so a
        # retry replays it rather than building a second one. Survives
        # reset_caches(); cleared when the commit resolves.
        self._prepared: dict | None = None
        self._prepared_rows: int = 0
        # The Kafka identity the prepared payload was built for. A retry
        # is recognized by THIS, not by re-deriving the key: the key
        # depends on the live table incarnation, which the retry path
        # deliberately does not re-resolve.
        self._prepared_offsets: tuple | None = None
        # How many times this payload has been sent. >1 on success means
        # the commit was RESOLVED BY REPLAY: the server either answered
        # from its receipt or applied it now, and either way the rows
        # published exactly once.
        self._prepared_sends: int = 0
        # The Kafka identity of the one flush that has already been
        # rebuilt after a 422 `object_prefix_violation` at commit. The
        # server's `retry: re-prepare` buys exactly one rebuild against a
        # fresh read of the layout; a second refusal of the same flush is
        # the layout the sink writes disagreeing with the one the server
        # enforces, and is permanent. Survives `reset_caches()` (main.py
        # calls that between the two attempts); cleared when a commit
        # lands. See `_object_prefix_refused`.
        self._object_prefix_rebuilt_for: tuple | None = None
        # STARTUP network validation. Everything else in this class is
        # lazy, and that is fine — but the catalog is the one thing whose
        # absence config.py and the README both describe as a "startup
        # error", and resolving it here is what makes that true. Before
        # this, a wrong HOGLAKE_URL, wrong S3-adjacent credentials or a
        # catalog nobody had created surfaced on the FIRST FLUSH: the pod
        # started, passed its probes, took its partitions, built lag, and
        # only then began crash-looping. One request at construction
        # turns all of that into a pod that never claims to be ready.
        self._catalog = self._resolve_catalog()
        # And the other half of "startup means startup": the catalog
        # resolve proves the CONTROL PLANE is reachable, which says
        # nothing about object storage. pyarrow resolves credentials
        # lazily, so a role without the bucket grant surfaced as an S3
        # 403 on the first upload — by which time the pod was ready,
        # held partitions and had built lag.
        self._probe_object_store(self._catalog)

    def _make_client(self, timeout: float) -> HoglakeClient:
        """A pyhoglake client for this destination. ONE constructor for the
        write path's client and the peek's, so an auth, header or endpoint
        setting added to one cannot miss the other."""
        return HoglakeClient(
            self._cfg.hoglake_url,
            s3=S3Config(
                access_key=self._cfg.hoglake_s3_access_key,
                secret_key=self._cfg.hoglake_s3_secret_key,
                endpoint_override=self._cfg.hoglake_s3_endpoint,
                region=self._cfg.hoglake_s3_region,
            ),
            timeout=timeout,
            auth=self._auth,
        )

    # -- Sink protocol -----------------------------------------------------

    def write(
        self,
        batch: pa.Table,
        *,
        kafka_offsets: tuple[tuple[str, int, int, int], ...] | None = None,
        trigger: str | None = None,
    ) -> int:
        """Publish `batch` as ONE idempotent commit.

        `kafka_offsets` is the flush's identity — the
        `(topic, partition, first, last)` quadruples covering everything
        in the pending buffer — and it is what makes a retry a REPLAY
        instead of a second write. See `_flush_key`. Absent, the flush is
        anonymous and falls back to at-least-once (a lost response
        duplicates); main.py always supplies it, direct callers usually
        should not care.

        `trigger` is what made main.py flush now (`size`, `time`,
        `final`). It reaches only the snapshot's commit message and is
        deliberately NOT part of the identity: the same rows flushed for
        a different reason are the same rows, and putting the trigger in
        the key would make a restart that flushes on size what a running
        pod flushed on time into a second publication.

        Returns the number of rows THIS CALL published. That is normally
        the batch's row count, and it is 0 when the batch was skipped
        whole — or when the server answered from a receipt, because then
        this process published nothing and `records_written_total` must
        not claim otherwise.
        """
        check_reserved_collision(batch.schema, RESERVED_COLUMNS, "Hoglake")
        # The batch AS HANDED IN, before the unwritable columns go and
        # before `_inserted_at` is stamped on, so a poison producer key
        # cannot quietly shrink the size an operator reconciles with.
        #
        # APPROXIMATELY the flush gate's `pending_bytes`, not equal to
        # it: main.py measures the gate per accumulated table and hands
        # the sink the CONSOLIDATED one, so this is measured after
        # `pa.concat_tables` (with type promotion) and after the sort.
        # Measured drift is about +0.75% for a sorted flush and about
        # +10% when the batch carried schema drift. Close enough to
        # compare against MILLPOND_FLUSH_SIZE and
        # `millpond_flush_size_bytes`; never a figure to reconcile byte
        # for byte.
        arrow_bytes = batch.nbytes
        had_columns = batch.num_columns > 0
        batch = _drop_unwritable_columns(batch)
        if had_columns and batch.num_columns == 0:
            # Every field was unwritable (poison producer). These records
            # ARE lost; skip the flush rather than crash-looping the
            # partition on the same batch forever.
            log.warning(
                "Skipping batch of %d record(s): every column had an unwritable name",
                batch.num_rows,
            )
            metrics.records_skipped_total.labels(reason="unsafe_field_name").inc(batch.num_rows)
            return 0
        if batch.num_rows == 0:
            # The Sink contract says this never happens (main.py gates on
            # pending_records > 0) — but a commit must register at least
            # one file with at least one row, so a zero-row batch would be
            # a 422 the retry loop could never clear.
            return 0

        identity = tuple(kafka_offsets) if kafka_offsets else None
        if identity is not None and self._prepared is not None and self._prepared_offsets == identity:
            # A retry of a flush whose registration is already uploaded.
            # Replay it byte-identically; never rebuild it.
            #
            # Matched on the Kafka identity rather than on a re-derived
            # key: the key is a function of the live table incarnation,
            # and re-resolving that here would silently rebase a frozen
            # payload's name onto a table it was not prepared for. The
            # payload carries its own `expected_table_uuid`; the commit
            # is where that gets judged.
            return self._commit_prepared()
        # Anything still held at this point belongs to a flush that never
        # resolved and never will — a different offset range, or an
        # anonymous batch, which has no identity to replay under. Its
        # upload is already in object storage with nothing referencing it.
        self._discard_prepared("superseded by a new flush", reason="superseded")

        batch = self._stamp_inserted_at(batch)
        table = self._ensure_table(batch.schema)
        key = self._flush_key(self._table_uuid, kafka_offsets)
        batch = self._evolve_and_align(table, batch)
        facts = _FlushFacts(kafka_offsets=identity or (), arrow_bytes=arrow_bytes, trigger=trigger)
        # Timed as a whole, both prepares of the self-heal below included
        # and whether or not it raises: this is what one attempt's local
        # work costs for THIS batch, and `write_flush_deadline` reserves
        # it for the next attempt. A prepare that raised partway is a
        # lower bound on the next, which is the right direction to err.
        prepare_started = self._monotonic()
        try:
            payload = self._prepare_with_self_heal(table, batch, key, facts)
        finally:
            self._last_prepare_s = self._monotonic() - prepare_started
        if payload is None:
            # Every group's object-prefix value was unplaceable (D3):
            # `_prepare` booked and logged the rows, nothing was uploaded,
            # and a commit must register at least one row — so there is
            # nothing to publish and nothing a retry could change. The
            # offsets commit over the rows, as over any skipped record.
            return 0
        self._prepared = payload
        # `_prepared_rows` was set by `_prepare`: the rows of the groups it
        # kept, not the batch's — the unplaceable groups are not in the
        # payload and must not be in `records_written_total`.
        self._prepared_offsets = identity
        self._prepared_sends = 0
        return self._commit_prepared()

    def _prepare_with_self_heal(self, table, batch: pa.Table, key: str, facts: _FlushFacts) -> dict | None:
        """`_prepare`, retried ONCE after an alignment refusal against a
        fresh read of the destination. Split out of `write()` so the
        whole of an attempt's local work — both prepares, when there are
        two — is timed as one for `write_flush_deadline`."""
        try:
            return self._prepare(table, batch, key, facts)
        except ValidationError as e:
            # Concurrent-DDL race (found by the live integration suite):
            # another writer's add_column can land between this sink's
            # alignment and the pre-flight resolve, and the strict
            # alignment then refuses the batch for lacking the brand-new
            # column. Refresh, null-fill, and retry ONCE; a second
            # refusal is a real error.
            #
            # `_prepare` null-fills against its own freshly adopted
            # columns, so this is the SECOND line of defence, not the
            # first — it covers a column that appears between that
            # null-fill and pyhoglake's own pre-flight resolve one round
            # trip later. The refusal is always pyhoglake's, and always a
            # 422: the `KeyError` this once also caught came from a
            # `select` that the same null-fill made incapable of raising
            # one.
            if not _is_alignment_refusal(e):
                raise
            # `totals=False`: a writer read wants the uuid, the columns
            # and the specs, and the totals are a count plus two sums
            # over every live file row of the table (~15M on prod-us).
            # This sink has never read one.
            self._adopt_info(table.info(totals=False))
            batch = self._null_fill_missing(batch)
            return self._prepare(table, batch, key, facts)

    def reset_caches(self) -> None:
        """Drop the resolved table/schema handles so the next attempt
        re-resolves.

        The PREPARED PAYLOAD survives a reset only while it is still
        SENDABLE: it is the record of an upload that already happened,
        and the whole point of holding it is that the retry replays the
        same registration instead of minting new paths. A payload the
        server has already refused with a response is not sendable — see
        `_commit_prepared`, which drops it at the refusal rather than
        leaving a reset to do a job it cannot do from here."""
        self._table = None
        self._table_uuid = None
        self._live_columns = {}
        # The cached shape goes with the handle it was read through: a
        # reset means "re-resolve the destination", and keeping a shape
        # read off the old handle would let the next flush build against
        # a table this pod has not re-reconciled.
        self._drop_cached_shape()

    def _drop_cached_shape(self) -> None:
        """Forget the destination's cached shape, so the next flush
        re-reads it.

        Separate from `reset_caches()` because it is a strictly smaller
        thing: the resolved handle and the incarnation stay, which is
        right for every event that invalidates the SHAPE without
        invalidating the identity (a commit refusal, a 410, a reused-key
        422). `reset_caches()` is the superset and calls this.
        """
        self._live_info = None
        self._live_info_read_at = None
        # The reconciliation verdict belongs to the shape it was reached
        # on; a new read has to be re-checked against config.
        self._reconciled_specs = None

    def close(self) -> None:
        # A payload still held at shutdown is an upload nobody will ever
        # reference: SIGTERM between prepare and commit. Nothing on the
        # server reclaims client uploads, so the count is the only trace
        # it leaves.
        self._discard_prepared("the sink closed before its commit resolved", reason="shutdown")
        self._client.close()
        self._peek_client.close()

    # -- retry policy (read by main._write_with_retry) ---------------------

    @staticmethod
    def is_retryable(exc: BaseException) -> bool:
        """See the module-level `is_retryable`. Exposed on the sink so
        the retry loop can consult it without main.py importing
        pyhoglake for a DuckLake-only deployment."""
        return is_retryable(exc)

    def write_retry_budget(self) -> tuple[int, float]:
        """(attempts, base backoff seconds) for this destination.

        DuckLake's three attempts were always the OUTER ring of a retry
        scheme whose inner loop runs `ducklake_max_retry_count` times
        (100 by default here). Hoglake has no inner loop: pyhoglake
        issues one request and raises. Three attempts against a catalog
        under commit-admission backpressure — which answers
        `Retry-After: 1` and expects to be asked again — is a crash
        loop dressed as a retry policy. HOGLAKE_MAX_RETRY_COUNT sets it;
        see config.py for how the default interacts with the liveness
        deadline."""
        return self._cfg.hoglake_max_retry_count, _WRITE_BASE_DELAY_S

    def json_text_columns(self) -> frozenset[str]:
        """The destination's live `variant` columns: the batch columns
        whose values must reach this sink as JSON text.

        Read by main.py before EVERY `arrow_converter.convert`, duck-typed
        like the retry hooks, so the converter serializes every
        non-string value in these columns as JSON (`True` -> `true`, a
        bool column -> a string column) while every other column keeps
        the `str()` form DuckLake and string columns have always had.

        KNOWN BEFORE THE FIRST CONVERSION, not after the first write. The
        answer is normally the write path's cached shape (`_live_columns`,
        filled at resolve and refreshed on every alter receipt and TTL
        re-read). When that is empty — before the first write, or after
        `reset_caches()` when the retry that followed REPLAYED a prepared
        payload and never re-resolved — or past its TTL, the destination
        is read here, READ-ONLY ("the peek", `_peek_live_shape`):
        namespace and table GET plus one `info(totals=False)`, under the
        peek's own short-timeout client, adopted into the same TTL'd
        cache the write path reads (`_adopt_info`), so the next
        `_table_info` costs no second request. No create, no DDL, no spec
        declaration: those stay with `_ensure_table`, which still runs in
        full on the write (the handle `self._table` is not set here). A
        NOT NULL variant column, or a variant column under the Python
        codec, is refused here with the same `HoglakeSinkError` the
        resolve would raise, which is permanent either way.

        A table that does not exist is an empty set — the write would
        create it from the batch, with no variant column — and is
        remembered for a TTL (negative cache) so a pipeline waiting for
        its first flush does not GET the missing table on every poll.
        A table that is found missing while a shape is cached (dropped
        under the pod) drops the cached shape and answers empty too; the
        write path re-resolves on its own NotFound.

        A FAILED PEEK IS NOT A FATAL ERROR ONCE ANYTHING HAS BEEN READ.
        A variant declaration is immutable, so the last shape this
        process read (`_known_json_text_columns`, which survives
        `reset_caches()`) is a safe answer during a catalog blip; the one
        thing it can miss is a variant column added since, which the TTL
        window already accepts. So: with a last-known answer the peek is
        ONE short attempt, best effort — on failure it logs (at most once
        a minute), counts `millpond_errors_total{type="hoglake_shape_peek"}`,
        answers from what it knows and does not read again for
        `_PEEK_RETRY_AFTER_S` (or until the write path adopts a shape),
        leaving the outage to the write path's ladder, which is built
        for it. Only when NOTHING has ever
        been read — the pod's first conversion — is there no safe answer
        (a str()-ed bool is a lost value under `null`, a wedged partition
        under `raise`), and then the peek retries `_PEEK_ATTEMPTS` times
        under the write timeout (the resolve is the totals-scanning GET,
        see the constants) and raises a
        `HoglakeSinkError` (permanent, so nothing retries it): the
        consume loop exits, the pod restarts and the poll replays. There
        is nothing in the pending buffer at a first conversion, so main's
        final flush has nothing to attempt and no ladder to run.
        """
        if self._live_columns and self._live_info_is_fresh():
            return self._json_text_columns_from_cache()
        known = self._known_json_text_columns
        if known is not None and (self._table_known_missing() or self._peek_recently_failed()):
            return self._json_text_columns_from_cache() if self._live_columns else known
        try:
            self._peek_live_shape(attempts=_PEEK_ATTEMPTS if known is None else 1)
        except HoglakeSinkError:
            raise
        except Exception as e:  # noqa: BLE001 - classified by `known`
            if known is None:
                raise HoglakeSinkError(
                    f"could not read the shape of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} before "
                    f"this process's first conversion ({e}); a batch cannot be converted correctly without knowing "
                    f"which columns are variant, so the pod stops and the poll replays"
                ) from e
            self._note_peek_failure(e)
            return self._json_text_columns_from_cache() if self._live_columns else known
        return self._json_text_columns_from_cache()

    def _json_text_columns_from_cache(self) -> frozenset[str]:
        return frozenset(name for name, column in self._live_columns.items() if column.type == "variant")

    def _peek_recently_failed(self) -> bool:
        """A peek failed within `_PEEK_RETRY_AFTER_S` and no shape has been
        adopted since: serve the known answer, do not read again yet."""
        if self._peek_failed_at is None:
            return False
        return self._monotonic() - self._peek_failed_at < _PEEK_RETRY_AFTER_S

    def _table_known_missing(self) -> bool:
        """The peek found no table within the last TTL, and nothing has
        adopted a shape since (`_adopt_columns` clears the stamp)."""
        if self._peek_missing_at is None:
            return False
        return self._monotonic() - self._peek_missing_at < self._live_info_ttl_s()

    def _peek_live_shape(self, *, attempts: int) -> None:
        """Read the destination's shape into the TTL'd cache without
        claiming, creating or altering anything. The table handle is
        resolved ONCE (list_namespaces + the totals-scanning table GET,
        under the write timeout) and kept; every later peek is one cheap
        `info(totals=False)` under the short timeout. The handle is
        dropped when the table is found missing. Raises after `attempts`
        transient failures, or at once on a permanent one. See
        `json_text_columns`."""
        cfg = self._cfg
        for attempt in range(attempts):
            try:
                if self._peek_table is None:
                    self._set_peek_timeout(self._peek_resolve_timeout_s)
                    if self._peek_catalog is None:
                        self._peek_catalog = self._peek_client.catalog(cfg.hoglake_catalog)
                    try:
                        self._peek_table = self._peek_catalog.namespace(cfg.hoglake_namespace).table(cfg.hoglake_table)
                    except NotFoundError:
                        self._note_table_missing()
                        return
                    self._set_peek_timeout(_PEEK_TIMEOUT_S)
                try:
                    # Cheap: the handle's resolve already paid the totals
                    # scan, and pyhoglake offers no public way at the
                    # TableInfo it read; this asks without the totals.
                    info = self._peek_table.info(totals=False)
                except NotFoundError:
                    self._peek_table = None
                    self._note_table_missing()
                    return
                self._adopt_info(info)
                return
            except HoglakeSinkError:
                raise
            except Exception as e:  # noqa: BLE001 - classified below
                if not is_retryable(e) or attempt == attempts - 1:
                    raise
                delay = _PEEK_BACKOFF_S * (attempt + 1)
                log.warning(
                    "Reading the shape of %s.%s before converting a batch failed (%s); retrying in %.0fs",
                    cfg.hoglake_namespace,
                    cfg.hoglake_table,
                    e,
                    delay,
                )
                time.sleep(delay)

    def _set_peek_timeout(self, seconds: float) -> None:
        """Change the peek client's request timeout: the (clamped) resolve
        timeout for the one totals-scanning resolve, the short one for
        refreshes. Through pyhoglake's private httpx client, so best
        effort — a pyhoglake that restructures its transport leaves the
        peek at the timeout it was built with (the resolve one), which is
        slower, never wrong; said once, at WARNING, because it is a pin
        on a private attribute that has quietly come loose."""
        try:
            self._peek_client._http.timeout = httpx.Timeout(seconds)
        except Exception:  # noqa: BLE001 - optional, never fatal
            if not self._peek_timeout_warned:
                self._peek_timeout_warned = True
                log.warning(
                    "Could not set the shape peek's request timeout (pyhoglake's HoglakeClient no longer exposes "
                    "_http); the peek keeps its %.0fs resolve timeout for every request",
                    self._peek_resolve_timeout_s,
                )

    def _note_table_missing(self) -> None:
        """The peek found no table: nothing to encode into until the first
        write creates one (and millpond never creates a variant column).
        Any cached shape belonged to a table that is gone."""
        self._drop_cached_shape()
        self._live_columns = {}
        self._known_json_text_columns = frozenset()
        self._peek_missing_at = self._monotonic()

    def _note_peek_failure(self, error: BaseException) -> None:
        metrics.errors_total.labels(type="hoglake_shape_peek").inc()
        now = self._monotonic()
        self._peek_failed_at = now
        if self._peek_warned_at is not None and now - self._peek_warned_at < _PEEK_LOG_INTERVAL_S:
            return
        self._peek_warned_at = now
        log.warning(
            "Could not refresh the shape of %s.%s before converting a batch (%s); converting with the last shape "
            "this process read (variant columns: %s). The write path will meet the same outage with its own retries",
            self._cfg.hoglake_namespace,
            self._cfg.hoglake_table,
            error,
            sorted(self._known_json_text_columns or ()),
        )

    def write_flush_deadline(self) -> tuple[float, float]:
        """(wall-clock budget for one flush, seconds to reserve for the
        next attempt) — the runtime liveness bound main.py enforces.

        The attempt COUNT was never the real limit. The consume loop is
        single threaded, so every second a flush spends retrying is a
        second `server.HealthState` sees no poll, and three of the things
        a flush spends it on are invisible to the startup arithmetic: the
        server's `Retry-After` floors each backoff gap (clamped per gap,
        not in sum), a rebuild re-resolves the destination and re-reads
        its shape under their own timeouts, and a stalled object store
        can hold one `prepare_append_tables` far past its allowance
        because boto3 retries per object and the fan-out waits for every
        upload in flight before it re-raises.

        So main.py stops on the CLOCK instead, and the outcome is the one
        exhausting the attempts already produces — raise the last error,
        crash, restart, replay from Kafka — except that it happens while
        the process can still say why. `DuckLakeSink` does not implement
        this: its inner loop has its own bound and its outer three
        attempts cannot approach the deadline.

        The reservation is MEASURED, not assumed, once there is a
        measurement: the last prepare's wall time (encode + upload) times
        a safety factor, floored at the config constant. A VARIANT flush
        is why — its encode is per-attempt work whose size is the batch's
        (a 2 GiB whale flush is seconds under the Rust codec, a minute or
        more under Python), and a fixed 20 s would let main.py start an
        attempt the liveness probe kills halfway through. main.py reads
        this after every failed attempt, so the first attempt's cost
        prices the second."""
        return config_hoglake_flush_deadline(self._cfg.hoglake_request_timeout_s, self._last_prepare_s)

    def _note_response(self, response) -> None:
        """httpx response hook: remember a Retry-After from a response
        that is telling us to back off. Deliberately narrow — only
        statuses that mean "not now" leave a hint, so a stray header on
        a 200 cannot slow the pipeline down."""
        if response.status_code not in _RETRY_AFTER_STATUS:
            return
        raw = response.headers.get("retry-after")
        if raw is None:
            return
        try:
            # Delta-seconds only. The HTTP-date form is legal but hoglake
            # never sends it, and guessing at clock skew to honor one
            # would be worse than falling back to our own curve.
            self._retry_after = float(raw.strip())
        except ValueError:
            log.debug("Ignoring non-numeric Retry-After %r", raw)

    def retry_after_hint(self) -> float | None:
        """The server's own backoff advice for the most recent refusal,
        consumed once. One-shot on purpose: a hint left over from a
        failure two flushes ago must not govern an unrelated retry."""
        hint, self._retry_after = self._retry_after, None
        return hint

    # -- idempotent publication --------------------------------------------

    def _flush_key(self, table_uuid: str | None, kafka_offsets) -> str:
        """The commit's idempotency key: a UUIDv5 over the destination
        table INCARNATION and the complete Kafka offset range being
        flushed.

        DERIVED, not random, and that is the entire mechanism. A key is a
        name for "these rows, published to this table", so the retry of a
        flush whose response was lost carries the same name as the commit
        that may already have landed, and the server answers from its
        receipt instead of writing again.

        A key that names something OTHER than the row set is worse than
        no key at all, because the server's answer is then a statement
        about a different publication. Both halves of this name exist for
        that reason:

        * `table_uuid`, not just the table NAME. Receipts live per
          catalog and survive a table drop — hoglake has no cascade from
          the table to the receipts. Without the incarnation in the key,
          a dropped-and-recreated table answers a flush from its
          PREDECESSOR's receipt, and millpond advances Kafka offsets over
          rows that are in a table that no longer exists.

          It is `self._table_uuid` — the incarnation `_ensure_table`
          resolved and reconciled — and never the pyhoglake handle's own
          `table_uuid`, which any `Table.info()` rebases onto whatever
          the name resolves to now.

          That choice does NOT buy the refusal of a drop+recreate under
          the cached handle: the refusal is entirely the
          `expected_table_uuid` pin `_prepare` puts on the payload, and
          any flush whose key ever reaches the server has already
          cleared that pre-flight — so by then the key and the pin
          necessarily name the same incarnation whichever source the key
          was read from. Feeding this the rebasable `table.table_uuid`
          would be caught by the pin, not by anything here.

          What it buys is that they are named from ONE source by
          construction, rather than by the pin happening to fire: a key
          derived from the handle would name an incarnation the payload
          was never prepared against, and the only thing standing
          between that and a receipt lookup under the wrong name would
          be a guard one layer down. The correct source is also the free
          one, so there is no reason to take the other.
        * BOTH ends of each partition's range, not just the high end.
          "Everything up to 41" is not a row set: after a rewind, a flush
          of [0, 41] and an earlier flush of [30, 41] share a name, and
          the earlier one's receipt reports the larger flush as already
          published.

        `topic:partition:first-last` keeps the partition in each line so
        two partitions cannot swap ranges and hash the same; the lines
        are sorted so the same flush described in any order is the same
        name.

        Without offsets (a direct caller, not main.py) the key is random,
        which is honest: an anonymous batch has no identity to recognize
        it by on a retry, so it keeps the old at-least-once behaviour
        rather than pretending to more.
        """
        if not kafka_offsets:
            log.debug("Flush has no Kafka identity; commit falls back to at-least-once")
            return str(uuid.uuid4())
        cfg = self._cfg
        name = "\n".join(
            [
                f"{cfg.hoglake_catalog}/{cfg.hoglake_namespace}/{cfg.hoglake_table}/{table_uuid}",
                *(f"{topic}:{partition}:{first}-{last}" for topic, partition, first, last in sorted(kafka_offsets)),
            ]
        )
        return str(uuid.uuid5(_IDEMPOTENCY_NAMESPACE, name))

    def _prepare(self, table, batch: pa.Table, key: str, facts: _FlushFacts) -> dict | None:
        """Encode the batch's parquet, upload it, and return the commit
        request — WITHOUT publishing it. None when every partition group
        was unplaceable under the table's object prefix (D3): nothing was
        encoded or uploaded, and `write()` publishes nothing.

        The split is what makes the retry safe: after this returns, the
        objects exist in object storage and the request that registers
        them is a value we can hold and re-send verbatim. pyhoglake's
        `prepare_append_tables` owns the encode, the upload and the
        registration's stats/footer conventions; each group goes straight
        from Arrow to a parquet buffer to one `PutObject`, with no temp
        file to write, fsync, re-read for the footer and re-read for the
        upload.

        MEMORY, because the disk path's whole argument was about it,
        and the honest accounting is bigger than "input plus buffers".
        Alive at once, at the moment the uploads run:

        * main.py's `consolidated` table, which the retry loop holds for
          the whole of `_write_with_retry` — so it survives every
          attempt, not just this one;
        * `aligned`, the result of `select(...).cast(target)`. A cast
          that changes a type (int32 -> int64, string -> uuid) allocates
          a new buffer; one that does not is zero-copy, so this is
          somewhere between a few columns and a full copy depending on
          how much the batch drifted;
        * `groups`, which `_partition_groups` materialises with
          `data.filter(mask)` per tuple — a full copy of the rows,
          partitioned, so about one more `aligned` in total;
        * every encoded parquet buffer, which pyhoglake holds all of at
          once because it validates every group before the first upload
          starts (that is what keeps a refusal from orphaning objects).

        So roughly THREE Arrow-sized copies plus the parquet, not one
        plus the parquet. What makes it a non-issue is the last term
        being small: the prod-us events writer's ~105K rows over ~271
        partitions encode to a few MiB, because parquet of a sorted,
        dictionary-friendly column set is a fraction of its Arrow
        representation and the per-group split means no single buffer is
        the whole flush. The Arrow side is bounded by MILLPOND_FLUSH_SIZE
        — 100 MiB at the code default, and ~1 GiB in the prod-us values
        file, which is the figure the 16 GiB pod sizing was done
        against; do not read that number as a property of the code.
        Nothing is held twice within this method: there is no temp file
        and the upload reads the buffer it was handed.

        Partition fanout is ours to compute because the prepared path
        puts row-to-partition correctness on the caller — transform math
        still comes from pyhoglake.transforms, so the Iceberg semantics
        have exactly one implementation.

        VARIANT. A live `variant` column has no one Arrow type, so the
        target schema is built with `variant="json"`: the column is
        `pa.json_()`, an extension over utf8 carrying the field id and
        pyhoglake's variant marker. The batch's same-named column is the
        JSON string the arrow_converter produced (an events_json
        `properties` is a string holding JSON text), and `.cast(target)`
        retypes it string -> json WITHOUT copying — same buffers, it is a
        relabel — and without parsing anything here. pyhoglake then
        encodes each group's JSON into the column's declared shredded
        layout inside `prepare_append_tables` (`write_part`), group by
        group, before the first upload, so an invalid row orphans
        nothing. What happens to such a row is `variant_on_invalid`:
        `null` lands SQL NULL in that column and counts it, `raise`
        fails the flush with `VariantEncodingError` (a `ValidationError`,
        so `is_retryable` calls it permanent). The `VariantReport`
        collects every group's counts for `_observe_variant_rows`.
        millpond never declares a variant column (`_add_columns` adds
        strings and typed scalars only); it writes into what the table
        declares.

        THE MEMORY ACCOUNTING ABOVE DOES CHANGE for a variant column,
        and not in the parquet term. The json relabel is free, but
        pyhoglake encodes each group's text into the column's shredded
        storage — a struct of binary and typed leaves, built in full
        before it is written — so for the duration of the group's
        `write_part` the encoded storage is alive beside the group's
        Arrow text. Measured on the whales shape (seven variant columns,
        ~13.8 KB/row): a transient of about +1.1x the group's Arrow size
        (+603 MiB over a 540 MiB group; +1,762 MiB over 1,316 MiB of
        input). Per group, not per flush, so with the 2 GiB flush split
        over ~532 (team, month) groups the transient is a few MiB at a
        time — but the groups are skewed (a handful of teams are most of
        the rows), and the worst case is one group that is most of the
        flush. Sizing rule, then: the three Arrow-sized copies above,
        plus the parquet, plus 1.1x the LARGEST group; for a hot 2 GiB
        flush that lands at roughly 8.5-9 GiB of peak RSS, which is why
        the whales pods are 32 GiB and not 16. The CPU term grows too —
        the encode is the parquet write's new dominant term, see README
        "VARIANT columns".

        OBJECT PREFIX. When `info.object_prefix_field` is set the
        placement is pyhoglake's, not this module's: `prepare_append_tables`
        reads each group's identity partition value for the field off the
        tuple this fanout hands it, writes the file under
        `<data_path>/data/<ns>/<table>/<field>=<value>/<idempotency_key>/
        <uuid>-<i>.parquet`, and proves the value on the file's own footer
        before the upload (both bounds equal to it, no nulls) — the check
        the server repeats at commit. What changes here is one step
        between the fanout and the prepare: `_skip_unplaceable_groups`
        removes every group whose value names no prefix (null, negative,
        non-canonical, out of range — pyhoglake's own predicate, D3), so
        no group pyhoglake would refuse reaches it; the skipped rows are
        booked on `records_skipped_total` when the flush publishes. The
        stats registration is the other visible
        difference (D4): with `deferred_stats=True` on such a table
        pyhoglake never registers a file `pending`. A file at or over
        `footer_stats_min_bytes` registers every stat, a smaller one the
        prefix field's alone (`stats_field_ids={field_id}`), and either is
        `provided` at commit — mountable and compactable with no hydrator
        pass, which is what keeps a tenant-scoped mount from waiting on
        hydrator throughput. The threshold is pyhoglake's default
        (`DEFAULT_FOOTER_STATS_MIN_BYTES`, 1 MiB, the server's own
        `HOGLAKE_HYDRATOR_MIN_FILE_BYTES` default) and is deliberately not
        passed: no deployment has asked for another value, and a knob
        nobody asked for is a knob to misconfigure.
        The commit message still says `stats=deferred` for such a flush —
        the token names the configuration, not the registration shape.
        Orphans are unchanged in kind and changed in place: one request's
        files sit in as many `<field>=<value>/<idempotency_key>/`
        directories as it has values, which is one more reason
        `_count_orphans` names uris and not a prefix.
        """
        info = self._table_info(table)
        target = columns_to_arrow_schema(info.columns, variant="json")
        report = VariantReport()
        # Null-fill against THESE columns, not the ones the caller
        # aligned to. `_evolve_and_align` filled against the schema it
        # resolved a round trip ago; a concurrent writer's add_column
        # since then puts a name in `target` that the batch does not
        # carry, and `pa.Table.select` answers a missing name with a
        # KeyError — which is not a ValidationError, so the self-heal in
        # `write()` never saw it, and is not a pyhoglake type, so the
        # retry loop called it transient and burned the whole budget
        # before crashing the pod. Fill first, select second: the select
        # can then only ever narrow.
        batch = self._null_fill_missing(batch)
        # Cast to the strict schema in two steps when the object-prefix
        # column is NOT NULL: see `_grouping_schema`. The groups pyhoglake
        # receives carry `target` exactly, either way.
        grouping = _grouping_schema(target, info)
        aligned = batch.select(list(target.names)).cast(grouping)
        groups, skipped = _skip_unplaceable_groups(_partition_groups(aligned, info), info)
        if grouping is not target:
            groups = [(values, part.cast(target)) for values, part in groups]
        self._prepared_skipped = skipped or None
        self._prepared_rows = sum(part.num_rows for _, part in groups)
        if not groups:
            # Every group unplaceable. Booked NOW rather than at a commit
            # that will never happen: nothing was uploaded, so there is
            # nothing to rebuild and no second booking to fear.
            self._book_skipped()
            return None
        # Formatted BEFORE the first byte is uploaded, and that ordering
        # is a safety property, not a style choice. Everything that can
        # fail after the upload has to account for the objects it
        # abandons — that is what the `_count_orphans` arm around
        # `prepare_append_tables` is for. A formatting failure raised
        # after it (a future field that is not a string, say) would
        # leave those objects with no count, no log and no metric, and
        # the retry loop would call the TypeError transient and spend
        # the whole budget re-raising it. Here it can only fail before
        # anything exists to orphan.
        #
        # `files` is `len(groups)`: the fanout writes one parquet per
        # partition tuple, which is the same list the upload is built
        # from and the same count `_commit_prepared` books on
        # `hoglake_files_written_total`. `partitions` counts the tuples
        # themselves — every group but the unpartitioned table's single
        # `None`, so a `(None,)` tuple (a null source value, which
        # Iceberg gives its own partition) IS counted. Both counts, and
        # `records`, are of the groups KEPT: an unplaceable group is in
        # neither the payload nor the message.
        message = format_commit_message(
            records=self._prepared_rows,
            files=len(groups),
            partitions=sum(1 for partition_values, _ in groups if partition_values is not None),
            arrow_bytes=facts.arrow_bytes,
            trigger=facts.trigger,
            version=self._version,
            table_uuid=self._table_uuid,
            codec=self._codec,
            stats=self._stats,
            kafka_offsets=facts.kafka_offsets,
        )
        try:
            payload = table.prepare_append_tables(
                [(part, partition_values) for partition_values, part in groups],
                idempotency_key=key,
                # PINNED, not defaulted. Left to its default, pyhoglake
                # reads `self.table_uuid` off its own `_info` — which any
                # refresh of its cache rebases onto whatever the name
                # resolves to NOW. The client's pre-flight and the
                # server's guard would then both compare fresh against
                # fresh and pass over a drop+recreate that happened under
                # the cached handle. Naming the resolved-and-reconciled
                # incarnation instead makes the guards fire, and
                # `IncarnationChangedError` is retryable, so reset_caches
                # re-resolves and `_reconcile_specs` finally runs against
                # the table we are writing to.
                expected_table_uuid=self._table_uuid,
                # Explicit, never left to pyhoglake: its default is
                # pyarrow's SNAPPY. See config._hoglake_compression.
                compression=self._cfg.hoglake_compression,
                compression_level=self._cfg.hoglake_compression_level,
                # Explicit as well: pyhoglake's default is False (ship
                # inline). `bool()` so an unset field on a hand-built
                # Config ships stats rather than passing None through.
                # See config._hoglake_deferred_stats.
                deferred_stats=bool(self._cfg.hoglake_deferred_stats),
                # Only read when the destination has a variant column;
                # harmless otherwise. The report is filled group by group
                # and read below, after a SUCCESSFUL prepare — a prepare
                # that raised may have counted part of its input.
                variant_on_invalid=self._variant_on_invalid,
                variant_report=report,
            )
        except Exception as e:
            # ONE arm, because there is now one question and pyhoglake
            # answers it. Since 1.1.1 every exception leaving
            # `prepare_append_files` / `prepare_append_tables` carries
            # what it had already written: `uploaded_files` is how many
            # uploads CLOSED cleanly, `uploaded_uris` names exactly
            # those. A refusal raised before the first upload carries 0
            # and (), so the same read covers the validation refusals,
            # the catalog-side failures and the object-store ones alike.
            #
            # This used to be three arms whose only content was an
            # argument about where in someone else's control flow each
            # failure could fire ("a validation refusal is a property of
            # the whole set, so it fires at index 0"; "the catalog work
            # all precedes the upload loop"). The arguments were
            # re-derived from pyhoglake's source and happened to hold,
            # but deducing another library's progress is exactly the
            # defect class this branch already shipped twice — once
            # booking a whole fanout that was never uploaded, once
            # booking N-1 for a refusal that self-heals into a successful
            # flush. Read the number; do not reconstruct it. The
            # concurrent fan-out makes that literal rather than merely
            # prudent: the uris are no longer a contiguous PREFIX of the
            # groups, because group 7 can land while group 6 fails, so
            # position and count say nothing about any particular file.
            #
            # getattr with defaults, and not only for an older client:
            # pyhoglake stamps best-effort and suppresses the
            # AttributeError from an exception type whose __slots__
            # refuse the attributes. Zero is then the honest answer, and
            # raising an AttributeError over a live object-store failure
            # is not.
            #
            # The file that FAILED is in neither number — its upload may
            # never have opened, or may have closed badly over a
            # TRUNCATED object that really is there. So the count is a
            # lower bound on objects present, and the log says so.
            _count_orphans(
                int(getattr(e, "uploaded_files", 0)),
                f"the prepared upload of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} "
                f"failed partway through a {len(groups)}-file fanout. The file it failed on is not "
                "among these and is not counted, but a failed close can leave a truncated object, "
                "so treat it as possibly present too",
                getattr(e, "uploaded_uris", ()),
                reason="prepare_failed",
            )
            # The encode's COST was spent whether or not the prepare
            # returned — an upload that failed after every group was
            # encoded is the common shape — so the attempt's wall
            # seconds, shadow seconds, recursion-bound rows and the
            # codec counters (routes, fallbacks) are booked here too,
            # under the same guard as on the success path. The ROW
            # counts are not: they are booked when a payload publishes.
            if any(c.type == "variant" for c in info.columns):
                try:
                    self._observe_variant_encode(report)
                except Exception:  # noqa: BLE001 - a metric, never the error being raised
                    log.exception("Could not book the variant encode of a failed prepare")
            else:
                self._codec_counts.sync()
            if _is_native_codec_refusal(e):
                # PYHOGLAKE_VARIANT_CODEC=native and the Rust codec is
                # missing or does not take this table's declaration. A
                # bare RuntimeError, which `is_retryable` would read as
                # transient; it is config, and permanent.
                raise HoglakeSinkError(
                    f"{e}. The variant codec is required by PYHOGLAKE_VARIANT_CODEC=native and cannot encode "
                    f"{self._cfg.hoglake_namespace}.{self._cfg.hoglake_table}; install pyhoglake-variant-codec "
                    f"(the `variant-codec` extra) or set PYHOGLAKE_VARIANT_CODEC=auto. Not retried."
                ) from e
            raise
        # `read_snapshot` STAYS ON THE PAYLOAD, and it is the conflict
        # basis the whole zero-read flush rests on: `prepare_append_*`
        # binds it to the snapshot the cached `TableInfo` was resolved
        # at, and the server's conflict scan then answers exactly the
        # question this sink used to re-read the table to answer — "did
        # anything alter, drop or recreate this table since my read".
        # Anything that did comes back as a typed, non-retryable refusal
        # (`ddl_since_read_snapshot` / `table_recreated` /
        # `ReadSnapshotExpiredError`), which `_commit_prepared` answers
        # by discarding the payload and rebuilding against a fresh read
        # — the recovery a frozen payload actually has. Stripping it
        # (which this module did, back when the only answer was an
        # untyped retryable 409 that a replay could never clear) also
        # leaves the commit with NO conflict window at all, and the
        # server's HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS flag exists
        # to refuse that shape once the fleet stops sending it: a
        # partitioned append whose values were computed under a spec
        # nobody can prove is still live.
        payload["author"] = self._author
        # Attached HERE, with the payload, and not at commit time: the
        # payload is held across retries and re-sent verbatim, so a
        # message attached beside the author is the same message on
        # every attempt by construction rather than by the formatter
        # happening to be deterministic.
        payload["message"] = message
        # Two bookings with two clocks, and the split is the point. The
        # ENCODE's cost (wall seconds, the codec's routes and fallbacks)
        # was spent by this attempt whether or not its commit lands, so
        # it is booked here, per prepare. The ROWS it describes are
        # booked when the payload is PUBLISHED (`_commit_prepared`): a
        # commit refused as `ddl_since_read_snapshot` is rebuilt, the
        # rebuild re-encodes the same rows into a fresh report, and
        # booking per prepare counted them twice. Held on the sink
        # beside the payload; `_clear_prepared` drops both.
        #
        # Both guarded, because both run AFTER the upload: a metrics
        # failure raised from here would leave the objects just uploaded
        # with no payload to register them and no orphan count — the
        # exact gap `_count_orphans` exists to close — and the retry
        # loop would call the TypeError transient.
        has_variant = any(c.type == "variant" for c in info.columns)
        self._prepared_variant = report if has_variant else None
        if has_variant:
            try:
                self._observe_variant_encode(report)
                self._log_invalid_rows(report, groups, info, facts)
            except Exception:  # noqa: BLE001 - a metric or a log line, never an orphaned upload
                log.exception("Could not book the variant encode of this flush; the upload is unaffected")
        return payload

    def _observe_variant_encode(self, report: VariantReport) -> None:
        """Book what one PREPARE spent on the variant encode: the wall
        seconds (histogram, by codec), the shadow's seconds, the rows
        whose verdict fell to the recursion bound, and pyhoglake's
        per-process codec counters (routed, fallbacks, shadow calls).
        Per attempt, deliberately — a rebuild spends it again."""
        if report.rows:
            # A prepare that failed before any group was encoded has a
            # report of nothing; a 0 s observation would say otherwise.
            metrics.variant_encode_seconds.labels(backend=self._variant_backend).observe(report.encode_seconds)
        if report.shadow_seconds:
            metrics.variant_shadow_seconds_total.inc(report.shadow_seconds)
        if report.recursion_bound_rows:
            metrics.variant_codec_recursion_bound_rows_total.inc(report.recursion_bound_rows)
        self._codec_counts.sync()
        for reason, count in report.codec_fallbacks.items():
            # Transitional, while both codecs exist: the Rust result was
            # not used and Python encoded the call again. Correct output
            # either way, so a warning, not an error; the count is on
            # `variant_codec_fallback_total` via the sync above.
            log.warning("Variant codec fell back to Python %d time(s) this prepare: %s", count, reason)

    def _book_skipped(self) -> None:
        """Book the published flush's unplaceable rows (D3) on
        `records_skipped_total{reason="unplaceable_object_prefix_value"}`
        and log them once, naming the distinct values (bounded).

        Once per PUBLISHED flush, like `_observe_variant_rows` and for the
        same reason: a commit refused as a re-prepare is rebuilt and the
        rebuild skips the same groups again, so booking per prepare would
        count the rows twice. The all-unplaceable flush, which has no
        commit, books from `_prepare` directly. Guarded: this runs after
        the rows are published, and a metrics failure is not worth
        raising over a flush that succeeded.
        """
        skipped = self._prepared_skipped
        self._prepared_skipped = None
        if not skipped:
            return
        try:
            rows = sum(skipped.values())
            values = sorted(skipped, key=lambda v: (v is not None, v or ""))
            listed = ", ".join("null" if v is None else repr(v) for v in values[:_UNPLACEABLE_VALUES_LOGGED])
            omitted = max(0, len(values) - _UNPLACEABLE_VALUES_LOGGED)
            field = self._live_info.object_prefix_field if self._live_info is not None else "<field>"
            log.warning(
                "Skipped %d record(s) in %d partition group(s) whose %r names no object prefix of %s.%s "
                "(every file sits under %s=<value>/; a null, negative, non-canonical or out-of-range value "
                "cannot be placed): %s%s. Counted on records_skipped_total{reason=%r}; these records are "
                "lost and the offsets commit over them.",
                rows,
                len(skipped),
                field,
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
                field,
                listed,
                f" and {omitted} more value(s)" if omitted else "",
                _UNPLACEABLE_REASON,
            )
            metrics.records_skipped_total.labels(reason=_UNPLACEABLE_REASON).inc(rows)
        except Exception:  # noqa: BLE001 - a metric or a log line, never a failed flush
            log.exception("Could not book the skipped rows of this flush; the publication is unaffected")

    def _observe_variant_rows(self) -> None:
        """Book the published flush's `VariantReport` on the row metrics.

        pyhoglake fills ONE report for the whole prepare — every group,
        every variant column — so the counts come in two shapes and the
        metrics follow them honestly rather than inventing a split:

        * `rows`, `sql_null_rows`, `invalid_rows` and `invalid_by_reason`
          are summed over the destination's variant columns: a 1000-row
          flush into a table with seven variant columns reports
          `rows == 7000`. `millpond_variant_rows_total{outcome}` and
          `millpond_variant_invalid_total{reason}` therefore count
          COLUMN-ROWS and carry no `column` label — pyhoglake does not
          say which column an invalid row was in, except for the first
          (`first_invalid`, logged at prepare).
        * `typed` / `fallback` / `variant_null` are keyed by
          `(column, declared path)`, so `millpond_variant_values_total`
          carries both. Paths are the declaration's (at most 1000 per
          column, fixed for the column's life), never a payload key, so
          the series set is bounded by the table, not the data.

        Called once per PUBLISHED flush — after a commit the server
        applied, and after one it answered from a receipt. The receipt
        case is at-least-once, like everything else about it: if the
        process that published died between its commit and its offset
        commit, it booked these rows already and this replay books them
        again; if it died between the upload and the commit response,
        nobody did. The metric cannot tell the two apart any more than
        `records_written_total` can, and a duplicate count on a pod
        crash is the cheaper error. Never for a refused commit: its
        rebuild books its own.
        Guarded, because it runs after the rows are published and a
        failure here must not make the loop replay a landed commit.
        """
        report = self._prepared_variant
        if report is None:
            return
        try:
            encoded = report.rows - report.sql_null_rows - report.invalid_rows
            metrics.variant_rows_total.labels(outcome="encoded").inc(encoded)
            metrics.variant_rows_total.labels(outcome="sql_null").inc(report.sql_null_rows)
            metrics.variant_rows_total.labels(outcome="invalid").inc(report.invalid_rows)
            for reason, count in report.invalid_by_reason.items():
                metrics.variant_invalid_total.labels(reason=reason).inc(count)
            for outcome, counts in (
                ("typed", report.typed),
                ("fallback", report.fallback),
                ("variant_null", report.variant_null),
            ):
                for (column, path), count in counts.items():
                    if count:
                        metrics.variant_values_total.labels(column=column or "", path=path, outcome=outcome).inc(count)
        except Exception:  # noqa: BLE001 - a metric, never a replayed commit
            log.exception("Could not book the variant rows of this flush; the commit is unaffected")

    def _log_invalid_rows(self, report: VariantReport, groups, info, facts: _FlushFacts) -> None:
        """One warning per prepare with invalid rows, naming a record an
        operator can find: the Kafka offset range(s) of the flush, and
        — where the batch carries a `uuid` column — the uuid and partition
        values of one invalid row.

        pyhoglake's `first_invalid` is `(column, row, reason, path)` with
        `row` an index INTO ITS GROUP, and the report does not say which
        group. So the row is located here: every group long enough to
        hold that index has its text at that index re-encoded on its own
        (`variant.encode_json`, one value, `raise`), and the first whose
        refusal matches the reported reason is the one. Hundreds of
        single-value encodes at most, microseconds each, only on a
        prepare that had an invalid row; and if no group matches (a
        pyhoglake whose numbering changed) the line still carries the
        offsets, which bound the search to one flush."""
        if not report.invalid_rows:
            return
        column, row, reason, path = report.first_invalid or (None, -1, "unknown", None)
        where = self._locate_invalid_row(groups, info, column, row, reason)
        offsets = ", ".join(f"{t}[{p}]:{a}-{b}" for t, p, a, b in facts.kafka_offsets) or "unknown (anonymous flush)"
        log.warning(
            "Variant encode nulled %d value(s) this flush (%s); one such row: column %r%s (%s), %s; "
            "Kafka offsets %s. The rows landed with SQL NULL in that column",
            report.invalid_rows,
            ", ".join(f"{r}={n}" for r, n in sorted(report.invalid_by_reason.items())),
            column,
            f" at {path}" if path else "",
            reason,
            where,
            offsets,
        )

    @staticmethod
    def _locate_invalid_row(groups, info, column: str | None, row: int, reason: str) -> str:
        """`uuid=... partition=...` for the first group whose value at
        `row` of `column` the encoder refuses for `reason`, or a note
        that it could not be found. Best effort: never raises."""
        try:
            from pyhoglake import variant

            live = next((c for c in info.columns if c.name == column), None)
            if live is None or row < 0:
                return f"row {row} of its partition group (uuid unknown)"
            for partition_values, group in groups:
                if row >= group.num_rows or column not in group.schema.names:
                    continue
                text = group.column(column)[row].as_py()
                if text is None:
                    continue
                try:
                    variant.encode_json([text], live, on_invalid="raise")
                except VariantEncodingError as refused:
                    if refused.reason != reason:
                        continue
                    uuid_ = group.column("uuid")[row].as_py() if "uuid" in group.schema.names else None
                    return (
                        f"uuid={uuid_!r} partition={list(partition_values) if partition_values is not None else None} "
                        f"row {row} of its group"
                    )
            return f"row {row} of its partition group (uuid not located)"
        except Exception as e:  # noqa: BLE001 - a log line, never a failure
            return f"row {row} of its partition group (uuid not located: {e})"

    def _commit_prepared(self) -> int:
        """Publish the prepared request, or recognize that it is already
        published.

        Transport failures here are UNCERTAIN, never clean failures: a
        timeout or a reset means the commit may have applied and the
        answer was lost. The payload therefore stays cached and the
        exception propagates, so main.py's retry loop sends THE SAME
        request again — which the server resolves under the per-catalog
        commit lock: a receipt for this key returns the original result
        without writing, and no receipt means it really did not land.
        Either way the rows publish exactly once.

        A refusal the server ANSWERED is the opposite case, and the
        payload must not survive it. One commit is one transaction: a
        409 or a 422 means zero rows were written and means the same
        thing to every identical resend, so holding the payload turned
        `reset_caches()` into a no-op (the replay short-circuits before
        the table is ever re-resolved) and burned the retry budget on a
        request that could not change. Those clear the payload here, at
        the refusal — the one place that knows the server judged it.
        """
        payload = self._prepared
        if payload is None:  # unreachable; an explicit raise, not an assert (python -O strips those)
            raise HoglakeSinkError("_commit_prepared called with no prepared payload")
        files = payload["appends"][0]["files"]
        self._prepared_sends += 1
        replayed = self._prepared_sends > 1
        try:
            # `table=` whenever the sink still holds the handle the
            # payload was prepared from: pyhoglake then invalidates THAT
            # Table's writer cache on a `re_prepare` refusal, so the
            # rebuild cannot send the same stale `read_snapshot` and take
            # the same refusal again. `reset_caches()` drops the handle
            # and so the cache with it, which is the other way out — but
            # one that depends on main.py calling it, and a livelock is
            # too expensive a thing to leave to a caller's manners. None
            # here is the cross-process form (a replay after a reset),
            # where there is no cache left to invalidate.
            self._catalog.commit_prepared(payload, table=self._table)
        except HoglakeError as e:
            # FIRST, before any branch, and unconditionally for the whole
            # HoglakeError family: drop our cached shape.
            #
            # THE INVARIANT IS "THE SINK IS NEVER STALER THAN
            # PYHOGLAKE'S CACHE" (see `_live_info_ttl_s`), and the TTL
            # alone does not hold it here. pyhoglake drops `Table._cache`
            # on a `re_prepare` refusal, on a 410, AND on any
            # non-retryable refusal whose `read_snapshot` matched that
            # cache (`_basis_was_cached`) — which includes the reused-key
            # 422 this method turns into SUCCESS, and a 5xx, since
            # `HoglakeError.retryable` is False on the base class. Every
            # one of those leaves pyhoglake re-reading on its next
            # prepare and us not, so pyhoglake's basis moves forward
            # first and the same-arity spec-drift hole reopens INSIDE the
            # TTL — the exact state the quarter-retention exists to make
            # unreachable.
            #
            # So the rule is not "mirror pyhoglake's predicate" (two
            # copies of someone else's invalidation logic, drifting) but
            # "any event that can drop pyhoglake's cache drops ours". It
            # over-drops on a plain `commit_conflict`, where pyhoglake
            # keeps its cache; the cost is one table read on the rebuild
            # that refusal already forces, and the alternative is
            # re-deriving `_basis_was_cached` from a private helper.
            #
            # Transport failures are deliberately NOT here: they are not
            # HoglakeErrors, nothing was judged, pyhoglake's cache is
            # untouched, and the payload is replayed verbatim with its
            # basis frozen — so there is nothing to be stale about.
            self._drop_cached_shape()
            # ONE arm, deliberately. Split across `except ValidationError`
            # and `except HoglakeError` this read as two rules, but
            # pyhoglake maps every 422 on the wire to `ValidationError`
            # — so the first arm swallowed all of them and the second's
            # 422 case could not execute. Two paths that state the same
            # rule, one of them unreachable, is how the rule ends up
            # stated differently in each.
            if isinstance(e, ValidationError) and _REUSED_KEY_MARKER in _error_text(e):
                return self._accept_already_published(payload, files)
            if isinstance(e, _DESTINATION_MOVED):
                raise self._destination_moved(e) from e
            if isinstance(e, ObjectPrefixViolationError):
                # Before the generic 422 arm: a `ValidationError` by
                # class, but the server's `retry: re-prepare` on it is
                # the rebuild recovery, once. See `_object_prefix_refused`.
                raise self._object_prefix_refused(e) from e
            if _is_answered_refusal(e):
                self._discard_prepared(f"the server refused the commit with a {e.status_code}", reason="commit_refused")
            raise
        # A landed commit proves the layout the sink writes is the one the
        # server enforces, so a later object-prefix refusal gets its own
        # one rebuild rather than inheriting this flush's verdict.
        self._object_prefix_rebuilt_for = None
        # One parquet per partition tuple per flush (fanout appends) —
        # the hoglake compaction-debt feed rate. Counted AFTER the commit
        # returns: files this process uploaded but did not get registered
        # are orphans, not writes.
        metrics.hoglake_files_written_total.inc(len(files))
        # Fanout, off the same registered-file list and under the same
        # "only what the commit published" rule. See `_observe_fanout`.
        _observe_fanout(files)
        # The variant row counts, under the same rule: once, for the rows
        # this commit published — and the rows it skipped as unplaceable.
        self._observe_variant_rows()
        self._book_skipped()
        if replayed:
            # The healthy half of the replay story, and previously
            # invisible: a commit that was re-sent after an uncertain
            # outcome and came back 200. The server either answered from
            # its receipt or applied it now; either way these rows
            # published exactly once, and an operator watching a flapping
            # network wants to see this rate rather than infer it.
            metrics.hoglake_commit_replays_total.labels(outcome="replayed").inc()
        rows = self._prepared_rows
        self._clear_prepared()
        return rows

    def _destination_moved(self, refused: HoglakeError) -> HoglakeSinkError:
        """Turn one of the server's typed re-prepare refusals into this
        sink's own retryable stop, having abandoned the payload it refused.

        THIS IS WHERE THE PRE-COMMIT TABLE READ WENT. The sink used to
        re-read the destination immediately before every publish, to
        refuse a table that had moved under an already-uploaded payload:

        * the INCARNATION, because the server's `expected_table_uuid`
          guard answered with a bare 409 that said nothing about which
          table it meant;
        * the PARTITION SPEC, because a file is registered with the
          partition VALUES the client computed and stamped with the
          table's CURRENT spec_id. Re-spec from `identity(team_id)` to
          `bucket(team_id, 16)` — same arity, so the server's arity check
          passes — while a payload is in flight, and the file lands
          stamped as bucketed while carrying identity values. Every
          future scan prunes it wrongly, forever, with nothing anywhere
          saying so.

        Both are now answered by the payload's own `read_snapshot`,
        atomically and under the commit lock. What that is and is not:

        * a partition-spec change is an ALTER, and
          `AlterService.alterTable` mints exactly one `table_altered`
          change row per alter regardless of op count. An append-only
          commit's conflict scan counts `table_dropped` and
          `table_altered` on every table it touches since
          `read_snapshot` (`CommitService.checkConflicts`), and binds
          nothing else for an append — `requireUnchangedTables` scopes
          the row-content kinds to DELETE targets — so every row it can
          match is DDL and the refusal is always the typed
          `ddl_since_read_snapshot`, never a bare conflict. Schema DDL
          (another pod's add_column) lands in the same arm, which is why
          stripping `read_snapshot` used to be necessary and is now
          wrong.
        * THE WINDOW ONLY COVERS DRIFT INSIDE IT, and for the partition
          spec that is NOT automatic — it is bought by
          `_live_info_ttl_s`. `read_snapshot` comes from pyhoglake's
          writer cache, which refreshes itself at half the catalog's
          retention; past a refresh the basis sits AHEAD of an older
          alter, the scan finds nothing, and a payload built under the
          superseded spec is accepted and stamped with the live spec_id.
          Re-reading this sink's own copy at a quarter of the retention
          is what makes that state unreachable: the sink is never staler
          than `Table._cache`, so a spec the commit cannot see is a spec
          no flush is still computing under. This bullet is the whole
          reason the TTL exists; deleting it reopens silent permanent
          mis-pruning.
        * a drop+recreate is covered by NEITHER the window nor the TTL,
          because the scan keys on the resolved (new) table id. It is
          closed solely by `expected_table_uuid` -> 409
          `table_recreated`, which `_prepare` pins to the
          resolved-and-reconciled incarnation.
        * and a `read_snapshot` that sank below the catalog's expiry
          floor is a 410: the server cannot evaluate the conflict window
          at all, so it refuses rather than accepting on a basis it
          cannot check.

        What the server DOES do strictly better than the read this
        replaced is the timing: the check is inside the commit
        transaction under the catalog lock, so there is no residual
        window between checking and committing — which the client-side
        read admitted to having and could not close.

        Each is non-retryable AS THIS PAYLOAD — the basis is frozen in a
        request that must be replayed byte-identically, and the floor and
        the alter only ever move forward — and each is cleared by a
        REBUILT flush. So the payload is discarded (its uploads are
        orphans, counted by `_discard_prepared`), the sink's cached table
        info is dropped so the rebuild reads fresh, and the refusal
        becomes a `HoglakeSinkError(retryable=True)`: one type, because
        there is one recovery, and retryable because `is_retryable` would
        otherwise call `ReadSnapshotExpiredError` permanent and re-raise
        instead of rebuilding. The server's own exception stays attached
        as `__cause__`.
        """
        table_name = f"{self._cfg.hoglake_namespace}.{self._cfg.hoglake_table}"
        prepared_for = (self._prepared or {}).get("appends", [{}])[0].get("expected_table_uuid")
        if isinstance(refused, IncarnationChangedError):
            reason = "table_recreated"
            why = "the destination table was recreated before the commit"
            message = (
                f"table {table_name} was recreated while this flush was in flight: prepared "
                f"against table_uuid {prepared_for}, which the name no longer resolves to "
                f"({refused}). The prepared upload is abandoned; the flush rebuilds against the "
                f"live incarnation."
            )
        elif isinstance(refused, ReadSnapshotExpiredError):
            reason = "read_snapshot_expired"
            why = "the prepared commit's read_snapshot fell below the catalog's expiry floor"
            message = (
                f"the conflict basis of this flush to {table_name} expired before the commit "
                f"landed ({refused}); the server cannot judge the prepared files against a "
                f"snapshot it no longer retains. The prepared upload is abandoned; the flush "
                f"rebuilds against a retained snapshot."
            )
        else:
            reason = "ddl_since_read_snapshot"
            why = "DDL landed on the destination before the commit"
            message = (
                f"schema or partition spec of {table_name} changed while this flush was in "
                f"flight ({refused}); the prepared files were built against the old layout and "
                f"would be registered under the new one. Rebuilding the flush."
            )
        self._discard_prepared(why, reason=reason)
        # The cached shape is stale BY THE SERVER'S OWN STATEMENT, so the
        # rebuild must read rather than re-derive the same doomed payload
        # from it. pyhoglake invalidates its own cache on these refusals
        # (`Catalog._commit` does it for a `re_prepare`); this is the half
        # that lives on millpond's side of the line.
        if isinstance(refused, IncarnationChangedError):
            # A RECREATION INVALIDATES THE IDENTITY, NOT JUST THE SHAPE,
            # so this one needs the full reset. `self._table` is a handle
            # to a name and `self._table_uuid` is the dead incarnation —
            # and `_table_uuid` is what `_prepare` pins
            # `expected_table_uuid` to AND what `_flush_key` hashes. Drop
            # only the shape and a retry re-prepares against the corpse:
            # same expected uuid, same key, refused identically, for the
            # whole retry budget. main.py's loop calls `reset_caches()`
            # between attempts and would mask it, which is exactly why it
            # is wrong to rely on: a caller that retries without
            # resetting (or any future one) gets a livelock out of a
            # refusal that is supposed to be recoverable.
            self.reset_caches()
        else:
            self._drop_cached_shape()
        return HoglakeSinkError(message, retryable=True)

    def _object_prefix_refused(self, refused: ObjectPrefixViolationError) -> HoglakeSinkError:
        """Turn the server's 422 `object_prefix_violation` on a prepared
        commit into this sink's own stop: retryable ONCE per flush, then
        permanent.

        The refusal means the payload registered a file the table's
        object-prefix layout does not admit — not under its value's
        `<field>=<value>/`, or whose stats do not prove the value. Zero
        rows were written, and replaying the payload cannot change the
        answer (the paths are frozen in it), so like the three
        `_DESTINATION_MOVED` refusals it is a re-prepare: the server says
        so (`retry: re-prepare`, `re_prepare=True` on the class).

        Unlike those three, a rebuild only helps for ONE cause: the sink
        and pyhoglake resolved the table before the option was set on it
        (the setup script's `set_object_prefix_field` racing the first
        flush), prepared under the root layout, and the server now
        enforces the prefixed one. A fresh read shows the field, pyhoglake
        places the rebuilt files under it, and the commit lands. Every
        other cause is permanent — the layout the client writes disagrees
        with the one the server checks (a server rule newer than this
        client's pyhoglake; damaged table metadata) — and a rebuild
        reproduces the refusal
        exactly, orphaning another fanout of objects each time until the
        retry budget is spent. So the FIRST refusal of a flush abandons
        the payload (its uploads are orphans, counted under
        `object_prefix_violation`), drops the cached shape so the rebuild
        reads fresh, and returns a retryable stop; the SECOND refusal of
        the same flush returns a permanent one, with the server's detail
        attached. "The same flush" is its Kafka identity, which survives
        `reset_caches()` — main.py calls that between the attempts. An
        ANONYMOUS flush (no offsets; direct callers only, main.py always
        names its flushes) is identified by its idempotency key, which
        `_flush_key` mints fresh per anonymous call: such a caller's
        retry is a new flush by construction, so each gets its one
        rebuild and none inherits another's verdict.

        pyhoglake drops its own writer cache on this refusal
        (`Catalog._commit`, `re_prepare`), and `_commit_prepared` dropped
        this sink's cached shape before branching here, so the rebuild
        reads fresh on both sides and its `read_snapshot` moves forward.
        """
        table_name = f"{self._cfg.hoglake_namespace}.{self._cfg.hoglake_table}"
        identity = self._prepared_offsets or ("idempotency_key", (self._prepared or {}).get("idempotency_key"))
        rebuilt_already = self._object_prefix_rebuilt_for == identity
        self._discard_prepared(
            "the server refused the commit as object_prefix_violation (files not under the table's "
            "object-prefix layout)",
            reason=_OBJECT_PREFIX_REFUSED_REASON,
        )
        if not rebuilt_already:
            self._object_prefix_rebuilt_for = identity
            return HoglakeSinkError(
                f"the server refused this flush to {table_name} as object_prefix_violation ({refused}): the "
                f"prepared files were not placed under the table's object-prefix layout, which the sink read "
                f"before the option was set on the table. The prepared upload is abandoned; the flush rebuilds "
                f"once against a fresh read of the layout.",
                retryable=True,
            )
        return HoglakeSinkError(
            f"the server refused this flush to {table_name} as object_prefix_violation twice — once against "
            f"the cached layout and again after a rebuild against a fresh read ({refused}). The files pyhoglake "
            f"places are not the files the server admits, so no retry can land them: the server's object-prefix "
            f"rules are newer than this client's placement, or the table's layout is damaged. Not retried.",
            retryable=False,
        )

    def _accept_already_published(self, payload: dict, files: list) -> int:
        """The receipt exists and our request is not the one it was
        written for. Decide whether that means the rows are in the lake.

        The key names (catalog, namespace, table, table_uuid, the full
        offset range per partition), and the server writes a receipt only
        in the same transaction that publishes. So a receipt under this
        key is a statement that THIS range was published to THIS
        incarnation — by a previous process, whose payload differed from
        ours in the parts that cannot be reproduced (a fresh
        `_inserted_at` stamp, fresh uuid4 object names). That is the
        crash-restart case, and it is the case the receipt exists for:
        failing on it would wedge the partition forever on rows that are
        already there.

        What this must never do is accept on the strength of a receipt
        that belongs somewhere else, and nothing here has to check that
        because `_flush_key` already has: `table_uuid` is one of the
        components hashed into the key, so a receipt found under THIS key
        was written by a commit to THIS incarnation. The key is the
        whole of that argument, and it has to be, because NOTHING
        re-checks the live incarnation at replay time — not this method,
        and not the server.

        THE SERVER ANSWERS FROM THE RECEIPT BEFORE IT RESOLVES ANY
        TABLE. `CommitService.doCommit` looks the receipt up first
        (v1.3.7, lines ~456-527) and returns it on a hit; table
        resolution and the `expected_table_uuid` check are after that
        (~555+). So the sequence commit-applies-to-A, response-lost,
        A-dropped-and-recreated-as-B, replay returns A's receipt and this
        sink accepts — with no 409 `table_recreated` anywhere, even
        though the name now resolves to B.

        That is the intended exactly-once-by-key semantics, not a hole in
        it. The rows WERE published, into the table that existed when the
        commit landed. The drop destroyed them afterwards, which is what
        a drop does. Re-publishing them into B would publish the same
        flush twice — and the flush's own offsets have already advanced
        past it, so there would be nothing to tell the two apart later.
        Accepting is the only answer that does not invent a write.

        Rows returned: ZERO. This process published nothing — some
        earlier one did — and `records_written_total` counts rows this
        process wrote. Reporting the batch size here is how a writer came
        to claim eight rows for a range that had three in the lake.

        What the key does NOT name, and therefore cannot tell apart:

        * two writers over the same offsets whose FILTERS differ, so the
          same range means different rows;
        * two writers whose topic NAMES coincide on different Kafka
          clusters. There is no broker or cluster identity in the key at
          all, so `events:0:30-41` on one cluster and `events:0:30-41`
          on another hash identically, and the second pipeline's flush
          is answered by the first's receipt;
        * two consumer groups on the same topic, for the same reason.

        Broker identity is deliberately NOT in the key. millpond has no
        stable cluster id to put there — `BROKER_SOURCE` is a free-text
        metrics label and `KAFKA_BOOTSTRAP_SERVERS` is a rotated,
        load-balanced address list, not an identity — so the only
        candidates are config strings that change while the cluster does
        not. Putting one in the key means every edit to it re-randomizes
        the identity of every in-flight flush and reopens the duplicate
        window for exactly one restart, which is the same hazard
        `_IDEMPOTENCY_NAMESPACE` is pinned against. The residual it would
        buy is small in exchange: every case above needs two pipelines
        already writing one table with config that disagrees about what
        that table contains.

        All three are deployment faults, not recoverable states: the
        offsets advance over whichever publication landed first, and the
        warning below names the key so it can be traced.
        """
        log.warning(
            "Kafka offsets for this flush were already published to %s.%s under a different "
            "registration (idempotency key %s); this process publishes nothing and orphans %d "
            "uploaded file(s)",
            self._cfg.hoglake_namespace,
            self._cfg.hoglake_table,
            payload["idempotency_key"],
            len(files),
        )
        metrics.hoglake_commit_replays_total.labels(outcome="already_published").inc()
        _count_orphans(
            len(files),
            "the offset range was already published",
            [f["path"] for f in files],
            reason="already_published",
        )
        # The rows ARE in the lake, once, under this key. Whether the
        # process that put them there booked them depends on where it
        # died (after its commit landed: yes; before the response: no),
        # so this may count them a second time — at-least-once, as the
        # receipt path is for the rows themselves. Booked rather than
        # skipped because a pod that dies mid-commit is the common case
        # the receipt exists for, and an undercount there is silent.
        self._observe_variant_rows()
        self._book_skipped()
        self._clear_prepared()
        return 0

    def _discard_prepared(self, why: str, *, reason: str) -> None:
        """Drop a prepared payload that will never be published, and
        account for the upload it leaves behind."""
        if self._prepared is None:
            return
        files = self._prepared["appends"][0]["files"]
        log.warning(
            "Abandoning a prepared hoglake commit (idempotency key %s, %d file(s)): %s",
            self._prepared.get("idempotency_key"),
            len(files),
            why,
        )
        _count_orphans(len(files), why, [f["path"] for f in files], reason=reason)
        self._clear_prepared()

    def _clear_prepared(self) -> None:
        self._prepared = None
        self._prepared_rows = 0
        self._prepared_offsets = None
        self._prepared_sends = 0
        self._prepared_variant = None
        self._prepared_skipped = None

    # -- metadata column ---------------------------------------------------

    @staticmethod
    def _stamp_inserted_at(batch: pa.Table) -> pa.Table:
        """Append `_inserted_at` (timestamptz, micros, UTC), one value for
        the whole flush. DuckLake's SQL `NOW()` allows microsecond drift
        within a flush; here every row of a flush shares the timestamp —
        strictly more useful for flush forensics, and partition
        transforms bucket identically."""
        now = datetime.now(UTC)
        col = pa.repeat(pa.scalar(now, type=_INSERTED_AT_TYPE), batch.num_rows)
        return batch.append_column(pa.field(_INSERTED_AT, _INSERTED_AT_TYPE, nullable=True), col)

    # -- bootstrap ---------------------------------------------------------

    def _resolve_catalog(self):
        """Resolve (or create) the catalog. Called from __init__, so every
        failure here is a startup failure with a message that says what to
        fix."""
        cfg = self._cfg
        try:
            return self._client.catalog(cfg.hoglake_catalog)
        except NotFoundError:
            if cfg.hoglake_data_path is None:
                raise HoglakeSinkError(
                    f"hoglake catalog {cfg.hoglake_catalog!r} does not exist and HOGLAKE_DATA_PATH "
                    f"is not set; create the catalog first or set HOGLAKE_DATA_PATH to let "
                    f"millpond create it"
                ) from None
            try:
                catalog = self._client.create_catalog(
                    cfg.hoglake_catalog,
                    cfg.hoglake_data_path,
                    author=self._author,
                    message=self._ddl_message(
                        f"startup for {cfg.topic}",
                        f"create catalog {cfg.hoglake_catalog}",
                        "HOGLAKE_DATA_PATH",
                    ),
                )
                log.info("Created hoglake catalog %s (data_path=%s)", cfg.hoglake_catalog, cfg.hoglake_data_path)
                return catalog
            except AlreadyExistsError:
                # Another pod created it between our GET and our POST.
                return self._client.catalog(cfg.hoglake_catalog)
        except (HoglakeError, httpx.HTTPError, OSError) as e:
            # Bad URL, DNS, TLS, a control plane that is down, a proxy
            # answering HTML: all of it lands here, and all of it is a
            # deployment problem the operator can see from the message.
            raise HoglakeSinkError(
                f"cannot reach the hoglake control plane at {cfg.hoglake_url!r} to resolve "
                f"catalog {cfg.hoglake_catalog!r}: {e}"
            ) from e

    def _probe_object_store(self, catalog) -> None:
        """One authenticated object-store WRITE at startup. Called from
        __init__, once, and never again.

        A zero-byte upload of a fixed marker key under the catalog's
        data path (`<data_path>/_millpond/probe`). Not literally a
        PutObject: pyarrow's `open_output_stream` opens a multipart
        upload eagerly, so closing an empty stream sends
        CreateMultipartUpload -> UploadPart(1, 0 bytes) ->
        CompleteMultipartUpload — one empty part, which both S3 and
        MinIO accept. All three calls are authorised by `s3:PutObject`;
        `s3:AbortMultipartUpload` is not exercised on this path, it is
        what cleans up an upload that dies half-way.

        The shape is chosen for three reasons:

        * It issues a real write request with the sink's own
          credentials, which is the point of writing rather than
          looking. It is no longer the same CALL the flush path makes,
          and that claim died at pyhoglake 1.3.7: with boto3 present (the `fast-upload` extra, which
          millpond pins) `prepare_append_tables` puts every object at or
          under 8 MiB up as a single boto3 `put_object`
          (`pyhoglake/upload.py`, `perform_upload`), and only a larger
          one still takes `open_output_stream`. So the probe exercises
          the fallback transport rather than the hot one, and in
          particular it resolves credentials through Arrow's C++ SDK
          chain while the hot path resolves them through botocore's.
          pyhoglake hands both the same explicit fields — access key,
          secret key, endpoint override with the same path-style
          addressing, region when one is set — and refuses ambient
          endpoint configuration on both sides, so a static-key or
          IRSA-by-omission deployment proves the same identity either
          way. What the probe does NOT cover is a divergence between the
          two chains themselves (botocore falls back to its own default
          region and leans on S3's region redirect where Arrow resolves
          the bucket's region), which is the one reason
          `HOGLAKE_S3_REGION` is worth setting explicitly even under
          IRSA.
        * It proves the grant the sink actually needs. The role carries
          write access under this prefix and nothing else — a LIST would
          test `s3:ListBucket`, a permission the role is not meant to
          have, and so would pass or fail for reasons unrelated to
          writing.
        * It cannot pass on a bucket that is not there. pyarrow's
          `get_file_info(FileSelector(..., allow_not_found=True))` maps
          a NoSuchBucket 404 to an empty listing, so a typo'd bucket in
          HOGLAKE_DATA_PATH read as "empty prefix, all good" and failed
          on the first flush — by which time the catalog row has frozen
          the bad path and hoglake has no route to delete a catalog.
          `allow_not_found=False` does not distinguish the two either.

        The marker is FIXED, so a restart overwrites it: at most one
        object per catalog data path, forever. It is not registered in
        any table, and hoglake never touches unregistered objects —
        `cleanup` drains only paths the server queued, and `verify` is
        metadata-only — so the marker is inert, not orphan debt.

        It is deliberately not retried and not a liveness check. A
        credential chain that resolves nothing and a policy that grants
        nothing are both config, and config does not heal by waiting.
        """
        # pyhoglake builds the S3FileSystem lazily and keeps it private.
        # Unlike the `_http` Retry-After hook, an absent accessor here
        # is FATAL rather than a shrug: this is the proof that the pod
        # can write at all, and a guard that quietly stops guarding when
        # an attribute is renamed is worse than no guard, because the
        # deployment still reads as verified.
        accessor = getattr(self._client, "_filesystem", None)
        if accessor is None:
            raise HoglakeSinkError(
                "pyhoglake no longer exposes the filesystem; the startup probe cannot run, and "
                "millpond will not start without proving it can write to the catalog's data path"
            )
        uri = _probe_marker_uri(catalog.data_path)
        key = uri.removeprefix("s3://")
        try:
            with accessor().open_output_stream(key):
                # Zero bytes: the upload itself is the entire question.
                # The `with` is load-bearing — the request is only sent
                # on close, and leaving that to the garbage collector
                # would make the probe's verdict depend on refcounting.
                pass
        except Exception as e:  # noqa: BLE001 - every failure here is the same startup refusal
            raise HoglakeSinkError(
                f"cannot write the startup probe object {uri} for hoglake catalog "
                f"{self._cfg.hoglake_catalog!r} using {self._credential_source}: {e}. "
                f"{_probe_hint(str(e), self._credential_source)}"
            ) from e
        log.info(
            "hoglake object store auth source: %s — probe wrote %s (endpoint=%s, region=%s)",
            self._credential_source,
            uri,
            self._cfg.hoglake_s3_endpoint or "AWS default",
            self._cfg.hoglake_s3_region or "unset",
        )

    def _ensure_table(self, batch_schema: pa.Schema):
        """Resolve (or create) namespace -> table, tolerating concurrent
        creation by other pods at every level, and reconcile the live
        partition spec / sort order against config. Cached for the sink's
        lifetime; reset_caches() drops the cache.

        `self._table` is assigned LAST, deliberately: any failure in here
        — including a spec declaration the server refuses — must leave
        the cache empty so the next attempt re-checks instead of
        returning a table whose layout was never declared."""
        if self._table is not None:
            return self._table

        cfg = self._cfg
        catalog = self._catalog
        try:
            ns = catalog.namespace(cfg.hoglake_namespace)
        except NotFoundError:
            try:
                ns = catalog.create_namespace(
                    cfg.hoglake_namespace,
                    author=self._author,
                    message=self._ddl_message(
                        f"first flush of {cfg.topic}", f"create namespace {cfg.hoglake_namespace}"
                    ),
                )
                log.info("Created hoglake namespace %s", cfg.hoglake_namespace)
            except AlreadyExistsError:
                ns = catalog.namespace(cfg.hoglake_namespace)

        try:
            table = ns.table(cfg.hoglake_table)
            log.info("Hoglake table %s.%s already exists", cfg.hoglake_namespace, cfg.hoglake_table)
            self._adopt_columns(table.columns)
            # Against the shape `json_text_columns` read moments ago when
            # it is still fresh (the same TTL the steady state trusts),
            # so the first write does not read what the conversion
            # before it already did; a cold cache reads here, as before.
            self._reconcile_specs(table, self._live_info if self._live_info_is_fresh() else None)
        except NotFoundError:
            table = self._create_table(ns, batch_schema)

        self._table = table
        self._table_uuid = table.table_uuid
        return table

    def _create_table(self, ns, batch_schema: pa.Schema):
        """Create the events table from the (sanitized, stamped) batch
        schema, then declare the partition spec and sort order in one
        alter. Concurrent creation loses cleanly to the winner."""
        cfg = self._cfg
        schema = table_schema_for_batch(batch_schema)
        try:
            # The message names the layout knobs even though the specs
            # land in the alter that follows: they are part of the
            # definition this config produced, and the snapshot that
            # created the table is where an operator looks for it.
            table = ns.create_table(
                cfg.hoglake_table,
                schema,
                author=self._author,
                message=self._ddl_message(
                    f"first flush of {cfg.topic}",
                    f"create table {cfg.hoglake_namespace}.{cfg.hoglake_table} with {len(schema)} columns "
                    f"from the batch",
                    "HOGLAKE_PARTITION_BY",
                    "MILLPOND_SORT_BY",
                    "MILLPOND_TYPED_COLUMNS",
                ),
            )
            log.info(
                "Created hoglake table %s.%s with %d columns",
                cfg.hoglake_namespace,
                cfg.hoglake_table,
                len(schema),
            )
        except AlreadyExistsError:
            # Another pod won the race. The winner declares the specs —
            # but "the winner will do it" is exactly the assumption that
            # made the old create-then-alter window permanent, so the
            # loser verifies rather than trusting.
            log.info("Hoglake table %s created by another pod, continuing", cfg.hoglake_table)
            table = ns.table(cfg.hoglake_table)
            self._adopt_columns(table.columns)
            self._reconcile_specs(table)
            return table

        self._adopt_columns(table.columns)
        self._declare_specs(table, why=f"first flush of {cfg.topic}")
        return table

    def _declare_specs(self, table, *, why: str, partition: bool = True, sort: bool = True) -> None:
        """Declare the configured partition spec + sort order in ONE
        alter, then VERIFY the result.

        `partition` / `sort` narrow the declaration to the halves that
        are actually missing. Re-declaring a spec the table already
        carries is not free: it is a DDL commit through the per-catalog
        lock, it races every other writer's DDL, and it re-versions a
        layout nobody asked to change.

        `why` opens the alter's snapshot message: a fresh table and an
        existing one that lost its spec to a failed bootstrap are the
        same ops for different reasons, and only the second is an
        incident worth reading about later.

        Creation is two round trips and the gap between them is the
        hazard: the table exists before its layout does. Only a
        concurrent-DDL 409 is absorbed here (another pod declaring the
        same config-identical specs); anything else propagates, and
        because `_ensure_table` has not cached the table yet, the next
        attempt re-resolves, finds the table present and unspecced, and
        tries again — failing identically for as long as the config is
        wrong, which is the point. Silently writing to a table whose
        partitioning the operator asked for and never got is the failure
        mode this replaces."""
        spec_ops = self._spec_ops(partition=partition, sort=sort)
        if not spec_ops:
            return
        # Named from the ops actually sent, not from the knobs: a sort
        # field missing from the schema drops the sort op, and the
        # message must not claim a declaration that never happened.
        declared = {op.op for op in spec_ops}
        halves = [
            (label, setting)
            for op, label, setting in (
                ("set_partition_spec", "partition spec", "HOGLAKE_PARTITION_BY"),
                ("set_sort_order", "sort order", "MILLPOND_SORT_BY"),
            )
            if op in declared
        ]
        message = self._ddl_message(
            why,
            "declare " + " and ".join(label for label, _ in halves),
            *(setting for _, setting in halves),
        )
        try:
            info = table.alter(spec_ops, author=self._author, message=message)
            log.info("Declared hoglake table specs: %s", ", ".join(op.op for op in spec_ops))
        except CommitConflictError as e:
            # Concurrent DDL (another pod racing the same specs). The
            # specs are config-identical across the fleet, so the winner
            # declared the same thing — which the verification below
            # proves rather than assumes.
            log.info("Hoglake spec DDL raced another writer, continuing: %s", e)
            info = table.info(totals=False)
        self._adopt_info(info)
        self._verify_specs(info)

    def _reconcile_specs(self, table, info=None) -> None:
        """Compare the live partition spec / sort order of an EXISTING
        table against config.

        The spec was only ever declared at CREATE, so a deployed pipeline
        that changed HOGLAKE_PARTITION_BY silently kept the old layout,
        and a pod whose config had lost its partitioning (the
        MILLPOND_DESTINATION flip nulls the DuckLake partition var) wrote
        on as if nothing had happened. Config is the declaration of what
        this table's layout is; a pod that disagrees with the table it
        writes to stops, loudly, the same way MILLPOND_VARIANT_COLUMNS
        stops a hoglake pod at startup.

        The one divergence that is NOT an error is an unspecced table
        that config says should be specced: that is the create-then-alter
        window reopening, and declaring the spec is the recovery. On an
        object-prefix table that recovery is unreachable by construction
        — the server sets the option only on a spec with the identity
        field and refuses every spec change that drops it, so the live
        spec is never empty — and `_check_object_prefix` (run on the
        adopt, before the compare below) has already refused a config
        without `identity(<field>)`. The only alter this method can issue
        on such a table is therefore none: the spec either matches config
        and nothing is declared, or disagrees and the pod stops.

        `info` lets a caller that has JUST read the table hand its copy
        over instead of paying a second request — which is what
        `_table_info` does on every TTL refresh, and the reason this
        check is no longer a pod-start-only tripwire. Omitted, it reads."""
        # No early out when both knobs are unset: "config says
        # unpartitioned, the table is partitioned" is the destination-flip
        # case, and it is the one this check exists for.
        if info is None:
            info = self._adopt_info(table.info(totals=False))
        want_partition = self._want_partition_fields()
        live_partition = _partition_tuples(info.partition_spec)
        if live_partition and live_partition != want_partition:
            raise HoglakeSinkError(
                f"live partition spec of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} "
                f"{_describe_partition(info.partition_spec, self._live_columns)} does not match "
                f"HOGLAKE_PARTITION_BY {self._describe_configured_partition()}. Hoglake never "
                f"re-specs a table behind your back and millpond will not write under a layout "
                f"nobody declared: set HOGLAKE_PARTITION_BY to the live spec, or point this "
                f"pipeline at a new table."
            )

        want_sort = self._want_sort_fields()
        live_sort = _sort_tuples(info.sort_spec)
        sort_known = self._cfg.sort_by is None or want_sort is not None
        if sort_known and live_sort and live_sort != (want_sort or ()):
            raise HoglakeSinkError(
                f"live sort order of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does "
                f"not match MILLPOND_SORT_BY {self._cfg.sort_by!r}. The sort order is advisory for "
                f"writers but BINDING for hoglake compaction, so a mismatch means compaction "
                f"re-sorts every file this pod writes: set MILLPOND_SORT_BY to the live order, or "
                f"point this pipeline at a new table."
            )

        if want_partition and not live_partition:
            log.warning(
                "Hoglake table %s.%s exists with no partition spec but HOGLAKE_PARTITION_BY is set; "
                "declaring it now (a previous bootstrap created the table and failed before its "
                "spec landed)",
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
            )
            self._declare_specs(table, why=f"recovery for {self._cfg.topic} (table exists without its partition spec)")
        elif want_sort and not live_sort:
            log.warning(
                "Hoglake table %s.%s exists with no sort order but MILLPOND_SORT_BY is set; declaring it now",
                self._cfg.hoglake_namespace,
                self._cfg.hoglake_table,
            )
            # The sort alone: the partition spec is already live and
            # already matches (the mismatch check above ran first).
            self._declare_specs(
                table,
                why=f"recovery for {self._cfg.topic} (table exists without its sort order)",
                partition=False,
            )
        # The verdict, recorded against the shape it was reached ON —
        # `self._live_info` rather than `info`, because a declaration
        # above re-adopts a post-alter shape and THAT is what the next
        # flush will build from. `_table_info` compares against this to
        # decide whether an adopted shape still has a verdict; see the
        # "a fresh shape is not a reconciled shape" note there.
        self._reconciled_specs = _spec_identity(self._live_info or info)

    def _verify_specs(self, info) -> None:
        """Post-condition on the declaration: the live layout IS what
        config asked for. A server that accepted the alter and applied
        something else, or a code path that skipped an op, both end here
        rather than in a silently mis-laid-out table."""
        want_partition = self._want_partition_fields()
        if want_partition and _partition_tuples(info.partition_spec) != want_partition:
            raise HoglakeSinkError(
                f"hoglake table {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does not "
                f"carry the partition spec that was just declared for it "
                f"(HOGLAKE_PARTITION_BY {self._describe_configured_partition()}); refusing to "
                f"write to an undeclared layout"
            )
        want_sort = self._want_sort_fields()
        if want_sort and _sort_tuples(info.sort_spec) != want_sort:
            raise HoglakeSinkError(
                f"hoglake table {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} does not "
                f"carry the sort order that was just declared for it "
                f"(MILLPOND_SORT_BY {self._cfg.sort_by!r})"
            )

    def _want_partition_fields(self) -> tuple[tuple[int, str, int | None], ...]:
        """Configured partition spec as (source_field_id, transform,
        param) triples, resolved against the live columns."""
        cfg = self._cfg
        if not cfg.hoglake_partition_by:
            return ()
        fid = {name: col.field_id for name, col in self._live_columns.items()}
        out = []
        for column, transform, param in cfg.hoglake_partition_by:
            if column not in fid:
                raise HoglakeSinkError(
                    f"HOGLAKE_PARTITION_BY column {column!r} is not in the table schema "
                    f"(columns: {sorted(fid)}); partition columns must exist in the source "
                    f"events (or be _inserted_at)"
                )
            out.append((fid[column], transform, param))
        return tuple(out)

    def _want_sort_fields(self) -> tuple[tuple[int, str, str], ...] | None:
        """Configured sort order as (source_field_id, direction,
        null_order) triples, or None when a sort field is missing from the
        table schema — the pre-existing non-fatal degrade (the batch is
        still pre-sorted on the fields that ARE present)."""
        cfg = self._cfg
        if not cfg.sort_by:
            return ()
        fid = {name: col.field_id for name, col in self._live_columns.items()}
        missing = [c for c in cfg.sort_by if c not in fid]
        if missing:
            log.warning(
                "MILLPOND_SORT_BY field(s) %s missing from the hoglake table schema; "
                "skipping the sort-order declaration (batches are still pre-sorted on "
                "the fields present)",
                missing,
            )
            return None
        # asc + nulls_last mirrors main._apply_sort (ascending,
        # null_placement="at_end") so the declared order is the order
        # millpond actually writes.
        return tuple((fid[c], "asc", "nulls_last") for c in cfg.sort_by)

    def _describe_configured_partition(self) -> str:
        return ", ".join(
            f"{t}({c}{', ' + str(p) if p is not None else ''})" for c, t, p in (self._cfg.hoglake_partition_by or ())
        )

    def _ddl_message(self, why: str, what: str, *settings: str) -> str:
        """The `message` of a DDL snapshot: `<why>: <what>` and then each
        named setting as `; NAME=value`, in the order given.

        The data commits say WHAT landed and where it came from in
        Kafka (`format_commit_message`); a DDL snapshot has to say which
        configuration produced the definition, because that is the
        question its snapshot gets asked later — "why is this column
        uuid", "who asked for this partition spec", "is this table from
        before or after the MILLPOND_SORT_BY change". So the settings are
        named by their environment variable, in their own grammar, and
        an unset one reads `unset` rather than vanishing: a table created
        unpartitioned because the knob was missing is exactly the case
        worth being able to see.

        DETERMINISTIC for a given config and change: no clock, no
        counter, nothing per-attempt. A retry after a lost response and a
        second pod racing the same DDL describe it identically.
        """
        cfg = self._cfg
        values = {
            "HOGLAKE_DATA_PATH": cfg.hoglake_data_path,
            "HOGLAKE_PARTITION_BY": self._describe_configured_partition(),
            "MILLPOND_SORT_BY": ",".join(cfg.sort_by or ()),
            "MILLPOND_TYPED_COLUMNS": ",".join(f"{name}:{type_}" for name, type_ in (cfg.typed_columns or ())),
        }
        return "; ".join([f"{why}: {what}", *(f"{name}={values[name] or 'unset'}" for name in settings)])

    def _spec_ops(self, *, partition: bool = True, sort: bool = True) -> list[AlterOp]:
        """Partition-spec + sort-order alter ops, built from the same
        resolved tuples the reconciliation and verification paths
        compare against — so what is declared, what is checked and what
        the error message names can never drift apart.

        Partition columns must exist (fatal — data layout is load-bearing
        and the operator asked for it); sort fields degrade non-fatally
        like main._apply_sort's missing-field skip (the sort spec is
        advisory for writers, binding only for compaction).
        """
        spec_ops: list[AlterOp] = []
        partition_fields = self._want_partition_fields() if partition else ()
        if partition_fields:
            spec_ops.append(ops.set_partition_spec([ops.partition_field(*f) for f in partition_fields]))
        sort_fields = self._want_sort_fields() if sort else ()
        if sort_fields:
            spec_ops.append(
                AlterOp(
                    "set_sort_order",
                    {
                        "sort_fields": [
                            {"source_field_id": fid, "direction": direction, "null_order": null_order}
                            for fid, direction, null_order in sort_fields
                        ]
                    },
                )
            )
        return spec_ops

    # -- schema evolution + alignment -------------------------------------

    def _adopt_columns(self, columns) -> None:
        self._live_columns = {c.name: c for c in columns}
        # The table exists and this is its shape: the hook's last-known
        # answer, and the end of any "table missing" negative cache.
        self._known_json_text_columns = frozenset(c.name for c in columns if c.type == "variant")
        self._peek_missing_at = None
        self._peek_failed_at = None
        self._check_variant_columns(columns)

    def _check_variant_columns(self, columns) -> None:
        """Refuse a destination with a `variant` column this process would
        encode in Python (unless MILLPOND_ALLOW_PYTHON_VARIANT_CODEC), and
        one with a NOT NULL `variant` column under EITHER invalid-row
        policy.

        millpond's contract for a column the batch does not carry is
        SQL NULL (`_null_fill_missing`, the `INSERT BY NAME` rule), and an
        events_json producer omits a key whenever it has nothing to say —
        so a NOT NULL variant column is a column this sink WILL write a
        SQL NULL into, on the first batch that lacks the key or carries
        `null` for it. Under `null`, pyhoglake refuses the policy itself
        (an invalid row would have nowhere to go). Under `raise` it
        accepts the policy and then raises `sql_null_in_not_null` on that
        row — permanently, every attempt, so the pod crash-loops on a
        batch nothing upstream will change: a wedge whose cause is the
        table's constraint, not a bad record. Both are refused here, with
        the reason, rather than there, without it.

        When: whenever a shape is adopted — the first resolve (which is
        on the FIRST WRITE, not at construction: `_ensure_table` is lazy,
        so the pod has passed its probes and taken its partitions by
        then, same as every other resolve-time stop), every alter
        receipt, and every TTL re-read, so a NOT NULL variant column
        another writer adds under a running pod stops it at the next of
        those rather than on the first flush after. What it buys over
        pyhoglake's own refusal is the message and the moment: before the
        table is claimed, before any group is encoded, before any upload
        — nothing to orphan — and with the constraint named. Not
        retryable: neither waiting nor re-resolving changes a column's
        nullability.
        """
        variant = [c for c in columns if c.type == "variant"]
        if variant and self._variant_backend == "python" and not self._cfg.hoglake_allow_python_variant_codec:
            # The codec refusal lives here, with the table, and not at
            # startup: a string-only pod never encodes and is not coupled
            # to the codec it would never use, while a table that has (or
            # gains, under a running pod) a variant column is refused at
            # the same moment and under the same rule as the NOT NULL
            # check below. See config._hoglake_allow_python_variant_codec.
            raise HoglakeSinkError(
                f"{self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} has variant column(s) "
                f"{[c.name for c in variant]} and {python_variant_codec_refusal()}"
            )
        not_null = [c.name for c in variant if not c.nullable]
        if not_null:
            raise HoglakeSinkError(
                f"variant column(s) {not_null} of {self._cfg.hoglake_namespace}.{self._cfg.hoglake_table} are "
                f"NOT NULL. millpond writes SQL NULL into a column the batch does not carry (an absent key, a "
                f"JSON null, or under MILLPOND_HOGLAKE_VARIANT_ON_INVALID=null a row the encoder refused), so a "
                f"NOT NULL variant column is one every such batch fails on — under `null` pyhoglake refuses the "
                f"policy, under `raise` it raises sql_null_in_not_null on the row, forever. Make the column "
                f"nullable."
            )

    def _adopt_info(self, info):
        """Take a `TableInfo` as the destination's live shape, and stamp
        when it was read.

        One function, so the columns and the specs can never come from
        two different reads: `_live_columns` drives schema evolution and
        `info.partition_spec` drives the fanout, and a flush that
        computed partition values under one read while aligning columns
        to another would register files nothing could detect as wrong.

        Every producer of a whole `TableInfo` routes through here — an
        `info()` read and an `alter()` receipt alike. pyhoglake's
        `Table.alter` re-seeds its OWN writer cache from the same
        receipt (at the post-alter snapshot) and `Table.info` does the
        same through `Table._adopt`, so every stamp taken here coincides
        with a stamp taken there. `_live_info_ttl_s` depends on that.
        """
        self._live_info = info
        self._live_info_read_at = self._monotonic()
        self._adopt_columns(info.columns)
        # After the columns: the check resolves the field through them.
        self._check_object_prefix(info)
        return info

    def _check_object_prefix(self, info) -> None:
        """Refuse a destination whose object-prefix layout this sink cannot
        write under, at adopt, with the reason.

        `TableInfo.object_prefix_field` set means the server admits a
        data file only under `<data_path>/data/<ns>/<table>/<field>=
        <value>/`, value the file's identity partition value for the
        field, proven by the file's own footer stats; pyhoglake's writers
        (1.4.9+) do that placement from the same `TableInfo`. What is
        checked here, cheaply and against the same read: the field is a
        live column of an integer type the layout takes
        (`object_prefix.MAX_VALUE`), the live spec has exactly one
        identity field on it (`object_prefix.key_index`), and
        HOGLAKE_PARTITION_BY carries that same `identity(<field>)`. The
        first two are layouts the server never installs — pyhoglake would
        refuse them at prepare as a `ValidationError` — and are here so
        the message names the table rather than a group. The third is
        millpond's half of "the spec never drops the identity field": a
        config without it would fail `_reconcile_specs` anyway (a live
        spec the config disagrees with), but this names the field and the
        reason, and it runs before any spec alter could be built. The
        server refuses such an alter too; this makes sure millpond never
        asks.

        When: every shape adoption, like `_check_variant_columns` — the
        first resolve, every alter receipt, every TTL re-read, and the
        read-only peek `json_text_columns` makes before a conversion,
        which raises it from the consume loop exactly as the NOT NULL
        check is. Not retryable: neither waiting nor re-reading changes
        the config.
        """
        field = info.object_prefix_field
        if field is None:
            return
        where = f"{self._cfg.hoglake_namespace}.{self._cfg.hoglake_table}"
        column = self._live_columns.get(field)
        if column is None or column.type not in object_prefix.MAX_VALUE:
            found = "not a live column" if column is None else f"a {column.type!r} column"
            raise HoglakeSinkError(
                f"object-prefix field {field!r} of {where} is {found}, not one of "
                f"{sorted(object_prefix.MAX_VALUE)}; no file can be placed under it. The server never installs "
                f"this layout — the table's metadata is damaged or the server is newer than this client."
            )
        spec = info.partition_spec
        if spec is None or object_prefix.key_index(spec.fields, column.field_id) is None:
            raise HoglakeSinkError(
                f"object-prefix field {field!r} of {where} has no single identity field in the live partition "
                f"spec ({_describe_partition(spec, self._live_columns) or 'unpartitioned'}); the prefix is named "
                f"by that value, so no file can be placed. The server never installs this layout."
            )
        if (field, "identity", None) not in (self._cfg.hoglake_partition_by or ()):
            raise HoglakeSinkError(
                f"{where} names its object prefix by identity({field}), but HOGLAKE_PARTITION_BY "
                f"{self._describe_configured_partition() or 'is unset'} does not carry identity({field}). The "
                f"server refuses any spec that drops that field and millpond will not ask for one: set "
                f"HOGLAKE_PARTITION_BY to the live spec "
                f"({_describe_partition(spec, self._live_columns)}), or point this pipeline at another table."
            )

    def _live_info_ttl_s(self) -> float:
        """How long the cached shape may be used before it is re-read:
        a QUARTER of the catalog's snapshot retention.

        THE NUMBER IS NOT A GUESS, it is half of pyhoglake's own
        threshold, and the factor-of-two is the only thing standing
        between this sink and silently mis-partitioned files.

        `Table._cache_is_usable` lets pyhoglake prepare against its
        cached info while that cache is younger than retention/2; past
        that, `_prepared_read` re-reads and the `read_snapshot` it sends
        moves forward with it. Suppose the sink had no TTL. A pod runs
        for an hour, an operator re-specs the table from
        `identity(team_id)` to `bucket(team_id, 16)`, pyhoglake's cache
        ages out and self-refreshes past the alter, and the next flush:
        computes partition values from the sink's hour-old spec
        (identity), passes pyhoglake's only structural check on them
        (arity, which is 1 either way), passes the footer/schema compare
        (no column changed), and commits with a `read_snapshot` NEWER
        than the alter — so the conflict scan finds nothing, the server
        accepts, and the files are stamped with the live spec_id while
        carrying values computed under the old transform. Every future
        scan prunes them wrongly, forever, with nothing anywhere saying
        so. That is the hazard the deleted pre-commit table read used to
        catch, and a TTH strictly shorter than pyhoglake's is what
        replaces it: the sink always re-reads FIRST, so the spec it
        computes under is never older than the basis the commit carries.
        Expressed as an invariant — THIS CACHE IS NEVER STALER THAN
        `Table._cache` — which holds by construction because every
        refresh here goes through `table.info()`, which re-seeds that
        cache in the same call.

        Retention comes from `Catalog._retention_seconds()`: the SAME
        cached value pyhoglake's own threshold is computed from, so the
        two cannot disagree about the period even when an operator
        shortens retention live and one side has not noticed yet. It is
        one options GET per Catalog object, not per flush, so this stays
        a zero-request check in steady state. A private accessor, hence
        the guard: a pyhoglake that renames it falls back to the same
        assumed retention pyhoglake assumes when it cannot read one,
        which keeps the factor-of-two intact.

        `inf` (retention disabled) is correct as "never re-read for
        staleness": with no expiry floor pyhoglake's cache never ages
        out either, so its `read_snapshot` stays pinned at the sink's own
        read and any later alter is INSIDE the conflict window — the
        commit is refused rather than accepted wrong. The invariant holds
        trivially there; it is the finite case that needs the quarter.
        """
        read = getattr(self._catalog, "_retention_seconds", None)
        if read is None:
            return _ASSUMED_RETENTION_S / 4
        try:
            retention = read()
        except Exception:  # noqa: BLE001 - a retention read must never fail a flush
            return _ASSUMED_RETENTION_S / 4
        if not isinstance(retention, int | float) or isinstance(retention, bool) or retention <= 0:
            # Includes a test double that answers something unusable:
            # falling back is right, silently treating the TTL as zero or
            # as infinite is not.
            return _ASSUMED_RETENTION_S / 4
        return float(retention) / 4

    def _live_info_is_fresh(self) -> bool:
        if self._live_info is None or self._live_info_read_at is None:
            return False
        return self._monotonic() - self._live_info_read_at < self._live_info_ttl_s()

    def _table_info(self, table):
        """The destination's live shape, from this sink's cached copy,
        re-reading only on a cold cache or past `_live_info_ttl_s`.

        ZERO TABLE READS PER STEADY-STATE FLUSH is the point, and the
        correctness argument is not "the shape probably has not
        changed". It is two things together, and neither is sufficient
        alone:

        * THE CONFLICT WINDOW. The commit carries the `read_snapshot`
          this shape was resolved at, and the server refuses it if
          anything altered, dropped or recreated the table since —
          atomically, zero rows written, as the typed
          `ddl_since_read_snapshot` (or 410 when the basis itself
          expired), after which `_destination_moved` drops this cache and
          the flush rebuilds against a fresh read.
        * THE TTL, because the window only covers drift that is INSIDE
          it. pyhoglake refreshes its own cache at retention/2 and the
          `read_snapshot` moves forward with it, so a spec change older
          than that would be outside the window with nothing on the file
          to contradict it. Re-reading at retention/4 keeps this cache
          at least as fresh as pyhoglake's, which is what keeps every
          spec change the commit cannot see from existing. See
          `_live_info_ttl_s` for the worked example.

        So, precisely:

        * COLUMN drift is caught without the TTL. pyhoglake builds the
          destination schema from its OWN (possibly fresher) read and
          compares it against the encoded parquet footer, so a column
          this copy has not seen is a `ValidationError` —
          `write()`'s alignment self-heal answers it by re-reading,
          null-filling and preparing once more.
        * SPEC drift is caught by the TTL plus the conflict window, as
          above. Nothing on the file or in the request encodes the
          transform a partition value was computed under, which is why
          there is no third mechanism to fall back on.
        * A DROP + RECREATE is covered by NEITHER: the conflict scan
          keys on the resolved (new) table id, so the old incarnation's
          `table_dropped` row is outside its window. It is closed solely
          by `expected_table_uuid` -> 409 `table_recreated`, which
          `_prepare` pins to the resolved-and-reconciled incarnation.

        The periodic re-read is also the ONLY place a long-lived pod can
        notice that config and the live layout have diverged, so it runs
        `_reconcile_specs` on the info it just read. Without that, the
        "a pod that disagrees with the table it writes to stops" rule
        the README advertises only ever fired at pod start, and a
        re-spec under a running fleet produced the silent mis-partitioning
        above on every pod that had not restarted.

        `totals=False` on the read: the writer needs the uuid, the
        columns and the specs. The totals are a count and two sums over
        every live file row of the table, which on prod-us is ~15M rows
        and the whole cost of the call, and nothing in this sink has ever
        read one.
        """
        if self._live_info_is_fresh():
            if _spec_identity(self._live_info) != self._reconciled_specs:
                # A FRESH SHAPE IS NOT NECESSARILY A RECONCILED ONE, and
                # the early return above used to assume it was.
                # `_adopt_info` is reached from schema evolution
                # (`_add_column`, `_promote_column`, their alter
                # receipts) and from `write()`'s alignment self-heal —
                # all of which adopt whatever layout is live and re-stamp
                # the clock, with no config check anywhere. So an
                # external re-spec picked up through one of those paths
                # was written under, and a pod drifting columns often
                # enough could postpone the TTL's own reconciliation
                # indefinitely by keeping the shape perpetually fresh.
                #
                # Comparing layouts costs no request, so the check runs
                # whenever the layout is not the one a verdict was
                # reached on — which is once per actual spec change, not
                # once per flush.
                self._reconcile_specs(table, self._live_info)
            return self._live_info
        info = self._adopt_info(table.info(totals=False))
        # Against the info just read, so this costs no second request —
        # and AFTER the adopt, because the comparison resolves config
        # column names through `self._live_columns`.
        self._reconcile_specs(table, info)
        # `_reconcile_specs` may declare a missing spec, which re-adopts
        # a post-alter info; return the field rather than the local.
        return self._live_info

    def _evolve_and_align(self, table, batch: pa.Table) -> pa.Table:
        """Reconcile the batch schema with the live table schema.

        Mirrors SchemaManager.evolve: one alter per column, per-column
        failures are logged + `errors_total{type="schema"}` + degraded —
        never fatal to the flush. Then align for pyhoglake's strict
        `_align_table`: drop batch columns the table refused, null-fill
        table columns the batch lacks (DuckLake's `INSERT BY NAME`
        equivalent). Name matching is EXACT — hoglake identifiers are
        case-sensitive, unlike DuckDB's case-insensitive resolution.

        ONE add failure is fatal to the flush, deliberately: an
        `add_column` refused `idless_files_present` (a live file without
        field ids) raises `IdlessFilesLiveError` rather than
        degrading, because the degradation — drop the column, publish the
        rest, commit the offsets — loses that column's values for good.
        """
        failed: list[str] = list(
            self._add_columns(table, [f for f in batch.schema if f.name not in self._live_columns])
        )
        rewrites: list[tuple[str, str, str]] = []
        stringify: list[str] = []
        for field in batch.schema:
            live = self._live_columns.get(field.name)
            if live is None:
                continue
            if live.type == "variant":
                # A string batch column against a live variant column is
                # THE expected shape, not drift: the arrow_converter
                # produces JSON text and `_prepare` retypes it to
                # `pa.json_()` for pyhoglake to encode. No promotion (a
                # variant is declared once and never altered), no
                # rewrite, no warning, no metric. Two things are done
                # here, both in `_json_text_column`: an empty string is
                # made SQL NULL (it is not JSON, and a producer that
                # writes "" for "nothing" should land as nothing, not as
                # an invalid row — millrace's rule too); and a batch
                # column that is not text at all is serialized to JSON
                # text, so `5`, `true`, a struct or a list is the JSON
                # value 5, true, {...} or [...]. An all-null column casts
                # to json on its own.
                if not pa.types.is_null(field.type):
                    stringify.append(field.name)
                continue
            try:
                want, _params = arrow_type_to_coltype(field.type)
            except UnsupportedTypeError:
                log.warning(
                    "Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type
                )
                metrics.errors_total.labels(type="schema").inc()
                failed.append(field.name)
                continue
            if live.type != want:
                promoted = _PROMOTIONS.get((live.type, want))
                if promoted is not None:
                    self._promote_column(table, field.name, live.type, promoted)
                elif (live.type, want) in _UUID_REWRITES:
                    rewrites.append((field.name, live.type, want))
                # Else: leave the column; append()'s cast to the live type
                # decides (all-null wobble casts cleanly; genuine type
                # garbage fails the flush loudly — same posture as
                # DuckLake's INSERT-side cast).

        if failed:
            batch = batch.drop_columns(failed)
        for name, live_type, want in rewrites:
            batch = self._rewrite_column(batch, name, live_type, want)
        for name in stringify:
            index = batch.schema.get_field_index(name)
            column = _json_text_column(batch.column(index))
            if column is not batch.column(index):
                # The column's OWN type: a large_string column keeps its
                # type through the "" -> null rewrite, and a field that
                # said `string` over it is an ArrowInvalid the retry loop
                # would call transient.
                batch = batch.set_column(index, pa.field(name, column.type, nullable=True), column)

        return self._null_fill_missing(batch)

    def _rewrite_column(self, batch: pa.Table, name: str, live_type: str, want: str) -> pa.Table:
        """Convert a batch column between uuid bytes and uuid text, whichever
        way the live hoglake column needs it.

        The degradation the docstring above promises, for the two live/batch
        pairs where "leave it to the append-side cast" is not a degradation at
        all but a permanent wedge. Neither direction casts:

        * live `string`, batch `uuid` — `pa.uuid()` is an extension over
          `fixed_size_binary(16)` and pyarrow casts its storage to utf8 by
          REINTERPRETING the bytes, so 16 arbitrary bytes raise
          `ArrowInvalid: Invalid UTF8 payload`;
        * live `uuid`, batch `string` — the cast is to `pa.uuid()` (plain
          `fixed_size_binary(16)` before pyhoglake 1.3.0; either way pyarrow
          reports the STORAGE type in the message) and raises
          `ArrowInvalid: Failed casting from string to fixed_size_binary[16]:
          widths must match`, because 36 characters of text are not 16 bytes.

        Both are deterministic on the batch, so both are non-retryable: the
        flush crashes on attempt 1 with the offsets uncommitted, the restart
        re-consumes the same batch, and the partition stops forever.

        Neither shape is hypothetical, and they are the two halves of the same
        operation. Adding `<col>:uuid` to MILLPOND_TYPED_COLUMNS points a
        `uuid` batch at a table whose column has been `string` since it was
        created from unpinned batches; REMOVING the pin — a rollback, or one
        pod on a mixed fleet that has not taken the new config yet — points a
        `string` batch at a table another pod created as `uuid`. A pin an
        operator cannot safely apply is bad; a pin they cannot safely roll
        back is worse.

        Hoglake has no `string <-> uuid` promotion in either direction (nor
        does Iceberg), so no DDL is attempted and the LIVE column wins: the
        rows land in whatever shape the table already has, byte-for-byte the
        same value either way. Getting a real `uuid` column means creating the
        table WITH the pin, or a recreate. The mismatch is loud while it
        lasts: `errors_total{type="schema"}` every flush, plus one warning per
        column per direction. Text that is not a UUID is nulled, exactly as
        the coercer would have nulled it, and bumps
        `errors_total{type="column_coercion"}` as well — a type mismatch and
        dropped values are different events and read as different series.
        """
        index = batch.schema.get_field_index(name)
        if index < 0:
            # Unreachable: the caller only ever passes names it read off this
            # batch's own schema. Asserted anyway, because `get_field_index`
            # answers -1 for a miss and `set_column(-1, ...)` would silently
            # rewrite the LAST column instead of failing.
            raise ValueError(f"column {name!r} is not in the batch schema {batch.schema.names}")
        column = batch.column(index)
        combined = column.combine_chunks() if isinstance(column, pa.ChunkedArray) else column

        if want == "uuid":
            # Batch carries uuid bytes, live column is text. `.storage` only
            # exists on the extension array: pyhoglake maps a BARE
            # `fixed_size_binary(16)` to "uuid" as well, and that one is
            # already its own storage.
            storage = combined.storage if isinstance(combined, pa.ExtensionArray) else combined
            values = pa.array(
                [None if raw is None else arrow_converter.uuid_text(raw) for raw in storage.to_pylist()],
                type=pa.string(),
            )
            field = pa.field(name, pa.string(), nullable=True)
            nulled = 0
        else:
            # Batch carries uuid text, live column is uuid.
            decoded: list[bytes | None] = []
            nulled = 0
            for text in combined.to_pylist():
                if text is None:
                    decoded.append(None)
                    continue
                try:
                    decoded.append(arrow_converter.uuid_bytes(text))
                except ValueError:
                    decoded.append(None)
                    nulled += 1
            values = pa.ExtensionArray.from_storage(
                arrow_converter.UUID_TYPE,
                pa.array(decoded, type=arrow_converter.UUID_STORAGE_TYPE),
            )
            field = pa.field(name, arrow_converter.UUID_TYPE, nullable=True)

        key = (name, live_type, want)
        if key not in _uuid_rewrite_warned:
            log.warning(
                "Column %r arrives as hoglake type %r but the live column is %r; hoglake has no "
                "%s->%s promotion, so it is rewritten to the live shape for this and every "
                "following flush. A real %r column needs the table created that way (or recreated).",
                name,
                want,
                live_type,
                live_type,
                want,
                want,
            )
            _uuid_rewrite_warned.add(key)
        metrics.errors_total.labels(type="schema").inc()
        if nulled:
            log.warning("Rewrite of column %s to %s nulled %d unparseable value(s) this batch", name, live_type, nulled)
            metrics.errors_total.labels(type="column_coercion").inc()
        return batch.set_column(index, field, values)

    def _null_fill_missing(self, batch: pa.Table) -> pa.Table:
        """Null-fill live table columns absent from the batch (removed or
        renamed upstream, or added by another writer) — the equivalent of
        DuckLake's `INSERT BY NAME` filling unnamed columns with NULL.

        `variant="json"`: a variant column has no one Arrow type and the
        default form refuses it (`UnsupportedTypeError`). The json form
        is the one `_prepare` casts to, so an all-null `pa.json_()`
        column is what pyhoglake receives — every row SQL NULL, which it
        accepts because `_check_variant_columns` has already refused any
        NOT NULL variant column at resolve."""
        for name, col in self._live_columns.items():
            if name not in batch.schema.names:
                arrow_field = column_to_arrow_field(col, variant="json")
                batch = batch.append_column(
                    pa.field(name, arrow_field.type, nullable=True),
                    pa.nulls(batch.num_rows, type=arrow_field.type),
                )
        return batch

    def _add_columns(self, table, fields: list[pa.Field]) -> list[str]:
        """ADD every new column in ONE alter; return the names that are
        still not live afterwards (the caller drops those from the batch).

        `/alter` applies its op list in order, atomically, as a single
        DDL commit — one snapshot, one schema-version bump, one trip
        through the per-catalog commit lock. Issuing a commit per column
        multiplied that traffic by the width of the schema drift, and
        gave every column its own chance to lose the concurrent-DDL race
        against another pod doing the same thing.

        The batch is an optimization and never a semantics change: one
        unacceptable column fails the whole transaction (nothing
        applies), so a failure falls back to the per-column loop, where
        each column degrades on its own exactly as before. Except for
        `idless_files_present`, which is about the TABLE rather than any
        one column: it raises from here (see IdlessFilesLiveError).
        """
        if not fields:
            return []
        add_ops: list[AlterOp] = []
        addable: list[pa.Field] = []
        added: list[str] = []
        failed: list[str] = []
        for field in fields:
            try:
                type_name, _ = arrow_type_to_coltype(field.type)
            except UnsupportedTypeError:
                # Dropped before the alter is built: a column hoglake has
                # no type for must not take the other columns' DDL down
                # with it.
                log.warning(
                    "Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type
                )
                metrics.errors_total.labels(type="schema").inc()
                failed.append(field.name)
                continue
            log.info("Schema evolution: adding hoglake column %s (%s)", field.name, type_name)
            add_ops.append(ops.add_column(field.name, field.type))
            addable.append(field)
            added.append(f"{field.name} ({type_name})")

        if not add_ops:
            return failed
        if len(add_ops) > 1:
            try:
                self._adopt_info(
                    table.alter(
                        add_ops,
                        author=self._author,
                        message=self._ddl_message(
                            f"schema evolution from {self._cfg.topic}", "add columns " + ", ".join(added)
                        ),
                    )
                )
                metrics.schema_columns_added_total.inc(len(add_ops))
                return failed
            except IdlessFilesPresentError as e:
                # Before the generic arm: the per-column fallback would
                # be refused the same way, and its HoglakeError arm drops
                # the column. See IdlessFilesLiveError.
                raise self._idless_refusal([f.name for f in addable], e) from e
            except HoglakeError as e:
                log.info("Batched add of %d column(s) failed (%s); retrying per column", len(add_ops), e)
        for field in addable:
            if not self._add_column(table, field):
                failed.append(field.name)
        return failed

    def _add_column(self, table, field: pa.Field) -> bool:
        """ADD a new column; True when the column is live afterwards."""
        try:
            type_name, _ = arrow_type_to_coltype(field.type)
        except UnsupportedTypeError:
            log.warning("Column %r has no hoglake type mapping (%s); dropping it this flush", field.name, field.type)
            metrics.errors_total.labels(type="schema").inc()
            return False
        log.info("Schema evolution: adding hoglake column %s (%s)", field.name, type_name)
        try:
            self._adopt_info(
                table.alter(
                    [ops.add_column(field.name, field.type)],
                    author=self._author,
                    message=self._ddl_message(
                        f"schema evolution from {self._cfg.topic}", f"add column {field.name} ({type_name})"
                    ),
                )
            )
            metrics.schema_columns_added_total.inc()
            return True
        except IdlessFilesPresentError as e:
            # Not a concurrent-DDL conflict and not a reason to degrade:
            # fail the flush so the column is never dropped. See
            # IdlessFilesLiveError.
            raise self._idless_refusal([field.name], e) from e
        except CommitConflictError:
            # Concurrent DDL — typically another pod adding the same
            # column. Re-resolve; present means someone won the race.
            self._adopt_info(table.info(totals=False))
            if field.name in self._live_columns:
                log.info("Column %s added by another writer, continuing", field.name)
                return True
            log.warning("Failed to add column %s: concurrent DDL conflict", field.name)
            metrics.errors_total.labels(type="schema").inc()
            return False
        except HoglakeError as e:
            log.warning("Failed to add column %s: %s", field.name, e)
            metrics.errors_total.labels(type="schema").inc()
            return False

    def _idless_refusal(self, columns: list[str], e: IdlessFilesPresentError) -> IdlessFilesLiveError:
        """Log the server's `idless_files_present` refusal and build the
        permanent flush failure that replaces dropping the column(s)."""
        detail = e.detail or str(e)
        log.error(
            "Adding hoglake column(s) %s refused: idless_files_present (%s). The table holds a live file "
            "without parquet field ids, or the server is older than hoglake 1.4.6 (which also counts files "
            "pending hydration); failing the flush (offsets not committed). Retrying does not clear it: "
            "compact or rewrite the file, or retire the table",
            ", ".join(columns),
            detail,
        )
        return IdlessFilesLiveError(
            f"add_column {', '.join(columns)} refused by the server (idless_files_present: {detail}); "
            "the flush is refused rather than published without the column"
        )

    def _promote_column(self, table, name: str, from_type: str, to_type: str) -> None:
        """Widen a live column (int->long / float->double); failures are
        non-fatal — the column stays at its live type and the append-side
        cast decides per value."""
        log.info("Schema evolution: promoting hoglake column %s from %s to %s", name, from_type, to_type)
        try:
            self._adopt_info(
                table.alter(
                    [ops.promote_column(name, to_type)],
                    author=self._author,
                    message=self._ddl_message(
                        f"schema evolution from {self._cfg.topic}",
                        f"promote column {name} from {from_type} to {to_type}",
                    ),
                )
            )
            metrics.schema_columns_widened_total.inc()
        except CommitConflictError:
            self._adopt_info(table.info(totals=False))
            live = self._live_columns.get(name)
            if live is not None and live.type == to_type:
                log.info("Column %s promoted by another writer, continuing", name)
                metrics.schema_columns_widened_total.inc()
            else:
                log.warning("Failed to promote column %s to %s: concurrent DDL conflict", name, to_type)
                metrics.errors_total.labels(type="schema").inc()
        except HoglakeError as e:
            log.warning("Failed to promote column %s from %s to %s: %s", name, from_type, to_type, e)
            metrics.errors_total.labels(type="schema").inc()
