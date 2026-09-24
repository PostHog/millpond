# Millpond — Kafka to DuckLake

A standalone Python app that consumes from a Kafka topic and writes to a [DuckLake](https://github.com/duckdb/ducklake) table. Single thread, single loop, no Kafka Connect. One deployment writes to exactly one table.

**Contents:**
[Naming](#naming) | [Why](#why) | [Architecture](#architecture) | [Destination](#destination) | [Record Handling](#record-handling) | [Adaptive Backpressure](#adaptive-backpressure) | [Performance](#performance) | [Resource Footprint](#resource-footprint) | [Setup](#setup) | [Development](#development) | [Configuration](#configuration) | [Releases](#releases) | [Deployment](#deployment) | [Partitioning](#partitioning) | [Object Sizing](#object-sizing) | [Error Handling](#error-handling-and-retries) | [Multiple Pipelines](#multiple-pipelines) | [AWS Credential Isolation](#aws-credential-isolation) | [Operational Notes](#operational-notes) | [tools](tools/README.md)

## Naming

<img src="imgs/500px-Hagley_mill_race.jpeg" alt="A mill pond" width="300" align="right">

> **millpond** (noun): a pond created by damming a stream to produce a head of water for operating a mill.
> — [Merriam-Webster](https://www.merriam-webster.com/dictionary/millpond)

Millpond accumulates a stream of Kafka records until a threshold is reached, then releases them into a downstream lake. Like a [mill pond](https://en.wikipedia.org/wiki/Mill_pond) feeding a lake.

## Why

Kafka Connect imposes ~1100 lines of lock management, scheduled executors, and rebalance handling to work around its lack of backpressure and explicit offset control. Millpond replaces all of that with:

```
loop:
  consume() → JSON → Arrow → accumulate
  when buffer full or time elapsed:
    write to lake → commit offsets
```

Single thread, single loop. Kafka is the buffer. Offset commit is explicit (after successful write only). No data loss window.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       └─ Single loop: consume → convert → [coerce] → [filter] → accumulate → [sort] → flush → commit
```

- One topic and one table per deployment
- Static partition assignment via pod ordinal — no consumer groups
- If a pod dies, its partitions stop being consumed until K8s restarts it
- Optional coercion, filter, and sort stages — see [Record Handling](#record-handling) below
- The hot path is one thread; auxiliary threads only: an HTTP server on `:8000` (`/metrics`, `/healthz`, `/readyz`), librdkafka's stats callback, and the optional include-values poller

## Destination

Millpond writes to one of two destinations, selected by `MILLPOND_DESTINATION` (default `ducklake`). A single deployment writes to exactly one table — there is no per-batch routing, and a pod's destination is fixed for its lifetime.

|  | DuckLake | Hoglake |
|---|---|---|
| Catalog | Postgres (via DuckDB ducklake extension) | The hoglake control plane (REST service over Postgres; clients never touch its database) |
| Storage | S3 / S3-compatible | S3 / S3-compatible — millpond writes the parquet itself via pyhoglake and registers it in a footer-shipping commit; the server never opens data files |
| Reader ecosystem | DuckDB-native; growing third-party support | hoglake duckdb-client, Trino connector, changefeed consumers (hedgerow) |
| Partitioning | Caller-supplied via `DUCKLAKE_PARTITION_BY`; arbitrary DDL expression | `HOGLAKE_PARTITION_BY` mapped onto Iceberg-semantics transforms (`identity`, `year`, `month`, `day`, `hour`, `bucket(col, N)`) at table creation, verified afterwards, and reconciled against the live table on every resolve; one parquet file per partition tuple per flush, all in one atomic commit |
| Sort order | Not declared (batches pre-sorted via `MILLPOND_SORT_BY`) | `MILLPOND_SORT_BY` additionally declared as the table's sort order at creation (advisory for writers, binding for hoglake compaction), and reconciled like the partition spec |
| Schema evolution | DuckDB DDL (`ADD COLUMN IF NOT EXISTS`, `ALTER COLUMN SET DATA TYPE` with widening enforcement) | Typed alter ops (`add_column`; `promote_column` for `int→long`, `float→double`); same per-column degrade-and-metric posture |
| VARIANT dual-write | Supported (`MILLPOND_VARIANT_COLUMNS`) | **Not supported — rejected at startup.** Events land as TEXT (`properties` stays a JSON string). Deferred until hoglake grows a variant path millpond can target. |
| Maintenance tooling | Bundled (`tools/ducklake_maintenance.py` CronJob CLI, `tools/ducklake_metrics.py` exporter — daemon or one-shot push) | Server-side (hoglake expiry/cleanup/compaction loops) — nothing bundled here |
| `_inserted_at` column | Added at INSERT via DuckDB `NOW()` (per-row, microsecond drift possible within a flush) | Stamped Arrow-side, one timestamptz value per flush (every row in a flush shares it) |
| Multi-pod concurrent writes | Native; idempotent DDL handles races | Native; appends never conflict with appends, concurrent DDL 409s are absorbed by re-resolve, and the incarnation guard (`expected_table_uuid`) refuses cross-incarnation appends atomically |
| Startup validation | Connects in the sink constructor | Resolves the catalog in the sink constructor, so a bad URL, bad credentials or a missing catalog fails before the pod claims readiness |
| Commit retries | `DUCKLAKE_MAX_RETRY_COUNT` (inner loop, default 100) under millpond's 3 outer attempts | `HOGLAKE_MAX_RETRY_COUNT` outer attempts (default 8; there is no inner loop), honoring the server's `Retry-After` on 503 backpressure as a floor under the exponential curve |

Both sinks (`millpond/ducklake.py`, `millpond/hoglake.py`) implement the `Sink` protocol (`millpond/sink.py`) and expose three methods to `main.py`: `write(batch) -> int`, `reset_caches()`, `close()`. `make_sink(cfg)` dispatches on the destination with lazy backend imports.

Both destinations are at-least-once at the pipeline level: Kafka offsets commit only after a successful write, so a pod that dies between the write and the offset commit replays its last batch on restart.

The hoglake path is stronger than that *within a process*, and it has to be. The catalog is reached over HTTP, so a commit the server applied whose response is lost looks exactly like a commit that never happened, and hoglake deliberately permits the same data-file path to be registered twice — a naive retry publishes the rows again. So each flush is published under an idempotency key naming the destination table incarnation and the complete Kafka offset range it covers, and the uploaded registration is held in memory across retries. The retry is then byte-identically the same request, and the server answers it from its receipt without writing.

**What it does not buy is exactly-once across a restart.** Nothing is held across a process boundary: the rebuilt flush stamps a fresh `_inserted_at` and uploads under fresh object names, so the replayed request is never byte-identical. All the key can do there is recognize a repeated *boundary* and decline to publish over it twice — and the boundary is not reproducible in general, because it is wherever the size and time triggers happened to cut (the size trigger accumulates per poll batch, the time trigger is wall-clock, the allowlist is mutable, and every partition in the flush has to coincide). Across process boundaries this pipeline is at-least-once, with the duplicate opportunistically suppressed when the boundary does repeat.

| Failure | Result |
|---|---|
| Commit response lost (timeout, reset, 502) | Retry replays the identical request; the rows publish **exactly once** (`millpond_hoglake_commit_replays_total{outcome="replayed"}`). |
| Pod dies after the commit applied, before the offsets committed, and the rebuilt flush cuts at the *same* boundary | Recognized by its key and accepted without republishing (`…{outcome="already_published"}`); the flush reports **zero** rows written, because this process published none. The rebuilt upload is orphaned — see below. |
| …and the rebuilt flush cuts at a *different* boundary | A different publication by every name anyone has: **the rows land twice**. At-least-once, as above. |
| Commit refused (409 conflict, 422 validation) | Zero rows registered, atomically. The prepared payload is dropped (the server has judged it; an identical resend gets the identical answer) and the uploaded parquet is orphaned. |
| Pod dies before the commit is sent | Nothing published; Kafka replays. The upload, if it happened, is orphaned. |
| Table dropped and recreated under the same name | Refused, never published, in both halves of the window. A recreation the sink has not re-resolved (its cached handle is a *name*, not an incarnation) is caught by the client pre-flight, before anything is uploaded; one that lands after the upload is caught before the commit is sent, or by the server's own `expected_table_uuid` guard, and the prepared payload is dropped and orphaned. Retryable either way: the flush re-resolves, reconciles the recreated table against config, and rebuilds against it — under a key that names the live incarnation, so the new table can never answer from its predecessor's receipt. |

**Orphaned objects are a real, unreclaimed cost.** Hoglake's `cleanup` reclaims only files the *server* queued for removal (snapshot expiry, table drop, compaction staging); files a client uploaded are not in that set, and automated cleanup for them is explicitly future work on the hoglake side. Every parquet millpond uploads without registering is therefore billed storage until somebody deletes it. `millpond_hoglake_orphaned_files_total` counts them, and the warning that accompanies every increment names the **full object URI of each one** (capped per line, with a count of any it left out).

> **Delete those URIs, never the `…/{idempotency_key}/` prefix they share.** Object names under that prefix are `{uuid4}-{index}.parquet`, so a retry under the *same* key uploads fresh names beside the old ones. Sweeping the prefix after a later attempt succeeded deletes live, committed files. (An earlier revision of this document, and of the log line itself, advised exactly that sweep. It was wrong.)

The counter is **exact**, on every path. Where a prepared payload exists the files are known to be in object storage and known to number `len(files)`: a commit the server refuses, a rebuilt flush the receipt declines, a payload superseded by the next flush, and a payload still unpublished at `close()`. A flush that fails *inside* `prepare` used to be the gap — millpond had no way to know how far the upload loop got, so it counted nothing and logged an invented upper bound. pyhoglake ≥ 1.1.1 closes it: every exception leaving `prepare_append_files` carries `uploaded_files` (uploads whose output stream closed cleanly) and `uploaded_uris` (their URIs), and a refusal raised before the first upload carries `0` / `()`. millpond reads those rather than reasoning about where in someone else's loop a given failure fires — a deduction that was wrong twice on this branch. Retries do not inflate anything: a held payload is re-sent, not re-uploaded.

Two caveats remain, both pushing the same way — the counter can *undercount*, and still never invents an object:

- **The file a `prepare` failed on is in neither number.** Its upload may never have opened, or may have closed badly over a *truncated* object that really is in storage. The count is therefore a lower bound on objects present, and that one file has to be treated as possibly-there. The warning says so.
- **The stamp is best effort at the source.** An older pyhoglake does not set the attributes at all, and pyhoglake suppresses the `AttributeError` from an exception type whose `__slots__` refuse them. millpond reads with a `getattr` default of `0`, so either case books nothing rather than raising a second error over a live object-store failure.

And, as before, nothing can count the orphan a `SIGKILL` leaves between the upload and the commit.

## Record Handling

Several optional stages sit between Kafka conversion and the sink, applied in this order: column type coercion → allowlist filter → denylist filter → pending buffer → pre-write sort. Each is disabled when its env vars are unset.

### Allowlist filter

Drops records whose value in a configured field is not in a configured allowlist. Applied after JSON→Arrow conversion (and type coercion), before records enter the pending buffer.

```
MILLPOND_FILTER_KEEP_FIELD_NAME=team_id
MILLPOND_FILTER_VALUES=2,4,1956,69
```

Values auto-detect: tokens that all parse as integers become an int allowlist; otherwise the whole list is treated as strings.

Three skip reasons are tracked on `millpond_records_skipped_total`:

- `filter_field_missing` — column absent from this batch's schema, null for that row, or column type is not filterable (only integer, string and `uuid`-pinned columns are supported; bool, float, timestamp, struct, list, etc. are rejected explicitly to avoid silent surprising matches under PyArrow's `safe=True` cast semantics).
- `filter_excluded` — column present and non-null but value not in the allowlist. Expected steady-state drop reason.
- `filter_value_invalid` — the column is `uuid`-pinned and **no** configured value parses as a UUID, so nothing can be admitted. Its own label rather than `filter_field_missing`, which is a statement about the batch's schema: a bad include set and a renamed column need different fixes and must not share a series. Individual unparseable values do not land here — they are dropped from the comparison and counted on `millpond_errors_total{type="filter_value_invalid"}` while the rest of the set still applies.

Kept rows are also counted per matched value on `millpond_filter_matched_total{value=…}` (cardinality bounded by the include set).

### Denylist filter

Drops records whose value in `MILLPOND_FILTER_DROP_FIELD_NAME` is in `MILLPOND_FILTER_DROP_VALUES` — its own values var, never shared with the keep list. Runs after the keep-filter, so the composed semantics are keep ∩ ¬drop (e.g. a CP-driven include set minus an operator blacklist muting one tenant during an incident). Dropped rows count as `records_skipped_total{reason="filter_dropped"}`.

Failure semantics are deliberately the opposite of the keep-filter's: an allowlist that can't evaluate fails **closed** (drops the batch), a denylist that can't evaluate fails **open** — a missing field, unsupported column type, or incompatible value cast keeps the batch unchanged with a warning, so a schema hiccup can't turn the blacklist into data loss. Null field values are kept (a null can't match the blacklist).

### Dynamic allowlist source

The allowlist can be sourced at runtime from an HTTP endpoint instead of being fixed at startup: `MILLPOND_INCLUDE_VALUES_URL` names a URL returning a JSON array of scalars (ints or strings, matching the static list's type), polled on a background thread (default every 60s, ±10% jitter). Millpond knows nothing about the endpoint's meaning — the URL and an optional auth header (`MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME` + `_AUTH_TOKEN`) are plain config.

How the static list and the polled set interact:

| `MILLPOND_FILTER_VALUES` (static) | `MILLPOND_INCLUDE_VALUES_URL` | `MILLPOND_INCLUDE_VALUES_MODE` | Effective allowlist | Endpoint's role |
|---|---|---|---|---|
| set | unset | unset | the static list | none — today's behavior, unchanged |
| set | set | `shadow` (default) | the static list | observability only: polled each interval, exports diff-vs-static gauges and staleness; its values are never applied |
| set | set | `authoritative` | the polled set **∪ the static list** | live: the endpoint governs everything it serves; the static list is a permanent manual floor ("pins") — values the endpoint has never heard of (legacy/grandfathered) stay included and can only be removed by a config deploy. Startup **blocks** until the first successful poll (no proceed-on-stale-bootstrap) |
| unset | unset | unset | no filter — all records kept | — |
| unset | set | any | **startup error** | the URL requires an active keep-filter, which requires static values |
| any | unset | set (or auth vars set) | **startup error** | MODE/auth without a URL means a dynamic source was intended; refusing beats silently running static-only |

In `authoritative` mode the polled set changes under safety rules shaped by a consequence asymmetry — an erroneous addition writes surplus rows, an erroneous removal silently drops records with no recovery:

- **Additions** apply on the first successful poll that shows them.
- **Removals** require `MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS` (default 5) *consecutive successful* polls with the value absent. Failed polls freeze the countdown; a reappearing value resets it. Statically-pinned values are exempt regardless of endpoint state — a pin stays served even if the endpoint once served it and later dropped it; removing a pin is a config deploy.
- **Poll failures** keep the last-known-good set indefinitely; staleness is observable via `millpond_include_values_last_success_timestamp_seconds`.
- **Refused polls** (counted on `millpond_include_values_refused_total{reason}`) keep the set and advance nothing: empty arrays (`empty` — never removal evidence when endpoint-managed values are held; a pins-only set accepts an empty endpoint as a legitimate steady state), removals of more than half of the *endpoint-managed slice* at once (`bulk_removal` — measured against current minus endpoint-invisible pins; the refused poll's **additions still apply**, additions being the safe direction), and int↔str type changes (`type_flip` — a type-flipped set would fail the filter's cast against the column and drop whole batches).

Rollout is designed to be shadow-first: run `shadow`, watch `millpond_include_values_shadow_only_static` / `_shadow_only_remote`, and flip to `authoritative` once `shadow_only_static` equals the intentional pin *count* (`millpond_include_values_pinned_only` — the pins are logged at startup so membership is checkable) and `shadow_only_remote` matches the expected dynamic expansion. The shadow prober carries the same pins as authoritative mode, so its size/pending-removal gauges predict exactly the set the flip would serve. `millpond_include_values_pinned` / `_pinned_only` survive the flip (the shadow gauges don't), keeping pin/endpoint divergence observable in authoritative mode.

**Prune the static list to the intentional pins before flipping.** Every static value is a permanent pin: a static list that fully mirrors the endpoint at flip time leaves the endpoint with nothing it can ever remove — the damping machinery goes dead with `pending_removals` reading a healthy-looking 0. `pinned_only` at its expected count (vs `pinned` ≈ the whole set) is the tell. `millpond_include_values_mode` reports which mode each replica actually runs, so a fleet-level flip gate can't pass vacuously on a replica that never got the URL.

### Pre-write sort

Sorts the consolidated batch by one or more columns ascending, right before `sink.write()`. The sink sees pre-sorted data, which improves Parquet compression (especially for low-cardinality keys like `team_id`) and downstream reader predicate pushdown.

```
MILLPOND_SORT_BY=team_id,timestamp
```

Sort order is left-to-right (`team_id` primary, `timestamp` secondary). Direction is ascending only today; if you need descending, file an issue. PyArrow's sort is stable, so equal-key rows preserve their consume order.

If any sort field is missing from a batch's schema, the sort is skipped (records still flow through, just unsorted), `millpond_sort_skipped_total{reason="field_missing"}` increments by the record count, and a warning logs once per distinct missing-fields pattern (per pod lifetime — prevents log floods under sustained misconfiguration).

Per-flush cost is ~50–200 ms on a 256 MB / 30k-row batch. Peak memory roughly doubles during the sort because `pa.Table.take()` rewrites a fresh copy of every column; budget accordingly relative to the pod's memory limit.

### Column type coercion

JSON carries no type schema, so millpond infers a column's type from its values. When the values don't carry enough type information the inference diverges from the destination DuckLake column, and DuckLake's widening-only schema evolution then rejects the narrowing `ALTER` every flush (the insert stalls under DuckLake). `MILLPOND_TYPED_COLUMNS` pins named columns to a target type *before* the write, so the batch type matches the destination — the insert is a typed append with no DDL, and freshly-created tables get the right type from the start.

Format is comma-separated `column:type` pairs; supported types are `timestamptz`, `bigint`, `double`, `boolean`, `varchar`, `uuid`. No target is destination-restricted — every one of them has a native column type on both DuckLake and hoglake (unlike `MILLPOND_VARIANT_COLUMNS`, which is a startup error on hoglake). For re-pointing a consumer at the duckling backfill's `posthog.events` (its `events` table has 8 `TIMESTAMPTZ` columns and `project_id BIGINT`):

```
MILLPOND_TYPED_COLUMNS=timestamp:timestamptz,created_at:timestamptz,person_created_at:timestamptz,group0_created_at:timestamptz,group1_created_at:timestamptz,group2_created_at:timestamptz,group3_created_at:timestamptz,group4_created_at:timestamptz,project_id:bigint
```

Why those columns: date-times arrive as strings, so inference types them `VARCHAR` against a `TIMESTAMPTZ` column; and `project_id` is the one numeric column the producer serializes as explicit JSON `null` (no `skip_serializing_if`), so an all-null batch infers `VARCHAR` against `BIGINT`. Pinning both makes the re-point fully clean. (`person_mode`/`historical_migration` need no pin — they serialize as a string and an omitted-or-bool respectively, matching the table.)

For the hoglake `events_raw` table, add the two ClickHouse `UUID` columns (`uuid` and `person_id`, both `UUID` in the events DDL) to the timestamp and id pins:

```
MILLPOND_TYPED_COLUMNS=uuid:uuid,person_id:uuid,team_id:bigint,project_id:bigint,timestamp:timestamptz,created_at:timestamptz,captured_at:timestamptz,person_created_at:timestamptz
```

`captured_at` is producer-side — an ISO string with no column in the ClickHouse DDL — and `project_id` is producer-side too and can arrive all-null, which is exactly the case that infers `VARCHAR` without a pin.

**The `uuid` wire form.** `uuid` accepts an enumerated set of shapes: the canonical hyphenated form, 32 hex digits without hyphens, and either of those wrapped in `{...}` or prefixed with `urn:uuid:`. The hex digits are case-insensitive; the `urn:uuid:` prefix is **not** (`URN:UUID:…` is refused). RFC 4122 variant and version bits are deliberately **not** validated — ClickHouse `UUID` is an opaque 128-bit value and PostHog writes both v4 and v7. Anything else is nulled, including the values `uuid.UUID` itself would silently remap: `urn:` or `uuid:` appearing anywhere in the string, hyphens at non-canonical offsets (`0123-4567-89ab-cdef-0123-4567-89ab-cdef` parses in the stdlib), radix prefixes (`0x…`), and PEP 515 underscores. It produces `pa.uuid()` — pyarrow's canonical UUID extension type, an extension over `fixed_size_binary(16)` holding the UUID's 16 big-endian bytes. (It has existed since pyarrow 18; millpond's floor is 21, set by pyhoglake 1.3.0.) That one wire form is correct on both destinations, which is why it is the one emitted:

| Destination | Result |
|-------------|--------|
| hoglake | The `uuid` column type. pyhoglake ≥ 1.3.0 answers a uuid column with `pa.uuid()` and accepts bare `fixed_size_binary(16)` as the same thing. |
| DuckLake | A native DuckDB `UUID` column. |

Plain `pa.binary(16)` — the same bytes without the extension type — is **not** used, and a batch that carries one is adopted into `pa.uuid()` rather than passed through. The reason is DuckLake: there the bare form lands as a `BLOB`, silently, from byte-identical data. On hoglake it would now survive either way, because `_prepare` casts to a schema that names `pa.uuid()` and that cast upgrades the bare form — but the coercer has one output type, not one per destination. (A `binary`/`large_binary` column whose values are all exactly 16 bytes is adopted the same way; other widths are nulled per value.)

**The uploaded parquet carries the UUID annotation** (pyhoglake ≥ 1.3.0). pyarrow stamps `FIXED_LEN_BYTE_ARRAY(16)` + `LogicalTypeAnnotation.uuidType()` for `pa.uuid()` and nothing at all for `pa.binary(16)`, and that annotation is what an Iceberg reader — the Trino hoglake connector among them — binds a `uuid` column through. `HoglakeSink._prepare` casts each batch to `columns_to_arrow_schema(info.columns)`, and since 1.3.0 that schema names `pa.uuid()` for a uuid column, so the coercer's extension column passes through unchanged and the annotation reaches the file. 1.3.0 also accepts **both** spellings on append (`pa.uuid()` and bare `fixed_size_binary(16)`), so a mixed fleet where some pods predate this pin keeps writing to the same table. Before 1.3.0 the schema named `pa.binary(16)`, the annotation was lost, and writing it anyway had the file refused — the two sides had to move together, which is why the floor is a pin rather than a workaround here. (1.3.0 requires pyarrow ≥ 21, which is why millpond's pyarrow floor moved with it.)

**Downstream stages work on the storage array.** `pa.uuid()` is an extension type and pyarrow's compute kernels refuse one, so the two pre-sink stages reach past it to the `fixed_size_binary(16)` storage:

- `MILLPOND_SORT_BY` on a uuid-pinned column sorts the 16 big-endian bytes, which is bytewise-identical to canonical text order. A sort key whose type has neither a kernel nor sortable storage (a `list`, a `map`) now skips the sort with `millpond_sort_skipped_total{reason="unsortable_type"}` and one warning per key set, instead of killing the flush with its offsets uncommitted.
- `MILLPOND_FILTER_KEEP_FIELD_NAME` / `MILLPOND_FILTER_DROP_FIELD_NAME` on a uuid-pinned column compare the same bytes; the configured values are parsed as UUIDs first. A static filter value that is not a UUID is a **startup error** — without the check, the keep direction would drop every batch and the drop direction would stop denying, both silently and permanently. A value served later by a dynamic include-values poll cannot be refused at startup. It cannot match a uuid column under any encoding, so it is **dropped from the comparison and the rest of the set still applies** — the allowlist keeps admitting the teams that are on it, the denylist keeps denying the ones it can. That costs one `millpond_errors_total{type="filter_value_invalid"}` and one warning (deduped on field, set size and first offender — never on the values themselves, which an authoritative source rebuilds on every membership change). Only a set with *no* usable value left falls back to the filter's failure direction: the allowlist admits nothing, under its own `records_skipped_total{reason="filter_value_invalid"}` rather than `filter_field_missing`, so a bad include set never reads as a renamed column; the denylist denies nothing. One caveat on the startup check: `MILLPOND_FILTER_VALUES` is parsed as integers when every entry is all-decimal, and a UUID whose 32 hex digits happen to contain no letters (~1 in 10^7) is all-decimal — so such a value is refused at load even though it is a perfectly good UUID. Write it in canonical hyphenated form, which is never all-decimal.

**The pin is two-way on an existing hoglake table.** Neither hoglake nor Iceberg has a `string ↔ uuid` promotion in either direction, so on a table that already exists the LIVE column type wins and the batch column is rewritten to match it. Both directions are ordinary operations and both used to be a permanent wedge:

| Live column | Batch column | How you get there | What happens |
|---|---|---|---|
| `string` | `uuid` | Adding `<col>:uuid` to a table created from unpinned batches | Written back as **canonical lowercase hyphenated** text |
| `uuid` | `string` | Removing the pin (a rollback), or one pod on a mixed fleet still running the old config | Text parsed back to the 16 bytes |

Neither direction casts: `pa.uuid() → string` reinterprets the 16 bytes as UTF-8 and raises `Invalid UTF8 payload`, `string → fixed_size_binary(16)` raises `widths must match`. Both are deterministic on the batch, so the retry path correctly classifies them permanent — which means the flush died on attempt 1 with its offsets uncommitted and the restart re-consumed the same batch forever. Each rewrite bumps `millpond_errors_total{type="schema"}` every flush and logs once per column per direction. Values that are not UUIDs are nulled, in **both** directions, with `millpond_errors_total{type="column_coercion"}`: in the `string → uuid` row by the rewrite itself, and in the `uuid → string` row earlier, by the coercer, before the batch ever reaches a sink.

**Neither direction is byte-preserving**, and the `string → uuid` row is the one to watch: a rewrite normalizes. Only one of the five accepted input shapes survives unchanged — an already-canonical lowercase hyphenated string. Uppercase hex, bare 32 hex, `{…}` and `urn:uuid:…` all come back canonical lowercase hyphenated, because that is the only rendering 16 bytes have. If a downstream reader matches those columns as text, a pin applied and rolled back is a value change for it even where nothing was nulled.

**No DDL is attempted in either direction, so the pin does not change an existing column's type** — getting a real `uuid` column means creating the table with the pin in place (or recreating it).

The DuckLake sink has the same two directions and the same "live column wins" rule, implemented in `ducklake._align_uuid_columns`:

- **live `UUID`, batch VARCHAR** — parsed before the INSERT, by the same code the pin runs. Not optional: DuckDB's implicit VARCHAR→UUID cast is all-or-nothing, so one unparseable value raises `ConversionException` for the whole insert on every retry; it also refuses `urn:uuid:…`, which millpond's own grammar accepts.
- **live VARCHAR, batch `uuid`** — nothing to do. DuckDB casts UUID→VARCHAR natively and losslessly on INSERT and the rows land as canonical text. It costs one refused `ALTER … SET DATA TYPE UUID` per flush, counted on `millpond_errors_total{type="schema"}`, like any other pinned-vs-live mismatch.

Cost is ~11.8 ms per 27k-row column (~0.44 µs/value; ~23 ms for the `uuid` + `person_id` pair) — pyarrow compute has no hex-decode kernel, so the bytes come from a per-value length/offset check plus `bytes.fromhex`. That is under `MILLPOND_SORT_BY`'s 50–200 ms per flush and far under the flush budget at both the dev rate (~1.1k rec/s per pod) and prod (30k+). A batch containing an unparseable value falls to the shared per-value path and costs ~134 ms for 27k rows — the cold path, on a batch that is already anomalous.

The timestamp wire format is space-separated, UTC implied, with 0, 3, or 6 fractional digits depending on column/producer (e.g. `2024-01-01 12:00:00.123`); all parse. Each cleanly-coerced column increments `millpond_columns_coerced_total{target_type=...}` **once per column per batch** — a column with even one unconvertible value is not counted there at all, it increments `millpond_errors_total{type="column_coercion"}` instead (also once per column per batch, not once per bad value). The two are mutually exclusive per column per batch, so a column that is drifting reads as a flat `columns_coerced_total` next to a rising `errors_total`, never as both. Coercion is **non-fatal and type-consistent**: a present, configured column is always emitted as the target type — values that can't be cast (a producer format/type drift) are **nulled** (only the unconvertible ones; good values in the batch are kept), never left as the source type. That keeps buffered batches concat-compatible at flush and bumps `millpond_errors_total{type="column_coercion"}` so the drift is loud via metrics, without crashing the pod or risking offset commits past unwritten records. Columns absent from a batch or already the target type are left untouched, so the same map is safe across heterogeneous batches.

### VARIANT dual-write (JSON properties → shredded VARIANT)

JSON property blobs (e.g. PostHog `properties`) land as VARCHAR today. `MILLPOND_VARIANT_COLUMNS` dual-writes listed source columns into companion VARIANT columns so DuckDB can auto-shred common sub-fields into typed Parquet columns, without dropping the original string:

```
MILLPOND_VARIANT_COLUMNS=properties,person_properties
```

For each listed source present in a batch, millpond:

1. Keeps the original column as-is (VARCHAR JSON text)
2. `ADD COLUMN IF NOT EXISTS {name}_variant VARIANT` on the DuckLake table
3. Projects `try_cast(try_cast(col AS JSON) AS VARIANT) AS {name}_variant` on INSERT

Malformed JSON nulls only the VARIANT companion (the string column still lands). DuckDB shreds VARIANT on Parquet write automatically — no millpond-side shredding config. Existing tables get the companion column via schema evolution on the first dual-write flush; historical rows keep a NULL companion until rewritten.

Degrades without crash-looping when dual-write cannot run cleanly:

- Payload fields named `{name}_variant` (any casing — DuckDB identifiers are case-insensitive) are stripped non-fatally (`variant_companion_columns_dropped_total`; records still land minus the field) so a poison key cannot evolve a VARCHAR companion or bind-conflict the INSERT. Writers also strip any payload field whose *live* table column is VARIANT, so a pod whose `MILLPOND_VARIANT_COLUMNS` is unset or stale (mixed fleet) cannot corrupt a companion via the implicit VARCHAR→VARIANT cast. A batch left with zero columns by the strip is skipped whole (`records_skipped_total{reason="variant_companion_collision"}` — those records *are* lost and excluded from `records_written_total`) instead of crash-looping the partition.
- If `{name}_variant` already exists as a non-VARIANT type, or ADD COLUMN fails, that source is omitted from the VARIANT projection (string column still writes); `errors_total{type="schema"}` is bumped. DuckLake cannot `ALTER VARCHAR → VARIANT`.
- Integers in `(INT64_MAX, UINT64_MAX]` are **rewritten as JSON strings in the companion only** (`variant_values_coerced_total`); the source column keeps its original bytes, and every other field of the row keeps its normal type. `try_cast` is not protection here: such a value casts into a perfectly valid VARIANT and reads back fine, then overflows when DuckDB shreds VARIANT into typed Parquet columns on write — which crash-looped every prod NRT consumer on 2026-08-12 (offsets never advance, so one poison batch wedges the partition forever). That window is exactly the hazard: values at or below `INT64_MAX` (nanosecond timestamps, snowflake ids), above `UINT64_MAX` (DuckDB keeps those as VARIANT strings itself), and negatives all shred fine and are left alone. The rewrite happens in Arrow before the INSERT, so it also covers the *inlined* write path, where the value would otherwise commit into catalog state and only detonate later during `ducklake_flush_inlined_data`. Cost is one vectorized regex scan per flush (~10ms per 8k-row batch); rows are only JSON-parsed when that scan finds a candidate.
- As a backstop, if the INSERT still fails with an out-of-range conversion error, the flush is retried string-only (`variant_write_fallback_total`, `errors_total{type="variant_write"}`). This should stay at zero — the known residual is a non-string source column holding an unsigned integer above `INT64_MAX`, which the Arrow-side scan cannot reach — and the cost when it fires is the whole batch's companions plus an abandoned Parquet file. Only that specific error signature is absorbed; commit contention and IO failures propagate to the write-retry path, which classifies them and reloads the schema cache.

This is an opt-in migration step: readers can move from `json_extract(properties, …)` to `properties_variant."$browser"` (etc.) once the companion is populated, then a later cutover can drop the string column if desired. Dual-write (new column) is the supported path for that reason.

**Production caveats (canary first):**

- **Key cardinality / shredding.** DuckDB auto-shreds VARIANT from the structure it sees at Parquet write time. PostHog-scale `properties` have a long tail of custom keys; a flush can produce very wide Parquet schemas (hundreds–thousands of shredded leaf fields). Prefer canarying on a filtered consumer or lower-cardinality table before enabling fleet-wide on `events`.
- **Memory.** Dual-write keeps the VARCHAR column and materializes VARIANT at INSERT — peak flush memory is higher than string-only. Leave headroom vs `FLUSH_SIZE` and the pod limit when turning this on for large property blobs.
- **Test coverage.** Unit/integration dual-write tests exercise the SQL cast and companion DDL against plain DuckDB; they do not exercise DuckLake catalog DDL, Parquet shredding, or data-inlining edge cases. Validate shredding and file shape on a real DuckLake canary before relying on query performance.

## Adaptive Backpressure

The consume batch size automatically scales based on how full the pending buffer is relative to the flush threshold. When the buffer is empty, millpond consumes at full speed. As the buffer approaches the flush size, the batch size drops proportionally, smoothing throughput during catchup and traffic spikes. OOM prevention comes from bounding librdkafka's internal fetch buffer via `queued.max.messages.kbytes` (16MB per partition).

```
fullness = pending_bytes / flush_size
batch_size = max(10, int(CONSUME_BATCH_SIZE * (1.0 - fullness)))
```

Metrics: `millpond_buffer_fullness` and `millpond_consume_batch_size_current`.

## Performance

The hot path is all C/C++: librdkafka → orjson → PyArrow → DuckDB (zero-copy Arrow scan). Python is glue.

## Resource Footprint

| | Kafka Connect worker | Millpond pod |
|-|---------------------|-----------|
| Memory request | 4-8Gi (JVM heap) | 256Mi |
| Memory limit | 8-16Gi | 512Mi |
| Steady-state | ~4GB (JVM + framework + GC headroom) | ~250-300MB |

No JVM, no framework, no GC heap overhead. ~16x less memory per pod. The entire runtime is C/C++ libraries with a Python glue layer.

## Setup

Requires [Flox](https://flox.dev):

```bash
flox activate
just sync
just run
```

## Development

```bash
just fmt               # format code
just lint              # lint code
just test              # run unit tests
just test-integration  # run integration tests (in-memory DuckDB — fast, no docker stack)
just test-e2e          # run E2E tests (docker-compose, builds stack automatically)
just test-hoglake-integration  # hoglake sink vs a real hoglake server (throwaway stack, high ports)
just test-hoglake-e2e  # Kafka -> main.py -> hoglake end to end (same throwaway stack)
just ci                # format check + lint + unit tests
just up                # start docker-compose stack (DuckLake — plaintext Kafka)
just up-ssl            # start docker-compose stack (DuckLake — SSL Kafka, closer to prod)
just down              # stop docker-compose stack
just down-ssl          # stop SSL docker-compose stack
```

### SSL Kafka Testing

The `just up-ssl` recipe generates self-signed certs and runs Kafka with SSL listeners, matching the production MSK configuration. This exercises the `KAFKA_CONSUMER_*` env var override path that isn't tested with plaintext Kafka.

Requires Docker (uses `keytool` from the Kafka container image for cert generation).

### DuckLake Maintenance and state metrics

The `tools/` directory ships two DuckLake-only operational binaries inside the same image as the writer:

- **`tools/ducklake_maintenance.py`** — CLI for snapshot expiry (incl. Postgres-native `expire-snapshots`), file cleanup, orphan recovery, tiered compaction, fsck, and one-shot repairs (`repair-partition-values`, `dedup-deletions`, `purge-orphan-stats`). Runs as a K8s CronJob.
- **`tools/ducklake_metrics.py`** — Catalog-side lake-state metrics, either as a long-running Prometheus-exposition daemon or in one-shot push mode (`--once`, POSTing to `DUCKLAKE_METRICS_PUSH_URL` — the per-tenant metrics CronJob path).

`tools/justfile` (copied to `/justfile` in the image) wraps both. Recipe groups:

- `interactive` — `shell`: a DuckDB shell wired to the DuckLake with the same session setup as the subcommands
- `lifecycle` — snapshot + file lifecycle: `expire`/`expire-snapshots` (+ chain-safe `expire-7d`), `cleanup`/`cleanup-all`/`cleanup-all-safe`, orphan handling (`find-orphans`, `heal-orphans`, `delete-orphaned-files`, `fsck`), `dedup-deletions`, `purge-orphan-stats`, `repair-partition-values`, `maintain`, `checkpoint`. Destructive recipes print the target catalog and, on a TTY, demand confirmation.
- `compaction` — tiered `compact-to-tier-{1,2,3}` (+ dry-runs), `compact-all-tiers`, `compact-probe`, and chain-safe no-arg wrappers (`compact-all-tiers-default`) so the tenant-maintenance CronJob can run one flat recipe chain
- `bootstrap` — `bootstrap-indexes`: the DuckLake catalog btrees (compaction scans, snapshot-range reads, per-file joins) via `psql`, all `CREATE INDEX CONCURRENTLY IF NOT EXISTS`
- `metrics` — `ducklake-metrics` daemon recipes and chain-safe `metrics-once` for the per-tenant metrics CronJob

Subcommand and YAML schema reference and the full env-var contract live in [`tools/README.md`](tools/README.md). Both binaries reuse the writer's `DUCKLAKE_RDS_*` / `DUCKDB_S3_*` / `DUCKLAKE_DATA_PATH` env vars.

## Configuration

All configuration via environment variables.

### Core (always required)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka broker addresses |
| `KAFKA_TOPIC` | yes | | Topic to consume |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas (must match `spec.replicas`) |
| `MILLPOND_DESTINATION` | no | `ducklake` | Destination — `ducklake` or `hoglake`; anything else raises at startup. Case-insensitive; empty/whitespace falls back to `ducklake`. Only the selected destination's env group is read (stray vars from the other backend are ignored). |
| `FLUSH_SIZE` | no | `104857600` | Flush after this many bytes of accumulated Arrow data (default 100MB) |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `GROUP_ID` | no | `millpond-{topic}-{table}` for `ducklake`, `millpond-{destination}-{topic}-{table}` otherwise | Kafka group.id — used for offset storage in `__consumer_offsets` only, no consumer group semantics. Changing this loses committed offsets and triggers full replay, which is why the DuckLake form is frozen. Non-DuckLake destinations carry the destination name so a shadow deployment (same topic, same table name, other destination) cannot share an offset namespace with the pipeline it shadows and split the partitions between them. |
| `KAFKA_AUTO_OFFSET_RESET` | no | `earliest` | Applied only when no offset is committed for a partition: `earliest` (backfill/catch-up) or `latest` (NRT consumers — don't replay the retention window). `KAFKA_CONSUMER_AUTO_OFFSET_RESET` is rejected at startup; use this var. |
| `KAFKA_CONSUMER_*` | no | | Passthrough to librdkafka: `KAFKA_CONSUMER_SECURITY_PROTOCOL=SASL_SSL` → `security.protocol=SASL_SSL`. `KAFKA_CONSUMER_QUEUED_MAX_MESSAGES_KBYTES` overrides the 16MB-per-partition fetch-buffer default. `sasl.mechanisms=OAUTHBEARER` enables the MSK IAM token callback. |
| `BROKER_SOURCE` | no | | Broker label attached to every metric (e.g. `msk`, `warpstream`) |
| `CONSUME_BATCH_SIZE` | no | `1000` | Max messages per `consume()` call — amortizes Python↔C boundary cost |
| `FETCH_MIN_BYTES` | no | `1048576` | Broker accumulates at least this many bytes before responding (1MB) |
| `FETCH_MAX_WAIT_MS` | no | `500` | Max broker wait when `fetch.min.bytes` not yet satisfied |
| `STATS_INTERVAL_MS` | no | `5000` | librdkafka internal stats emission interval (0 to disable) |
| `LOG_LEVEL` | no | `INFO` | Python log level (DEBUG, INFO, WARNING, ERROR) |
| `MILLPOND_HTTP_PORT` | no | `8000` | Port for the /metrics + /healthz + /readyz HTTP server. Exists for test harnesses running millpond as a host process; charts and probes depend on the default. |

### DuckLake

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DUCKLAKE_TABLE` | yes | | Target DuckLake table name |
| `DUCKLAKE_SCHEMA` | no | `main` | Target DuckLake schema |
| `DUCKLAKE_DATA_PATH` | yes | | S3 path for DuckLake data files |
| `DUCKLAKE_CONNECTION` | yes | | DuckDB connection string |
| `DUCKLAKE_RDS_HOST` | yes | | Postgres host for DuckLake metadata |
| `DUCKLAKE_RDS_PORT` | no | `5432` | Postgres port |
| `DUCKLAKE_RDS_DATABASE` | no | `ducklake` | Postgres database name |
| `DUCKLAKE_RDS_USERNAME` | no | `ducklake` | Postgres username |
| `DUCKLAKE_RDS_PASSWORD` | yes | | Postgres password |
| `DUCKLAKE_MAX_RETRY_COUNT` | no | `100` | DuckLake commit-retry budget (`SET ducklake_max_retry_count`). DuckLake's own default of 10 starves multi-writer deployments — losers of the snapshot-id race surface as `ducklake_snapshot_pkey` duplicate-key errors. Must be positive. |
| `DUCKLAKE_PARTITION_BY` | no | | Hive-style partition expression (e.g. `year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)`). Applied via `ALTER TABLE SET PARTITIONED BY` on first write. |
| `DUCKDB_S3_ACCESS_KEY_ID` | yes | | Static S3 access key for DuckDB |
| `DUCKDB_S3_SECRET_ACCESS_KEY` | yes | | Static S3 secret for DuckDB |
| `DUCKDB_S3_REGION` | no | | S3 region |
| `DUCKDB_S3_ENDPOINT` | no | | S3 endpoint override (MinIO, etc.) |
| `DUCKDB_S3_USE_SSL` | no | | `true` / `false` |
| `DUCKDB_S3_URL_STYLE` | no | | `vhost` / `path` |

### Hoglake

Read only when `MILLPOND_DESTINATION=hoglake`. Names are validated at startup against the server's identifier rules (catalog: `[a-z][a-z0-9_-]{0,62}`; namespace/table: `[A-Za-z_][A-Za-z0-9_-]{0,127}`).

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `HOGLAKE_URL` | yes | | Control-plane base URL (`/v1` is appended by the client) |
| `HOGLAKE_CATALOG` | yes | | Catalog name |
| `HOGLAKE_NAMESPACE` | yes | | Namespace (created on first write if absent) |
| `HOGLAKE_TABLE` | yes | | Table name (created on first write from the batch schema + `_inserted_at`) |
| `HOGLAKE_DATA_PATH` | no | | When set, a missing catalog is created with this data path on first write. Unset: a missing catalog is a startup-shaped error (catalog provisioning stays an ops decision). Validated as an `s3://bucket/prefix` URI at startup: it is frozen into the catalog row at creation and hoglake has no delete-catalog route, so a typo mints a permanently unusable catalog under a name nobody can reuse. |
| `HOGLAKE_S3_ACCESS_KEY` | no | | S3 access key for the parquet write path (pyhoglake writes the files; separate from the DuckLake `DUCKDB_S3_*` vars). Optional, but only together with the secret key — see below. An empty or whitespace-only value counts as unset, like the endpoint and region. |
| `HOGLAKE_S3_SECRET_KEY` | no | | S3 secret key. Set both keys or neither; one without the other is refused at startup, naming both variables. Empty counts as unset here too — a chart rendering `value: ""` for an absent Secret means "use the credential chain". |
| `HOGLAKE_S3_ENDPOINT` | no | | S3 endpoint override (MinIO etc.); unset = AWS. Independent of the keys. |
| `HOGLAKE_S3_REGION` | no | | S3 region. Independent of the keys. Under the default credential chain the SDK falls back to `AWS_REGION`, which the IRSA webhook injects, so this is not strictly required — set it anyway, so a pod pointed at a bucket in another region fails the startup probe instead of signing for the wrong one. |
| `HOGLAKE_PARTITION_BY` | no | | Comma-separated partition expression mapped to hoglake transforms at table creation: bare `col` (identity), `year(col)`/`month(col)`/`day(col)`/`hour(col)`, `bucket(col, N)`. Anything outside that vocabulary refuses startup with a clear error — never a per-batch failure. Typical: `team_id,month(_inserted_at)`. |
| `HOGLAKE_MAX_RETRY_COUNT` | no | `8` | Write-path retry attempts. The DuckLake counterpart tunes an *inner* loop under millpond's 3 outer attempts; hoglake has no inner loop, so this IS the budget. Must be positive, and bounded with the timeout below. |
| `HOGLAKE_REQUEST_TIMEOUT_S` | no | `45` | Per-request HTTP timeout for the catalog client. Deliberately above pyhoglake's hardcoded 30s, which equals the server's own commit-lock admission bound: at an equal timeout the client gives up at the instant the server would have answered `503` + `Retry-After`, so its explicit backpressure signal is nearly unreachable and arrives as a transport failure instead. |

`HOGLAKE_MAX_RETRY_COUNT` x `HOGLAKE_REQUEST_TIMEOUT_S` bounds how long one flush can sit inside `sink.write()`, and the consume loop is single-threaded: the liveness probe fails the pod after 480s without a poll. **`load()` refuses a combination that can exceed it** — the defaults come to ~474s of the 480s, which is deliberately close to the line (a catalog that has answered nothing in eight minutes is one the pod should die over) but leaves a raise of either knob nowhere to hide.

**The object-store keys are optional, and optional as a PAIR.** Set both for static credentials — that is the MinIO shape, used by local dev and CI. Set neither and pyhoglake passes no key to pyarrow's `S3FileSystem`, which falls through to the AWS SDK's default credential chain; in Kubernetes that is the ServiceAccount's web-identity token (IRSA), the same way the hoglake server itself authenticates, so a pod needs no Secret at all. One key without the other is refused at startup, naming both variables. pyarrow would refuse it too (`S3FileSystem(access_key=...)` with no secret raises `ValueError`), but only when pyhoglake first builds the filesystem and in a message that names neither variable — the config check just moves that to the front and says which half is missing. `HOGLAKE_S3_ENDPOINT` and `HOGLAKE_S3_REGION` are independent of the choice.

**The minimum grant is small.** The sink needs `s3:PutObject` under its data path, plus `s3:AbortMultipartUpload` so an interrupted upload can be cleaned up, and nothing else — it never lists the bucket, and the catalog (not the client) owns every read. The startup probe below issues the same `open_output_stream` call the flush path uploads with, so what it proves is the write request millpond will actually make.

**The catalog client has no authentication or TLS credential surface.** The hoglake control plane does not authenticate requests in this version, and `HoglakeClient` accepts no token, header, client certificate or verification setting — `HOGLAKE_URL` must therefore be a trusted-network endpoint (cluster-internal service DNS, not a public hostname). The `HOGLAKE_S3_*` credentials are object-store credentials only; they have nothing to do with reaching the catalog.

Semantics on the hoglake path:

- **Text only.** `properties` and every other JSON payload lands as a string column, exactly as the arrow converter produces it. `MILLPOND_VARIANT_COLUMNS` combined with `MILLPOND_DESTINATION=hoglake` is a startup error (hoglake has no VARIANT column type — deferred, not silently skipped).
- **Startup.** The catalog is resolved when the sink is constructed, so a wrong `HOGLAKE_URL`, an unreachable control plane or a missing catalog (without `HOGLAKE_DATA_PATH`) fails at startup rather than on the first flush, after the pod has passed its probes and built lag.
- **Credential probe.** The catalog resolve proves the control plane is reachable and says nothing about object storage, and pyarrow resolves S3 credentials lazily — so a role without the write grant used to surface as an S3 403 on the first upload, at the same bad moment. The constructor therefore performs a zero-byte upload of a marker object at `<data_path>/_millpond/probe` and refuses to start if it fails. Not literally one `PutObject`: pyarrow's `open_output_stream` opens a multipart upload eagerly, so an empty stream closes as CreateMultipartUpload → UploadPart(1, 0 bytes) → CompleteMultipartUpload (one empty part, which S3 and MinIO both accept), all three authorised by `s3:PutObject`. An upload rather than a listing, for three reasons: it is the *same* `open_output_stream` call pyhoglake uploads each parquet file with, so it is mechanically predictive of the write path rather than a nearby cheap operation; it tests the grant the sink actually has, never `ListBucket`; and it cannot pass on a bucket that does not exist — pyarrow's listing maps a `NoSuchBucket` 404 onto an empty prefix, so a typo in `HOGLAKE_DATA_PATH` read as healthy and then failed on the first flush, after the catalog row had frozen the bad path. The marker key is fixed, so every boot overwrites it: at most one object per catalog data path, and hoglake never touches objects it did not register (`cleanup` drains only server-queued paths, `verify` is metadata-only), so it is inert rather than orphan debt. The refusal names the marker path, the catalog, the credential source in use and the SDK's own error text, plus the setting to look at — the bucket on `NoSuchBucket`, `HOGLAKE_S3_REGION` / the pod's `AWS_REGION` on `AuthorizationHeaderMalformed` or `PermanentRedirect`, and the static `HOGLAKE_S3_*` keys on `InvalidAccessKeyId` / `SignatureDoesNotMatch`. It is not retried and it is not a liveness check: a missing grant is config, and config does not heal by waiting. The probe's one log line is also the startup record of the credential source (never the key). There is no metric.
- **Bootstrap.** First write ensures namespace → table (concurrent creation by other pods is tolerated at every level) and declares the partition spec + sort order in one alter with field ids resolved from the created schema, then verifies the result. A partition column missing from the schema is fatal; a missing sort field skips the sort-order declaration with a warning (batches are still pre-sorted on present fields).
- **Reconciliation.** Creation is two round trips, so a failure between them leaves a table that exists and has no layout. Every later resolve therefore compares the live partition spec and sort order against config: a match proceeds, an unspecced table that config says should be specced gets its declaration (the recovery), and any genuine divergence — a changed `HOGLAKE_PARTITION_BY`, a changed bucket count, a partitioned table under a config that says nothing — is fatal and stays fatal, with the live spec printed in `HOGLAKE_PARTITION_BY`'s own grammar. The intent is that a config typo can never leave a permanently unpartitioned table behind. Two consequences to plan for: **changing the partition spec of a live table is not supported by changing config** (point the pipeline at a new table), and a table whose sort order was declared by something else needs `MILLPOND_SORT_BY` set to match.
- **Publication.** Each flush is a prepared upload plus one idempotent commit keyed on its Kafka offset range — see [Destination](#destination) for what that guarantees and what it orphans.
- **Snapshot message.** Every commit records an author (`millpond/<table>/<ordinal>`) and a message. The message says what the flush was and where it came from in Kafka:

  ```
  records=26402 files=31 partitions=31 arrow_bytes=268435456 trigger=size millpond=v0.0.42 table=6f1b6d2e-4c5a-4f3e-9b7a-2d8c1e0a4f77
  offsets clickhouse_events_json p3:4128819-4155470 p19:4130021-4156802 ... (32)
  ```

  Line 1 is a fixed sequence of `key=value` pairs, in that order, and no value holds a space. `records` is the rows the flush published; `files` the objects it wrote; `partitions` how many partition tuples the fanout produced (`0` on an unpartitioned table, and equal to `files` on a partitioned one, because the fanout writes one object per tuple); `arrow_bytes` approximately the size the flush gate measured, taken from the Arrow batch after consolidation (the concat, its type promotion and the sort all move it a little, typically under 1%, more when the batch carried schema drift); `trigger` why the flush happened now (`size`, `interval`, `final` for the flush at shutdown, or `unknown`); `millpond` the running version (`MILLPOND_SERVICE_VERSION`, else the package version); and `table` the destination table's incarnation, so two flushes of one offset range into a dropped-and-recreated table do not read as the same publication. Line 2 lists every partition range the flush covered, sorted by partition number, with the topic named once and the range count at the end; a flush that spanned more than one topic gets one such line per topic. The offset ranges are the only record of which Kafka positions a snapshot holds, so this is what answers "is offset X in the lake", audits a replay after a crash, and reconciles a gap. The offsets line is bounded at 16 KiB, which holds about 780 ranges — one pod owning every partition of a 512-partition topic writes about 10.5 KiB and is never truncated. Over the bound the line keeps the ranges that fit and ends with `... (+<k> more)`; the summary line is never truncated. The message is the same text on every attempt of one flush, so a retry cannot describe its flush differently from the commit that landed.
- **Evolution.** New batch columns become a single batched `add_column` alter (one DDL commit for the whole drift, falling back to per-column if the batch is refused); `int→long` / `float→double` live-type mismatches → `promote_column`; failures degrade per column (logged + `millpond_errors_total{type="schema"}`). Name matching is exact — hoglake identifiers are case-sensitive, unlike DuckDB's case-insensitive resolution.
- **Dropped payload keys.** Three classes of column name are dropped per column rather than sent, each counted on `millpond_records_skipped_total{reason="unsafe_field_name"}`, so one poison producer key cannot wedge a partition: names the server reserves (the `_hog` prefix), names longer than the server's 128-character limit, and names outside millpond's shared `SAFE_IDENTIFIER` (`[a-zA-Z_][a-zA-Z0-9_]*`). That last gate is millpond's, not hoglake's: **hyphenated keys like `utm-source` are dropped even though the hoglake server would accept them**, because `SAFE_IDENTIFIER` is shared with the DuckLake backend, where such a name has to be quoted into generated SQL. Rename the key upstream if you need it. (Reserved-name collisions are narrower here than on DuckLake: only `_inserted_at` is reserved. `year`/`month`/`day`/`hour` are ordinary columns for hoglake, which partitions by catalog transforms rather than Hive directories.)
- **Metrics.** All existing counters work unchanged, plus three hoglake-only series: `millpond_hoglake_files_written_total` (parquet files registered per commit — with partitioned fanout, the compaction-debt feed rate), `millpond_hoglake_commit_replays_total{outcome}` (commits resolved from the server's receipt instead of publishing; non-zero is a duplicate that did not happen, a rising rate means pods are dying mid-flush), and `millpond_hoglake_orphaned_files_total` (uploads never registered — unreclaimed storage). `millpond_errors_total{type="hoglake_commit_contention"}` labels OCC 409s the way `ducklake_commit_contention` does for DuckLake; the DuckLake label is now gated by destination so a hoglake pod can never raise it.

### Optional record handling

See [Record Handling](#record-handling) for context. All variables below are optional; unset means the corresponding stage is disabled.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MILLPOND_FILTER_KEEP_FIELD_NAME` | no | | Column name to check against the allowlist. Must be set with `MILLPOND_FILTER_VALUES`. Validated as a safe identifier. |
| `MILLPOND_FILTER_VALUES` | no | | Comma-separated allowed values. Auto-detected as int if every token parses as an integer, string otherwise. Required when the keep field name is set. |
| `MILLPOND_FILTER_DROP_FIELD_NAME` | no | | Column name for the denylist filter. Must be set with `MILLPOND_FILTER_DROP_VALUES`. May be combined with the keep-filter (keep ∩ ¬drop). Validated as a safe identifier. |
| `MILLPOND_FILTER_DROP_VALUES` | no | | Comma-separated denied values; same int/string auto-detection. Required when the drop field name is set. |
| `MILLPOND_INCLUDE_VALUES_URL` | no | | HTTP endpoint returning a JSON array of allowlist values (see [Dynamic allowlist source](#dynamic-allowlist-source)). Requires the keep-filter to be configured. |
| `MILLPOND_INCLUDE_VALUES_MODE` | no | `shadow` | `shadow` (static authoritative, endpoint observed for diff metrics) or `authoritative` (polled set live). Only valid with the URL set. |
| `MILLPOND_INCLUDE_VALUES_POLL_INTERVAL_S` | no | `60` | Poll cadence, jittered ±10%. |
| `MILLPOND_INCLUDE_VALUES_REMOVAL_POLLS` | no | `5` | Consecutive successful polls a value must be absent before removal. `1` disables damping (warned at startup). |
| `MILLPOND_INCLUDE_VALUES_REQUEST_TIMEOUT_S` | no | `10` | Per-request HTTP timeout. |
| `MILLPOND_INCLUDE_VALUES_STARTUP_TIMEOUT_S` | no | `60` | Authoritative mode: how long startup blocks for the first successful poll before failing the pod. |
| `MILLPOND_INCLUDE_VALUES_AUTH_HEADER_NAME` | no | | Header name sent with each poll (e.g. an internal-secret header). Must be set together with the token. Redirects are refused so the header can't leak cross-host. |
| `MILLPOND_INCLUDE_VALUES_AUTH_TOKEN` | no | | Header value. Must be set together with the header name. |
| `MILLPOND_SORT_BY` | no | | Comma-separated column names; the batch is sorted ascending by these in tuple order before each write. Missing fields cause the sort to be skipped (records still flow). |
| `MILLPOND_TYPED_COLUMNS` | no | | Comma-separated `column:type` pairs pinning columns to a target type before write (types: `timestamptz`, `bigint`, `double`, `boolean`, `varchar`, `uuid`). Works on both destinations — no type in the list is destination-restricted. Needed when writing into a table whose columns are already typed and JSON inference would diverge (date-times → `VARCHAR` vs `TIMESTAMPTZ`; all-null `project_id` → `VARCHAR` vs `BIGINT`; UUID strings → `VARCHAR` vs hoglake `uuid` / DuckDB `UUID`). Column names validated as safe identifiers; types validated against the allowlist. |
| `MILLPOND_VARIANT_COLUMNS` | no | | DuckLake destination only (rejected at startup with `hoglake`). Comma-separated source column names to dual-write as DuckLake `VARIANT` companions (`properties` → `properties_variant`). Original string columns are kept. Malformed JSON nulls only the VARIANT side. Column names validated as safe identifiers; names ending in `_variant` are rejected (list the source, not the derived column). |

### Log export (optional)

Application logs go to stdout; setting `POSTHOG_PROJECT_TOKEN` additionally exports them to PostHog Logs via OTLP/HTTP.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `POSTHOG_PROJECT_TOKEN` | no | | Enables the OTLP export when set. No `MILLPOND_` prefix — it's the PostHog-wide secret name. |
| `POSTHOG_LOGS_ENDPOINT` | no | `https://us.i.posthog.com/i/v1/logs` | Override for EU or self-hosted PostHog |
| `MILLPOND_SERVICE_NAMESPACE` | no | `millpond` | OTLP `service.namespace` resource attribute |
| `MILLPOND_SERVICE_INSTANCE_ID` | no | | OTLP `service.instance.id` — typically the chart's consumer key (e.g. `events`) |
| `MILLPOND_SERVICE_VERSION` | no | package version | OTLP `service.version` override (e.g. the image digest) |

## Releases

Every merge to `main` automatically:
1. Bumps the patch version (`v0.0.1` → `v0.0.2`)
2. Builds and pushes a Docker image to `ghcr.io/posthog/millpond:<tag>`
3. Creates a GitHub release with changelog

Images: `ghcr.io/posthog/millpond:v0.0.X` or `ghcr.io/posthog/millpond:latest`

A manual `promote-to-prod` workflow retags a chosen release (default: latest) as `ghcr.io/posthog/millpond:prod`.

## Deployment

```bash
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/pdb.yaml
kubectl apply -f k8s/statefulset.yaml
```

Partition count is discovered at startup via `admin.list_topics(topic=cfg.topic, timeout=30)` (an `AdminClient`, not the consumer instance). Each pod computes its partition assignment from its ordinal:

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

Before assigning, the consumer recovers committed offsets that have fallen below the broker's log-start-offset: stale offsets are re-committed to the `KAFKA_AUTO_OFFSET_RESET` target, and lag/offset gauges are seeded for every assigned partition — so quiet partitions don't report `offset_out_of_range` and inflated `millpond_consumer_lag` across restarts.

### Updating

Rolling updates are a poor fit — pods with different `REPLICA_COUNT` values cause double-assignment or gaps. Since Kafka is the durable buffer:

1. **Canary**: Deploy one pod with the new version, verify metrics
2. **Graceful shutdown**: Scale to 0 (pods flush and commit)
3. **Full redeploy**: Update image/config, scale back up from committed offsets

Downtime = drain time + startup time (~2-3 min). Kafka buffers trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically.

## Partitioning

Set `DUCKLAKE_PARTITION_BY` to enable Hive-style partitioning on S3. Files are written into `key=value/` directories (e.g. `year=2026/month=3/day=23/hour=21/*.parquet`), enabling S3 prefix filtering, bulk lifecycle rules, and partition discovery by external tools.

```bash
DUCKLAKE_PARTITION_BY="year(_inserted_at),month(_inserted_at),day(_inserted_at),hour(_inserted_at)"
```

Partition on `_inserted_at` (always a real TIMESTAMP), not source `timestamp` fields (typically VARCHAR). Applied via `ALTER TABLE SET PARTITIONED BY` on first write — idempotent, safe for multiple pods and restarts. If added to an existing unpartitioned table, new files get HSP layout while old files remain flat; DuckLake queries both transparently via metadata.

For the hoglake destination, set `HOGLAKE_PARTITION_BY` instead (same expression style, restricted to hoglake's transform vocabulary — see the [Hoglake config](#hoglake)). Partition tuples are computed client-side by pyhoglake under the table's live spec; the batch fans out into one parquet file per tuple, registered in one atomic commit, and each file carries its `partition_values`. The temporal transforms are Iceberg-semantics epoch-relative ints, not Hive `key=value` directories.

## Object Sizing

S3 throughput scales with object size — small objects (<1MB) waste per-request overhead, while larger objects (128MB+) maximize GET/PUT throughput. Millpond flushes are triggered by whichever comes first: `FLUSH_SIZE` (Arrow bytes in memory) or `FLUSH_INTERVAL_MS` (wall clock). The resulting Parquet file is typically **3-4x smaller** than the Arrow representation due to columnar encoding and compression.

At steady state with moderate volume, most flushes are **time-triggered** — the interval expires before the size ceiling is hit. Object size is therefore driven by: `(msgs/s per pod) × (bytes/msg as Parquet) × (flush interval)`.

### Sizing by volume

Assuming ~366 bytes/row in Parquet (7-column event schema), 512 partitions, 8 replicas (64 partitions/pod):

| Per-partition msg/s | Total msg/s | Per-pod msg/s | Parquet/file @60s | Parquet/file @90s | Memory/pod @90s |
|---|---|---|---|---|---|
| 500 | 256K | 32K | ~11MB | ~17MB | 512Mi |
| 1K | 512K | 64K | ~23MB | ~34MB | 512Mi |
| 2K | 1M | 128K | ~45MB | ~68MB | 512Mi |
| 4K | 2M | 256K | ~90MB | ~135MB | 640Mi |
| 9.5K (peak) | 4.9M | 608K | ~213MB | ~320MB | 1Gi |

### Recommended settings for ~128MB target objects

For a pipeline averaging 4K msg/s per partition with 512 partitions and 8 replicas:

```yaml
FLUSH_SIZE: "1073741824"       # 1GB Arrow ceiling (safety valve for burst/catchup)
FLUSH_INTERVAL_MS: "90000"     # 90s — produces ~135MB Parquet at mean volume
```

Memory limit: 640Mi (90s × 256K msg/s × ~1KB Arrow/msg ≈ ~230MB Arrow + DuckDB + librdkafka overhead).

At peak (9.5K/partition), the size trigger fires at ~35s producing ~320MB objects — acceptable, and the pod stays within 1Gi.

### When to add a merge job

If your volume is low enough that time-triggered flushes produce <10MB objects, run periodic compaction. The `ducklake_maintenance.py compact` subcommand implements a tiered strategy: small files merge frequently into medium files, medium files merge less often into large files. Tier ranges, `target_file_size` save/restore semantics, and the `--threads` / `--memory-limit` knobs are documented in [`tools/README.md`](tools/README.md). This is an out-of-band maintenance operation, not part of the hot path.

See the [sizing calculator](https://posthog.github.io/millpond/sizing-calculator.html) for interactive estimates.

## Error Handling and Retries

The flush path has two failure points, each with its own retry policy:

| Operation | Attempts | Backoff between failures | On exhaustion |
|-----------|----------|--------------------------|---------------|
| Lake write (DuckLake) | 3 | 1s then 2s, each jittered upward by up to 25% (last attempt raises immediately) | Re-raise → pod crashes, K8s restarts, replays from last committed offset |
| Lake write (hoglake) | `HOGLAKE_MAX_RETRY_COUNT`, default 8 | 1s doubling, capped at 30s, jittered upward by up to 25%, floored by the server's `Retry-After` when it sends one | as above |
| Offset commit | 3 | 0.5s, 1s (last attempt raises immediately) | Re-raise → pod crashes, replays from last committed offset (duplicates bounded by one flush batch) |

The two write budgets differ because the backends do: DuckLake retries *internally* (`DUCKLAKE_MAX_RETRY_COUNT`, default 100) underneath millpond's three attempts, while pyhoglake issues one request and raises. Three attempts against a catalog whose backpressure signal is `503` + `Retry-After: 1` — an explicit "the commit queue is convoyed, ask again" — is a crash loop wearing a retry policy's clothes.

The *jitter* is shared by both, deliberately. It exists so a fleet of pods refused by one event does not wake in lockstep and re-form the convoy on every rung, and DuckLake pods contend for the same Postgres catalog commit lock, so they have the same problem. Scoping it to the hoglake path would mean two retry curves to keep in step for a spread of at most 25%: on DuckLake that is 3.75s of backoff across the ladder instead of 3s, well inside the liveness deadline, and it is clamped to the same 30s ceiling as everything else.

The hoglake sink also gets to *veto* a retry: a failure it classifies as permanent (422 validation, 410 expired, an unsupported type) re-raises immediately instead of burning the ladder on a request that cannot become valid by waiting. 408, 429, every 5xx, transport errors, and a commit conflict stay retryable.

Write failures are classified before counting: DuckLake catalog commit contention (retry-budget exhaustion, duplicate-key / serialization errors from Postgres) increments `errors_total{type="ducklake_commit_contention"}` — only on a DuckLake pod, since the match is on generic Postgres wording the hoglake control plane can produce too; hoglake OCC 409s increment `errors_total{type="hoglake_commit_contention"}`; everything else increments `errors_total{type="write_retry"}`. Commit failures increment `errors_total{type="offset_commit"}`. Transient vs persistent failures stay distinguishable in dashboards.

The write-retry loop catches `Exception` broadly to cover the backend's failure modes — `duckdb.Error` for DuckLake; `OSError` for S3; `KafkaException` for broker disconnects. Each retry invokes `sink.reset_caches()` to drop cached table/schema state so the next attempt re-checks the catalog (covers the case where another pod evolved the schema or recreated the table between attempts).

**Why crash after exhausting retries?** A persistent write failure means S3 or the catalog is down — continuing would just accumulate pending data in memory until OOM. A persistent commit failure means the Kafka coordinator is unreachable — the write already succeeded, but without committed offsets the next restart will replay the batch (at-least-once duplicates). In both cases, crashing lets K8s apply its restart backoff, and Kafka holds the data safely until the dependency recovers.

## Multiple Pipelines

Each topic→table mapping is a separate StatefulSet. The application doesn't change — just the env vars. Template with Helm:

```yaml
# values.yaml
pipelines:
  events:
    topic: clickhouse_events_json
    table: events
    partitions: 512
    replicas: 8
  sessions:
    topic: clickhouse_sessions_json
    table: sessions
    partitions: 64
    replicas: 4
  logs:
    topic: app_logs
    table: logs
    partitions: 128
    replicas: 8
```

One `range` over `pipelines` in the StatefulSet template produces N independent StatefulSets. Adding a pipeline is adding a block to `values.yaml` and running `helm upgrade`.

## AWS Credential Isolation

Millpond uses two separate AWS credential paths that must not interfere with each other:

| Component | Auth | Credential source |
|---|---|---|
| Kafka (MSK) | SASL/OAUTHBEARER | IRSA (standard AWS credential chain) |
| S3 (lake data files) | Static IAM keys | `DUCKDB_S3_*` |

The S3 path does not use the standard `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars — those take precedence in the credential chain and would shadow the IRSA role used for Kafka authentication. Do not rename the `DUCKDB_S3_*` env vars to the standard AWS names.

DuckDB's [aws extension does not support IRSA](https://github.com/duckdb/duckdb-aws/issues/31) — it cannot perform the `AssumeRoleWithWebIdentity` token exchange that IRSA requires, hence the static keys.

## Operational Notes

### Periodic MSK IAM auth errors

When using MSK IAM authentication (SASL/OAUTHBEARER), you will see periodic bursts of `connection reset by peer` and `SASL OAUTHBEARER mechanism handshake failed` errors in the logs every ~48 minutes. These are **expected and harmless**.

librdkafka does not re-authenticate on existing connections when the OAUTHBEARER token refreshes ([KIP-255](https://cwiki.apache.org/confluence/display/KAFKA/KIP-255%3A+OAuth+Authentication+via+SASL%2FOAUTHBEARER)). Instead, the MSK broker closes the connection when the old token expires (~15 min lifetime), and librdkafka reconnects with the refreshed token. The ~48 minute interval corresponds to the IRSA projected token refresh (80% of the default 1-hour TTL).

The errors come from librdkafka's internal logger (the `%3|...|FAIL|` lines) and bypass Python's log formatting. They auto-resolve within seconds with no data loss.

Related issues:
- [confluent-kafka-python #1485](https://github.com/confluentinc/confluent-kafka-python/issues/1485) — oauth token not refreshing on existing connections
- [aws-msk-iam-auth #143](https://github.com/aws/aws-msk-iam-auth/issues/143) — re-authentication fails with OAUTHBEARER
- [aws-msk-iam-auth #176](https://github.com/aws/aws-msk-iam-auth/issues/176) — second re-authentication fails with default credentials

## Note
This project should absolutely be called TableFowl, but that would be an [SEO](https://www.confluent.io/product/tableflow/) and linguistic palaver.

---

Photo: Public Domain, [Wikimedia Commons](https://commons.wikimedia.org/w/index.php?curid=695982)
