# Lakekeeper concurrent-commit characterisation

Empirical measurements of Lakekeeper's behaviour under N concurrent Iceberg
table commits, with cross-reference to production load shapes and the
structural floors imposed by the Iceberg spec.

## TL;DR — expected production behaviour

Projecting today's `IcebergSink.write()` path against Lakekeeper, using observed
per-pod cadence (~11 s/flush, ~0.09 commits/s/pod) and measured catalog latency
(p50 ~100 ms, p95 ~400 ms):

| N pods writing one table | Retry-exhaustion rate (3 attempts, p95-anchored) | Operational characterisation |
|------:|----:|---|
|   2 |  ~0.03% | invisible |
|   8 |  ~1.6%  | minor noise |
|  16 |  ~8.5%  | edge of viability |
|  **32**  |  **~33%**   | **~1 in 3 flushes ends in pod restart + Kafka offset replay + downstream duplicates** |
|  64 | ~73%   | pods restart more often than they ingest |
| 128 | ~97% + catalog saturates at the structural ~5–10 commits/s/table ceiling |  |

The cliff sits at **N ≈ 16 (p95) to N ≈ 64 (p50)**. Current production load is over the p95 cliff. Using p50 instead moves the cliff right by ~4× but doesn't change the curve shape.

Two structural facts that bound any version of this:

- **Catalog choice doesn't change the shape.** The same optimistic-concurrency model holds across Lakekeeper, tabulario/iceberg-rest, Glue, and any current Iceberg REST catalog. Switching implementations changes the latency constants, not the 1/N contention curve we measured.
- **Three S3 PUTs per commit are irreducible.** Each Iceberg commit writes a new manifest, manifest list, and metadata.json. This bounds *any* Iceberg catalog at roughly 5–10 commits/s/table independent of vendor, language, or hardware tier.

Rest of the document derives these numbers and details the structural facts.

## What was measured

- Catalog: Lakekeeper `quay.io/lakekeeper/catalog:latest-main`, Postgres 17 backend, MinIO storage
- Topology: catalog, Postgres, MinIO, and N writer processes all on the same docker bridge network (matches production deployment shape)
- Workload: 100 rows × 50 batches per writer against a single shared Iceberg table, fresh table per writer-count run
- Retries: **disabled** in the driver — we characterise raw catalog behaviour, not what Millpond's `_write_with_retry` would absorb
- Driver process model: `multiprocessing.spawn`, one `IcebergSink` per writer process (separate pyiceberg catalog clients)
- Test harness: `tests/stress/test_lakekeeper_concurrent.py` (host) + `tests/stress/stress_driver.py` (in-network driver). Reproduce with `just stress-lakekeeper`.

## Raw measurements

| N writers | success | conflict | error | p50 ms | p95 ms | p99 ms |
|----------:|--------:|---------:|------:|-------:|-------:|-------:|
|  2 |  53.0% |  47.0% | 0 |  82 | 194 | 302 |
|  4 |  26.5% |  73.5% | 0 | 103 | 229 | 336 |
|  8 |  13.0% |  87.0% | 0 | 114 | 187 | 264 |
| 16 |   6.6% |  93.4% | 0 |  98 | 138 | 162 |
| 32 |   4.6% |  95.4% | 0 | 190 | 407 | 539 |

Per-attempt success rate scales as ~1/N — the signature of naive optimistic concurrency under uniform-arrival concurrent commits. The p50 commit latency holds around 100ms; p95 climbs into the 200–400ms band under contention. Zero non-conflict errors at every N — the stack is healthy, the contention is the only thing failing requests.

### Two regimes (worst case vs production cadence)

The stress test is deliberately a **worst-case characterisation**: writers fire commits as fast as possible, with no flush-cadence pacing. In that regime, per-pod commit rate is bounded by `1 / commit_latency` ≈ 10 commits/sec; fleet rate is N × 10/s; collisions are near-certain at any meaningful N. The 53% success at N=2 isn't a production prediction — it's the answer to "if both writers commit constantly, how often does one win?"

Production behaviour is set by **flush cadence**, not by catalog speed. A pod can't commit faster than it can fill its `FLUSH_SIZE` buffer or reach `FLUSH_INTERVAL_MS`. So the collision math projects very differently when the input rate is rate-limited by data inflow:

- Stress test: `rate ≈ 1 / commit_latency` → `collision ≈ 1 - exp(-N × ~10)` ≈ "always"
- Production: `rate ≈ N / per_pod_flush_cadence` → `collision ≈ 1 - exp(-N × commit_latency / cadence)`

Both numbers are valid; they answer different questions. The test tells you the *shape* of the contention curve; the production parameters tell you where you sit on that curve.

## Catalog concurrency model

Inspection of Lakekeeper's commit handler — `crates/lakekeeper/src/implementations/postgres/tabular/table/commit.rs` — shows the catalog uses Postgres-level optimistic concurrency:

```sql
UPDATE tabular
   SET metadata_location = $new
 WHERE warehouse_id = $w AND tabular_id = $t
   AND metadata_location IS NOT DISTINCT FROM $old
```

If another writer has advanced `metadata_location` between when this commit loaded the parent snapshot and when it issued the UPDATE, the UPDATE matches zero rows. Lakekeeper detects this in `verify_commit_completeness` and returns HTTP 409 with type `ConcurrentUpdateError`.

This is the same algorithmic contract as `tabulario/iceberg-rest`. The implementations differ in detection mechanism (Postgres `UPDATE … WHERE` vs in-JVM snapshot-id check) and in the latency constants, not in concurrency semantics. There is no server-side queueing of non-conflicting appends in current Lakekeeper.

## Structural floor on commit latency

Per the Iceberg spec, every commit to a table produces:

1. A new **manifest file** (entries for the data files being added in this commit)
2. A new **manifest list** (references the new manifest + the previous snapshot's manifests)
3. A new **metadata.json** (snapshot history + current snapshot pointer + schemas + partition specs)

The catalog must write all three to S3 before flipping the metadata pointer. That's **three S3 PUTs per commit** as an irreducible cost under fast-append (see below) — true of Lakekeeper, true of tabulario, true of Glue, true of any future Iceberg catalog. At typical S3 small-object PUT latency (50–150ms each), the floor on commit latency is roughly 150–450ms, with parallelism potentially overlapping some.

The implied ceiling on sustained single-table commit rate is therefore **~5–10 commits/sec/table**, against any Iceberg catalog implementation. Above that, requests queue server-side or contend client-side.

### Fast-append vs merge-append

PyIceberg's `Table.append()` resolves to a fast-append snapshot producer by default (`pyiceberg/table/__init__.py:441`; `MANIFEST_MERGE_ENABLED_DEFAULT = False`). The current `IcebergSink.write()` path inherits this without setting any table property, so **every commit is a fast-append**.

The two variants differ in what gets read/written per commit:

| Step | Fast-append (current) | Merge-append |
|---|---|---|
| Reads existing manifests at commit time | none (referenced by URL only) | reads merge-candidates ≤ `commit.manifest.min-count-to-merge` (default 100) |
| Manifest writes per commit | 1 (the new one) | 1 + any merged outputs |
| Manifest list writes per commit | 1 | 1 |
| metadata.json writes per commit | 1 | 1 |
| Manifest count growth per commit | +1 | 0 or net negative |

Fast-append minimises per-commit S3 work; merge-append is more expensive at write time but keeps manifest count bounded. Java Iceberg defaults to merge-append; PyIceberg defaults to fast-append.

The trade-off shows up read-side: the manifest list grows by one entry per commit, and readers planning a scan must list/open all of them. This is what makes `Table.rewrite_manifests()` compaction operationally important for any deployment that commits frequently — the more so on fast-append.

### Metadata size growth

Metadata file sizes — and therefore the floor latency — grow over time as the table accumulates snapshot history and manifests-since-last-compaction. PyIceberg exposes the maintenance primitives:

- `Table.expire_snapshots()` — drop old snapshot entries from metadata.json
- `Table.rewrite_manifests()` — compact many small manifests into fewer large ones
- `Table.delete_orphan_files()` — sweep parquet files no live snapshot references

The DuckLake analog is `tools/ducklake_maintenance.py`; no Iceberg counterpart exists in this repo today.

## Production-scale interpretation

Representative observed load for one Millpond pod on a large topic, aggregated over ~19,000 flushes:

| Per-pod metric | Value |
|---|---|
| Mean flush cadence (`elapsed` per flush) | ~11 s |
| Min / max flush cadence | 1.9 s / 37.1 s |
| Mean write phase (`write`, S3 PUT bundled with current catalog commit) | ~3.3 s |
| Records per flush | ~29 k |
| Bytes per flush | ~256 MB |
| Per-pod commit rate | ~0.09 commits/s |
| Per-pod data throughput | ~23 MB/s |

Per-pod flush is S3-bound (the parquet PUT dominates the `write` phase), not catalog-bound. Doubling the pod count would roughly halve buffer-fill time per pod → fleet commit rate stays in the same band. Multiple Millpond deployments share one catalog → catalog-side contention compounds across tables.

Projecting onto the contention curve. Fleet rate per table = N × 0.09/s, against the measured p95 commit latency of 400 ms; collision probability per attempt = `1 - exp(-rate × latency)`; post-retry exhaustion probability under the current 3-attempt deterministic-backoff budget approximated as the third power of the per-attempt rate (true with independent attempts; underestimates correlated retries):

| N pods | Fleet rate (×/s) | rate × p95 latency | Per-attempt collision | After 3 retries |
|------:|------:|------:|------:|------:|
|   2 | 0.18 | 0.07 |  ~7% |  ~0.03% |
|   4 | 0.36 | 0.14 | ~13% |  ~0.3% |
|   8 | 0.73 | 0.29 | ~25% |  ~1.6% |
|  16 | 1.45 | 0.58 | ~44% |  ~8.5% |
|  32 | 2.91 | 1.16 | ~69% |  ~33% |
|  64 | 5.81 | 2.32 | ~90% |  ~73% |
| 128 | 11.6 | 4.65 | ~99% |  ~97% |

The scaling cliff is around N ≈ 16: below that, retry headroom absorbs contention; above that, retry exhaustion starts forcing pod restarts (re-processing Kafka offsets → downstream duplicates) at a rate that scales rapidly with N.

Using p50 (100 ms) instead of p95 in the same projection moves the cliff right by roughly a factor of 4 — N ≈ 64 instead of N ≈ 16 — but the curve shape is the same.

The implied **single-table commit-rate ceiling of ~5–10/s** from the structural floor section means that as N grows beyond ~50, the catalog itself becomes the bottleneck and the per-pod commit cadence stretches regardless of how much data is buffered.

## Iceberg vs DuckLake atomicity

The current `IcebergSink.write()` calls PyIceberg's `Table.append(batch)`, which is a two-step operation:

1. PyArrow writes the parquet file to S3 (under the table's location)
2. PyIceberg issues a `POST` to the catalog committing a new snapshot that references the file by URL

The catalog never copies or scans the parquet bytes — it stores the URL and per-file statistics extracted from the parquet footer. This is **register, not materialize**.

| Phase | DuckLake | Iceberg `append()` | Iceberg `add_files()` |
|---|---|---|---|
| Data lands on S3 | inside Postgres transaction | step 1 (PyArrow) | step 1 (PyArrow), can be batched independently |
| Catalog records it | same transaction | step 2 (POST) | step 2 (POST) |
| Orphan possible between phases? | no | yes (one parquet) | yes (K parquet) |
| Kafka offset committed? | only after INSERT returns | only after `append()` returns | only after `add_files()` returns |
| Cleanup tool | `tools/ducklake_maintenance.py` | none in repo (PyIceberg `delete_orphan_files()`) | same |

End-to-end progress semantics — Kafka offsets advance only after the lake-side write returns success — are preserved in all three modes. What changes between DuckLake and Iceberg is the possibility of S3 files that exist but were never registered (from retries on commit conflict, or from a pod crash between PyArrow's PUT and the catalog POST). Today's `_write_with_retry` already produces one orphan per failed attempt, since each `Table.append()` writes a fresh parquet (new UUID) before issuing the commit.

## `add_files` batching

PyIceberg's `Table.add_files(file_paths=[...])` registers pre-existing S3 parquet files with the table without re-writing them. Implementation (`pyiceberg/io/pyarrow.py:parquet_file_to_data_file`):

```
add_files([paths]) →
  for path in paths:
    pq.read_metadata(stream)   ← reads parquet footer only (~tens of KB)
    extract stats from footer  ← row count, col min/max, null counts
    build DataFile (in-memory)
  commit ONE snapshot containing N DataFiles
```

No data scan — the footer already carries the statistics Iceberg's manifest entries need. Per-file cost is one S3 GET-Range on the footer, a few ms.

Implication for the rate math: a pod that buffers K parquet files locally on S3 before issuing a single `add_files()` call pays one commit per K files. The fleet commit rate drops by K× at the same data throughput.

| K (files per commit) | Per-pod commit cadence at current data rate | Fleet rate (32 pods) | Per-attempt collision at p50 100ms | Per-attempt collision at p95 400ms |
|---:|---:|---:|---:|---:|
|  1 (today) |  13s | 2.46/s | ~22% | ~63% |
|  5 |  65s | 0.49/s |  ~5% | ~18% |
| 10 | 130s | 0.25/s | ~2.5% |  ~9% |
| 20 | 260s | 0.12/s | ~1.2% |  ~5% |

`check_duplicate_files=False` should be passed when the writer knows the files are fresh — the default triggers a `data_files` scan over the live snapshot to verify the paths aren't already referenced, which scales with table size.

Partition discovery in `add_files` derives each file's partition tuple from the column min/max in the parquet footer. This only works cleanly if a parquet file's rows are **single-partition** (all rows share the same `year/month/day/hour`). Today's `IcebergSink._add_metadata_columns` stamps a single `_inserted_at` per batch, so per-flush files are single-partition by construction; the invariant must be preserved if files are batched.

## Levers

| Lever | Effect on commit rate | Effect on latency floor | Atomicity | Cost |
|---|---|---|---|---|
| Larger `FLUSH_SIZE` / `FLUSH_INTERVAL_MS` | Linear reduction in commit rate | None | Preserved | Larger buffer memory; longer visibility lag |
| `add_files(K files per commit)` | K× reduction in commit rate | None | Preserved (Kafka offsets gated on `add_files` return) | K orphans per failed commit instead of 1; deferred visibility |
| Jittered retry backoff in `_write_with_retry` | None | None | Preserved | Reduces retry-storm probability under contention |
| `expire_snapshots` + `rewrite_manifests` cron | None | Keeps floor near ~2 S3 PUTs minimum | Preserved | New maintenance job to deploy/monitor |
| Upstream Lakekeeper patch: server-side merge of non-conflicting appends | None (still 5–10/s/table ceiling) | None (still two S3 PUTs) | Preserved | Open-source contribution work; outside this repo |
| Table sharding (per-ordinal or per-namespace) | Multiplies ceiling by shard count | None | Preserved per shard | Readers must UNION or use a logical view |
| Single-committer fanout (separate service consumes file-ready notifications) | Eliminates client-side contention | None | Kafka offset commit decouples from catalog commit — visibility lag, separate operational signal | New service; introduces inter-component coordination |
| Catalog switch to one with native server-side commit queuing | Depends on impl | Depends on impl | Preserved | Operational lift |

## Code changes already on this branch

To make Millpond work with Lakekeeper at all (independent of any architectural decision), `millpond/iceberg.py` was changed to pin PyIceberg's FileIO:

```python
"py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
```

Without this, Lakekeeper's storage-profile response routes PyIceberg through `FsspecFileIO`, which depends on the `s3fs` package (not currently a Millpond dependency). The pin forces PyArrow's S3 client, which already ships with PyIceberg's `[pyarrow]` extra.

The warehouse config used in the stress compose disables `remote-signing-enabled` for the same reason: Lakekeeper's default S3V4 remote-signer triggers the fsspec routing. Production warehouses created via the management API will need the same setting unless `s3fs` is added as a runtime dep.

## Caveats on the measurements

1. **Local laptop.** Lakekeeper, Postgres, MinIO, and all writers share one machine's CPU and a docker bridge network. Production likely sees lower commit latency from a dedicated Postgres, possibly higher from cross-AZ networking. Net direction unknown until measured in a target environment.
2. **Synthetic arrival pattern.** Writers commit as fast as possible (no flush-interval pacing). Real Millpond's `FLUSH_SIZE` / `FLUSH_INTERVAL_MS` produce a more bursty steady state; the 1/N law from this test is an upper bound on contention probability under uniform arrival.
3. **No schema evolution.** All workers share one fixed schema. Concurrent schema changes are a separate commit class with their own contention story; not characterised here.
4. **Single table.** Catalog-side contention across multiple concurrently-written tables (shared Postgres, shared S3 metadata-write bandwidth) is not captured by these runs.
5. **One Lakekeeper version.** `quay.io/lakekeeper/catalog:latest-main` at the commit pinned during this work. Upstream changes to commit handling would invalidate the latency constants.

## Reproducibility

```
just stress-lakekeeper                              # runs the full N=2…32 sweep
cat tests/stress/summary.json                       # machine-readable last-run results
docker compose -f tests/stress/compose.lakekeeper.yaml --profile driver down -v
```

Adjust `STRESS_WRITER_COUNTS`, `STRESS_COMMITS_PER_WRITER`, `STRESS_ROWS_PER_COMMIT` in `tests/stress/compose.lakekeeper.yaml` to extend the sweep.
