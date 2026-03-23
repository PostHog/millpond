# Millpond — Kafka to DuckLake

A standalone Python app that consumes from a Kafka topic and writes to a DuckLake table. Single thread, single loop, no Kafka Connect.

## Naming

<img src="imgs/500px-Hagley_mill_race.jpeg" alt="A mill pond" width="300" align="right">

> **millpond** (noun): a pond created by damming a stream to produce a head of water for operating a mill.
> — [Merriam-Webster](https://www.merriam-webster.com/dictionary/millpond)

Millpond accumulates a stream of Kafka records until a threshold is reached, then releases them into the [DuckLake](https://github.com/duckdb/ducklake). Like a [mill pond](https://en.wikipedia.org/wiki/Mill_pond) feeding a lake.

## Why

Kafka Connect imposes ~1100 lines of lock management, scheduled executors, and rebalance handling to work around its lack of backpressure and explicit offset control. Millpond replaces all of that with:

```
loop:
  consume() → JSON → Arrow → accumulate
  when buffer full or time elapsed:
    write to DuckLake → commit offsets
```

Single thread, single loop. Kafka is the buffer. Offset commit is explicit (after successful write only). No data loss window.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       └─ Single loop: consume → convert → accumulate → flush → commit
```

- One topic and one table per deployment
- Static partition assignment via pod ordinal — no consumer groups
- If a pod dies, its partitions stop being consumed until K8s restarts it

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
just fmt          # format code
just lint         # lint code
just test         # run unit tests
just ci           # format check + lint + test
```

## Configuration

All configuration via environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | yes | | Kafka broker addresses |
| `KAFKA_TOPIC` | yes | | Topic to consume |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas (must match `spec.replicas`) |
| `DUCKLAKE_TABLE` | yes | | Target DuckLake table name |
| `DUCKLAKE_DATA_PATH` | yes | | S3 path for DuckLake data files |
| `DUCKLAKE_METADATA_URL` | yes | | JDBC URL for DuckLake metadata (Postgres) |
| `DUCKLAKE_CONNECTION` | yes | | DuckDB connection string |
| `FLUSH_SIZE` | no | `104857600` | Flush after this many bytes of accumulated Arrow data (default 100MB) |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `GROUP_ID` | no | `millpond-{topic}-{table}` | Kafka group.id — used for offset storage in `__consumer_offsets` only, no consumer group semantics. Changing this loses committed offsets and triggers full replay. |
| `CONSUME_BATCH_SIZE` | no | `1000` | Max messages per `consume()` call — amortizes Python↔C boundary cost |
| `FETCH_MIN_BYTES` | no | `1048576` | Broker accumulates at least this many bytes before responding (1MB) |
| `FETCH_MAX_WAIT_MS` | no | `500` | Max broker wait when `fetch.min.bytes` not yet satisfied |
| `LOG_LEVEL` | no | `INFO` | Python log level (DEBUG, INFO, WARNING, ERROR) |

## Deployment

```bash
just build        # build Docker image
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/statefulset.yaml
```

Partition count is discovered at startup via `consumer.list_topics()`. Each pod computes its partition assignment from its ordinal:

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

### Updating

Rolling updates are a poor fit — pods with different `REPLICA_COUNT` values cause double-assignment or gaps. Since Kafka is the durable buffer:

1. **Canary**: Deploy one pod with the new version, verify metrics
2. **Graceful shutdown**: Scale to 0 (pods flush and commit)
3. **Full redeploy**: Update image/config, scale back up from committed offsets

Downtime = drain time + startup time (~2-3 min). Kafka buffers trivially.

**Never `kubectl scale` without updating `REPLICA_COUNT`.** Use Helm to manage both atomically.

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

## TODO

### Pre-production
- [ ] Integration tests for write path and schema evolution
- [ ] E2E test via docker-compose with assertions

### Nice to have
- [ ] Add librdkafka consumer metrics (confluent-kafka exposes internal stats via `statistics.interval.ms`)
- [ ] Measure intra-batch schema variability (how often do keys differ across records in a single consume batch?)

## NOTE
This project should absolutely be called TableFowl, but that would be an [SEO](https://www.confluent.io/product/tableflow/) and linguistic palaver.

---

Photo: Public Domain, [Wikimedia Commons](https://commons.wikimedia.org/w/index.php?curid=695982)
