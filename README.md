# DSK2D — Dead Simple Kafka to DuckLake

A standalone Python app that consumes from a Kafka topic and writes to a DuckLake table. Two threads, one blocking queue, no Kafka Connect.

## Why

Kafka Connect imposes ~1100 lines of lock management, scheduled executors, and rebalance handling to work around its lack of backpressure and explicit offset control. DSK2D replaces all of that with:

```
Consumer thread:
  poll() → JSON → Arrow → enqueue (blocks when buffer full)

Writer thread:
  dequeue → accumulate → DuckLake write → commit offsets
```

Backpressure is implicit. Offset commit is explicit (after successful write only). No data loss window.

## Architecture

```
K8s StatefulSet (N replicas)
  └─ Pod (ordinal 0..N-1)
       ├─ Consumer Thread
       ├─ Writer Thread
       └─ BlockingQueue (bounded by BUFFER_MAX_BYTES)
```

- One topic per deployment, one table per deployment
- Static partition assignment via pod ordinal — no consumer groups
- If a pod dies, its partitions stop being consumed until K8s restarts it

## Performance

The hot path is all C/C++: librdkafka → orjson → PyArrow → DuckDB (zero-copy Arrow scan). Python is glue.

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
| `KAFKA_PARTITION_COUNT` | yes | | Total partitions for the topic |
| `REPLICA_COUNT` | yes | | Number of StatefulSet replicas |
| `DUCKLAKE_TABLE` | yes | | Target DuckLake table name |
| `DUCKLAKE_DATA_PATH` | yes | | S3 path for DuckLake data files |
| `DUCKLAKE_METADATA_URL` | yes | | JDBC URL for DuckLake metadata (Postgres) |
| `DUCKLAKE_CONNECTION` | yes | | DuckDB connection string |
| `FLUSH_SIZE` | no | `100000` | Flush after this many bytes of Arrow data |
| `FLUSH_INTERVAL_MS` | no | `60000` | Flush after this many ms |
| `BUFFER_MAX_BYTES` | no | `268435456` | Max Arrow bytes in queue before backpressure (256MB) |
| `MAX_POLL_INTERVAL_MS` | no | `300000` | Kafka max.poll.interval.ms |
| `GROUP_ID` | no | `dsk2d-{topic}-{table}` | Kafka group.id (used for offset storage only) |

## Deployment

```bash
just build        # build Docker image
kubectl apply -f k8s/statefulset.yaml
```

Each pod computes its partition assignment from its ordinal:

```python
my_partitions = [p for p in range(partition_count) if p % replica_count == ordinal]
```

Scaling requires updating both `spec.replicas` and the `REPLICA_COUNT` env var.
