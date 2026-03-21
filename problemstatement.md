# Dead Simple Kafka to DuckLake (DSK2D)

## The ideal design

```
Consumer thread:
  loop:
    poll() from Kafka
    convert records to Arrow
    enqueue(batch) — blocks when buffer is full

Writer thread:
  loop:
    batch = queue.take() — blocks when empty
    write batch to DuckLake
    commit offsets for the records in the batch
    signal queue to unblock
```

Two threads, one blocking queue, ~80 lines of core logic. Backpressure is
implicit: when the writer can't keep up, the queue fills, the consumer blocks,
poll() stops, and Kafka waits. No rebalance because the consumer is still
alive — it's just not calling poll() fast enough. `max.poll.interval.ms` is
the only knob.

Offset commit is explicit: only after a successful write. No data loss window.

## Why Kafka Connect can't do this

Kafka is already a queue — it has buffering, backpressure, and offset management built in. Kafka Connect forces the connector to re-implement worse versions of all three because the framework won't let you use them directly.

Connect owns the consumer. The connector is a plugin that implements `SinkTask`:

```java
void start(Map<String, String> config);
void open(Collection<TopicPartition> partitions);
void put(Collection<SinkRecord> records);
void close(Collection<TopicPartition> partitions);
void stop();
```

Connect runs its own poll loop, calls `put()` with records. If `put()` returns
without throwing, Connect considers those records handled and will commit their
offsets on the next `offset.flush.interval.ms` tick.

### No backpressure

There's no way to say "I'm not ready for more records." If you block in `put()`
waiting for a flush, you block the Connect worker thread, which blocks the
consumer poll loop. If `put()` takes longer than `max.poll.interval.ms`, the
consumer is evicted from the group and a rebalance starts.

### No explicit offset control

Connect commits offsets based on what `preCommit()` returns. The default
implementation returns everything `put()` accepted. To defer offset commits
until after a successful write, you'd need to override `preCommit()` with
per-partition offset tracking — reintroducing per-partition state into a
per-table buffering model.

### No consumer configuration

Connect controls consumer settings. Connectors can only influence behavior
through `consumer.override.*` properties, which Strimzi passes through.
You can't set a custom `ConsumerRebalanceListener`, can't control poll
timing, can't use `pause()`/`resume()` for backpressure (Connect calls
these internally for its own flow control).

### Rebalance handling is Connect's problem (and yours)

Connect handles consumer group membership, partition assignment, and
rebalances. The connector sees `open(partitions)` and `close(partitions)`.
During a rebalance:

1. `close(revoked)` is called — flush what you have
2. `open(assigned)` is called — create new state
3. But the scheduler thread may still be flushing from the old assignment
4. And the Connect framework may have already committed offsets for records
   you haven't flushed yet

This is where most of the complexity in DucklakeSinkTask lives.

## What Connect forces you to build

Because Connect doesn't provide a blocking queue with backpressure, the
connector reimplements one:

| Concept | Blocking queue design | Kafka Connect equivalent |
|---------|----------------------|--------------------------|
| Buffer | `BlockingQueue<Batch>` | `TableBuffer` + `tableLock` |
| Backpressure | Queue blocks producer | None — must accept all records in `put()` |
| Drain trigger | Consumer thread blocks on `take()` | Scheduled executor checks thresholds every 1s |
| Write serialization | Single consumer thread | `tableFlushLock` prevents concurrent writes |
| Offset commit | Explicit after successful write | Implicit — Connect commits after `put()` returns |
| Failure handling | Don't commit offsets | Data loss — offsets already committed, fail task via `schedulerError` |
| Backpressure signal | Queue full → producer blocks | `max.poll.interval.ms` → rebalance |

The result: ~1100 lines of lock management, volatile fields, scheduled
executors, threshold checks in four places, scoped rebalance handling, and
a two-lock protocol — to do what a `LinkedBlockingQueue` does in 3 lines.

## DSK2D: what a standalone app looks like

### Architecture

```
┌─────────────────────────────────────────────────┐
│                   K8s Pod                       │
│                                                 │
│  ┌──────────────┐    ┌───────────────────────┐  │
│  │   Consumer    │    │      Writer           │  │
│  │   Thread      │───▶│      Thread           │  │
│  │              │ BB  │                       │  │
│  │  poll()      │ QQ  │  DuckDB INSERT INTO   │  │
│  │  convert     │    │  lake.main.<table>    │  │
│  │  enqueue     │    │  commit offsets       │  │
│  └──────────────┘    └───────────────────────┘  │
│                                                 │
│  Config: topic, table, bootstrap.servers,       │
│          ducklake connection, S3 credentials    │
└─────────────────────────────────────────────────┘
```

### Core loop (consumer thread)

```
consumer = new KafkaConsumer(config)
partitions = computeAssignment(topic, partitionCount, replicaCount, ordinal)
consumer.assign(partitions)  // static assignment, no consumer group

while (!shutdown):
    records = consumer.poll(Duration.ofMillis(100))
    if records.isEmpty(): continue

    batch = convertToArrow(records)

    // Backpressure: blocks when buffer exceeds threshold
    queue.put(batch)  // LinkedBlockingQueue with capacity limit
```

### Core loop (writer thread)

```
while (!shutdown):
    batch = queue.take()  // blocks until data available

    // Accumulate batches until flush threshold met
    pending.add(batch)
    if !shouldFlush(pending): continue

    // Write to DuckLake
    consolidated = consolidate(pending)
    duckdb.execute("INSERT INTO lake.main.{table} SELECT * FROM arrow_scan(?)", consolidated)

    // Only commit after successful write
    consumer.commit(offsetsFor(pending), asynchronous=False)
    pending.clear()
```

### What you get

| Property | Kafka Connect | DSK2D |
|----------|--------------|-------|
| Backpressure | None (rebalance on timeout) | Blocking queue |
| Offset safety | Data loss window (put → commit gap) | Commit after write |
| Code complexity | ~1100 lines + framework | ~200 lines |
| Deployment | Strimzi CRD, Connect worker cluster | Single K8s Deployment |
| Scaling | tasksMax + consumer group | replicas + static partition assignment |
| Rebalance handling | Connect framework (complex) | None — static assignment via pod ordinal |
| Monitoring | JMX → JMX Exporter → Prometheus | Direct Prometheus client |
| Schema evolution | Managed in connector | Same DuckLake DDL |
| Multi-table | topic2table.map config | One deployment per table (simple) or router |

### Scaling model

Each pod runs one consumer with statically assigned partitions. K8s manages
pod lifecycle, partition assignment is computed from pod ordinal. No consumer
groups, no rebalances, no Connect worker cluster, no Strimzi operator,
no KafkaConnect CRD, no connector plugins.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dsk2d-events
spec:
  replicas: 8  # 8 pods, partitions distributed by ordinal
  template:
    spec:
      containers:
      - name: dsk2d
        env:
        - name: KAFKA_BOOTSTRAP_SERVERS
          value: "broker:9094"
        - name: KAFKA_TOPIC
          value: "clickhouse_events_json"
        - name: REPLICA_COUNT
          value: "8"
        - name: DUCKLAKE_CONNECTION
          value: "jdbc:duckdb:"
        - name: DUCKLAKE_TABLE
          value: "events"
        - name: DUCKLAKE_DATA_PATH
          value: "s3://posthog-ducklake-prod-us/data"
        - name: DUCKLAKE_METADATA_URL
          value: "jdbc:postgresql://rds-host/ducklake"
        - name: FLUSH_SIZE
          value: "104857600"  # 100MB
        - name: FLUSH_INTERVAL_MS
          value: "60000"
        - name: BUFFER_MAX_BYTES
          value: "268435456"  # 256MB — queue blocks above this
```

### What you lose

- **Connector ecosystem**: No other connectors in the same worker. Each
  table needs its own deployment (or a multi-table router, but that adds
  complexity back).
- **Strimzi management**: No declarative connector lifecycle, no auto-restart
  CRD. Need your own health checks and restart policy (K8s handles this
  natively).
- **Connect's offset storage**: Offsets stored in Kafka's `__consumer_offsets`
  directly, not in Connect's internal topics. This is actually simpler.
- **Schema registry integration**: If you need Avro/Protobuf deserialization
  with schema registry, you handle it yourself. For schemaless JSON (which
  is what PostHog uses), irrelevant.

### Why this matters for the DuckLake contention problem

The core issue — 512 partitions producing 512 separate DuckLake writes — is
a Kafka Connect artifact. Connect assigns partitions to tasks, tasks buffer
per-partition, and the connector has to reassemble per-table writes from
per-partition buffers.

With a standalone consumer, you control the entire pipeline:
- Poll records from all assigned partitions
- Convert to Arrow in one batch
- Write to DuckLake once
- Commit offsets

No per-partition buffering, no consolidation step, no two-lock protocol.
The DuckLake contention problem doesn't exist because there's only ever
one write per consumer per flush cycle, by construction.
