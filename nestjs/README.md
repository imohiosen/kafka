# NestJS Kafka producer/consumer (tuning workshop)

The TypeScript sample for the [Kafka tuning workshop](../readme.md). It does the
same job as the Spring Boot app so you can compare how tuning concepts map across
stacks.

It uses **kafkajs directly** (not `@nestjs/microservices`) on purpose: the whole
point is to see every producer/consumer tuning option explicitly. All the knobs
live in one place — [`src/kafka.config.ts`](./src/kafka.config.ts) — read from
environment variables, each commented and mapped to its Kafka config name.

What it gives you:

- `POST /produce?count=N&size=BYTES` — produce N messages (~BYTES each) to the
  topic and get back `{ sent, elapsedMs }`.
- A consumer that joins the group on startup and logs **throughput (msgs/sec)**
  every ~5s plus a running total — so you can watch a knob change land.

## Run it

```bash
cd nestjs
npm install
npm run start:dev      # watch mode; `npm run start` for one-shot, `npm run build` to compile
```

The app listens on `http://localhost:3000`. The consumer starts reading
immediately and logs throughput as messages arrive.

### Point it at a broker

Defaults to `localhost:9092`. Override via env (copy `.env.example` to `.env`, or
export inline):

```bash
cp .env.example .env
# edit .env ...

# or one-off:
KAFKA_BOOTSTRAP_SERVERS=my-broker:9092 npm run start:dev
```

If you're running the workshop's Strimzi cluster, port-forward the bootstrap
service first:

```bash
kubectl -n kafka port-forward svc/my-cluster-kafka-bootstrap 9092:9092
```

### Generate load

```bash
# defaults: count=1000, size=200 bytes (matches the benchmark RECORD_SIZE)
curl -X POST "http://localhost:3000/produce"

# custom volume + message size
curl -X POST "http://localhost:3000/produce?count=100000&size=512"
# -> {"sent":100000,"elapsedMs":1234}
```

Then watch the app log lines like:

```
[ConsumerService] throughput=42000 msgs/sec (last 5.0s) | total=100000
```

## The tuning loop

Change **one** knob in `.env`, restart, fire the same `/produce`, and compare the
`elapsedMs` (producer side) and the `throughput=` log (consumer side). To re-read
a topic from the start, bump `KAFKA_GROUP_ID` to a fresh value so there's no
committed offset.

## Tuning knobs (env vars)

kafkajs names options differently from the Java client; the right-hand column is
the equivalent Kafka config name used by the Spring Boot app and the
`kafka-*-perf-test.sh` benchmarks.

### Connection

| Env var | Default | Kafka config | Notes |
| --- | --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `bootstrap.servers` | Comma-separated broker list. |
| `KAFKA_CLIENT_ID` | `nestjs-workshop` | `client.id` | Cosmetic; shows in broker logs/metrics. |
| `KAFKA_TOPIC` | `workshop.events` | (topic) | Shared topic — keep as-is. |
| `PORT` | `3000` | — | HTTP port for `/produce`. |

### Producer

| Env var | Default | Kafka config | What it does |
| --- | --- | --- | --- |
| `KAFKA_PRODUCER_ACKS` | `-1` | `acks` | Replicas that must ack. `-1`=all (durable, slower), `1`=leader, `0`=fire-and-forget. |
| `KAFKA_PRODUCER_LINGER_MS` | `5` | `linger.ms` | App-level batching window before a flush. `0` = send immediately. |
| `KAFKA_PRODUCER_BATCH_MESSAGES` | `5000` | ~ `batch.size` | Flush early once this many messages are buffered. |
| `KAFKA_PRODUCER_COMPRESSION` | `gzip` | `compression.type` | `none\|gzip\|snappy\|lz4\|zstd`. Only **gzip** is built into kafkajs (see below). |
| `KAFKA_PRODUCER_IDEMPOTENT` | `false` | `enable.idempotence` | Dedupes retries (exactly-once write); forces `acks=-1`, `max.in.flight<=5`. |

> **Compression codecs:** kafkajs ships only **GZIP**. To use the others, install
> and register a codec at startup:
> `kafkajs-lz4`, `kafkajs-snappy`, or `@kafkajs/zstd`. We default to gzip so the
> sample runs with zero native dependencies. (The benchmark scripts default to
> `lz4` because the Java client bundles it.)

### Consumer

| Env var | Default | Kafka config | What it does |
| --- | --- | --- | --- |
| `KAFKA_GROUP_ID` | `nestjs-workshop` | `group.id` | Consumer group; members share partitions (parallelism ≤ #partitions). |
| `KAFKA_CONSUMER_MIN_BYTES` | `1` | `fetch.min.bytes` | Broker waits for this much data per fetch. Higher → fatter fetches, more throughput. |
| `KAFKA_CONSUMER_MAX_WAIT_MS` | `500` | `fetch.max.wait.ms` | Caps how long the broker waits to satisfy `minBytes`. |
| `KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION` | `1048576` | `max.partition.fetch.bytes` | Max bytes per partition per fetch (1 MiB). |
| `KAFKA_CONSUMER_FROM_BEGINNING` | `true` | ~ `auto.offset.reset` | `true`≈earliest, `false`≈latest. Only applies with no committed offset. |
| `KAFKA_CONSUMER_AUTOCOMMIT_MS` | `5000` | `auto.commit.interval.ms` | How often offsets auto-commit. |

> **Auto-commit:** kafkajs's `eachMessage` runner auto-commits offsets after each
> handler resolves, giving **at-least-once** delivery (a crash re-delivers
> uncommitted messages). For at-most-once / precise control, switch to
> `autoCommit: false` and call `consumer.commitOffsets(...)` yourself.

## Message shape

Every message is JSON, identical to the Spring Boot app:

```json
{ "id": "uuid", "createdAt": "ISO-8601", "seq": 0, "payload": "xxxx…" }
```

`payload` is padded so the whole record is ~`size` bytes, giving the compression
and fetch-sizing knobs realistic data to work on.
