# Spring Boot — Kafka tuning workshop app

A minimal Spring Boot (Java 17) producer + consumer for the Kafka tuning workshop.
You drive load with a REST endpoint and watch consumer throughput in the logs while
you flip producer/consumer knobs via environment variables.

It mirrors the sibling NestJS app and shares the same conventions:

- Topic: `workshop.events`
- Consumer group: `spring-workshop`
- Message JSON shape: `{ "id": "<uuid>", "createdAt": "<ISO-8601>", "seq": <long>, "payload": "<string>" }`

---

## Run

```bash
# from this directory (spring-boot/)
./mvnw spring-boot:run
```

The app starts an HTTP server on `:8080` and a Kafka consumer that logs throughput
roughly every 5 seconds.

### Point it at a broker

By default it connects to `localhost:9092` (the plain Docker local-dev path). To use
the workshop Strimzi cluster, set the external bootstrap address:

```bash
KAFKA_BOOTSTRAP_SERVERS=<node-ip>:32094 ./mvnw spring-boot:run
```

---

## Produce some load

`POST /produce?count=N&size=BYTES` sends `N` JSON messages (payload padded to ~`BYTES`)
and returns how long it took.

```bash
# 1000 messages of ~200 bytes (the defaults)
curl -X POST "http://localhost:8080/produce"

# 50k messages of ~512 bytes
curl -X POST "http://localhost:8080/produce?count=50000&size=512"
```

Response:

```json
{ "sent": 50000, "elapsedMs": 1234 }
```

Then watch the consumer log lines like:

```
consumed 12345 msg in 5001 ms = 2469 msg/sec | total 50000
```

---

## Tuning knobs (env vars)

Every knob below is overridable from the environment — no file edits needed. Change
**one** at a time, re-run `/produce`, and compare the throughput log.

```bash
# durability vs. speed
ACKS=1   ./mvnw spring-boot:run
ACKS=all ./mvnw spring-boot:run

# batching
LINGER_MS=0  BATCH_SIZE=16384  ./mvnw spring-boot:run
LINGER_MS=20 BATCH_SIZE=262144 ./mvnw spring-boot:run
```

### Producer

| Env var | Property | Default | What it does |
| --- | --- | --- | --- |
| `ACKS` | `acks` | `all` | Replicas that must ack a write. `0`=fire-and-forget, `1`=leader only, `all`=strongest durability. |
| `LINGER_MS` | `linger.ms` | `5` | Wait this long (ms) to fill a batch before sending. Higher = more throughput, more latency. |
| `BATCH_SIZE` | `batch.size` | `16384` | Max bytes per per-partition batch. Bigger = fatter requests, higher throughput. |
| `COMPRESSION_TYPE` | `compression.type` | `lz4` | `none`/`gzip`/`snappy`/`lz4`/`zstd`. Trades CPU for network/disk. |
| `ENABLE_IDEMPOTENCE` | `enable.idempotence` | `true` | Dedupes retries so each record is written once. Requires `acks=all`. |

### Consumer

| Env var | Property | Default | What it does |
| --- | --- | --- | --- |
| `KAFKA_GROUP_ID` | `group.id` | `spring-workshop` | Consumer group. Members share partitions = parallelism. |
| `FETCH_MIN_BYTES` | `fetch.min.bytes` | `1` | Min bytes the broker accumulates before answering a fetch. Higher = fewer/fatter fetches. |
| `FETCH_MAX_WAIT_MS` | `fetch.max.wait.ms` | `500` | Max wait (ms) to satisfy `fetch.min.bytes`. Caps the latency cost above. |
| `MAX_POLL_RECORDS` | `max.poll.records` | `500` | Max records returned per `poll()`. Lower if per-record processing is slow. |
| `ENABLE_AUTO_COMMIT` | `enable.auto.commit` | `true` | Auto-commit offsets on a timer. Turn off for precise at-least-once control. |
| `AUTO_OFFSET_RESET` | `auto.offset.reset` | `earliest` | Start point with no committed offset. `earliest`=replay, `latest`=only new. |

### Other

| Env var | Default | What it does |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address(es), comma-separated. |
| `KAFKA_TOPIC` | `workshop.events` | Topic to produce to / consume from. |
| `SERVER_PORT` | `8080` | HTTP port for `/produce`. |

All knobs live in [`src/main/resources/application.yml`](src/main/resources/application.yml)
— the comments there are part of the lesson.
