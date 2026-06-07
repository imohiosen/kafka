# Benchmarks

Load-test scripts for the tuning modules. They wrap Kafka's built-in perf tools and
run them **inside the Strimzi broker pod**, so you don't need Kafka installed locally —
only `kubectl` pointed at the workshop cluster.

## Prerequisites

- The Kafka cluster is running (see the root [readme](../readme.md)) and `kubectl` works.
- Confirm the broker pod name (default assumes `my-cluster-dual-role-0`):

  ```bash
  kubectl get pods -n kafka
  ```

  If it differs, pass it: `KAFKA_POD=<pod-name> ./producer-perf.sh`.

## Usage

```bash
chmod +x *.sh

# Producer throughput/latency
./producer-perf.sh

# Consumer throughput (produce some data first)
./consumer-perf.sh
```

## The tuning loop

Change **one** knob, re-run, and compare the reported numbers. Examples:

```bash
# Durability vs. speed
ACKS=1   ./producer-perf.sh
ACKS=all ./producer-perf.sh

# Batching
LINGER_MS=0  BATCH_SIZE=16384  ./producer-perf.sh
LINGER_MS=20 BATCH_SIZE=262144 ./producer-perf.sh

# Compression
COMPRESSION=none ./producer-perf.sh
COMPRESSION=zstd ./producer-perf.sh

# Consumer fetch sizing
FETCH_MIN_BYTES=1      ./consumer-perf.sh
FETCH_MIN_BYTES=100000 ./consumer-perf.sh
```

## Env vars

| Var | Default | Applies to |
| --- | --- | --- |
| `KAFKA_NS` | `kafka` | both |
| `KAFKA_POD` | `my-cluster-dual-role-0` | both |
| `BOOTSTRAP_SERVERS` | `my-cluster-kafka-bootstrap:9092` | both |
| `TOPIC` | `workshop.events` | both |
| `RECORDS` / `RECORD_SIZE` / `THROUGHPUT` | `1000000` / `200` / `-1` | producer |
| `ACKS` / `LINGER_MS` / `BATCH_SIZE` / `COMPRESSION` | `all` / `5` / `16384` / `lz4` | producer |
| `MESSAGES` / `GROUP` | `1000000` / `perf-consumer` | consumer |
| `FETCH_MIN_BYTES` / `FETCH_MAX_WAIT_MS` / `MAX_PARTITION_FETCH_BYTES` | `1` / `500` / `1048576` | consumer |
