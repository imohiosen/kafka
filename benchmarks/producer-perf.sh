#!/usr/bin/env bash
#
# Producer throughput/latency benchmark.
#
# Runs Kafka's built-in kafka-producer-perf-test.sh inside the Strimzi broker pod
# (so you don't need Kafka installed locally). Change the tuning knobs via env vars
# and compare the reported records/sec and latency percentiles.
#
# Example — compare durability cost:
#   ACKS=1   ./producer-perf.sh
#   ACKS=all ./producer-perf.sh
#
# Example — compare batching:
#   LINGER_MS=0  BATCH_SIZE=16384  ./producer-perf.sh
#   LINGER_MS=20 BATCH_SIZE=262144 ./producer-perf.sh
set -euo pipefail

NS="${KAFKA_NS:-kafka}"
POD="${KAFKA_POD:-my-cluster-dual-role-0}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-my-cluster-kafka-bootstrap:9092}"
TOPIC="${TOPIC:-workshop.events}"

RECORDS="${RECORDS:-1000000}"
RECORD_SIZE="${RECORD_SIZE:-200}"   # bytes per record
THROUGHPUT="${THROUGHPUT:--1}"      # -1 = produce as fast as possible

# ---- Tuning knobs (the point of the exercise) ----
ACKS="${ACKS:-all}"                 # 0 | 1 | all
LINGER_MS="${LINGER_MS:-5}"         # wait this long to fill a batch
BATCH_SIZE="${BATCH_SIZE:-16384}"   # max bytes per partition batch
COMPRESSION="${COMPRESSION:-lz4}"   # none | gzip | snappy | lz4 | zstd

# Set PRINT_METRICS=1 to append --print-metrics and filter the dump down to the
# handful of producer metrics that explain batching/throughput/latency. Used by
# exercises/01-producer-performance-tuning.md. (Run with COMPRESSION=none so the
# reported batch-size-avg reflects real record bytes, not compressed bytes.)
PRINT_METRICS="${PRINT_METRICS:-0}"
METRICS_FILTER='batch-size-avg|bufferpool-wait-ratio|outgoing-byte-rate|request-latency-avg|record-queue-time-avg|records sent|records/sec'

echo ">> producer-perf | acks=$ACKS linger.ms=$LINGER_MS batch.size=$BATCH_SIZE compression=$COMPRESSION"
echo ">> $RECORDS records x ${RECORD_SIZE}B @ throughput=$THROUGHPUT -> $TOPIC via $BOOTSTRAP"

perf_args=(
  --topic "$TOPIC"
  --num-records "$RECORDS"
  --record-size "$RECORD_SIZE"
  --throughput "$THROUGHPUT"
  --producer-props
    bootstrap.servers="$BOOTSTRAP"
    acks="$ACKS"
    linger.ms="$LINGER_MS"
    batch.size="$BATCH_SIZE"
    compression.type="$COMPRESSION"
)

if [[ "$PRINT_METRICS" != "0" && "$PRINT_METRICS" != "false" ]]; then
  perf_args+=(--print-metrics)
  # grep may exit non-zero if nothing matches; don't let that kill the script.
  kubectl exec -n "$NS" "$POD" -- bin/kafka-producer-perf-test.sh "${perf_args[@]}" \
    | grep -E "$METRICS_FILTER" || true
else
  kubectl exec -n "$NS" "$POD" -- bin/kafka-producer-perf-test.sh "${perf_args[@]}"
fi
