#!/usr/bin/env bash
#
# Consumer throughput benchmark.
#
# Runs Kafka's built-in kafka-consumer-perf-test.sh inside the Strimzi broker pod.
# Tune fetch behavior via env vars and compare MB/sec and nMsg/sec.
#
# Example — compare fetch sizing:
#   FETCH_MIN_BYTES=1      ./consumer-perf.sh     # low latency, more requests
#   FETCH_MIN_BYTES=100000 ./consumer-perf.sh     # fatter fetches, more throughput
#
# Produce some data first (see producer-perf.sh).
set -euo pipefail

NS="${KAFKA_NS:-kafka}"
POD="${KAFKA_POD:-my-cluster-dual-role-0}"
BOOTSTRAP="${BOOTSTRAP_SERVERS:-my-cluster-kafka-bootstrap:9092}"
TOPIC="${TOPIC:-workshop.events}"
MESSAGES="${MESSAGES:-1000000}"
GROUP="${GROUP:-perf-consumer}"

# ---- Tuning knobs ----
FETCH_MIN_BYTES="${FETCH_MIN_BYTES:-1}"
FETCH_MAX_WAIT_MS="${FETCH_MAX_WAIT_MS:-500}"
MAX_PARTITION_FETCH_BYTES="${MAX_PARTITION_FETCH_BYTES:-1048576}"

echo ">> consumer-perf | fetch.min.bytes=$FETCH_MIN_BYTES fetch.max.wait.ms=$FETCH_MAX_WAIT_MS max.partition.fetch.bytes=$MAX_PARTITION_FETCH_BYTES"
echo ">> reading up to $MESSAGES msgs from $TOPIC (group=$GROUP) via $BOOTSTRAP"

# Write a consumer config inside the pod, then run the perf test against it.
kubectl exec -n "$NS" "$POD" -- sh -c "
cat > /tmp/perf-consumer.properties <<EOF
bootstrap.servers=$BOOTSTRAP
fetch.min.bytes=$FETCH_MIN_BYTES
fetch.max.wait.ms=$FETCH_MAX_WAIT_MS
max.partition.fetch.bytes=$MAX_PARTITION_FETCH_BYTES
EOF
bin/kafka-consumer-perf-test.sh \
  --bootstrap-server $BOOTSTRAP \
  --topic $TOPIC \
  --messages $MESSAGES \
  --group $GROUP \
  --consumer.config /tmp/perf-consumer.properties \
  --show-detailed-stats
"
