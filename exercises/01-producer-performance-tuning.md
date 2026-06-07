# Exercise 1 — Producer Performance Tuning (`linger.ms` & `batch.size`)

Now that you have a high-level feel for Kafka internals, let's hit the ground running with
some **performance tuning**. In this exercise you'll produce events to the workshop topic,
tweak two of the most important producer settings, and watch the impact they have on
**throughput** and **latency**.

We'll do this against the workshop's own **Strimzi** cluster (the one you stood up with
Terraform), driving Kafka's built-in `kafka-producer-perf-test` through the repo's
[`benchmarks/producer-perf.sh`](../benchmarks/producer-perf.sh) wrapper. No Kafka install
on your laptop is needed — the script runs the perf tool *inside the broker pod*.

> **Heads-up on cost.** This cluster runs on a paid Hetzner server provisioned by
> Terraform. When you're done, jump to [Teardown](#8-teardown-stop-paying-for-the-cluster)
> and destroy it so it doesn't quietly accrue cost.

---

## What you'll learn

- What `linger.ms` and `batch.size` actually do, and how they trade latency for throughput.
- How to read producer client metrics (`batch-size-avg`, `outgoing-byte-rate`,
  `request-latency-avg`, …) instead of guessing.
- Why the **defaults can be the right answer** for a given workload — tuning is workload-specific.

---

## 1. Prerequisites — verify your environment

The transcript version of this exercise starts by checking a Confluent Cloud config file.
Our connection lives in the cluster instead, so the equivalent step is to confirm the
**cluster is up and `kubectl` can reach it**.

```bash
# The cluster should report Ready (see the root readme for how to apply it).
kubectl get kafka my-cluster -n kafka

# Find the broker pod. The benchmark script defaults to my-cluster-dual-role-0;
# if yours differs, you'll pass it as KAFKA_POD=<name> below.
kubectl get pods -n kafka
```

If `kubectl` can't reach the cluster, or the pod isn't `Running`, revisit
[Setup in the root readme](../readme.md#setup) before continuing. If everything looks good,
we can start testing the producer.

```bash
cd benchmarks
chmod +x *.sh   # first time only
```

---

## 2. The two knobs

Two of the most important producer settings for throughput and latency are:

| Setting | What it does | Default |
| --- | --- | --- |
| `linger.ms` | The **maximum time** the producer waits to keep adding records to a batch before flushing it. | `0` |
| `batch.size` | The **maximum size (bytes)** a per-partition batch can reach before it's flushed. | ~16 KiB (`16384`) |

A batch is flushed when **either** limit is hit first. With `linger.ms=0`, a batch is
flushed almost as soon as the first record lands — so batches stay tiny. Raising `linger.ms`
gives records more time to accumulate into bigger batches *if there's enough incoming load
to fill them*.

In the next few tests we'll change these two values and watch how throughput and latency move.

> **Throttling.** To make throughput and latency variations easy to observe, we throttle
> the script to **200 records/sec** (`THROUGHPUT=200`) of **1000-byte** records
> (`RECORD_SIZE=1000`), for **3000 records** total (`RECORDS=3000`). We hold these — and
> `acks` — constant across all four runs; only `linger.ms` and `batch.size` change.

> **Why `COMPRESSION=none`?** The script defaults to `lz4`. With compression on,
> `batch-size-avg` reports *compressed* bytes and the per-record math below won't line up.
> Every command in this exercise sets `COMPRESSION=none` so the numbers reflect real record
> bytes. (Compression is its own experiment — try it afterward.)

---

## 3. How to read the metrics

`PRINT_METRICS=1` appends `--print-metrics` to the perf test and filters the dump down to
the handful of producer metrics that explain what's happening:

| Metric | What it tells you |
| --- | --- |
| `batch-size-avg` | Average bytes per flushed batch. Compare against your `batch.size` to see which limit is firing. |
| `bufferpool-wait-ratio` | Fraction of time batches wait on previously-sent requests. **`0` means the brokers are keeping up** — they process requests as fast as you send them. |
| `outgoing-byte-rate` | **Throughput** — bytes/sec leaving the producer. |
| `request-latency-avg` | **Latency** — average broker request round-trip (ms). |
| `record-queue-time-avg` | How long records sit in a batch before it's flushed. |

The exact numbers you see will differ from the samples below — look at the **direction**
each one moves between runs.

---

## 4. The four experiments

Run each command, then jot the four headline numbers (`batch-size-avg`,
`bufferpool-wait-ratio`, `outgoing-byte-rate`, `request-latency-avg`) into the
[results table](#5-record-your-results).

> If your broker pod isn't `my-cluster-dual-role-0`, prefix every command with
> `KAFKA_POD=<your-pod>`.

### Experiment 1 — defaults (baseline)

`linger.ms=0`, `batch.size=16384` — the producer defaults.

```bash
PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=3000 THROUGHPUT=200 \
  LINGER_MS=0 BATCH_SIZE=16384 \
  ./producer-perf.sh
```

**What to expect.** `batch-size-avg` comes out *much* lower than the 16384 limit — roughly
the size of a **single record** (~1000–1200 bytes). That's `linger.ms=0` at work: the batch
flushes almost as soon as the first record is added, so about one 1000-byte record lands in
each batch. `bufferpool-wait-ratio` should be `0` — the brokers keep up easily. These
`outgoing-byte-rate` and `request-latency-avg` values are your **baseline** to beat.

### Experiment 2 — give batches time to fill (`linger.ms=100`)

```bash
PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=3000 THROUGHPUT=200 \
  LINGER_MS=100 BATCH_SIZE=16384 \
  ./producer-perf.sh
```

**What to expect.** `batch-size-avg` jumps up — now it sits **just below `batch.size`
(16384)**, which tells you `batch.size` is the limit triggering the flush. But notice:
**throughput went down and latency went up** versus the baseline. That's the opposite of
what we wanted.

### Experiment 3 — raise the size limit (`batch.size=300000`)

If `batch.size` is the limiter, give it more room.

```bash
PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=3000 THROUGHPUT=200 \
  LINGER_MS=100 BATCH_SIZE=300000 \
  ./producer-perf.sh
```

**What to expect.** `batch-size-avg` rises a bit — but not by much, and nowhere near
300000. That tells you `linger.ms=100` isn't allowing **enough time** for batches to reach
the new size limit. Throughput and latency are still moving the wrong way.

### Experiment 4 — give it even more time (`linger.ms=1500`)

The obvious next move: let batches linger much longer.

```bash
PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=3000 THROUGHPUT=200 \
  LINGER_MS=1500 BATCH_SIZE=300000 \
  ./producer-perf.sh
```

**What to expect.** Now `batch-size-avg` gets close to the `batch.size` limit — the batches
are genuinely filling. And yet **throughput and latency are still worse than the baseline.**

### Conclusion

For an application producing **200 records/sec of 1000 bytes each**, the producer
**defaults** for `linger.ms` and `batch.size` are the best choice. Batching buys you
throughput when there's enough load to *fill* batches; at a light, throttled rate, lingering
just makes records wait — adding latency without a throughput win.

The lesson isn't "these settings are bad" — it's **measure for your actual workload**. We
only checked one record rate and size; a high-volume firehose behaves very differently
(next section).

---

## 5. Record your results

Fill this in as you go:

| Experiment | `linger.ms` | `batch.size` | `batch-size-avg` | `bufferpool-wait-ratio` | `outgoing-byte-rate` | `request-latency-avg` |
| --- | --- | --- | --- | --- | --- | --- |
| 1 — defaults | 0 | 16384 | | | | |
| 2 — linger 100 | 100 | 16384 | | | | |
| 3 — bigger batch | 100 | 300000 | | | | |
| 4 — long linger | 1500 | 300000 | | | | |

### Sample results (real run on the workshop cluster)

Your absolute numbers will differ — look at the **trend**. These are from an actual run on
the single-node Strimzi cluster (`cx23`), `COMPRESSION=none`, throttled to 200 rec/s of
1000-byte records:

| Experiment | `linger.ms` | `batch.size` | `batch-size-avg` | `bufferpool-wait-ratio` | `outgoing-byte-rate` (B/s) | `request-latency-avg` (ms) | end-to-end avg latency (ms) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 — defaults | 0 | 16384 | **1,159** | 0.000 | **75,910** | **1.02** | **2.7** |
| 2 — linger 100 | 100 | 16384 | 10,500 | 0.000 | 68,552 | 5.79 | 47 |
| 3 — bigger batch | 100 | 300000 | 22,164 | 0.000 | 68,275 | 6.14 | 63 |
| 4 — long linger | 1500 | 300000 | 216,641 | 0.000 | 68,151 | 9.36 | 733 |

**What the numbers say** — exactly the lesson:

- **Exp 1 → 2:** raising `linger.ms` to 100 fattened the batch from ~1 record (1,159 B) to
  ~10.5 KB (right up against the 16 KB `batch.size` limit — so *size* is now the trigger).
  But throughput **dropped** (75.9 → 68.6 KB/s) and latency **rose** (1.0 → 5.8 ms; ~2.7 →
  47 ms end-to-end). Wrong direction.
- **Exp 3:** giving `batch.size` more room (300 KB) only nudged the avg batch to ~22 KB —
  nowhere near the limit, because `linger.ms=100` doesn't allow enough time to fill it. Still
  worse.
- **Exp 4:** `linger.ms=1500` finally fills batches (~217 KB, near the 300 KB cap), but
  records now sit in the queue ~1 s (`record-queue-time-avg`) — end-to-end latency blew up to
  **733 ms** and throughput still didn't beat the baseline.
- `bufferpool-wait-ratio` stayed **0.000** throughout: the broker was never the bottleneck —
  the added latency came purely from the producer *waiting to batch*.

**Conclusion confirmed:** for 200 rec/s of 1000-byte records, the **defaults win**. Batching
pays off when there's enough incoming load to fill batches *without waiting*; under a light,
throttled rate, lingering just adds latency for no throughput gain.

---

## 6. Go further — model your own app

The throttle is what made the defaults win. Remove it and the trade-off flips: with records
arriving as fast as possible, larger batches and a little linger usually **raise** throughput.

```bash
# Unthrottled firehose: -1 = produce as fast as possible. Try a bigger run.
PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=1000000 THROUGHPUT=-1 \
  LINGER_MS=0 BATCH_SIZE=16384 \
  ./producer-perf.sh

PRINT_METRICS=1 COMPRESSION=none \
  RECORD_SIZE=1000 RECORDS=1000000 THROUGHPUT=-1 \
  LINGER_MS=20 BATCH_SIZE=262144 \
  ./producer-perf.sh
```

Change `RECORDS` and `RECORD_SIZE` to reflect an app you actually work on, then re-run the
linger/batch sweep. You can also bring the other knobs into play — `ACKS` (durability cost)
and `COMPRESSION` (CPU for network) — one at a time. See
[`benchmarks/README.md`](../benchmarks/README.md) for the full knob list.

---

## 7. App-based variant — the same knobs in real apps

The benchmark uses the raw perf tool, but the workshop apps expose the **exact same producer
knobs** as environment variables. Reproduce the linger effect through them and watch the
`/produce` response time and the consumer's throughput log move.

The producer's `linger.ms` / `batch.size` map across the three clients like this:

| Concept | perf-test / Spring (Java) | NestJS (kafkajs) |
| --- | --- | --- |
| linger | `LINGER_MS` (ms) | `KAFKA_PRODUCER_LINGER_MS` (ms) |
| batch size | `BATCH_SIZE` (**bytes**) | `KAFKA_PRODUCER_BATCH_MESSAGES` (**message count**) |
| compression | `COMPRESSION` / `COMPRESSION_TYPE` | `KAFKA_PRODUCER_COMPRESSION` |
| acks | `ACKS` | `KAFKA_PRODUCER_ACKS` (`-1` = `all`) |

> kafkajs has no byte-based `batch.size`; it batches by **message count**. At ~1000 bytes
> per record, the `BATCH_SIZE=300000` (bytes) experiment is roughly
> `KAFKA_PRODUCER_BATCH_MESSAGES=300`. Don't expect a byte-for-byte match with the Java side.

### Spring Boot

```bash
cd ../spring-boot

# Baseline: linger 0. Point at your broker's external bootstrap (e.g. <node-ip>:32094).
KAFKA_BOOTSTRAP_SERVERS=<broker:port> \
  LINGER_MS=0 BATCH_SIZE=16384 COMPRESSION_TYPE=none \
  ./mvnw spring-boot:run
# in another terminal:
curl -X POST "http://localhost:8080/produce?count=3000&size=1000"
#   -> { "sent": 3000, "elapsedMs": <time> }

# Stop the app, then re-run with linger 100 and compare elapsedMs + the consumer's
# "msg/sec" log line:
KAFKA_BOOTSTRAP_SERVERS=<broker:port> \
  LINGER_MS=100 BATCH_SIZE=16384 COMPRESSION_TYPE=none \
  ./mvnw spring-boot:run
curl -X POST "http://localhost:8080/produce?count=3000&size=1000"
```

`ProduceController` returns `{ sent, elapsedMs }` and logs the send time; `EventConsumer`
logs `... = N msg/sec` every ~5s. Those are your throughput/latency signals here.

### NestJS

```bash
cd ../nestjs
npm install   # first time only

KAFKA_BOOTSTRAP_SERVERS=<broker:port> \
  KAFKA_PRODUCER_LINGER_MS=0 KAFKA_PRODUCER_BATCH_MESSAGES=16 KAFKA_PRODUCER_COMPRESSION=none \
  npm run start:dev
curl -X POST "http://localhost:3000/produce?count=3000&size=1000"
#   -> { "sent": 3000, "elapsedMs": <time> }

# Re-run with more linger / a bigger message batch:
KAFKA_BOOTSTRAP_SERVERS=<broker:port> \
  KAFKA_PRODUCER_LINGER_MS=100 KAFKA_PRODUCER_BATCH_MESSAGES=300 KAFKA_PRODUCER_COMPRESSION=none \
  npm run start:dev
curl -X POST "http://localhost:3000/produce?count=3000&size=1000"
```

Same takeaway as the CLI runs: more linger at a light load adds latency without buying
throughput. The point of running it in both stacks is to see that the *concept* is identical
even though each client names the knobs differently.

---

## 8. Teardown (stop paying for the cluster)

When you're finished, tear the environment down so the Hetzner server doesn't keep accruing
cost. The transcript uses the Confluent CLI; our equivalent is **Terraform** (this repo
manages **all** Hetzner resources through Terraform — never delete them by hand in the
Hetzner console).

```bash
# Optional: remove the Kafka workload first (purely tidy; terraform destroy nukes the box anyway).
kubectl delete -f ../kafka/kafka-cluster.yaml -n kafka

# The real cost saver — destroy the Hetzner server + firewall.
cd ../infra/terraform
terraform destroy

# Sanity check: nothing should be left managed.
terraform state list
```

With the server destroyed, the teardown is complete and you're no longer being billed.

---

## Recap

You changed `linger.ms` and `batch.size`, read the producer metrics, and saw — concretely —
that batching trades latency for throughput, and that the **defaults were the best fit** for
a light, throttled workload. The crucial habit: **change one knob, measure, compare** — and
always tune for *your* record rate and size.

Next up: [the other workshop modules](README.md) — consumer tuning, delivery guarantees, and
comparing the two stacks.
