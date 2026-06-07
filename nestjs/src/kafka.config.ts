/**
 * ============================================================================
 *  CENTRAL KAFKA TUNING CONFIG  (the heart of this workshop)
 * ============================================================================
 *
 * Every knob you'll turn in the workshop lives here, read from environment
 * variables with sensible defaults. Change a value in `.env` (or export it),
 * restart, run a `/produce` load, and watch the consumer throughput log move.
 *
 * IMPORTANT: kafkajs (the Node client) names its options DIFFERENTLY from the
 * Java/Spring client and the `kafka-*-perf-test.sh` benchmark tools. Each option
 * below is annotated with the equivalent Kafka config name so you can map what
 * you learn here straight onto the Java app and the benchmark scripts.
 *
 * See: https://kafka.js.org/docs/producing  and  /docs/consuming
 */
import { CompressionTypes } from 'kafkajs';

// --- tiny helpers to parse env vars with defaults --------------------------
const str = (v: string | undefined, d: string) => (v && v.length ? v : d);
const num = (v: string | undefined, d: number) => {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
};
const bool = (v: string | undefined, d: boolean) =>
  v == null ? d : ['1', 'true', 'yes', 'on'].includes(v.toLowerCase());

/**
 * Map the human-friendly compression name from env to a kafkajs enum.
 *
 * NOTE: only GZIP is built into kafkajs out of the box. LZ4, Snappy and ZSTD
 * each require an EXTRA codec package, e.g.:
 *     npm i kafkajs-lz4          # then KafkaJS.CompressionCodecs[CompressionTypes.LZ4] = ...
 *     npm i kafkajs-snappy
 *     npm i @kafkajs/zstd
 * ...and you must register the codec at startup. We default to GZIP so this
 * sample runs with zero native deps. (The Java/perf-test side defaults to lz4.)
 */
function parseCompression(v: string | undefined): CompressionTypes {
  switch (str(v, 'gzip').toLowerCase()) {
    case 'none':
      return CompressionTypes.None;
    case 'gzip':
      return CompressionTypes.GZIP;
    case 'snappy':
      return CompressionTypes.Snappy; // needs kafkajs-snappy codec registered
    case 'lz4':
      return CompressionTypes.LZ4; // needs kafkajs-lz4 codec registered
    case 'zstd':
      return CompressionTypes.ZSTD; // needs @kafkajs/zstd codec registered
    default:
      return CompressionTypes.GZIP;
  }
}

/** Shared conventions — these MUST match the Spring Boot app and benchmarks. */
export const TOPIC = str(process.env.KAFKA_TOPIC, 'workshop.events');

export const buildKafkaConfig = () => ({
  // --- Connection (shared) -------------------------------------------------
  // Comma-separated broker list. Kafka config name: `bootstrap.servers`.
  brokers: str(process.env.KAFKA_BOOTSTRAP_SERVERS, 'localhost:9092')
    .split(',')
    .map((b) => b.trim())
    .filter(Boolean),

  // Client id shows up in broker logs/metrics; handy for telling apps apart.
  clientId: str(process.env.KAFKA_CLIENT_ID, 'nestjs-workshop'),

  topic: TOPIC,

  // =========================================================================
  //  PRODUCER TUNING
  // =========================================================================
  producer: {
    /**
     * acks  ->  Kafka `acks`
     * How many replicas must acknowledge a write before it's "done".
     *   0   = fire-and-forget (fastest, can lose data; kafkajs: acks=0)
     *   1   = leader only (default-ish; lost if leader dies before replication)
     *   -1  = all in-sync replicas ("all"; strongest durability, highest latency)
     * kafkajs uses the NUMBER -1 to mean "all" (the Java client uses the string "all").
     */
    acks: num(process.env.KAFKA_PRODUCER_ACKS, -1),

    /**
     * Batching / linger -> Kafka `linger.ms` (+ `batch.size`)
     * kafkajs has NO direct `linger.ms` knob. Instead, batching is driven by how
     * you send: many small `producer.send()` calls fired close together get
     * grouped, and `producer.sendBatch()` lets you hand it a pre-built batch.
     * To emulate "linger", we collect messages for up to this many ms (and/or up
     * to `lingerMaxMessages`) and flush them in one `sendBatch`. Set to 0 to send
     * each batch immediately (low latency, less batching).
     */
    lingerMs: num(process.env.KAFKA_PRODUCER_LINGER_MS, 5),

    /**
     * Soft cap on how many messages we let accumulate before flushing early,
     * so a huge `/produce?count=...` doesn't buffer everything in memory.
     * Loosely analogous to the effect of Kafka `batch.size` (which is in BYTES).
     */
    lingerMaxMessages: num(process.env.KAFKA_PRODUCER_BATCH_MESSAGES, 5000),

    /**
     * compression  ->  Kafka `compression.type`
     * Trades CPU for smaller network/disk footprint -> usually MORE throughput.
     * Default GZIP (built into kafkajs). See parseCompression() above for the
     * extra packages LZ4 / Snappy / ZSTD require.
     */
    compression: parseCompression(process.env.KAFKA_PRODUCER_COMPRESSION),

    /**
     * idempotent  ->  Kafka `enable.idempotence`
     * Dedupes retried sends so each record is written exactly once (no dupes on
     * retry) and preserves per-partition ordering. When true, kafkajs forces
     * acks=-1 and max.in.flight<=5 for you. Great for the "delivery guarantees"
     * module. Turn off to see at-least-once duplicate behavior on retries.
     */
    idempotent: bool(process.env.KAFKA_PRODUCER_IDEMPOTENT, false),
  },

  // =========================================================================
  //  CONSUMER TUNING
  // =========================================================================
  consumer: {
    /**
     * groupId  ->  Kafka `group.id`
     * The consumer group. Members of the same group SHARE the partitions of a
     * topic (that's how you scale out / parallelize). Parallelism is capped at
     * the number of partitions.
     */
    groupId: str(process.env.KAFKA_GROUP_ID, 'nestjs-workshop'),

    /**
     * minBytes  ->  Kafka `fetch.min.bytes`
     * Broker waits until it has at least this many bytes before answering a
     * fetch. Higher -> fewer, fatter fetches -> more throughput, a bit more
     * latency. (Benchmark env var: FETCH_MIN_BYTES.)
     */
    minBytes: num(process.env.KAFKA_CONSUMER_MIN_BYTES, 1),

    /**
     * maxWaitTimeInMs  ->  Kafka `fetch.max.wait.ms`
     * Caps how long the broker waits to satisfy `minBytes` before responding
     * anyway. This bounds the latency cost of a large `minBytes`.
     * (Benchmark env var: FETCH_MAX_WAIT_MS.)
     */
    maxWaitTimeInMs: num(process.env.KAFKA_CONSUMER_MAX_WAIT_MS, 500),

    /**
     * maxBytesPerPartition  ->  Kafka `max.partition.fetch.bytes`
     * Max bytes returned per partition per fetch. Bigger lets the consumer pull
     * more per round trip. (Benchmark env var: MAX_PARTITION_FETCH_BYTES.)
     */
    maxBytesPerPartition: num(
      process.env.KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION,
      1048576, // 1 MiB
    ),

    /**
     * fromBeginning  ->  ~ Kafka `auto.offset.reset`
     * Only matters when the group has NO committed offset yet.
     *   true  ~ auto.offset.reset=earliest  (replay the whole topic)
     *   false ~ auto.offset.reset=latest    (only brand-new messages)
     * Once offsets are committed, this is ignored and the group resumes where it
     * left off. Tip: change KAFKA_GROUP_ID to get a fresh group and re-read.
     */
    fromBeginning: bool(process.env.KAFKA_CONSUMER_FROM_BEGINNING, true),

    /**
     * AUTO-COMMIT NOTE  ->  Kafka `enable.auto.commit`
     * kafkajs's `consumer.run({ eachMessage })` AUTO-COMMITS offsets for you
     * after the handler resolves (roughly every `autoCommitInterval` ms /
     * `autoCommitThreshold` messages). That gives at-least-once delivery: a
     * crash mid-batch re-delivers uncommitted messages. For at-MOST-once or
     * precise control you'd set `autoCommit: false` in `consumer.run(...)` and
     * call `consumer.commitOffsets(...)` yourself. We keep auto-commit ON here
     * for simplicity; the value below tunes how often it fires.
     */
    autoCommitIntervalMs: num(process.env.KAFKA_CONSUMER_AUTOCOMMIT_MS, 5000),
  },
});

export type KafkaConfig = ReturnType<typeof buildKafkaConfig>;
