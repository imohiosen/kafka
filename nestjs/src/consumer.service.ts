/**
 * Consumer service.
 *
 * On startup it joins the consumer group, subscribes to the topic, and consumes
 * messages. It doesn't do real work with them — its job is to report THROUGHPUT
 * (messages/second) every ~5s plus a running total, so you can see the effect of
 * the consumer tuning knobs (minBytes, maxWaitTimeInMs, maxBytesPerPartition).
 */
import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Consumer, Kafka } from 'kafkajs';
import { buildKafkaConfig } from './kafka.config';
import { KAFKA_CLIENT } from './kafka-client.provider';

const REPORT_EVERY_MS = 5000;

@Injectable()
export class ConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(ConsumerService.name);
  private readonly cfg = buildKafkaConfig();
  private consumer!: Consumer;

  // Throughput accounting.
  private total = 0; // running total since startup
  private windowCount = 0; // messages seen in the current ~5s window
  private windowStart = Date.now();
  private reporter?: NodeJS.Timeout;

  constructor(@Inject(KAFKA_CLIENT) private readonly kafka: Kafka) {}

  async onModuleInit(): Promise<void> {
    const c = this.cfg.consumer;

    // Consumer-level tuning knobs (mapped to Kafka config names in kafka.config.ts).
    this.consumer = this.kafka.consumer({
      groupId: c.groupId, // Kafka `group.id`
      minBytes: c.minBytes, // Kafka `fetch.min.bytes`
      maxWaitTimeInMs: c.maxWaitTimeInMs, // Kafka `fetch.max.wait.ms`
      maxBytesPerPartition: c.maxBytesPerPartition, // Kafka `max.partition.fetch.bytes`
    });

    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.cfg.topic,
      fromBeginning: c.fromBeginning, // ~ Kafka `auto.offset.reset`
    });

    this.log.log(
      `Consumer joined group "${c.groupId}" on "${this.cfg.topic}" ` +
        `(minBytes=${c.minBytes}, maxWaitTimeInMs=${c.maxWaitTimeInMs}, ` +
        `maxBytesPerPartition=${c.maxBytesPerPartition}, ` +
        `fromBeginning=${c.fromBeginning})`,
    );

    // Start the periodic throughput report.
    this.reporter = setInterval(() => this.report(), REPORT_EVERY_MS);

    // `eachMessage` AUTO-COMMITS offsets after the handler resolves (see the
    // auto-commit note in kafka.config.ts). autoCommitInterval tunes how often.
    await this.consumer.run({
      autoCommitInterval: c.autoCommitIntervalMs, // Kafka `auto.commit.interval.ms`
      eachMessage: async () => {
        // Do "nothing" per message except count it — this is a throughput probe.
        this.total++;
        this.windowCount++;
      },
    });
  }

  async onModuleDestroy(): Promise<void> {
    if (this.reporter) clearInterval(this.reporter);
    await this.consumer?.disconnect();
  }

  /** Log msgs/sec for the last window plus the running total. */
  private report(): void {
    const now = Date.now();
    const elapsedSec = (now - this.windowStart) / 1000;
    if (this.windowCount > 0) {
      const rate = Math.round(this.windowCount / Math.max(elapsedSec, 0.001));
      this.log.log(
        `throughput=${rate} msgs/sec (last ${elapsedSec.toFixed(1)}s) | total=${this.total}`,
      );
    }
    // Reset the window.
    this.windowCount = 0;
    this.windowStart = now;
  }
}
