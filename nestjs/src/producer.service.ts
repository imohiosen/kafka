/**
 * Producer service.
 *
 * Creates one kafkajs Producer at startup and exposes `produce(count, size)`
 * which the HTTP controller calls. The producer tuning knobs (acks, compression,
 * idempotent, batching) all come from kafka.config.ts.
 */
import {
  Inject,
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { v4 as uuid } from 'uuid';
import { buildKafkaConfig } from './kafka.config';
import { KAFKA_CLIENT } from './kafka-client.provider';

export interface ProduceResult {
  sent: number;
  elapsedMs: number;
}

@Injectable()
export class ProducerService implements OnModuleInit, OnModuleDestroy {
  private readonly log = new Logger(ProducerService.name);
  private readonly cfg = buildKafkaConfig();
  private producer!: Producer;
  private seq = 0; // monotonically increasing sequence across the app's lifetime

  constructor(@Inject(KAFKA_CLIENT) private readonly kafka: Kafka) {}

  async onModuleInit(): Promise<void> {
    // `idempotent` -> Kafka `enable.idempotence` (see kafka.config.ts).
    this.producer = this.kafka.producer({
      idempotent: this.cfg.producer.idempotent,
    });
    await this.producer.connect();
    this.log.log(
      `Producer connected (acks=${this.cfg.producer.acks}, ` +
        `compression=${this.cfg.producer.compression}, ` +
        `idempotent=${this.cfg.producer.idempotent}, ` +
        `lingerMs=${this.cfg.producer.lingerMs})`,
    );
  }

  async onModuleDestroy(): Promise<void> {
    await this.producer?.disconnect();
  }

  /**
   * Produce `count` messages whose payload is padded to ~`size` bytes.
   *
   * To make the producer-side batching/linger knobs observable, we buffer
   * messages and flush them with `sendBatch()` either when the buffer reaches
   * `lingerMaxMessages` or after `lingerMs` of wall-clock time. This emulates
   * the Kafka `linger.ms` + `batch.size` behavior that kafkajs doesn't expose
   * as a single option.
   */
  async produce(count: number, size: number): Promise<ProduceResult> {
    const { acks, compression, lingerMs, lingerMaxMessages } =
      this.cfg.producer;
    const topic = this.cfg.topic;

    const start = Date.now();
    let buffer: { value: string }[] = [];
    let lastFlush = Date.now();
    let sent = 0;

    const flush = async () => {
      if (buffer.length === 0) return;
      const batch = buffer;
      buffer = [];
      // sendBatch groups everything into one produce request to the topic.
      await this.producer.sendBatch({
        acks, // Kafka `acks` (-1 == "all")
        compression, // Kafka `compression.type`
        topicMessages: [{ topic, messages: batch }],
      });
      sent += batch.length;
      lastFlush = Date.now();
    };

    for (let i = 0; i < count; i++) {
      buffer.push({ value: this.buildMessage(size) });

      const full = buffer.length >= lingerMaxMessages;
      const lingered = lingerMs > 0 && Date.now() - lastFlush >= lingerMs;
      // lingerMs === 0 means "don't wait": flush as soon as the buffer is full.
      if (full || lingered) {
        await flush();
      }
    }
    await flush(); // send whatever's left

    const elapsedMs = Date.now() - start;
    this.log.log(`Produced ${sent} msgs (~${size}B each) in ${elapsedMs}ms`);
    return { sent, elapsedMs };
  }

  /** Build one JSON message matching the shared shape used by both apps. */
  private buildMessage(size: number): string {
    const base = {
      id: uuid(), // uuid string
      createdAt: new Date().toISOString(), // ISO-8601
      seq: this.seq++, // number
      payload: '', // padded below to hit ~`size` bytes
    };
    // Pad `payload` so the whole JSON is roughly `size` bytes — gives the
    // compression / fetch-sizing knobs something realistic to chew on.
    const overhead = Buffer.byteLength(JSON.stringify(base), 'utf8');
    const padLen = Math.max(0, size - overhead);
    base.payload = 'x'.repeat(padLen);
    return JSON.stringify(base);
  }
}
