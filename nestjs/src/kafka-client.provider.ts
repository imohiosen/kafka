/**
 * A single shared kafkajs `Kafka` client for the whole app.
 *
 * One Kafka instance can create multiple producers/consumers, so we build it
 * once here and inject it wherever it's needed via the DI token `KAFKA_CLIENT`.
 */
import { Provider } from '@nestjs/common';
import { Kafka, logLevel } from 'kafkajs';
import { buildKafkaConfig } from './kafka.config';

export const KAFKA_CLIENT = 'KAFKA_CLIENT';

export const KafkaClientProvider: Provider = {
  provide: KAFKA_CLIENT,
  useFactory: (): Kafka => {
    const cfg = buildKafkaConfig();
    return new Kafka({
      clientId: cfg.clientId,
      brokers: cfg.brokers, // Kafka `bootstrap.servers`
      // WARN keeps the console readable during the workshop; raise to INFO/DEBUG
      // to peek at kafkajs's internal fetch/produce chatter.
      logLevel: logLevel.WARN,
    });
  },
};
