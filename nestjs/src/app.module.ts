/**
 * Root module: wires the shared Kafka client, the producer + consumer services,
 * and the HTTP controller together.
 */
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { KafkaClientProvider } from './kafka-client.provider';
import { ProducerService } from './producer.service';
import { ConsumerService } from './consumer.service';
import { ProduceController } from './produce.controller';

@Module({
  imports: [
    // Loads `.env` into process.env so kafka.config.ts can read the knobs.
    ConfigModule.forRoot({ isGlobal: true }),
  ],
  controllers: [ProduceController],
  providers: [KafkaClientProvider, ProducerService, ConsumerService],
})
export class AppModule {}
