// reflect-metadata must be imported once, before anything uses decorators.
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  // HTTP port for the /produce endpoint (separate from the Kafka broker port).
  const port = Number(process.env.PORT) || 3000;
  await app.listen(port);

  new Logger('Bootstrap').log(
    `Up on http://localhost:${port}  ->  POST /produce?count=1000&size=200`,
  );
}

bootstrap();
