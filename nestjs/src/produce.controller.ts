/**
 * HTTP entry point for generating load.
 *
 *   POST /produce?count=N&size=BYTES
 *
 * Produces N messages (each padded to ~BYTES) to the topic and returns
 * { sent, elapsedMs }. Defaults: count=1000, size=200 (matches the benchmark
 * RECORD_SIZE so numbers are comparable).
 */
import { Controller, Post, Query } from '@nestjs/common';
import { ProduceResult, ProducerService } from './producer.service';

@Controller()
export class ProduceController {
  constructor(private readonly producer: ProducerService) {}

  @Post('produce')
  async produce(
    @Query('count') count?: string,
    @Query('size') size?: string,
  ): Promise<ProduceResult> {
    // Parse + clamp the query params to safe, friendly defaults.
    const n = clampInt(count, 1000, 1, 10_000_000);
    const bytes = clampInt(size, 200, 1, 10_000_000);
    return this.producer.produce(n, bytes);
  }
}

function clampInt(
  raw: string | undefined,
  fallback: number,
  min: number,
  max: number,
): number {
  const v = parseInt(raw ?? '', 10);
  if (!Number.isFinite(v)) return fallback;
  return Math.min(max, Math.max(min, v));
}
