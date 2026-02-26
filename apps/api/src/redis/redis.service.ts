import { Injectable, OnModuleDestroy, Logger } from '@nestjs/common';
import Redis from 'ioredis';

export const DOCUMENT_INGEST_QUEUE = 'document:ingest';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private readonly client: Redis;

  constructor() {
    const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
    this.logger.log(
      `Connecting to Redis at ${url.replace(/:[^:@]+@/, ':***@')}`,
    );
    this.client = new Redis(url);
  }

  getClient(): Redis {
    return this.client;
  }

  async push(queue: string, payload: object): Promise<void> {
    this.logger.log(
      `push: queue=${queue} payloadKeys=${Object.keys(payload).join(',')}`,
    );
    const body = JSON.stringify(payload);
    await this.client.lpush(queue, body);
    this.logger.log(
      `push: job added to queue=${queue} (documentId=${(payload as { documentId?: string }).documentId ?? 'n/a'})`,
    );
  }

  async onModuleDestroy() {
    await this.client.quit();
  }
}
