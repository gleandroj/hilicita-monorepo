import { Injectable, OnModuleDestroy } from '@nestjs/common';
import Redis from 'ioredis';

export const DOCUMENT_INGEST_QUEUE = 'document:ingest';

@Injectable()
export class RedisService implements OnModuleDestroy {
  private readonly client: Redis;

  constructor() {
    const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
    this.client = new Redis(url);
  }

  getClient(): Redis {
    return this.client;
  }

  async push(queue: string, payload: object): Promise<void> {
    await this.client.lpush(queue, JSON.stringify(payload));
  }

  async onModuleDestroy() {
    await this.client.quit();
  }
}
