import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService, DOCUMENT_INGEST_QUEUE } from '../redis/redis.service';
import { writeFile, mkdir } from 'fs/promises';
import { join } from 'path';
import { randomUUID } from 'crypto';

const UPLOAD_DIR = process.env.UPLOAD_DIR ?? join(process.cwd(), 'uploads');

@Injectable()
export class DocumentsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
  ) {}

  async createAndEnqueue(
    userId: string,
    file: { buffer: Buffer; originalname?: string; mimetype: string },
  ): Promise<{ documentId: string; status: string }> {
    await mkdir(UPLOAD_DIR, { recursive: true });
    const documentId = randomUUID();
    const ext = file.originalname?.match(/\.[^.]+$/)?.[0] ?? '';
    const path = join(UPLOAD_DIR, `${documentId}${ext}`);
    await writeFile(path, file.buffer);

    await this.prisma.document.create({
      data: {
        id: documentId,
        userId,
        file_name: file.originalname ?? 'document',
        status: 'pending',
      },
    });

    await this.redis.push(DOCUMENT_INGEST_QUEUE, {
      documentId,
      userId,
      filePath: path,
      fileName: file.originalname,
    });

    return { documentId, status: 'pending' };
  }

  async getStatus(documentId: string, userId: string) {
    const doc = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
    });
    if (!doc) throw new NotFoundException('Document not found');
    return { id: doc.id, status: doc.status, file_name: doc.file_name };
  }
}
