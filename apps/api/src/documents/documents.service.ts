import { Injectable, BadRequestException, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService, DOCUMENT_INGEST_QUEUE } from '../redis/redis.service';
import { StorageService } from '../storage/storage.service';
import { randomUUID } from 'crypto';

@Injectable()
export class DocumentsService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
    private readonly storage: StorageService,
  ) {}

  async createAndEnqueue(
    userId: string,
    file: { buffer: Buffer; originalname?: string; mimetype: string },
  ): Promise<{ documentId: string; status: string }> {
    if (!this.storage.isConfigured()) {
      throw new BadRequestException(
        'File storage (MinIO) is not configured. Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.',
      );
    }
    const documentId = randomUUID();
    const ext = file.originalname?.match(/\.[^.]+$/)?.[0] ?? '';
    const storageKey = await this.storage.upload(
      userId,
      documentId,
      file.buffer,
      ext,
    );
    const fileUrl = await this.storage.getPresignedDownloadUrl(storageKey);

    await this.prisma.document.create({
      data: {
        id: documentId,
        userId,
        file_name: file.originalname ?? 'document',
        status: 'pending',
        storageKey,
      },
    });

    await this.redis.push(DOCUMENT_INGEST_QUEUE, {
      documentId,
      userId,
      fileUrl,
      fileName: file.originalname ?? 'document',
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

  async getChecklistForDocument(documentId: string, userId: string) {
    const doc = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
    });
    if (!doc) throw new NotFoundException('Document not found');
    const checklist = await this.prisma.checklist.findFirst({
      where: { documentId, userId },
    });
    if (!checklist) throw new NotFoundException('Checklist not found for this document');
    return { checklist: checklist.data as object };
  }
}
