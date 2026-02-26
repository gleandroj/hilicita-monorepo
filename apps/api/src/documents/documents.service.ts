import {
  Injectable,
  BadRequestException,
  NotFoundException,
  Logger,
} from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { RedisService, DOCUMENT_INGEST_QUEUE } from '../redis/redis.service';
import { StorageService } from '../storage/storage.service';
import { randomUUID } from 'crypto';

@Injectable()
export class DocumentsService {
  private readonly logger = new Logger(DocumentsService.name);

  constructor(
    private readonly prisma: PrismaService,
    private readonly redis: RedisService,
    private readonly storage: StorageService,
  ) {}

  async createAndEnqueue(
    userId: string,
    file: { buffer: Buffer; originalname?: string; mimetype: string },
  ): Promise<{ documentId: string; status: string }> {
    this.logger.log(
      `createAndEnqueue started for userId=${userId} fileName=${file.originalname ?? 'document'}`,
    );
    if (!this.storage.isConfigured()) {
      this.logger.error('Storage (MinIO) is not configured');
      throw new BadRequestException(
        'File storage (MinIO) is not configured. Set MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY.',
      );
    }
    const documentId = randomUUID();
    const ext = file.originalname?.match(/\.[^.]+$/)?.[0] ?? '';
    this.logger.log(
      `Generated documentId=${documentId} ext=${ext}, uploading to storage`,
    );
    const storageKey = await this.storage.upload(
      userId,
      documentId,
      file.buffer,
      ext,
    );
    this.logger.log(`Storage upload done, storageKey=${storageKey}`);
    const fileUrl = await this.storage.getPresignedDownloadUrl(storageKey);
    this.logger.log('Presigned download URL generated');

    this.logger.log(
      `Creating document record in DB for documentId=${documentId}`,
    );
    await this.prisma.document.create({
      data: {
        id: documentId,
        userId,
        file_name: file.originalname ?? 'document',
        status: 'pending',
        storageKey,
      },
    });
    this.logger.log('Document record created');

    const jobPayload = {
      documentId,
      userId,
      fileUrl,
      fileName: file.originalname ?? 'document',
    };
    this.logger.log(
      `Pushing job to queue ${DOCUMENT_INGEST_QUEUE} documentId=${documentId}`,
    );
    await this.redis.push(DOCUMENT_INGEST_QUEUE, jobPayload);
    this.logger.log(`Job enqueued successfully for documentId=${documentId}`);

    this.logger.log(
      `createAndEnqueue completed documentId=${documentId} status=pending`,
    );
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
    if (!checklist)
      throw new NotFoundException('Checklist not found for this document');
    return { checklist: checklist.data as object };
  }
}
