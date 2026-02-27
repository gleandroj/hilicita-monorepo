import {
  CreateBucketCommand,
  GetObjectCommand,
  HeadBucketCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { Injectable, Logger } from '@nestjs/common';

const BUCKET = process.env.MINIO_BUCKET ?? 'documents';
const PRESIGNED_TTL_SEC = 3600; // 1 hour for worker to download

@Injectable()
export class StorageService {
  private readonly logger = new Logger(StorageService.name);
  private readonly client: S3Client | null = null;

  constructor() {
    const endpoint =
      process.env.MINIO_ENDPOINT ||
      (process.env.MINIO_HOST && process.env.MINIO_PORT
        ? `http://${process.env.MINIO_HOST}:${process.env.MINIO_PORT}`
        : undefined);
    const accessKey = process.env.MINIO_ACCESS_KEY;
    const secretKey = process.env.MINIO_SECRET_KEY;
    if (endpoint && accessKey && secretKey) {
      this.client = new S3Client({
        endpoint,
        region: process.env.MINIO_REGION ?? 'us-east-1',
        credentials: { accessKeyId: accessKey, secretAccessKey: secretKey },
        forcePathStyle: true,
      });
    }
  }

  async onModuleInit() {
    if (!this.client) {
      this.logger.warn('MinIO not configured, storage disabled');
      return;
    }
    try {
      this.logger.log(`Checking bucket ${BUCKET}`);
      await this.client.send(new HeadBucketCommand({ Bucket: BUCKET }));
      this.logger.log(`Bucket ${BUCKET} exists`);
    } catch {
      this.logger.log(`Creating bucket ${BUCKET}`);
      await this.client.send(new CreateBucketCommand({ Bucket: BUCKET }));
      this.logger.log(`Bucket ${BUCKET} created`);
    }
  }

  isConfigured(): boolean {
    return this.client !== null;
  }

  /** Upload buffer and return object key. Key format: {userId}/{documentId}{ext} */
  async upload(
    userId: string,
    documentId: string,
    buffer: Buffer,
    ext: string,
  ): Promise<string> {
    if (!this.client) {
      this.logger.error('upload called but MinIO is not configured');
      throw new Error('MinIO is not configured');
    }
    const key = `${userId}/${documentId}${ext}`;
    this.logger.log(`upload: key=${key} size=${buffer.length} bytes`);
    await this.client.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
      }),
    );
    this.logger.log(`upload: completed key=${key}`);
    return key;
  }

  /** Generate a presigned GET URL for the worker to download the file. */
  async getPresignedDownloadUrl(storageKey: string): Promise<string> {
    if (!this.client) {
      this.logger.error(
        'getPresignedDownloadUrl called but MinIO is not configured',
      );
      throw new Error('MinIO is not configured');
    }
    this.logger.log(
      `getPresignedDownloadUrl: storageKey=${storageKey} ttl=${PRESIGNED_TTL_SEC}s`,
    );
    const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: storageKey });
    const url = await getSignedUrl(this.client, cmd, {
      expiresIn: PRESIGNED_TTL_SEC,
    });
    this.logger.log('getPresignedDownloadUrl: URL generated');
    return url;
  }
}
