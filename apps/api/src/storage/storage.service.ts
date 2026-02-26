import { Injectable, OnModuleInit } from '@nestjs/common';
import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  CreateBucketCommand,
  HeadBucketCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const BUCKET = process.env.MINIO_BUCKET ?? 'documents';
const PRESIGNED_TTL_SEC = 3600; // 1 hour for worker to download

@Injectable()
export class StorageService {
  private readonly client: S3Client | null = null;

  constructor() {
    const endpoint = process.env.MINIO_ENDPOINT;
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
    if (!this.client) return;
    try {
      await this.client.send(new HeadBucketCommand({ Bucket: BUCKET }));
    } catch {
      await this.client.send(new CreateBucketCommand({ Bucket: BUCKET }));
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
    if (!this.client) throw new Error('MinIO is not configured');
    const key = `${userId}/${documentId}${ext}`;
    await this.client.send(
      new PutObjectCommand({
        Bucket: BUCKET,
        Key: key,
        Body: buffer,
      }),
    );
    return key;
  }

  /** Generate a presigned GET URL for the worker to download the file. */
  async getPresignedDownloadUrl(storageKey: string): Promise<string> {
    if (!this.client) throw new Error('MinIO is not configured');
    const cmd = new GetObjectCommand({ Bucket: BUCKET, Key: storageKey });
    return getSignedUrl(this.client, cmd, { expiresIn: PRESIGNED_TTL_SEC });
  }
}
