import {
  BadRequestException,
  Controller,
  Get,
  Logger,
  Param,
  Post,
  UploadedFile,
  UseInterceptors,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import * as Auth from '@thallesp/nestjs-better-auth';
import { DocumentsService } from './documents.service';

@Controller('documents')
export class DocumentsController {
  private readonly logger = new Logger(DocumentsController.name);

  constructor(private readonly documentsService: DocumentsService) {}

  @Post('upload')
  @UseInterceptors(FileInterceptor('file'))
  async upload(
    @UploadedFile()
    file: { buffer: Buffer; originalname?: string; mimetype: string },
    @Auth.Session() session: Auth.UserSession,
  ) {
    this.logger.log('Upload request received');
    if (!session?.user?.id) {
      this.logger.warn('Upload rejected: no session or user id');
      throw new BadRequestException('Unauthorized');
    }
    this.logger.log(
      `Upload request from userId=${session.user.id}, filename=${file?.originalname ?? '(none)'}`,
    );
    if (!file) {
      this.logger.warn('Upload rejected: no file provided');
      throw new BadRequestException('File is required');
    }
    const allowed = ['application/pdf', 'text/csv', 'application/vnd.ms-excel'];
    if (
      !allowed.includes(file.mimetype) &&
      !file.originalname?.match(/\.(pdf|csv)$/i)
    ) {
      this.logger.warn(
        `Upload rejected: invalid type mimetype=${file.mimetype} filename=${file.originalname}`,
      );
      throw new BadRequestException('Only PDF and CSV files are allowed');
    }
    this.logger.log(
      `Validation passed, calling createAndEnqueue for userId=${session.user.id}`,
    );
    const result = await this.documentsService.createAndEnqueue(
      session.user.id,
      file,
    );
    this.logger.log(
      `Upload completed: documentId=${result.documentId} status=${result.status}`,
    );
    return result;
  }

  @Get(':id/status')
  async getStatus(
    @Param('id') id: string,
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new BadRequestException('Unauthorized');
    return this.documentsService.getStatus(id, session.user.id);
  }

  @Get(':id/checklist')
  async getChecklist(
    @Param('id') id: string,
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new BadRequestException('Unauthorized');
    return this.documentsService.getChecklistForDocument(id, session.user.id);
  }
}
