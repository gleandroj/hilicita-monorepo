import {
  Controller,
  Post,
  Get,
  Param,
  UseInterceptors,
  UploadedFile,
  BadRequestException,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import * as Auth from '@thallesp/nestjs-better-auth';
import { DocumentsService } from './documents.service';

@Controller('documents')
export class DocumentsController {
  constructor(private readonly documentsService: DocumentsService) {}

  @Post('upload')
  @UseInterceptors(FileInterceptor('file'))
  async upload(
    @UploadedFile()
    file: { buffer: Buffer; originalname?: string; mimetype: string },
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new BadRequestException('Unauthorized');
    if (!file) throw new BadRequestException('File is required');
    const allowed = ['application/pdf', 'text/csv', 'application/vnd.ms-excel'];
    if (
      !allowed.includes(file.mimetype) &&
      !file.originalname?.match(/\.(pdf|csv)$/i)
    ) {
      throw new BadRequestException('Only PDF and CSV files are allowed');
    }
    return this.documentsService.createAndEnqueue(session.user.id, file);
  }

  @Get(':id/status')
  async getStatus(
    @Param('id') id: string,
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new BadRequestException('Unauthorized');
    return this.documentsService.getStatus(id, session.user.id);
  }
}
