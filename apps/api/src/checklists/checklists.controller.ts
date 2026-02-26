import { Controller, Get, Post, Delete, Body, Param } from '@nestjs/common';
import * as Auth from '@thallesp/nestjs-better-auth';
import { ChecklistsService } from './checklists.service';

@Controller('checklists')
export class ChecklistsController {
  constructor(private readonly checklistsService: ChecklistsService) {}

  @Get()
  async list(@Auth.Session() session: Auth.UserSession) {
    if (!session?.user?.id) return [];
    return this.checklistsService.findAll(session.user.id);
  }

  @Get(':id')
  async getOne(
    @Param('id') id: string,
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new Error('Unauthorized');
    return this.checklistsService.findOne(id, session.user.id);
  }

  @Post()
  async create(
    @Auth.Session() session: Auth.UserSession,
    @Body()
    body: {
      file_name: string;
      data: object;
      pontuacao?: number | null;
      orgao?: string | null;
      objeto?: string | null;
      valor_total?: string | null;
    },
  ) {
    if (!session?.user?.id) throw new Error('Unauthorized');
    return this.checklistsService.create(session.user.id, body);
  }

  @Delete(':id')
  async delete(
    @Param('id') id: string,
    @Auth.Session() session: Auth.UserSession,
  ) {
    if (!session?.user?.id) throw new Error('Unauthorized');
    return this.checklistsService.delete(id, session.user.id);
  }
}
