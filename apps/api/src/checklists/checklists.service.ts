import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';

@Injectable()
export class ChecklistsService {
  constructor(private readonly prisma: PrismaService) {}

  async findAll(userId: string) {
    return this.prisma.checklist.findMany({
      where: { userId },
      orderBy: { created_at: 'desc' },
    });
  }

  async findOne(id: string, userId: string) {
    const c = await this.prisma.checklist.findFirst({
      where: { id, userId },
    });
    if (!c) throw new NotFoundException('Checklist not found');
    return c;
  }

  async create(
    userId: string,
    data: {
      file_name: string;
      data: object;
      pontuacao?: number | null;
      orgao?: string | null;
      objeto?: string | null;
      valor_total?: string | null;
    },
  ) {
    return this.prisma.checklist.create({
      data: {
        userId,
        file_name: data.file_name,
        data: data.data,
        pontuacao: data.pontuacao ?? null,
        orgao: data.orgao ?? null,
        objeto: data.objeto ?? null,
        valor_total: data.valor_total ?? null,
      },
    });
  }

  async delete(id: string, userId: string) {
    const c = await this.prisma.checklist.findFirst({
      where: { id, userId },
    });
    if (!c) throw new NotFoundException('Checklist not found');
    await this.prisma.checklist.delete({ where: { id } });
    return { ok: true };
  }
}
