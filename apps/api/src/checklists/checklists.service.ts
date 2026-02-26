import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../prisma/prisma.service';
import { LlmService } from '../llm/llm.service';
import Ajv from 'ajv';

const SYSTEM_PROMPT = `Você é um especialista em licitações brasileiras. Seu trabalho é extrair dados estruturados de editais de licitação.

Com base no contexto fornecido (chunks do documento), extraia as informações no formato JSON abaixo. Preencha todos os campos possíveis. Se não encontrar uma informação, use string vazia ou false.

Ao final, dê uma pontuação de 0-100 baseada em: valor do contrato, clareza dos requisitos, viabilidade de participação, e prazo disponível. Também forneça uma recomendação curta.

IMPORTANTE: Retorne APENAS o JSON válido, sem markdown, sem backticks, sem texto extra.

Estrutura esperada:
{
  "edital": {
    "orgao": "Nome do órgão licitante",
    "objeto": "Descrição do objeto",
    "dataSessao": "DD/MM/AAAA - HH:MM",
    "portal": "Local ou portal de submissão",
    "valorTotal": "R$ X.XXX,XX",
    "vigencia": "X meses",
    "modalidade": "Tipo de licitação",
    "concessionaria": "Nome e tarifa se aplicável",
    "consumoMensal": "X kWh",
    "consumoAnual": "X kWh"
  },
  "participacao": {
    "permiteConsorcio": false,
    "beneficiosMPE": true
  },
  "prazos": {
    "proposta": "DD/MM/AAAA - HH:MM",
    "esclarecimentos": "DD/MM/AAAA - HH:MM",
    "impugnacao": "DD/MM/AAAA - HH:MM"
  },
  "visitaTecnica": false,
  "documentos": [],
  "pontuacao": 75,
  "recomendacao": "Recomendação breve"
}`;

const TOP_K = 10;

@Injectable()
export class ChecklistsService {
  private readonly ajv = new Ajv();

  constructor(
    private readonly prisma: PrismaService,
    private readonly llm: LlmService,
  ) {}

  async findAll(userId: string) {
    return this.prisma.checklist.findMany({
      where: { userId },
      orderBy: { created_at: 'desc' },
    });
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

  async generate(
    documentId: string,
    userId: string,
    fileName: string,
  ): Promise<{ checklist: object }> {
    const doc = await this.prisma.document.findFirst({
      where: { id: documentId, userId },
    });
    if (!doc) throw new NotFoundException('Document not found');
    if (doc.status !== 'done') {
      throw new NotFoundException(
        'Document is not ready yet. Status: ' + doc.status,
      );
    }

    const queryEmbedding = await this.llm.embed(
      'edital licitação órgão objeto valor prazos documentos pontuação',
    );
    const embeddingStr = `[${queryEmbedding.join(',')}]`;

    const chunks = await this.prisma.$queryRawUnsafe<
      Array<{ content: string }>
    >(
      `SELECT content FROM "DocumentChunk"
       WHERE "documentId" = $1 AND "userId" = $2
       ORDER BY embedding <-> $3::vector
       LIMIT ${TOP_K}`,
      documentId,
      userId,
      embeddingStr,
    );

    const context = chunks.map((c) => c.content).join('\n\n');
    const userContent = `Contexto do documento (${fileName}):\n\n${context}\n\nExtraia o checklist em JSON.`;

    const raw = await this.llm.chat(SYSTEM_PROMPT, userContent);
    let clean = raw.trim();
    if (clean.startsWith('```')) {
      clean = clean.replace(/^```(?:json)?\n?/, '').replace(/\n?```$/, '');
    }
    const checklist = JSON.parse(clean) as object;

    const orgao = (checklist as any).edital?.orgao ?? null;
    const objeto = (checklist as any).edital?.objeto ?? null;
    const valorTotal = (checklist as any).edital?.valorTotal ?? null;
    const pontuacao = (checklist as any).pontuacao ?? null;

    await this.prisma.checklist.create({
      data: {
        userId,
        file_name: fileName,
        data: checklist,
        pontuacao: typeof pontuacao === 'number' ? pontuacao : null,
        orgao: orgao ?? null,
        objeto: objeto ?? null,
        valor_total: valorTotal ?? null,
      },
    });

    return { checklist };
  }
}
