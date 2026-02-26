import { Injectable } from '@nestjs/common';
import OpenAI from 'openai';

const EMBEDDING_MODEL = 'text-embedding-3-small';
const CHAT_MODEL = 'gpt-4o-mini';

@Injectable()
export class LlmService {
  private readonly openai: OpenAI | null = null;

  constructor() {
    const key = process.env.OPENAI_API_KEY;
    if (key) this.openai = new OpenAI({ apiKey: key });
  }

  async embed(text: string): Promise<number[]> {
    if (!this.openai) throw new Error('OPENAI_API_KEY is not set');
    const res = await this.openai.embeddings.create({
      model: EMBEDDING_MODEL,
      input: text.slice(0, 8000),
    });
    return res.data[0]?.embedding ?? [];
  }

  async chat(systemPrompt: string, userContent: string): Promise<string> {
    if (!this.openai) throw new Error('OPENAI_API_KEY is not set');
    const res = await this.openai.chat.completions.create({
      model: CHAT_MODEL,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userContent },
      ],
    });
    return res.choices[0]?.message?.content ?? '';
  }
}
