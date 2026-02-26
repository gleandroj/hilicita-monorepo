import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { PrismaModule } from './prisma/prisma.module';
import { RedisModule } from './redis/redis.module';
import { AuthModule } from './auth/auth.module';
import { LlmModule } from './llm/llm.module';
import { DocumentsModule } from './documents/documents.module';
import { ChecklistsModule } from './checklists/checklists.module';

@Module({
  imports: [
    PrismaModule,
    RedisModule,
    AuthModule,
    LlmModule,
    DocumentsModule,
    ChecklistsModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
