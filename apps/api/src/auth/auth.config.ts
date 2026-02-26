import { betterAuth } from 'better-auth';
import { prismaAdapter } from 'better-auth/adapters/prisma';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export const auth = betterAuth({
  database: prismaAdapter(prisma, {
    provider: 'postgresql',
  }),
  basePath: '/api/auth',
  secret:
    process.env.BETTER_AUTH_SECRET ?? 'development-secret-change-in-production',
  trustedOrigins: process.env.BETTER_AUTH_TRUSTED_ORIGINS?.split(',') ?? [],
});
