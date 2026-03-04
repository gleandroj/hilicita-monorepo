import { betterAuth } from 'better-auth';
import { prismaAdapter } from 'better-auth/adapters/prisma';
import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

const baseURL = process.env.BETTER_AUTH_URL; // In prod set to https://api.hilicita.com (no trailing slash)
if (baseURL) {
  console.log('[Better Auth] baseURL:', baseURL);
} else {
  console.warn(
    '[Better Auth] baseURL not set (BETTER_AUTH_URL); some flows may fail',
  );
}

export const auth = betterAuth({
  database: prismaAdapter(prisma, {
    provider: 'postgresql',
  }),
  basePath: '/api/auth',
  baseURL,
  secret:
    process.env.BETTER_AUTH_SECRET ?? 'development-secret-change-in-production',
  trustedOrigins: (() => {
    const raw =
      process.env.BETTER_AUTH_TRUSTED_ORIGINS?.split(',')
        .map((o) => o.trim().replace(/\/+$/, ''))
        .filter(Boolean) ?? [];
    console.log('[Better Auth] trustedOrigins:', raw.length ? raw : '(none)');
    return raw;
  })(),
  emailAndPassword: {
    enabled: true,
  },
  socialProviders: {
    google: {
      clientId: process.env.GOOGLE_CLIENT_ID as string,
      clientSecret: process.env.GOOGLE_CLIENT_SECRET as string,
    },
  },
});
