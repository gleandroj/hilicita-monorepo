# Hi Licita Monorepo

Monorepo for Hi Licita: document ingestion (PDF/CSV), semantic search, and checklist generation.

## Structure

- **apps/api** – NestJS backend: Better Auth, Prisma (PostgreSQL + pgvector), Redis queue, document upload, checklist CRUD and generate (LLM + JSON Schema).
- **apps/web** – Next.js frontend: upload UI, auth (Better Auth client), history.
- **apps/worker** – Python worker: consumes Redis queue, parses with Unstructured.io, embeds with OpenAI, writes to pgvector; updates document status.

## Prerequisites

- Node.js 18+
- Python 3.10+
- PostgreSQL with [pgvector](https://github.com/pgvector/pgvector) extension
- Redis

## Setup

### 1. Install dependencies

```bash
npm install
cd apps/api && npx prisma generate
```

### 2. Database

Create a Postgres database and enable pgvector:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Copy env examples and set `DATABASE_URL` in `apps/api` and `apps/worker`:

```bash
cp apps/api/.env.example apps/api/.env
cp apps/web/.env.example apps/web/.env
cp apps/worker/.env.example apps/worker/.env
```

Run migrations (from `apps/api`):

```bash
cd apps/api && npx prisma migrate deploy
```

### 3. API (Nest)

```bash
cd apps/api
# Set .env (DATABASE_URL, REDIS_URL, BETTER_AUTH_SECRET, OPENAI_API_KEY, etc.)
npm run dev
```

Runs at http://localhost:4000 by default.

### 4. Worker (Python)

```bash
cd apps/worker
python3 -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
# Set .env (REDIS_URL, DATABASE_URL, UNSTRUCTURED_API_KEY, OPENAI_API_KEY)
python worker.py
```

### 5. Web (Next.js)

```bash
cd apps/web
# Set NEXT_PUBLIC_API_URL=http://localhost:4000 in .env
npm run dev
```

Runs at http://localhost:3000.

## Flow

1. User uploads PDF/CSV in the Next.js app (authenticated via Better Auth).
2. Nest receives the file, stores it, creates a `Document` row (status `pending`), and pushes a job to Redis.
3. Python worker pops the job, parses with Unstructured.io, embeds with OpenAI, writes chunks to `document_chunks` (pgvector), and sets document status to `done`.
4. User (or frontend) calls Nest `POST /checklists/generate` with `documentId` and `fileName`. Nest runs semantic search (pgvector), calls the LLM, validates with JSON Schema, saves the checklist, and returns it.
5. Checklist list/delete: `GET /checklists`, `DELETE /checklists/:id`.

## Commands (from repo root)

- `npm run build` – build all apps
- `npm run dev` – run all apps in dev (Turbo)
- `npm run lint` – lint all apps

## Env summary

| App   | Key                   | Description                          |
|-------|-----------------------|--------------------------------------|
| api   | DATABASE_URL          | PostgreSQL connection string         |
| api   | REDIS_URL             | Redis connection string              |
| api   | BETTER_AUTH_SECRET    | Secret for session signing           |
| api   | BETTER_AUTH_TRUSTED_ORIGINS | Allowed origins (e.g. Next app) |
| api   | OPENAI_API_KEY        | For embeddings and checklist LLM     |
| web   | NEXT_PUBLIC_API_URL   | Nest API base URL                    |
| worker| REDIS_URL             | Same Redis as API                    |
| worker| DATABASE_URL          | Same Postgres as API                 |
| worker| UNSTRUCTURED_API_KEY  | Unstructured.io API key              |
| worker| OPENAI_API_KEY        | For embeddings                       |
