#!/usr/bin/env python3
"""
Document ingest worker: consumes Redis queue, parses with Unstructured.io,
generates embeddings, stores in pgvector. Updates document status in Postgres.
"""
import os
import json
import uuid
import logging
from pathlib import Path

import redis
from unstructured_client import UnstructuredClient
from unstructured_client.models import shared
from openai import OpenAI
import psycopg2
from psycopg2.extras import execute_values
import pgvector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "document:ingest"
DATABASE_URL = os.environ.get("DATABASE_URL")
UNSTRUCTURED_API_KEY = os.environ.get("UNSTRUCTURED_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def update_document_status(conn, document_id: str, status: str):
    with conn.cursor() as cur:
        cur.execute(
            'UPDATE "Document" SET status = %s WHERE id = %s',
            (status, document_id),
        )
    conn.commit()


def insert_chunks(conn, document_id: str, user_id: str, chunks: list[tuple[str, list[float]]]):
    with conn.cursor() as cur:
        for content, embedding in chunks:
            chunk_id = str(uuid.uuid4())
            embedding_str = "[" + ",".join(str(x) for x in embedding) + "]"
            cur.execute(
                """
                INSERT INTO "DocumentChunk" (id, "documentId", "userId", content, embedding)
                VALUES (%s, %s, %s, %s, %s::vector)
                """,
                (chunk_id, document_id, user_id, content, embedding_str),
            )
    conn.commit()


def parse_file(file_path: str, file_name: str) -> list[str]:
    """Parse PDF or CSV with Unstructured.io; return list of text chunks."""
    if not UNSTRUCTURED_API_KEY:
        raise RuntimeError("UNSTRUCTURED_API_KEY is not set")
    client = UnstructuredClient(api_key_auth=UNSTRUCTURED_API_KEY)
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(file_path)
    with open(file_path, "rb") as f:
        content = f.read()
    req = shared.PartitionParameters(
        files=shared.Files(content=content, file_name=file_name or path.name),
    )
    resp = client.general.partition(request=req)
    chunks = []
    if getattr(resp, "elements", None):
        for el in resp.elements:
            text = getattr(el, "text", None) or str(el)
            if text and text.strip():
                chunks.append(text)
    if not chunks:
        try:
            chunks = [path.read_text(errors="replace")[:50000]]
        except Exception:
            chunks = ["(no text extracted)"]
    return chunks


def embed_texts(openai_client: OpenAI, texts: list[str]) -> list[list[float]]:
    """Batch embed texts with OpenAI."""
    if not texts:
        return []
    embeddings = []
    batch_size = 100
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=batch)
        for d in resp.data:
            embeddings.append(d.embedding)
    return embeddings


def process_job(payload: dict):
    document_id = payload.get("documentId")
    user_id = payload.get("userId")
    file_path = payload.get("filePath")
    file_name = payload.get("fileName", "document")
    if not document_id or not user_id or not file_path:
        logger.error("Missing documentId, userId or filePath in job")
        return
    conn = get_conn()
    try:
        update_document_status(conn, document_id, "processing")
    finally:
        conn.close()
    try:
        chunks_text = parse_file(file_path, file_name)
        if not chunks_text:
            raise ValueError("No content extracted")
        openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
        if not openai_client:
            raise RuntimeError("OPENAI_API_KEY is not set")
        embeddings = embed_texts(openai_client, chunks_text)
        pairs = list(zip(chunks_text, embeddings))
        conn = get_conn()
        try:
            insert_chunks(conn, document_id, user_id, pairs)
            update_document_status(conn, document_id, "done")
            logger.info("Document %s done, %d chunks", document_id, len(pairs))
        finally:
            conn.close()
    except Exception as e:
        logger.exception("Job failed for %s: %s", document_id, e)
        conn = get_conn()
        try:
            update_document_status(conn, document_id, "failed")
        finally:
            conn.close()


def main():
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is required")
    r = redis.Redis.from_url(REDIS_URL)
    logger.info("Worker listening on %s queue %s", REDIS_URL, QUEUE_NAME)
    while True:
        _, raw = r.brpop(QUEUE_NAME, timeout=30)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
            process_job(payload)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in job: %s", e)
        except Exception as e:
            logger.exception("Worker error: %s", e)


if __name__ == "__main__":
    main()
