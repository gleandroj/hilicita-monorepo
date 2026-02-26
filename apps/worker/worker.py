#!/usr/bin/env python3
"""
Document ingest worker: consumes Redis queue, downloads file from presigned URL,
parses with open-source unstructured library, generates embeddings, stores in pgvector.
Updates document status in Postgres.
"""
import os
import json
from pathlib import Path

from dotenv import load_dotenv

# Load .env from worker directory (or repo root when run from monorepo)
_env_dir = Path(__file__).resolve().parent
load_dotenv(_env_dir / ".env")
load_dotenv(_env_dir.parent.parent / ".env")
import uuid
import logging
import tempfile
import urllib.request

import redis
from unstructured.partition.auto import partition
from openai import OpenAI
import psycopg2
import pgvector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "document:ingest"
DATABASE_URL = os.environ.get("DATABASE_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIM = 1536
CHAT_MODEL = "gpt-4o-mini"
CHECKLIST_QUERY = "edital licitação órgão objeto valor total processo interno prazos proposta esclarecimento impugnação documentação qualificação técnica jurídica fiscal econômica visita técnica sessão"
TOP_K = 10

CHECKLIST_SYSTEM_PROMPT = """Você é um especialista em licitações brasileiras. Seu trabalho é preencher um checklist estruturado com base no documento do edital, seguindo o modelo padrão de checklist de licitação.

Com base no contexto fornecido (trechos do documento), extraia todas as informações nos campos indicados. Use string vazia quando não encontrar a informação e false para campos booleanos quando não aplicável.

Para PRAZOS: preencha data e horário separadamente quando o edital informar (ex.: "Enviar proposta até 01/03/2025 14h" -> enviarPropostaAte: { "data": "01/03/2025", "horario": "14h" }).

Para DOCUMENTOS: agrupe por categoria (Atestado Técnico / Qualificação Jurídica-Fiscal / Qualificação Econômica / Declarações / Proposta / Outros). Cada item deve ter: documento (nome do documento solicitado), solicitado (true se o edital exige SIM), status e observacao quando houver.

Declaração de Visita Técnica: preencha visitaTecnica true se o edital exigir visita técnica obrigatória.

Ao final, dê uma pontuação de 0-100 (valor do contrato, clareza, viabilidade de participação, prazos) e uma recomendação curta."""

CHECKLIST_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "edital": {
            "type": "object",
            "properties": {
                "licitacao": {"type": "string"},
                "edital": {"type": "string"},
                "orgao": {"type": "string"},
                "objeto": {"type": "string"},
                "dataSessao": {"type": "string"},
                "portal": {"type": "string"},
                "numeroProcessoInterno": {"type": "string"},
                "totalReais": {"type": "string"},
                "valorEnergia": {"type": "string"},
                "volumeEnergia": {"type": "string"},
                "vigenciaContrato": {"type": "string"},
                "modalidadeConcessionaria": {"type": "string"},
                "prazoInicioInjecao": {"type": "string"},
            },
            "required": [
                "licitacao", "edital", "orgao", "objeto", "dataSessao", "portal",
                "numeroProcessoInterno", "totalReais", "valorEnergia", "volumeEnergia",
                "vigenciaContrato", "modalidadeConcessionaria", "prazoInicioInjecao",
            ],
            "additionalProperties": False,
        },
        "modalidadeLicitacao": {"type": "string"},
        "participacao": {
            "type": "object",
            "properties": {
                "permiteConsorcio": {"type": "boolean"},
                "beneficiosMPE": {"type": "boolean"},
                "itemEdital": {"type": "string"},
            },
            "required": ["permiteConsorcio", "beneficiosMPE", "itemEdital"],
            "additionalProperties": False,
        },
        "prazos": {
            "type": "object",
            "properties": {
                "enviarPropostaAte": {
                    "type": "object",
                    "properties": {"data": {"type": "string"}, "horario": {"type": "string"}},
                    "required": ["data", "horario"],
                    "additionalProperties": False,
                },
                "esclarecimentosAte": {
                    "type": "object",
                    "properties": {"data": {"type": "string"}, "horario": {"type": "string"}},
                    "required": ["data", "horario"],
                    "additionalProperties": False,
                },
                "impugnacaoAte": {
                    "type": "object",
                    "properties": {"data": {"type": "string"}, "horario": {"type": "string"}},
                    "required": ["data", "horario"],
                    "additionalProperties": False,
                },
                "contatoEsclarecimentoImpugnacao": {"type": "string"},
            },
            "required": [
                "enviarPropostaAte", "esclarecimentosAte", "impugnacaoAte",
                "contatoEsclarecimentoImpugnacao",
            ],
            "additionalProperties": False,
        },
        "documentos": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "categoria": {"type": "string"},
                    "itens": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "documento": {"type": "string"},
                                "solicitado": {"type": "boolean"},
                                "status": {"type": "string"},
                                "observacao": {"type": "string"},
                            },
                            "required": ["documento", "solicitado", "status", "observacao"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["categoria", "itens"],
                "additionalProperties": False,
            },
        },
        "visitaTecnica": {"type": "boolean"},
        "proposta": {
            "type": "object",
            "properties": {"validadeProposta": {"type": "string"}},
            "required": ["validadeProposta"],
            "additionalProperties": False,
        },
        "sessao": {
            "type": "object",
            "properties": {
                "diferencaEntreLances": {"type": "string"},
                "horasPropostaAjustada": {"type": "string"},
                "abertoFechado": {"type": "string"},
            },
            "required": ["diferencaEntreLances", "horasPropostaAjustada", "abertoFechado"],
            "additionalProperties": False,
        },
        "outrosEdital": {
            "type": "object",
            "properties": {"mecanismoPagamento": {"type": "string"}},
            "required": ["mecanismoPagamento"],
            "additionalProperties": False,
        },
        "responsavelAnalise": {"type": "string"},
        "pontuacao": {"type": "integer"},
        "recomendacao": {"type": "string"},
    },
    "required": [
        "edital", "modalidadeLicitacao", "participacao", "prazos", "documentos",
        "visitaTecnica", "proposta", "sessao", "outrosEdital",
        "responsavelAnalise", "pontuacao", "recomendacao",
    ],
    "additionalProperties": False,
}


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


def download_to_temp(file_url: str, file_name: str) -> str:
    """Download file from URL to a temporary file; return path. Caller must delete."""
    suffix = os.path.splitext(file_name or "document")[1] or ".bin"
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            with urllib.request.urlopen(file_url, timeout=300) as resp:
                f.write(resp.read())
        return path
    except Exception:
        os.unlink(path)
        raise


def parse_file(file_path: str, file_name: str) -> list[str]:
    """Parse PDF or CSV with open-source unstructured library; return list of text chunks."""
    elements = partition(filename=file_path)
    chunks = []
    for el in elements:
        text = getattr(el, "text", None) or str(el)
        if text and text.strip():
            chunks.append(text)
    if not chunks:
        try:
            with open(file_path, "r", errors="replace") as f:
                raw = f.read(50000)
                if raw.strip():
                    chunks = [raw]
        except Exception:
            pass
        if not chunks:
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


def get_relevant_chunks(conn, document_id: str, user_id: str, query_embedding: list[float]) -> list[str]:
    """Return top TOP_K chunk contents by similarity to query embedding."""
    embedding_str = "[" + ",".join(str(x) for x in query_embedding) + "]"
    with conn.cursor() as cur:
        cur.execute(
            """SELECT content FROM "DocumentChunk"
             WHERE "documentId" = %s AND "userId" = %s
             ORDER BY embedding <-> %s::vector
             LIMIT %s""",
            (document_id, user_id, embedding_str, TOP_K),
        )
        return [row[0] for row in cur.fetchall()]


def generate_checklist(openai_client: OpenAI, context: str, file_name: str) -> dict:
    """Call OpenAI with Structured Outputs; return checklist dict."""
    user_content = f"Contexto do documento ({file_name}):\n\n{context}\n\nExtraia o checklist em JSON."
    resp = openai_client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": CHECKLIST_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "licitacao_checklist",
                "strict": True,
                "schema": CHECKLIST_JSON_SCHEMA,
            },
        },
    )
    raw = (resp.choices[0].message.content or "").strip()
    return json.loads(raw)


def insert_checklist(
    conn,
    user_id: str,
    file_name: str,
    data: dict,
    document_id: str,
):
    ed = data.get("edital") or {}
    orgao = ed.get("orgao") or None
    objeto = ed.get("objeto") or None
    valor_total = ed.get("totalReais") or ed.get("valorTotal") or None
    pontuacao = data.get("pontuacao")
    if pontuacao is not None and not isinstance(pontuacao, int):
        pontuacao = int(pontuacao) if pontuacao else None
    checklist_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO "Checklist" (id, "userId", file_name, data, pontuacao, orgao, objeto, valor_total, "documentId")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                checklist_id,
                user_id,
                file_name,
                json.dumps(data),
                pontuacao,
                orgao,
                objeto,
                valor_total,
                document_id,
            ),
        )
    conn.commit()


def process_job(payload: dict):
    document_id = payload.get("documentId")
    user_id = payload.get("userId")
    file_url = payload.get("fileUrl")
    file_name = payload.get("fileName", "document")
    if not document_id or not user_id or not file_url:
        logger.error("Missing documentId, userId or fileUrl in job")
        return
    conn = get_conn()
    try:
        update_document_status(conn, document_id, "processing")
    finally:
        conn.close()
    temp_path = None
    try:
        temp_path = download_to_temp(file_url, file_name)
        chunks_text = parse_file(temp_path, file_name)
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
            logger.info("Document %s: %d chunks inserted", document_id, len(pairs))

            # Generate checklist with Structured Outputs and insert
            query_embedding = embed_texts(openai_client, [CHECKLIST_QUERY])[0]
            relevant = get_relevant_chunks(conn, document_id, user_id, query_embedding)
            context = "\n\n".join(relevant)
            checklist_data = generate_checklist(openai_client, context, file_name)
            insert_checklist(conn, user_id, file_name, checklist_data, document_id)
            logger.info("Document %s: checklist generated", document_id)

            update_document_status(conn, document_id, "done")
        finally:
            conn.close()
    except Exception as e:
        logger.exception("Job failed for %s: %s", document_id, e)
        conn = get_conn()
        try:
            update_document_status(conn, document_id, "failed")
        finally:
            conn.close()
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass


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
