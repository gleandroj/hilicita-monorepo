#!/usr/bin/env python3
"""
Document ingest worker: consumes Redis queue, downloads file from presigned URL,
parses with open-source unstructured library, and generates a checklist by sending
all parsed elements into the LLM prompt. Vector store is disabled by default but
can be re-enabled for semantic search over the document later.
Updates document status in Postgres.
"""
import os
import json
import time
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
QUEUE_NAME = "document:ingest"
DATABASE_URL = os.environ.get("DATABASE_URL")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "documents")
CHAT_MODEL = "gpt-4o-mini"
CHECKLIST_QUERY = "edital licitação órgão objeto valor total processo interno prazos proposta esclarecimento impugnação documentação qualificação técnica jurídica fiscal econômica visita técnica sessão"
TOP_K = 10

# When False, checklist uses full document text in the prompt; vector store is skipped (can re-enable later).
USE_VECTOR_STORE = False

CHECKLIST_SYSTEM_PROMPT = """Você é um especialista em licitações brasileiras. Seu trabalho é preencher um checklist estruturado com base no documento do edital, seguindo o modelo padrão de checklist de licitação.

Com base no contexto fornecido (conteúdo do documento), extraia todas as informações nos campos indicados. Use string vazia quando não encontrar a informação e false para campos booleanos quando não aplicável.

Para PRAZOS: preencha data e horário separadamente quando o edital informar (ex.: "Enviar proposta até 01/03/2025 14h" -> enviarPropostaAte: { "data": "01/03/2025", "horario": "14h" }).

Para DOCUMENTOS: agrupe por categoria. Use exatamente estas categorias quando houver itens no edital: "Atestado Técnico" (ou "QUALIFICAÇÃO TÉCNICA"), "Documentação", "Qualificação Jurídica-Fiscal", "Qualificação Econômica", "Declarações", "Proposta", "Outros". Para CADA linha do edital em cada seção, inclua um item com: referencia (número/item do edital, ex: 6.2.1.1.1, 8.2. - a.1), local (TR ou ED quando o edital indicar), documento (texto completo do documento exigido), solicitado (true se o edital exige SIM), status (string vazia se não aplicável), observacao (quando houver). Extraia TODOS os itens de Atestado Técnico / especificação técnica listados no edital, não apenas um resumo.

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
                                "referencia": {"type": "string"},
                                "local": {"type": "string"},
                                "documento": {"type": "string"},
                                "solicitado": {"type": "boolean"},
                                "status": {"type": "string"},
                                "observacao": {"type": "string"},
                            },
                            "required": ["referencia", "local", "documento", "solicitado", "status", "observacao"],
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


def _log_query(query: str, params: tuple) -> None:
    """Log the SQL query and parameters (truncate long values for readability)."""
    def truncate(v, max_len: int = 80):
        if isinstance(v, str) and len(v) > max_len:
            return v[:max_len] + "..."
        if isinstance(v, (list, tuple)) and len(v) > 5:
            return f"<{type(v).__name__} len={len(v)}>"
        return v
    safe_params = tuple(truncate(p, 200) for p in params)
    logger.info("SQL: %s", query.strip())
    logger.info("SQL params: %s", safe_params)


def update_document_status(conn, document_id: str, status: str):
    logger.info("Updating document status: documentId=%s status=%s", document_id, status)
    query = 'UPDATE "Document" SET status = %s WHERE id = %s'
    params = (status, document_id)
    _log_query(query, params)
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()
    logger.info("Document status updated: documentId=%s status=%s", document_id, status)


def update_document_vector_store(conn, document_id: str, vector_store_id: str):
    """Store the OpenAI vector store ID on the document."""
    logger.info("Updating document vectorStoreId: documentId=%s", document_id)
    query = 'UPDATE "Document" SET "vectorStoreId" = %s WHERE id = %s'
    params = (vector_store_id, document_id)
    _log_query(query, params)
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()


def download_to_temp(file_url: str, file_name: str) -> str:
    """Download file from URL to a temporary file; return path. Caller must delete."""
    logger.info("Downloading file: fileName=%s url_len=%d", file_name or "document", len(file_url))
    suffix = os.path.splitext(file_name or "document")[1] or ".bin"
    fd, path = tempfile.mkstemp(suffix=suffix)
    try:
        with os.fdopen(fd, "wb") as f:
            with urllib.request.urlopen(file_url, timeout=300) as resp:
                data = resp.read()
                f.write(data)
        logger.info("Download complete: path=%s size=%d bytes", path, len(data))
        return path
    except Exception:
        os.unlink(path)
        raise


def _s3_client():
    """Return boto3 S3 client for MinIO if configured, else None."""
    if not (MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_SECRET_KEY):
        return None
    try:
        import boto3
        from botocore.config import Config
        return boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
            region_name=os.environ.get("MINIO_REGION", "us-east-1"),
            config=Config(signature_version="s3v4"),
        )
    except Exception as e:
        logger.warning("MinIO client not available: %s", e)
        return None


def upload_debug_json(user_id: str, document_id: str, data: dict, suffix: str = "unstructured-debug") -> None:
    """Upload a JSON payload to the bucket for debugging (e.g. unstructured parse result, OpenAI responses)."""
    client = _s3_client()
    if not client:
        return
    key = f"{user_id}/{document_id}-{suffix}.json"
    try:
        body = json.dumps(data, ensure_ascii=False, indent=2)
        client.put_object(Bucket=MINIO_BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/json")
        logger.info("Debug JSON uploaded: bucket=%s key=%s size=%d bytes", MINIO_BUCKET, key, len(body))
    except Exception as e:
        logger.warning("Failed to upload debug JSON to bucket: %s", e)


# Language for unstructured partition (OCR and partitioning). "por" = Portuguese (pt-BR).
PARTITION_LANGUAGES = ["por"]


def parse_file(file_path: str, file_name: str) -> tuple[list[str], dict]:
    """Parse PDF or CSV with open-source unstructured library; return (chunks, debug_payload)."""
    logger.info("Parsing file: path=%s fileName=%s", file_path, file_name or "document")
    elements = partition(filename=file_path, languages=PARTITION_LANGUAGES)
    logger.info("Partition produced %d elements", len(elements))
    chunks = []
    elements_debug = []
    for el in elements:
        text = getattr(el, "text", None) or str(el)
        elements_debug.append({"type": type(el).__name__, "text": text})
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
    logger.info("Parse complete: %d text chunks", len(chunks))
    debug_payload = {"fileName": file_name or "document", "elementCount": len(elements), "elements": elements_debug, "chunks": chunks}
    return chunks, debug_payload


# Max characters per merged file to stay well under API token limits (~2M tokens per file).
MAX_CHARS_PER_MERGED_FILE = 500_000
CHUNK_SEP = "\n\n"


def _merge_chunks_to_temp_files(
    chunks: list[str], max_chars_per_file: int = MAX_CHARS_PER_MERGED_FILE
) -> list[str]:
    """Merge chunks into one or more temp text files. Returns list of temp file paths. Caller must delete."""
    if not chunks:
        return []
    paths = []
    current_parts = []
    current_len = 0
    try:
        for text in chunks:
            part = text.strip()
            if not part:
                continue
            part_len = len(part) + len(CHUNK_SEP)
            if current_parts and (current_len + part_len) > max_chars_per_file:
                fd, path = tempfile.mkstemp(suffix=".txt")
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    f.write(CHUNK_SEP.join(current_parts))
                paths.append(path)
                current_parts = []
                current_len = 0
            current_parts.append(part)
            current_len += part_len
        if current_parts:
            fd, path = tempfile.mkstemp(suffix=".txt")
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(CHUNK_SEP.join(current_parts))
            paths.append(path)
        return paths
    except Exception:
        for p in paths:
            try:
                os.unlink(p)
            except Exception:
                pass
        raise


def create_vector_store_and_upload_chunks(
    openai_client: OpenAI, document_id: str, chunks: list[str]
) -> str:
    """Create an OpenAI vector store, upload merged document file(s), wait until ready. Returns vector_store_id."""
    if not chunks:
        raise ValueError("No chunks to upload")
    merged_paths = _merge_chunks_to_temp_files(chunks)
    num_files = len(merged_paths)
    logger.info(
        "Creating vector store for documentId=%s with %d chunks merged into %d file(s)",
        document_id,
        len(chunks),
        num_files,
    )
    vs = openai_client.vector_stores.create(name=f"doc-{document_id}")
    vector_store_id = vs.id
    file_ids = []
    try:
        for path in merged_paths:
            with open(path, "rb") as f:
                file = openai_client.files.create(file=f, purpose="assistants")
            file_ids.append(file.id)
        if num_files == 1:
            openai_client.vector_stores.files.create(
                vector_store_id=vector_store_id, file_id=file_ids[0]
            )
        else:
            openai_client.vector_stores.file_batches.create_and_poll(
                vector_store_id=vector_store_id, file_ids=file_ids
            )
        _wait_for_vector_store_ready(openai_client, vector_store_id)
        return vector_store_id
    finally:
        for p in merged_paths:
            try:
                os.unlink(p)
            except Exception:
                pass


def _wait_for_vector_store_ready(openai_client: OpenAI, vector_store_id: str, max_wait_sec: int = 600):
    """Poll vector store until status is completed or failed."""
    start = time.monotonic()
    while (time.monotonic() - start) < max_wait_sec:
        vs = openai_client.vector_stores.retrieve(vector_store_id)
        status = getattr(vs, "status", None)
        counts = getattr(vs, "file_counts", None)
        total = getattr(counts, "total", 0) if counts else 0
        completed = getattr(counts, "completed", 0) if counts else 0
        failed = getattr(counts, "failed", 0) if counts else 0
        if status == "completed" or (total and total == completed):
            logger.info("Vector store %s ready (completed=%d)", vector_store_id, completed)
            return
        if status == "expired" or failed:
            raise RuntimeError(f"Vector store {vector_store_id} failed or expired: status={status}, file_counts={getattr(counts, '__dict__', counts)}")
        logger.debug("Vector store %s status=%s completed=%d/%d", vector_store_id, status, completed, total)
        time.sleep(2)
    raise TimeoutError(f"Vector store {vector_store_id} did not complete within {max_wait_sec}s")


def search_vector_store(
    openai_client: OpenAI, vector_store_id: str, query: str, max_results: int = TOP_K
) -> list[str]:
    """Search the vector store with a natural language query. Returns list of chunk texts."""
    logger.info("Searching vector store: vector_store_id=%s max_results=%d", vector_store_id, max_results)
    resp = openai_client.vector_stores.search(
        vector_store_id=vector_store_id,
        query=query,
        max_num_results=max_results,
    )
    texts = []
    for item in getattr(resp, "data", []):
        for content_block in getattr(item, "content", []):
            if getattr(content_block, "type", None) == "text":
                t = getattr(content_block, "text", None)
                if t and t.strip():
                    texts.append(t.strip())
    logger.info("Vector store search returned %d chunks", len(texts))
    return texts


def build_full_document_context(chunks: list[str]) -> str:
    """Build context string from all Unstructured elements (chunks); no truncation."""
    return CHUNK_SEP.join(c.strip() for c in chunks if c and c.strip())


def generate_checklist(openai_client: OpenAI, context: str, file_name: str) -> tuple[dict, dict]:
    """Call OpenAI with Structured Outputs; return (checklist dict, debug payload)."""
    logger.info("Generating checklist: fileName=%s context_len=%d", file_name or "document", len(context))
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
    data = json.loads(raw)
    usage = getattr(resp, "usage", None)
    openai_debug = {
        "model": getattr(resp, "model", CHAT_MODEL),
        "usage": {
            "prompt_tokens": getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "completion_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
        } if usage else None,
        "user_content": user_content,
        "raw_content": raw,
        "parsed_checklist": data,
    }
    logger.info("Checklist generated: fileName=%s", file_name or "document")
    return data, openai_debug


def insert_checklist(
    conn,
    user_id: str,
    file_name: str,
    data: dict,
    document_id: str,
):
    logger.info("Inserting checklist: documentId=%s userId=%s fileName=%s", document_id, user_id, file_name or "document")
    ed = data.get("edital") or {}
    orgao = ed.get("orgao") or None
    objeto = ed.get("objeto") or None
    valor_total = ed.get("totalReais") or ed.get("valorTotal") or None
    pontuacao = data.get("pontuacao")
    if pontuacao is not None and not isinstance(pontuacao, int):
        pontuacao = int(pontuacao) if pontuacao else None
    checklist_id = str(uuid.uuid4())
    query = """
            INSERT INTO "Checklist" (id, "userId", file_name, data, pontuacao, orgao, objeto, valor_total, "documentId")
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
    params = (
        checklist_id,
        user_id,
        file_name,
        json.dumps(data),
        pontuacao,
        orgao,
        objeto,
        valor_total,
        document_id,
    )
    _log_query(query, params)
    with conn.cursor() as cur:
        cur.execute(query, params)
    conn.commit()
    logger.info("Checklist inserted: documentId=%s checklistId=%s", document_id, checklist_id)


def process_job(payload: dict):
    document_id = payload.get("documentId")
    user_id = payload.get("userId")
    file_url = payload.get("fileUrl")
    file_name = payload.get("fileName", "document")
    logger.info(
        "Processing job: documentId=%s userId=%s fileName=%s",
        document_id,
        user_id,
        file_name,
    )
    if not document_id or not user_id or not file_url:
        logger.error("Missing documentId, userId or fileUrl in job payload=%s", payload)
        return
    logger.info("Opening DB connection to set status=processing")
    conn = get_conn()
    try:
        update_document_status(conn, document_id, "processing")
    finally:
        conn.close()
    temp_path = None
    try:
        logger.info("Downloading file for documentId=%s", document_id)
        temp_path = download_to_temp(file_url, file_name)
        logger.info("Parsing file for documentId=%s", document_id)
        chunks_text, unstructured_debug = parse_file(temp_path, file_name)
        upload_debug_json(user_id, document_id, unstructured_debug)
        if not chunks_text:
            raise ValueError("No content extracted")
        openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
        if not openai_client:
            raise RuntimeError("OPENAI_API_KEY is not set")
        conn = get_conn()
        try:
            if USE_VECTOR_STORE:
                logger.info("Creating OpenAI vector store and uploading %d chunks for documentId=%s", len(chunks_text), document_id)
                vector_store_id = create_vector_store_and_upload_chunks(openai_client, document_id, chunks_text)
                logger.info("Document %s: vector store %s ready", document_id, vector_store_id)
                update_document_vector_store(conn, document_id, vector_store_id)
                logger.info("Searching vector store for checklist context: documentId=%s", document_id)
                relevant = search_vector_store(openai_client, vector_store_id, CHECKLIST_QUERY)
                context = "\n\n".join(relevant)
            else:
                # Use all Unstructured elements in the prompt (no vector store)
                logger.info("Using full document (%d chunks) for checklist: documentId=%s", len(chunks_text), document_id)
                context = build_full_document_context(chunks_text)

            checklist_data, checklist_openai_debug = generate_checklist(openai_client, context, file_name)
            openai_debug = {"checklist": checklist_openai_debug}
            upload_debug_json(user_id, document_id, openai_debug, "openai-debug")
            insert_checklist(conn, user_id, file_name, checklist_data, document_id)
            logger.info("Document %s: checklist generated and inserted", document_id)

            logger.info("Setting documentId=%s status=done", document_id)
            update_document_status(conn, document_id, "done")
            logger.info("Job completed successfully: documentId=%s", document_id)
        finally:
            conn.close()
    except Exception as e:
        logger.exception("Job failed for %s: %s", document_id, e)
        logger.info("Setting documentId=%s status=failed", document_id)
        conn = get_conn()
        try:
            update_document_status(conn, document_id, "failed")
        finally:
            conn.close()
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
                logger.debug("Removed temp file: %s", temp_path)
            except Exception:
                pass


def main():
    logger.info("Worker starting: REDIS_URL=%s QUEUE=%s", REDIS_URL, QUEUE_NAME)
    if not DATABASE_URL:
        logger.error("DATABASE_URL is required")
        raise SystemExit("DATABASE_URL is required")
    if not OPENAI_API_KEY:
        logger.warning("OPENAI_API_KEY is not set; checklist generation will fail")
    r = redis.Redis.from_url(REDIS_URL)
    logger.info("Worker listening on queue %s (brpop timeout=30s)", QUEUE_NAME)
    while True:
        result = r.brpop(QUEUE_NAME, timeout=30)
        if result is None:
            continue
        _, raw = result
        logger.info("Job received from queue (payload_len=%d)", len(raw))
        try:
            payload = json.loads(raw)
            logger.info("Job payload parsed, documentId=%s", payload.get("documentId"))
            process_job(payload)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in job: %s", e)
        except Exception as e:
            logger.exception("Worker error: %s", e)


if __name__ == "__main__":
    main()
