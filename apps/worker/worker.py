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

# When True, send the PDF as file to OpenAI (Responses API input_file) instead of parsed structured elements.
# Can be overridden per job via payload "usePdfFile". Requires vision-capable model (e.g. gpt-4o, gpt-4o-mini).
USE_PDF_AS_FILE = os.environ.get("USE_PDF_AS_FILE", "false").lower() in ("true", "1")

# When True, generate checklist in multiple LLM calls (one per block) and merge. Improves accuracy for long editais.
USE_CHECKLIST_BLOCKS = os.environ.get("USE_CHECKLIST_BLOCKS", "true").lower() in ("true", "1")

# --- Single-call (legacy) system prompt: sectioned instructions for clarity when not using blocks ---
CHECKLIST_SYSTEM_PROMPT = """Você é um especialista em licitações brasileiras. Preencha o checklist estruturado com base no documento do edital, seguindo o modelo padrão de checklist de licitação.

Regras gerais: use string vazia quando não encontrar a informação; use false para campos booleanos quando não aplicável.

1) IDENTIFICAÇÃO DO EDITAL: Extraia órgão, número do edital, objeto (resumo do objeto da licitação), data da sessão (formato DD/MM/AAAA HH:MM quando houver), portal (ex.: Licitar Digital), número do processo interno (Nº ADM ou similar), valor total em R$ (por extenso quando houver), vigência do contrato, modalidade/concessionária (ex.: Neoenergia Pernambuco), prazo início injeção e valor/volume de energia quando aplicável.

2) MODALIDADE E PARTICIPAÇÃO: Modalidade da licitação (ex.: Pregão Eletrônico). Permite consórcio? Benefícios a MPE? Item do edital que trata disso (referência).

3) PRAZOS: Para cada prazo (enviar proposta, esclarecimentos, impugnação), preencha data e horário separadamente. Ex.: "Enviar proposta até 10/02/2026 9h00" → enviarPropostaAte: { "data": "10/02/2026", "horario": "9h00" }. Inclua contato para envio de esclarecimento/impugnação quando informado.

4) DOCUMENTOS: Agrupe por categoria. Use exatamente: "Atestado Técnico" (ou "QUALIFICAÇÃO TÉCNICA"), "Documentação", "Qualificação Jurídica-Fiscal", "Qualificação Econômica", "Declarações", "Proposta", "Outros". Para CADA item do edital inclua: referencia (número/item, ex: 6.2.1.1.1), local (TR ou ED quando indicado), documento (texto completo exigido), solicitado (true se exigido), status (vazio), observacao (quando houver). Extraia TODOS os itens listados, não resuma.

5) VISITA TÉCNICA E PROPOSTA: visitaTecnica = true apenas se o edital exigir visita técnica obrigatória. Validade da proposta (prazo em dias ou texto).

6) SESSÃO E OUTROS: Diferença entre lances, prazo para proposta ajustada, aberto/fechado. Mecanismo de pagamento quando citado.

7) ANÁLISE: Pontuação 0-100 (valor do contrato, clareza, viabilidade, prazos) e recomendação curta."""

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

# --- Per-block extraction: schema + prompt for each checklist section (improves accuracy) ---
CHECKLIST_BLOCKS = [
    {
        "key": "edital",
        "schema": {
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
            },
            "required": ["edital"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Sua tarefa é extrair APENAS os dados de IDENTIFICAÇÃO DO EDITAL.

No contexto do documento, localize e preencha:
- licitacao: órgão ou entidade realizadora (ex.: PREFEITURA DE RECIFE)
- edital: número do edital (ex.: 026/2025-GC-SEPLAG-007)
- orgao: órgão/administração (ex.: Prefeitura da Cidade do Recife)
- objeto: resumo do objeto da licitação (registro de preços, fornecimento, etc.)
- dataSessao: data e horário da sessão (formato DD/MM/AAAA HH:MM ou similar; ex.: 10/02/2026 09:00:00)
- portal: nome do portal (ex.: Licitar Digital)
- numeroProcessoInterno: número do processo/ADM (ex.: 81800)
- totalReais: valor total em R$ (número e/ou por extenso quando houver)
- valorEnergia, volumeEnergia: quando o edital for de energia
- vigenciaContrato: prazo (ex.: 12 meses, Registro de Preço)
- modalidadeConcessionaria: modalidade e concessionária (ex.: Neoenergia Pernambuco)
- prazoInicioInjecao: quando aplicável

Use string vazia ("") para qualquer campo não encontrado. Não invente dados.""",
    },
    {
        "key": "modalidade_participacao",
        "schema": {
            "type": "object",
            "properties": {
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
            },
            "required": ["modalidadeLicitacao", "participacao"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Extraia APENAS MODALIDADE E PARTICIPAÇÃO.

- modalidadeLicitacao: tipo da licitação (ex.: Pregão Eletrônico, Concorrência).
- participacao.permiteConsorcio: true somente se o edital PERMITE participação em consórcio; false se não permite ou não mencionar.
- participacao.beneficiosMPE: true somente se há benefícios a microempresa/pequeno porte; false caso contrário.
- participacao.itemEdital: referência ou trecho do edital que trata de participação/consórcio/MPE (ou string vazia).

Use false para booleanos quando não aplicável ou não informado.""",
    },
    {
        "key": "prazos",
        "schema": {
            "type": "object",
            "properties": {
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
            },
            "required": ["prazos"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Extraia APENAS os PRAZOS do edital.

Para cada prazo, preencha data e horário separadamente:
- enviarPropostaAte: data e horário limite para envio da proposta (ex.: "10/02/2026" e "9H00" ou "9h00")
- esclarecimentosAte: data e horário limite para pedidos de esclarecimento
- impugnacaoAte: data e horário limite para impugnação
- contatoEsclarecimentoImpugnacao: canal ou sistema para envio (ex.: LICITAR DIGITAL - sistema do certame)

Use strings vazias para data/horário quando não encontrado. Mantenha o formato de data como no edital (DD/MM/AAAA) e horário como informado.""",
    },
    {
        "key": "documentos",
        "schema": {
            "type": "object",
            "properties": {
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
            },
            "required": ["documentos"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Extraia APENAS a lista de DOCUMENTOS E QUALIFICAÇÃO exigidos pelo edital.

Agrupe por categoria. Use exatamente estas categorias quando houver itens: "Atestado Técnico" (ou "QUALIFICAÇÃO TÉCNICA"), "Documentação", "Qualificação Jurídica-Fiscal", "Qualificação Econômica", "Declarações", "Proposta", "Outros".

Para CADA item exigido no edital, inclua um elemento em itens com:
- referencia: número ou item do edital (ex.: 6.2.1.1.1, 8.2 - a.1)
- local: TR ou ED quando o edital indicar (termo de referência ou edital)
- documento: texto completo do documento exigido (não resuma)
- solicitado: true se o edital exige o documento
- status: string vazia
- observacao: quando houver

Extraia TODOS os itens listados (atestados técnicos, especificação técnica, documentação, etc.), um por um. Não agrupe em um único resumo. Retorne array vazio se não houver seção de documentos.""",
    },
    {
        "key": "visita_proposta",
        "schema": {
            "type": "object",
            "properties": {
                "visitaTecnica": {"type": "boolean"},
                "proposta": {
                    "type": "object",
                    "properties": {"validadeProposta": {"type": "string"}},
                    "required": ["validadeProposta"],
                    "additionalProperties": False,
                },
            },
            "required": ["visitaTecnica", "proposta"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Extraia APENAS VISITA TÉCNICA e PROPOSTA.

- visitaTecnica: true SOMENTE se o edital exigir visita técnica OBRIGATÓRIA; false se não obrigatória ou não mencionada.
- proposta.validadeProposta: prazo de validade da proposta (ex.: 60 dias, até a sessão, ou texto do edital). Use string vazia se não informado.""",
    },
    {
        "key": "sessao_outros",
        "schema": {
            "type": "object",
            "properties": {
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
            },
            "required": ["sessao", "outrosEdital"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Extraia APENAS dados da SESSÃO e OUTROS do edital.

- sessao.diferencaEntreLances: valor ou percentual mínimo entre lances (quando aplicável)
- sessao.horasPropostaAjustada: prazo para proposta ajustada (quando aplicável)
- sessao.abertoFechado: se sessão é aberta ou fechada (quando aplicável)
- outrosEdital.mecanismoPagamento: forma de pagamento (ex.: faturamento, medição). Use string vazia quando não encontrado.""",
    },
    {
        "key": "analise",
        "schema": {
            "type": "object",
            "properties": {
                "responsavelAnalise": {"type": "string"},
                "pontuacao": {"type": "integer"},
                "recomendacao": {"type": "string"},
            },
            "required": ["responsavelAnalise", "pontuacao", "recomendacao"],
            "additionalProperties": False,
        },
        "system_prompt": """Você é um especialista em licitações brasileiras. Com base no edital analisado, preencha a ANÁLISE FINAL.

- responsavelAnalise: string vazia (campo para preenchimento posterior pelo usuário).
- pontuacao: número inteiro de 0 a 100, considerando: valor do contrato, clareza do edital, viabilidade de participação e prazos.
- recomendacao: uma ou duas frases com recomendação objetiva (ex.: "Recomenda-se participar; prazos adequados e documentação clara." ou "Atenção ao prazo curto para esclarecimentos.").""",
    },
]


def _deep_merge_checklist(base: dict, block_result: dict) -> None:
    """Merge block_result into base in-place. For lists (e.g. documentos), replace; for dicts, deep merge."""
    for key, value in block_result.items():
        if key not in base:
            base[key] = value
        elif isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge_checklist(base[key], value)
        elif isinstance(value, list):
            base[key] = value
        else:
            base[key] = value


def _generate_one_block(
    openai_client: OpenAI,
    block: dict,
    context: str,
    file_name: str,
) -> dict:
    """Call LLM for a single checklist block; return the block result (subset of full checklist)."""
    name = block["key"]
    schema = block["schema"]
    system = block["system_prompt"]
    user_content = f"Contexto do documento ({file_name or 'document'}):\n\n{context}\n\nExtraia apenas a parte do checklist correspondente a este bloco e retorne em JSON."
    resp = openai_client.chat.completions.create(
        model=CHAT_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user_content},
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": f"checklist_block_{name}",
                "strict": True,
                "schema": schema,
            },
        },
    )
    raw = (resp.choices[0].message.content or "").strip()
    data = json.loads(raw)
    return data


def generate_checklist_blocks(openai_client: OpenAI, context: str, file_name: str) -> tuple[dict, dict]:
    """Generate checklist by running one LLM call per block and merging. Returns (checklist dict, debug payload)."""
    logger.info("Generating checklist by blocks: fileName=%s context_len=%d blocks=%d", file_name or "document", len(context), len(CHECKLIST_BLOCKS))
    merged = {}
    raw_by_block = {}
    for block in CHECKLIST_BLOCKS:
        name = block["key"]
        try:
            block_data = _generate_one_block(openai_client, block, context, file_name)
            raw_by_block[name] = block_data
            if name == "modalidade_participacao":
                merged["modalidadeLicitacao"] = block_data.get("modalidadeLicitacao", "")
                merged["participacao"] = block_data.get("participacao") or {}
            else:
                _deep_merge_checklist(merged, block_data)
        except Exception as e:
            logger.warning("Block %s failed: %s", name, e)
            raw_by_block[name] = {"_error": str(e)}
    # Ensure all required top-level keys exist for full schema
    default_edital = {
        "licitacao": "", "edital": "", "orgao": "", "objeto": "", "dataSessao": "", "portal": "",
        "numeroProcessoInterno": "", "totalReais": "", "valorEnergia": "", "volumeEnergia": "",
        "vigenciaContrato": "", "modalidadeConcessionaria": "", "prazoInicioInjecao": "",
    }
    for key in CHECKLIST_JSON_SCHEMA.get("required", []):
        if key not in merged:
            if key == "edital":
                merged["edital"] = default_edital.copy()
            elif key == "documentos":
                merged["documentos"] = []
            elif key == "participacao":
                merged["participacao"] = {"permiteConsorcio": False, "beneficiosMPE": False, "itemEdital": ""}
            elif key == "proposta":
                merged["proposta"] = {"validadeProposta": ""}
            elif key == "sessao":
                merged["sessao"] = {"diferencaEntreLances": "", "horasPropostaAjustada": "", "abertoFechado": ""}
            elif key == "outrosEdital":
                merged["outrosEdital"] = {"mecanismoPagamento": ""}
            elif key == "pontuacao":
                merged["pontuacao"] = 0
            elif key in ("responsavelAnalise", "recomendacao"):
                merged[key] = ""
            elif key == "visitaTecnica":
                merged["visitaTecnica"] = False
            else:
                merged[key] = ""
    openai_debug = {
        "mode": "blocks",
        "blocks": list(b["key"] for b in CHECKLIST_BLOCKS),
        "raw_by_block": raw_by_block,
    }
    logger.info("Checklist blocks merged: fileName=%s", file_name or "document")
    return merged, openai_debug


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


def _upload_pdf_to_openai(openai_client: OpenAI, pdf_path: str, file_name: str) -> str:
    """Upload PDF to OpenAI Files API (purpose=user_data) for use as input_file. Returns file_id."""
    logger.info("Uploading PDF to OpenAI Files API: path=%s fileName=%s", pdf_path, file_name or "document")
    with open(pdf_path, "rb") as f:
        file_obj = openai_client.files.create(file=f, purpose="user_data")
    file_id = file_obj.id
    logger.info("PDF uploaded: file_id=%s", file_id)
    return file_id


def generate_checklist(openai_client: OpenAI, context: str, file_name: str) -> tuple[dict, dict]:
    """Call OpenAI Chat Completions with structured elements text; return (checklist dict, debug payload)."""
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


def generate_checklist_from_pdf_file(
    openai_client: OpenAI, pdf_path: str, file_name: str
) -> tuple[dict, dict]:
    """Send PDF as file to OpenAI Responses API (input_file via file_id) and get checklist via structured output."""
    logger.info("Generating checklist from PDF file: fileName=%s path=%s", file_name or "document", pdf_path)
    file_id = _upload_pdf_to_openai(openai_client, pdf_path, file_name or "document.pdf")
    user_instruction = (
        "Com base no documento (edital de licitação) anexado, extraia todas as informações no checklist em JSON conforme o schema."
    )
    input_content = [
        {"type": "input_file", "file_id": file_id},
        {"type": "input_text", "text": user_instruction},
    ]
    try:
        resp = openai_client.responses.create(
            model=CHAT_MODEL,
            instructions=CHECKLIST_SYSTEM_PROMPT,
            input=[{"role": "user", "content": input_content}],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "licitacao_checklist",
                    "strict": True,
                    "schema": CHECKLIST_JSON_SCHEMA,
                }
            },
        )
    except Exception as e:
        if "input_file" in str(e).lower() or "file" in str(e).lower():
            logger.warning(
                "Responses API with input_file may require a vision model (e.g. gpt-4o). Falling back to chat completions. Error: %s",
                e,
            )
        raise
    raw = (getattr(resp, "output_text", None) or "").strip()
    if not raw:
        for item in getattr(resp, "output", []) or []:
            if getattr(item, "type", None) == "message":
                for content in getattr(item, "content", []) or []:
                    if getattr(content, "type", None) == "output_text":
                        raw = (getattr(content, "text", None) or "").strip()
                        break
            if raw:
                break
    if not raw:
        raise ValueError("No output_text in Responses API response")
    data = json.loads(raw)
    usage = getattr(resp, "usage", None)
    openai_debug = {
        "mode": "pdf_file",
        "model": getattr(resp, "model", CHAT_MODEL),
        "usage": {
            "prompt_tokens": getattr(usage, "input_tokens", None) or getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "output_tokens", None) or getattr(usage, "completion_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
        } if usage else None,
        "user_instruction": user_instruction,
        "raw_content": raw,
        "parsed_checklist": data,
    }
    logger.info("Checklist generated from PDF file: fileName=%s", file_name or "document")
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
    use_pdf_file = payload.get("usePdfFile", USE_PDF_AS_FILE)
    logger.info(
        "Processing job: documentId=%s userId=%s fileName=%s usePdfFile=%s",
        document_id,
        user_id,
        file_name,
        use_pdf_file,
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

        openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
        if not openai_client:
            raise RuntimeError("OPENAI_API_KEY is not set")

        if use_pdf_file:
            logger.info("Using PDF-as-file mode for documentId=%s", document_id)
            upload_debug_json(user_id, document_id, {"mode": "pdf_file", "fileName": file_name}, "unstructured-debug")
            checklist_data, checklist_openai_debug = generate_checklist_from_pdf_file(
                openai_client, temp_path, file_name
            )
            openai_debug = {"checklist": checklist_openai_debug}
            upload_debug_json(user_id, document_id, openai_debug, "openai-debug")
            conn = get_conn()
            try:
                insert_checklist(conn, user_id, file_name, checklist_data, document_id)
            finally:
                conn.close()
        else:
            logger.info("Parsing file for documentId=%s", document_id)
            chunks_text, unstructured_debug = parse_file(temp_path, file_name)
            upload_debug_json(user_id, document_id, unstructured_debug)
            if not chunks_text:
                raise ValueError("No content extracted")
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
                    logger.info("Using full document (%d chunks) for checklist: documentId=%s", len(chunks_text), document_id)
                    context = build_full_document_context(chunks_text)

                if USE_CHECKLIST_BLOCKS:
                    checklist_data, checklist_openai_debug = generate_checklist_blocks(openai_client, context, file_name)
                else:
                    checklist_data, checklist_openai_debug = generate_checklist(openai_client, context, file_name)
                openai_debug = {"checklist": checklist_openai_debug}
                upload_debug_json(user_id, document_id, openai_debug, "openai-debug")
                insert_checklist(conn, user_id, file_name, checklist_data, document_id)
            finally:
                conn.close()

        logger.info("Document %s: checklist generated and inserted", document_id)
        conn = get_conn()
        try:
            logger.info("Setting documentId=%s status=done", document_id)
            update_document_status(conn, document_id, "done")
        finally:
            conn.close()
        logger.info("Job completed successfully: documentId=%s", document_id)
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
