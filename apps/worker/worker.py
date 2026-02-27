#!/usr/bin/env python3
"""
Document ingest worker: consumes Redis queue, downloads file from presigned URL,
parses with unstructured, builds normalized chunks with embeddings, and generates
a checklist via retrieval-driven block extraction (one LLM call per block with
block-specific context). Updates document status in Postgres.
"""
import os
import re
import json
import time
import math
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
# Model for checklist extraction. gpt-4o is more accurate for long editais and PDF-as-file (vision); override with OPENAI_CHAT_MODEL for cost/speed.
CHAT_MODEL = os.environ.get("OPENAI_CHAT_MODEL", "gpt-4o")
CHECKLIST_QUERY = "edital licitação órgão objeto valor total processo interno prazos proposta esclarecimento impugnação documentação qualificação técnica jurídica fiscal econômica visita técnica sessão"
TOP_K = 10

# --- Retrieval-driven block extraction (default for checklist blocks) ---
CHUNK_MIN_CHARS = 800
CHUNK_MAX_CHARS = 1200
EMBEDDING_MODEL = "text-embedding-3-large"
TOP_K_RETRIEVAL = 12  # 8–15 per block
EVIDENCE_PROMPT_SUFFIX = """

Use ONLY the provided excerpts. If information is missing, return empty values. Do not infer or guess.
Every extracted field must be traceable to text evidence."""

# Heading patterns for section_hint (Brazilian bidding documents): (pattern, label)
HEADING_PATTERNS = [
    (re.compile(r"^\s*(\d+\.)\s", re.IGNORECASE), "numeric"),
    (re.compile(r"\bITEM\s+\d+", re.IGNORECASE), "ITEM"),
    (re.compile(r"\bCLÁUSULA\s+\d+", re.IGNORECASE), "CLÁUSULA"),
    (re.compile(r"\bDO\s+PRAZO\b", re.IGNORECASE), "DO PRAZO"),
    (re.compile(r"\bDOCUMENTAÇÃO\b", re.IGNORECASE), "DOCUMENTAÇÃO"),
    (re.compile(r"\bQUALIFICAÇÃO\b", re.IGNORECASE), "QUALIFICAÇÃO"),
    (re.compile(r"\bHABILITAÇÃO\b", re.IGNORECASE), "HABILITAÇÃO"),
    (re.compile(r"\bPRAZOS\b", re.IGNORECASE), "PRAZOS"),
    (re.compile(r"\bIDENTIFICAÇÃO\b", re.IGNORECASE), "IDENTIFICAÇÃO"),
    (re.compile(r"\bSESSÃO\b", re.IGNORECASE), "SESSÃO"),
]

# When False, checklist uses full document text in the prompt; vector store is skipped (legacy).
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

# --- Per-block extraction: schema + prompt + retrieval query for each checklist section ---
CHECKLIST_BLOCKS = [
    {
        "key": "edital",
        "query": "órgão edital número processo objeto sessão portal licitação valor total vigência modalidade",
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

Use string vazia ("") para qualquer campo não encontrado. Não invente dados.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "modalidade_participacao",
        "query": "modalidade licitação pregão concorrência consórcio MPE microempresa participação",
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

Use false para booleanos quando não aplicável ou não informado.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "prazos",
        "query": "prazo proposta esclarecimento impugnação data horário sessão envio limite",
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

Use strings vazias para data/horário quando não encontrado. Mantenha o formato de data como no edital (DD/MM/AAAA) e horário como informado.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "documentos",
        "query": "documentação habilitação qualificação técnica fiscal jurídica atestado declaração proposta exigido",
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

Extraia TODOS os itens listados (atestados técnicos, especificação técnica, documentação, etc.), um por um. Não agrupe em um único resumo. Retorne array vazio se não houver seção de documentos.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "visita_proposta",
        "query": "visita técnica obrigatória validade proposta prazo dias",
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
- proposta.validadeProposta: prazo de validade da proposta (ex.: 60 dias, até a sessão, ou texto do edital). Use string vazia se não informado.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "sessao_outros",
        "query": "lances aberto fechado proposta ajustada intervalo diferença pagamento mecanismo",
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
- outrosEdital.mecanismoPagamento: forma de pagamento (ex.: faturamento, medição). Use string vazia quando não encontrado.""" + EVIDENCE_PROMPT_SUFFIX,
    },
    {
        "key": "analise",
        "query": "valor contrato clareza viabilidade prazos recomendação análise",
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
- recomendacao: uma ou duas frases com recomendação objetiva (ex.: "Recomenda-se participar; prazos adequados e documentação clara." ou "Atenção ao prazo curto para esclarecimentos.").""" + EVIDENCE_PROMPT_SUFFIX,
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
) -> tuple[dict, str]:
    """Call LLM for a single checklist block; return (block result dict, raw JSON string)."""
    name = block["key"]
    schema = block["schema"]
    system = block["system_prompt"]
    user_content = (
        f"Trechos do documento ({file_name or 'document'}):\n\n{context}\n\n"
        "Extraia apenas a parte do checklist correspondente a este bloco com base EXCLUSIVAMENTE nos trechos acima. Retorne em JSON."
    )
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
    return data, raw


def generate_checklist_blocks(openai_client: OpenAI, context: str, file_name: str) -> tuple[dict, dict]:
    """Generate checklist by running one LLM call per block and merging (full context). Returns (checklist dict, debug payload)."""
    logger.info("Generating checklist by blocks: fileName=%s context_len=%d blocks=%d", file_name or "document", len(context), len(CHECKLIST_BLOCKS))
    merged = {}
    raw_by_block = {}
    for block in CHECKLIST_BLOCKS:
        name = block["key"]
        try:
            block_data, raw = _generate_one_block(openai_client, block, context, file_name)
            raw_by_block[name] = block_data
            if name == "modalidade_participacao":
                merged["modalidadeLicitacao"] = block_data.get("modalidadeLicitacao", "")
                merged["participacao"] = block_data.get("participacao") or {}
            else:
                _deep_merge_checklist(merged, block_data)
        except Exception as e:
            logger.warning("Block %s failed: %s", name, e)
            raw_by_block[name] = {"_error": str(e)}
    _fill_checklist_defaults(merged)
    openai_debug = {
        "mode": "blocks",
        "blocks": list(b["key"] for b in CHECKLIST_BLOCKS),
        "raw_by_block": raw_by_block,
    }
    logger.info("Checklist blocks merged: fileName=%s", file_name or "document")
    return merged, openai_debug


def _fill_checklist_defaults(merged: dict) -> None:
    """Ensure all required top-level keys exist (in-place)."""
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


def generate_checklist_blocks_retrieval(
    openai_client: OpenAI,
    normalized_chunks: list[dict],
    file_name: str,
) -> tuple[dict, dict]:
    """Retrieval-driven: one LLM call per block with block-specific context only. Returns (checklist dict, debug payload)."""
    logger.info(
        "Generating checklist by retrieval-driven blocks: fileName=%s chunks=%d blocks=%d",
        file_name or "document", len(normalized_chunks), len(CHECKLIST_BLOCKS),
    )
    chunks_with_embeddings = embed_chunks(openai_client, normalized_chunks)
    merged = {}
    raw_by_block = {}
    blocks_debug = []

    for block in CHECKLIST_BLOCKS:
        name = block["key"]
        query = block.get("query", name.replace("_", " "))
        try:
            context, retrieved_chunks = retrieve_for_block(
                openai_client, query, chunks_with_embeddings, top_k=TOP_K_RETRIEVAL
            )
            block_data, raw = _generate_one_block(openai_client, block, context, file_name)
            raw_by_block[name] = block_data
            if name == "modalidade_participacao":
                merged["modalidadeLicitacao"] = block_data.get("modalidadeLicitacao", "")
                merged["participacao"] = block_data.get("participacao") or {}
            else:
                _deep_merge_checklist(merged, block_data)
            llm_input = (
                f"Trechos do documento ({file_name or 'document'}):\n\n{context}\n\n"
                "Extraia apenas a parte do checklist correspondente a este bloco com base EXCLUSIVAMENTE nos trechos acima. Retorne em JSON."
            )
            blocks_debug.append({
                "block": name,
                "query": query,
                "retrieved_chunks": [{"chunk_id": c.get("chunk_id"), "page": c.get("page_number"), "text_preview": (c.get("text") or "")[:200]} for c in retrieved_chunks],
                "context_len": len(context),
                "llm_input": llm_input[:8000],
                "llm_output": raw[:2000] if raw else "",
            })
        except Exception as e:
            logger.warning("Block %s failed: %s", name, e)
            raw_by_block[name] = {"_error": str(e)}
            blocks_debug.append({"block": name, "query": query, "error": str(e)})

    _fill_checklist_defaults(merged)
    merged = normalize_checklist_result(merged)
    openai_debug = {
        "mode": "blocks_retrieval",
        "blocks": list(b["key"] for b in CHECKLIST_BLOCKS),
        "raw_by_block": raw_by_block,
        "blocks_debug": blocks_debug,
    }
    logger.info("Checklist blocks (retrieval) merged: fileName=%s", file_name or "document")
    return merged, openai_debug


# --- Post-processing: dates, currency, dedup, boolean defaults ---
DATE_DDMMYYYY = re.compile(r"(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{4})")


def _normalize_date(s: str) -> str:
    """Normalize date to DD/MM/YYYY when possible."""
    if not s or not isinstance(s, str):
        return s or ""
    s = s.strip()
    m = DATE_DDMMYYYY.search(s)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{int(d):02d}/{int(mo):02d}/{y}"
    return s


def _normalize_currency(s: str) -> str:
    """Ensure currency is prefixed with R$ when it looks like a value."""
    if not s or not isinstance(s, str):
        return s or ""
    s = s.strip()
    if s and re.search(r"[\d.,]+", s) and not s.upper().startswith("R$"):
        return "R$ " + s
    return s


def normalize_checklist_result(data: dict) -> dict:
    """Apply accuracy safeguards: normalize dates (DD/MM/YYYY), currency (R$), dedup document items, boolean defaults."""
    if not data:
        return data
    # Edital dates
    ed = data.get("edital") or {}
    if isinstance(ed, dict):
        for key in ("dataSessao",):
            if key in ed and ed[key]:
                ed[key] = _normalize_date(ed[key])
        for key in ("totalReais", "valorEnergia", "volumeEnergia"):
            if key in ed and ed[key]:
                ed[key] = _normalize_currency(ed[key])
    # Prazos dates
    prazos = data.get("prazos") or {}
    if isinstance(prazos, dict):
        for sub in ("enviarPropostaAte", "esclarecimentosAte", "impugnacaoAte"):
            obj = prazos.get(sub)
            if isinstance(obj, dict) and obj.get("data"):
                obj["data"] = _normalize_date(obj["data"])
    # Participação booleans
    part = data.get("participacao") or {}
    if isinstance(part, dict):
        part.setdefault("permiteConsorcio", False)
        part.setdefault("beneficiosMPE", False)
        if "permiteConsorcio" in part and not isinstance(part["permiteConsorcio"], bool):
            part["permiteConsorcio"] = bool(part["permiteConsorcio"])
        if "beneficiosMPE" in part and not isinstance(part["beneficiosMPE"], bool):
            part["beneficiosMPE"] = bool(part["beneficiosMPE"])
    data.setdefault("visitaTecnica", False)
    if not isinstance(data.get("visitaTecnica"), bool):
        data["visitaTecnica"] = bool(data.get("visitaTecnica"))
    # Remove duplicated document items (by documento text)
    docs = data.get("documentos") or []
    if isinstance(docs, list):
        seen = set()
        out = []
        for cat in docs:
            if not isinstance(cat, dict):
                out.append(cat)
                continue
            itens = cat.get("itens") or []
            new_itens = []
            for it in itens:
                if not isinstance(it, dict):
                    new_itens.append(it)
                    continue
                doc_text = (it.get("documento") or "").strip()
                key = (doc_text, it.get("referencia", ""))
                if key in seen:
                    continue
                seen.add(key)
                new_itens.append(it)
            out.append({**cat, "itens": new_itens})
        data["documentos"] = out
    return data


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


def _detect_section_hint(text: str) -> str:
    """Detect section heading from text (first ~500 chars) for Brazilian bidding docs."""
    if not text or not text.strip():
        return ""
    sample = text.strip()[:500] + "\n"
    for pattern, label in HEADING_PATTERNS:
        if pattern.search(sample):
            return label
    return ""


def _split_into_size_chunks(
    text: str, page_number: int | None, section_hint: str, chunk_id_prefix: str
) -> list[dict]:
    """Split text into chunks of CHUNK_MIN_CHARS–CHUNK_MAX_CHARS; preserve section_hint on first chunk."""
    text = (text or "").strip()
    if not text:
        return []
    chunks_out = []
    start = 0
    idx = 0
    while start < len(text):
        end = min(start + CHUNK_MAX_CHARS, len(text))
        if end < len(text):
            # Prefer break at paragraph or sentence
            break_at = text.rfind("\n\n", start, end + 1)
            if break_at == -1:
                break_at = text.rfind(". ", start, end + 1)
            if break_at == -1:
                break_at = text.rfind(" ", start, end + 1)
            if break_at > start and (break_at - start) >= CHUNK_MIN_CHARS:
                end = break_at + 1
        chunk_text = text[start:end].strip()
        if chunk_text:
            chunks_out.append({
                "text": chunk_text,
                "page_number": page_number,
                "section_hint": section_hint if idx == 0 else "",
                "chunk_id": f"{chunk_id_prefix}_{idx}",
            })
            idx += 1
        start = end
    return chunks_out


def parse_file_to_normalized_chunks(file_path: str, file_name: str) -> tuple[list[dict], dict]:
    """Parse file with unstructured, return normalized chunks (800–1200 chars) with metadata.
    Merges consecutive elements into chunks of CHUNK_MIN_CHARS–CHUNK_MAX_CHARS to avoid
    thousands of tiny chunks. Each chunk: { text, page_number, section_hint, chunk_id }.
    """
    logger.info("Parsing file to normalized chunks: path=%s fileName=%s", file_path, file_name or "document")
    elements = partition(filename=file_path, languages=PARTITION_LANGUAGES)
    logger.info("Partition produced %d elements", len(elements))
    # Build (text, page_number, section_hint) per element, then merge into 800–1200 char chunks
    segment_list = []
    for el in elements:
        text = getattr(el, "text", None) or str(el)
        if not text or not text.strip():
            continue
        meta = getattr(el, "metadata", None)
        page_number = getattr(meta, "page_number", None) if meta else None
        if page_number is not None and not isinstance(page_number, int):
            try:
                page_number = int(page_number)
            except (TypeError, ValueError):
                page_number = None
        section_hint = _detect_section_hint(text)
        segment_list.append({"text": text.strip(), "page_number": page_number, "section_hint": section_hint})
    # Merge segments into chunks of target size (800–1200 chars)
    all_chunks = []
    elements_debug = [{"segment_count": len(segment_list)}]
    base_id = str(uuid.uuid4())[:8]
    buf_text = []
    buf_page = None
    buf_hint = ""
    buf_len = 0
    chunk_idx = 0
    for seg in segment_list:
        t = seg["text"]
        if buf_page is None:
            buf_page = seg.get("page_number")
        if not buf_hint and seg.get("section_hint"):
            buf_hint = seg.get("section_hint")
        buf_text.append(t)
        buf_len = len(CHUNK_SEP.join(buf_text))
        # Flush when we have enough or adding more would exceed max
        should_flush = buf_len >= CHUNK_MIN_CHARS
        if should_flush or (buf_len > CHUNK_MAX_CHARS):
            combined = CHUNK_SEP.join(buf_text)
            if len(combined) > CHUNK_MAX_CHARS:
                sub_chunks = _split_into_size_chunks(combined, buf_page, buf_hint or _detect_section_hint(combined), f"{base_id}_c{chunk_idx}")
                for c in sub_chunks:
                    all_chunks.append(c)
                    chunk_idx += 1
            else:
                all_chunks.append({
                    "text": combined,
                    "page_number": buf_page,
                    "section_hint": buf_hint or _detect_section_hint(combined),
                    "chunk_id": f"{base_id}_c{chunk_idx}",
                })
                chunk_idx += 1
            buf_text = []
            buf_len = 0
            buf_page = None
            buf_hint = ""
    if buf_text:
        combined = CHUNK_SEP.join(buf_text)
        sub_chunks = _split_into_size_chunks(combined, buf_page, buf_hint or _detect_section_hint(combined), f"{base_id}_c{chunk_idx}")
        for c in sub_chunks:
            all_chunks.append(c)
    if not all_chunks:
        try:
            with open(file_path, "r", errors="replace") as f:
                raw = f.read(50000)
                if raw.strip():
                    all_chunks = [{
                        "text": raw.strip()[:CHUNK_MAX_CHARS],
                        "page_number": None,
                        "section_hint": "",
                        "chunk_id": f"{base_id}_fallback",
                    }]
        except Exception:
            pass
        if not all_chunks:
            all_chunks = [{
                "text": "(no text extracted)",
                "page_number": None,
                "section_hint": "",
                "chunk_id": f"{base_id}_empty",
            }]
    logger.info("Normalized chunks: %d (avg ~%d chars)", len(all_chunks), sum(len(c["text"]) for c in all_chunks) // max(1, len(all_chunks)))
    debug_payload = {
        "fileName": file_name or "document",
        "elementCount": len(elements),
        "elements": elements_debug,
        "chunkCount": len(all_chunks),
    }
    return all_chunks, debug_payload


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


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two vectors (pure Python, no numpy)."""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


# OpenAI embeddings API accepts at most 2048 inputs per request.
EMBEDDING_BATCH_SIZE = 2048


def embed_chunks(openai_client: OpenAI, chunks: list[dict]) -> list[tuple[dict, list[float]]]:
    """Embed each chunk's text with text-embedding-3-large. Batches requests to respect API limit (2048 inputs)."""
    if not chunks:
        return []
    # Ensure non-empty strings; API rejects invalid input
    chunks = [c for c in chunks if (c.get("text") or "").strip()]
    if not chunks:
        return []
    texts = [c["text"].strip() for c in chunks]
    logger.info("Embedding %d chunks with %s (batch size %d)", len(texts), EMBEDDING_MODEL, EMBEDDING_BATCH_SIZE)
    out = []
    for start in range(0, len(texts), EMBEDDING_BATCH_SIZE):
        batch_texts = texts[start : start + EMBEDDING_BATCH_SIZE]
        batch_chunks = chunks[start : start + EMBEDDING_BATCH_SIZE]
        resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=batch_texts)
        by_idx = {e.index: e.embedding for e in resp.data}
        for i, ch in enumerate(batch_chunks):
            emb = by_idx.get(i, [])
            out.append((ch, emb))
    return out


def embed_query(openai_client: OpenAI, query: str) -> list[float]:
    """Embed a single query string."""
    resp = openai_client.embeddings.create(model=EMBEDDING_MODEL, input=[query.strip() or " "])
    if resp.data:
        return resp.data[0].embedding
    return []


def retrieve_for_block(
    openai_client: OpenAI,
    query: str,
    chunks_with_embeddings: list[tuple[dict, list[float]]],
    top_k: int = TOP_K_RETRIEVAL,
) -> tuple[str, list[dict]]:
    """Run vector search for block query; return (context string, list of retrieved chunk dicts for debug)."""
    if not chunks_with_embeddings:
        return "", []
    query_emb = embed_query(openai_client, query)
    if not query_emb:
        return "", []
    scored = [
        (_cosine_similarity(emb, query_emb), ch)
        for ch, emb in chunks_with_embeddings
        if emb
    ]
    scored.sort(key=lambda x: -x[0])
    top = scored[:top_k]
    retrieved = [ch for _, ch in top]
    context = CHUNK_SEP.join(c["text"] for c in retrieved)
    logger.debug("Retrieval query=%r top_k=%d retrieved=%d context_len=%d", query[:50], top_k, len(retrieved), len(context))
    return context, retrieved


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


def _extract_output_text_from_response(resp) -> str:
    """Extract output_text from OpenAI Responses API response."""
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
    return raw


def _generate_one_block_from_pdf_file(
    openai_client: OpenAI, file_id: str, block: dict, file_name: str
) -> tuple[dict, str, object]:
    """One Responses API call with PDF file_id and block-specific schema/instructions. Returns (block result dict, raw JSON string, response)."""
    name = block["key"]
    schema = block["schema"]
    system = block["system_prompt"]
    user_instruction = (
        f"Com base no documento (edital de licitação) anexado, extraia APENAS a parte do checklist correspondente a este bloco. "
        f"Retorne em JSON estrito conforme o schema. Documento: {file_name or 'document'}."
    )
    input_content = [
        {"type": "input_file", "file_id": file_id},
        {"type": "input_text", "text": user_instruction},
    ]
    resp = openai_client.responses.create(
        model=CHAT_MODEL,
        instructions=system,
        input=[{"role": "user", "content": input_content}],
        text={
            "format": {
                "type": "json_schema",
                "name": f"checklist_block_{name}",
                "strict": True,
                "schema": schema,
            }
        },
    )
    raw = _extract_output_text_from_response(resp)
    if not raw:
        raise ValueError(f"No output_text in Responses API response for block {name}")
    data = json.loads(raw)
    return data, raw, resp


def generate_checklist_from_pdf_file(
    openai_client: OpenAI, pdf_path: str, file_name: str
) -> tuple[dict, dict]:
    """Send PDF as file to OpenAI Responses API; one call per CHECKLIST_BLOCK, then merge. Returns (checklist dict, debug payload)."""
    logger.info(
        "Generating checklist from PDF file (blocks): fileName=%s path=%s blocks=%d",
        file_name or "document", pdf_path, len(CHECKLIST_BLOCKS),
    )
    file_id = _upload_pdf_to_openai(openai_client, pdf_path, file_name or "document.pdf")
    merged = {}
    raw_by_block = {}
    total_usage = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    last_model = CHAT_MODEL
    for block in CHECKLIST_BLOCKS:
        name = block["key"]
        try:
            block_data, raw, resp = _generate_one_block_from_pdf_file(openai_client, file_id, block, file_name)
            raw_by_block[name] = block_data
            if name == "modalidade_participacao":
                merged["modalidadeLicitacao"] = block_data.get("modalidadeLicitacao", "")
                merged["participacao"] = block_data.get("participacao") or {}
            else:
                _deep_merge_checklist(merged, block_data)
            usage = getattr(resp, "usage", None)
            if usage:
                total_usage["prompt_tokens"] += getattr(usage, "input_tokens", None) or getattr(usage, "prompt_tokens", None) or 0
                total_usage["completion_tokens"] += getattr(usage, "output_tokens", None) or getattr(usage, "completion_tokens", None) or 0
                total_usage["total_tokens"] += getattr(usage, "total_tokens", None) or 0
            last_model = getattr(resp, "model", last_model)
        except Exception as e:
            if "input_file" in str(e).lower() or "file" in str(e).lower():
                logger.warning(
                    "Responses API with input_file may require a vision model (e.g. gpt-4o). Block %s failed: %s",
                    name, e,
                )
            logger.warning("Block %s failed (PDF file): %s", name, e)
            raw_by_block[name] = {"_error": str(e)}
    _fill_checklist_defaults(merged)
    merged = normalize_checklist_result(merged)
    openai_debug = {
        "mode": "pdf_file_blocks",
        "model": last_model,
        "usage": total_usage if total_usage["total_tokens"] else None,
        "blocks": [b["key"] for b in CHECKLIST_BLOCKS],
        "raw_by_block": raw_by_block,
    }
    logger.info("Checklist generated from PDF file (blocks): fileName=%s", file_name or "document")
    return merged, openai_debug


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
            conn = get_conn()
            try:
                if USE_CHECKLIST_BLOCKS:
                    # Retrieval-driven block extraction: normalized chunks → embeddings → one LLM call per block with block-specific context
                    logger.info("Using retrieval-driven block extraction for documentId=%s", document_id)
                    normalized_chunks, unstructured_debug = parse_file_to_normalized_chunks(temp_path, file_name)
                    upload_debug_json(user_id, document_id, unstructured_debug)
                    if not normalized_chunks:
                        raise ValueError("No content extracted")
                    checklist_data, checklist_openai_debug = generate_checklist_blocks_retrieval(
                        openai_client, normalized_chunks, file_name
                    )
                else:
                    # Legacy: full-document or vector-store context, single or multi-call
                    logger.info("Parsing file for documentId=%s (legacy path)", document_id)
                    chunks_text, unstructured_debug = parse_file(temp_path, file_name)
                    upload_debug_json(user_id, document_id, unstructured_debug)
                    if not chunks_text:
                        raise ValueError("No content extracted")
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
                    checklist_data, checklist_openai_debug = generate_checklist(openai_client, context, file_name)
                    checklist_data = normalize_checklist_result(checklist_data)

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
