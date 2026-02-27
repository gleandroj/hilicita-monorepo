import { CheckCircle2, XCircle, AlertCircle, Building2, Calendar, MapPin, DollarSign, Zap, FileText } from "lucide-react";
import { motion } from "framer-motion";
import { Badge } from "@/components/ui/badge";

/** Estrutura alinhada ao modelo Excel (CHECK LIST - Nº INTERNO - Nº EDITAL - ÓRGÃO). */
export interface ChecklistData {
  edital: {
    licitacao?: string;
    edital?: string;
    orgao: string;
    objeto: string;
    dataSessao: string;
    portal: string;
    numeroProcessoInterno?: string;
    /** Total em R$ (campo principal para listagem) */
    totalReais?: string;
    valorTotal?: string; // compat
    valorEnergia?: string;
    volumeEnergia?: string;
    vigenciaContrato?: string;
    vigencia?: string; // compat
    modalidadeConcessionaria?: string;
    modalidade?: string; // compat
    concessionaria?: string; // compat
    consumoMensal?: string;
    consumoAnual?: string;
    prazoInicioInjecao?: string;
  };
  modalidadeLicitacao?: string;
  participacao: {
    permiteConsorcio: boolean;
    beneficiosMPE: boolean;
    itemEdital?: string;
  };
  prazos: {
    enviarPropostaAte?: { data?: string; horario?: string; raw?: string };
    esclarecimentosAte?: { data?: string; horario?: string; raw?: string };
    impugnacaoAte?: { data?: string; horario?: string; raw?: string };
    contatoEsclarecimentoImpugnacao?: string;
    /** Compat: texto único */
    proposta?: string;
    esclarecimentos?: string;
    impugnacao?: string;
  };
  documentos: Array<{
    categoria: string;
    itens: Array<{
      documento?: string;
      descricao?: string; // compat
      referencia?: string;
      local?: string; // TR | ED
      solicitado: boolean;
      status?: string;
      observacao?: string;
      envelope?: string;
    }>;
  }>;
  visitaTecnica: boolean;
  proposta?: { validadeProposta?: string };
  sessao?: {
    diferencaEntreLances?: string;
    horasPropostaAjustada?: string;
    abertoFechado?: string;
    criterioJulgamento?: string;
    tempoDisputa?: string;
    tempoRandomico?: string;
    faseLances?: string;
    prazoPosLance?: string;
  };
  /** Schema v2: requisitos normalizados (worker também preenche documentos para compat) */
  requisitos?: Array<{
    categoria?: string;
    referencia?: string;
    local?: string;
    descricao?: string;
    obrigatorio?: boolean;
    etapa?: string;
    condicao?: string;
  }>;
  outrosEdital?: { mecanismoPagamento?: string };
  schemaVersion?: number;
  evidence?: Record<string, unknown>;
  responsavelAnalise?: string;
  pontuacao?: number;
  recomendacao?: string;
}

interface ChecklistResultProps {
  data: ChecklistData;
  fileName: string;
}

function hasAnySessaoField(s: ChecklistData["sessao"]): boolean {
  if (!s) return false;
  return !!(
    (s.diferencaEntreLances ?? "").trim() ||
    (s.horasPropostaAjustada ?? "").trim() ||
    (s.abertoFechado ?? "").trim() ||
    (s.criterioJulgamento ?? "").trim() ||
    (s.tempoDisputa ?? "").trim() ||
    (s.tempoRandomico ?? "").trim() ||
    (s.faseLances ?? "").trim() ||
    (s.prazoPosLance ?? "").trim()
  );
}

function formatPrazo(
  p: { data?: string; horario?: string; raw?: string } | string | undefined
): string {
  if (!p) return "";
  if (typeof p === "string") return p;
  if (p.raw && p.raw.trim()) return p.raw.trim();
  const parts = [p.data, p.horario].filter(Boolean);
  return parts.join(" ");
}

/** Remove JSON/evidence fragments that sometimes end up in mecanismoPagamento from the LLM. */
function sanitizeMecanismoPagamento(value: unknown, maxLen = 600): string {
  if (value == null) return "";
  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;
    if (typeof obj.mecanismoPagamento === "string") return sanitizeMecanismoPagamento(obj.mecanismoPagamento, maxLen);
    if (typeof obj.trecho === "string") return obj.trecho.slice(0, maxLen).trim();
    return "";
  }
  let s = String(value).trim();
  if (!s) return "";
  // Strip content that looks like embedded JSON/evidence (e.g. ', 'evidencia":{"trecho":"...')
  const evidenciaIdx = s.search(/\s*[,'"]\s*evidencia\s*[:{]/i);
  if (evidenciaIdx > 5) s = s.slice(0, evidenciaIdx);
  const trechoIdx = s.search(/\s*["']?\s*trecho\s*["']?\s*:/i);
  if (trechoIdx > 5) s = s.slice(0, trechoIdx);
  // Remove trailing JSON/backtick garbage (e.g. }}}'```)
  s = s.replace(/\s*[}'`]+(?:\s*[}'`]+)*\s*$/g, "").trim();
  if (s.length > maxLen) s = s.slice(0, maxLen).trim() + "…";
  return s;
}

const ChecklistResult = ({ data, fileName }: ChecklistResultProps) => {
  const scoreColor = (data.pontuacao ?? 0) >= 70 ? "text-success" : (data.pontuacao ?? 0) >= 40 ? "text-warning" : "text-destructive";
  const ed = data.edital;
  const valorTotal = ed.totalReais ?? ed.valorTotal ?? "";
  const vigencia = ed.vigenciaContrato ?? ed.vigencia ?? "";
  const modalidade = ed.modalidadeConcessionaria ?? ed.modalidade ?? "";
  const concessionaria = ed.concessionaria ?? "";

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4 }}
      className="space-y-6"
    >
      {/* Header with score */}
      <div className="flex items-start justify-between rounded-xl bg-card p-6 shadow-card border border-border">
        <div className="space-y-1">
          <h2 className="font-display text-2xl font-bold text-foreground">
            Checklist Extraído
          </h2>
          <p className="text-sm text-muted-foreground">{fileName}</p>
        </div>
        {data.pontuacao !== undefined && (
          <div className="text-right">
            <p className={`font-display text-4xl font-bold ${scoreColor}`}>
              {data.pontuacao}
            </p>
            <p className="text-xs text-muted-foreground">Pontuação</p>
          </div>
        )}
      </div>

      {/* Recommendation */}
      {data.recomendacao && (
        <div className={`rounded-xl p-4 border ${
          data.recomendacao.toLowerCase().includes("participar")
            ? "bg-primary/5 border-primary/20"
            : "bg-destructive/5 border-destructive/20"
        }`}>
          <div className="flex items-center gap-2">
            <AlertCircle className="h-5 w-5 text-primary" />
            <p className="font-display font-semibold text-foreground">Recomendação IA</p>
          </div>
          <p className="mt-1 text-sm text-muted-foreground">{data.recomendacao}</p>
        </div>
      )}

      {/* Edital Info Cards */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        <InfoCard icon={<Building2 className="h-4 w-4" />} label="Órgão" value={ed.orgao} />
        <InfoCard icon={<DollarSign className="h-4 w-4" />} label="Total (R$)" value={valorTotal} />
        <InfoCard icon={<Calendar className="h-4 w-4" />} label="Data da Sessão" value={ed.dataSessao} />
        <InfoCard icon={<MapPin className="h-4 w-4" />} label="Portal" value={ed.portal} />
        <InfoCard icon={<FileText className="h-4 w-4" />} label="Nº Processo Interno" value={ed.numeroProcessoInterno ?? ""} />
        <InfoCard icon={<Zap className="h-4 w-4" />} label="Valor da Energia" value={ed.valorEnergia ?? ""} />
        <InfoCard icon={<Zap className="h-4 w-4" />} label="Volume de Energia" value={ed.volumeEnergia ?? ""} />
        <InfoCard icon={<Calendar className="h-4 w-4" />} label="Vigência de Contrato" value={vigencia} />
      </div>

      {/* Objeto */}
      <div className="rounded-xl bg-card p-5 shadow-card border border-border">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Objeto</p>
        <p className="text-sm text-foreground leading-relaxed">{ed.objeto || "—"}</p>
      </div>

      {/* Modalidade da Licitação & Participação */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div className="rounded-xl bg-card p-5 shadow-card border border-border space-y-3">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Modalidade da Licitação</p>
          <p className="font-display font-semibold text-foreground">{modalidade || "—"}</p>
          {concessionaria && <p className="text-sm text-muted-foreground">{concessionaria}</p>}
          {ed.prazoInicioInjecao && (
            <p className="text-sm text-muted-foreground">Prazo para início injeção: {ed.prazoInicioInjecao}</p>
          )}
        </div>
        <div className="rounded-xl bg-card p-5 shadow-card border border-border space-y-3">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Participação</p>
          <StatusBadge label="Permite participação em consórcio" active={data.participacao.permiteConsorcio} />
          <StatusBadge label="Benefícios às MPE" active={data.participacao.beneficiosMPE} />
          <StatusBadge label="Visita Técnica Obrigatória" active={data.visitaTecnica} />
          {data.participacao.itemEdital && (
            <p className="text-sm text-muted-foreground pt-1">{data.participacao.itemEdital}</p>
          )}
        </div>
      </div>

      {/* Prazos */}
      <div className="rounded-xl bg-card p-5 shadow-card border border-border">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">Prazos</p>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
          <PrazoItem
            label="Enviar proposta até"
            value={(formatPrazo(data.prazos.enviarPropostaAte) || data.prazos.proposta) ?? ""}
          />
          <PrazoItem
            label="Esclarecimentos até"
            value={(formatPrazo(data.prazos.esclarecimentosAte) || data.prazos.esclarecimentos) ?? ""}
          />
          <PrazoItem
            label="Impugnação até"
            value={(formatPrazo(data.prazos.impugnacaoAte) || data.prazos.impugnacao) ?? ""}
          />
        </div>
        {data.prazos.contatoEsclarecimentoImpugnacao && (
          <p className="mt-3 text-sm text-muted-foreground">
            Contato para envio Esclarecimento/Impugnação: {data.prazos.contatoEsclarecimentoImpugnacao}
          </p>
        )}
      </div>

      {/* Proposta / Sessão / Outros */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
        {data.proposta?.validadeProposta && (
          <div className="rounded-xl bg-card p-4 shadow-card border border-border">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Validade da Proposta</p>
            <p className="text-sm text-foreground mt-1">{data.proposta.validadeProposta}</p>
          </div>
        )}
        {data.sessao && hasAnySessaoField(data.sessao) && (
          <div className="rounded-xl bg-card p-4 shadow-card border border-border space-y-3">
            <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Sessão e disputa</p>
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
              {data.sessao.diferencaEntreLances && (
                <div><span className="text-xs text-muted-foreground">Diferença entre lances</span><p className="text-sm text-foreground">{data.sessao.diferencaEntreLances}</p></div>
              )}
              {data.sessao.horasPropostaAjustada && (
                <div><span className="text-xs text-muted-foreground">Proposta ajustada</span><p className="text-sm text-foreground">{data.sessao.horasPropostaAjustada}</p></div>
              )}
              {data.sessao.abertoFechado && (
                <div><span className="text-xs text-muted-foreground">Aberto/fechado</span><p className="text-sm text-foreground">{data.sessao.abertoFechado}</p></div>
              )}
              {data.sessao.criterioJulgamento && (
                <div><span className="text-xs text-muted-foreground">Critério de julgamento</span><p className="text-sm text-foreground">{data.sessao.criterioJulgamento}</p></div>
              )}
              {data.sessao.tempoDisputa && (
                <div><span className="text-xs text-muted-foreground">Tempo de disputa</span><p className="text-sm text-foreground">{data.sessao.tempoDisputa}</p></div>
              )}
              {data.sessao.tempoRandomico && (
                <div><span className="text-xs text-muted-foreground">Tempo randômico</span><p className="text-sm text-foreground">{data.sessao.tempoRandomico}</p></div>
              )}
              {data.sessao.faseLances && (
                <div><span className="text-xs text-muted-foreground">Fase de lances</span><p className="text-sm text-foreground">{data.sessao.faseLances}</p></div>
              )}
              {data.sessao.prazoPosLance && (
                <div><span className="text-xs text-muted-foreground">Prazo pós-lance</span><p className="text-sm text-foreground">{data.sessao.prazoPosLance}</p></div>
              )}
            </div>
          </div>
        )}
        {(() => {
          const payment = sanitizeMecanismoPagamento(data.outrosEdital?.mecanismoPagamento ?? data.outrosEdital);
          return payment ? (
            <div className="rounded-xl bg-card p-4 shadow-card border border-border">
              <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Mecanismo de Pagamento</p>
              <p className="text-sm text-foreground mt-1 whitespace-pre-wrap break-words">{payment}</p>
            </div>
          ) : null;
        })()}
      </div>

      {/* Document Checklist */}
      {data.documentos?.map((cat, i) => (
        <motion.div
          key={cat.categoria || i}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: i * 0.1 }}
          className="rounded-xl bg-card shadow-card border border-border overflow-hidden"
        >
          <div className="border-b border-border bg-muted/30 px-5 py-3">
            <h3 className="font-display font-semibold text-foreground">{cat.categoria}</h3>
          </div>
          <div className="divide-y divide-border">
            {cat.itens?.map((item, j) => {
              const descricao = item.documento ?? item.descricao ?? "";
              return (
                <div key={j} className="flex items-start gap-3 px-5 py-3">
                  {item.solicitado ? (
                    <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-success" />
                  ) : (
                    <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-destructive" />
                  )}
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      {(item.referencia || item.local) && (
                        <Badge variant="outline" className="shrink-0 text-xs">
                          {[item.referencia, item.local].filter(Boolean).join(" · ")}
                        </Badge>
                      )}
                      <p className="text-sm text-foreground">{descricao}</p>
                    </div>
                    {(item.observacao || item.status) && (
                      <p className="mt-1 text-xs text-muted-foreground">
                        {[item.status, item.observacao].filter(Boolean).join(" · ")}
                      </p>
                    )}
                  </div>
                  {item.envelope && (
                    <span className="shrink-0 text-xs text-muted-foreground">{item.envelope}</span>
                  )}
                </div>
              );
            })}
          </div>
        </motion.div>
      ))}

      {data.responsavelAnalise && (
        <p className="text-xs text-muted-foreground">Responsável pela análise: {data.responsavelAnalise}</p>
      )}
    </motion.div>
  );
};

const InfoCard = ({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) => (
  <div className="rounded-xl bg-card p-4 shadow-card border border-border">
    <div className="flex items-center gap-2 text-muted-foreground mb-1">
      {icon}
      <span className="text-xs font-medium uppercase tracking-wider">{label}</span>
    </div>
    <p className="font-display font-semibold text-foreground text-sm">{value || "—"}</p>
  </div>
);

const StatusBadge = ({ label, active }: { label: string; active: boolean }) => (
  <div className="flex items-center gap-2">
    {active ? (
      <CheckCircle2 className="h-4 w-4 text-success" />
    ) : (
      <XCircle className="h-4 w-4 text-destructive" />
    )}
    <span className="text-sm text-foreground">{label}</span>
  </div>
);

const PrazoItem = ({ label, value }: { label: string; value: string }) => (
  <div className="rounded-lg bg-muted/50 p-3">
    <p className="text-xs text-muted-foreground">{label}</p>
    <p className="font-display font-medium text-foreground text-sm">{value || "—"}</p>
  </div>
);

export default ChecklistResult;
