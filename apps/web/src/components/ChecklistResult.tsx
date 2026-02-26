import { CheckCircle2, XCircle, AlertCircle, Download, Building2, Calendar, MapPin, DollarSign, Zap } from "lucide-react";
import { motion } from "framer-motion";
import { Badge } from "@/components/ui/badge";

export interface ChecklistData {
  edital: {
    orgao: string;
    objeto: string;
    dataSessao: string;
    portal: string;
    valorTotal: string;
    vigencia: string;
    modalidade: string;
    concessionaria: string;
    consumoMensal: string;
    consumoAnual: string;
  };
  participacao: {
    permiteConsorcio: boolean;
    beneficiosMPE: boolean;
  };
  prazos: {
    proposta: string;
    esclarecimentos: string;
    impugnacao: string;
  };
  visitaTecnica: boolean;
  documentos: Array<{
    categoria: string;
    itens: Array<{
      referencia: string;
      descricao: string;
      solicitado: boolean;
      envelope: string;
      observacao?: string;
    }>;
  }>;
  pontuacao?: number;
  recomendacao?: string;
}

interface ChecklistResultProps {
  data: ChecklistData;
  fileName: string;
}

const ChecklistResult = ({ data, fileName }: ChecklistResultProps) => {
  const scoreColor = (data.pontuacao ?? 0) >= 70 ? "text-success" : (data.pontuacao ?? 0) >= 40 ? "text-warning" : "text-destructive";

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
        <InfoCard icon={<Building2 className="h-4 w-4" />} label="Órgão" value={data.edital.orgao} />
        <InfoCard icon={<DollarSign className="h-4 w-4" />} label="Valor Total" value={data.edital.valorTotal} />
        <InfoCard icon={<Calendar className="h-4 w-4" />} label="Data da Sessão" value={data.edital.dataSessao} />
        <InfoCard icon={<MapPin className="h-4 w-4" />} label="Portal" value={data.edital.portal} />
        <InfoCard icon={<Zap className="h-4 w-4" />} label="Consumo Mensal" value={data.edital.consumoMensal} />
        <InfoCard icon={<Zap className="h-4 w-4" />} label="Consumo Anual" value={data.edital.consumoAnual} />
      </div>

      {/* Objeto */}
      <div className="rounded-xl bg-card p-5 shadow-card border border-border">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-2">Objeto</p>
        <p className="text-sm text-foreground leading-relaxed">{data.edital.objeto}</p>
      </div>

      {/* Modalidade & Participação */}
      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <div className="rounded-xl bg-card p-5 shadow-card border border-border space-y-3">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Modalidade</p>
          <p className="font-display font-semibold text-foreground">{data.edital.modalidade}</p>
          <p className="text-sm text-muted-foreground">{data.edital.concessionaria}</p>
          <p className="text-sm text-muted-foreground">Vigência: {data.edital.vigencia}</p>
        </div>
        <div className="rounded-xl bg-card p-5 shadow-card border border-border space-y-3">
          <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">Participação</p>
          <StatusBadge label="Consórcio" active={data.participacao.permiteConsorcio} />
          <StatusBadge label="Benefícios MPE" active={data.participacao.beneficiosMPE} />
          <StatusBadge label="Visita Técnica Obrigatória" active={data.visitaTecnica} />
        </div>
      </div>

      {/* Prazos */}
      <div className="rounded-xl bg-card p-5 shadow-card border border-border">
        <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">Prazos</p>
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
          <PrazoItem label="Proposta" value={data.prazos.proposta} />
          <PrazoItem label="Esclarecimentos" value={data.prazos.esclarecimentos} />
          <PrazoItem label="Impugnação" value={data.prazos.impugnacao} />
        </div>
      </div>

      {/* Document Checklist */}
      {data.documentos.map((cat, i) => (
        <motion.div
          key={cat.categoria}
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: i * 0.1 }}
          className="rounded-xl bg-card shadow-card border border-border overflow-hidden"
        >
          <div className="border-b border-border bg-muted/30 px-5 py-3">
            <h3 className="font-display font-semibold text-foreground">{cat.categoria}</h3>
          </div>
          <div className="divide-y divide-border">
            {cat.itens.map((item, j) => (
              <div key={j} className="flex items-start gap-3 px-5 py-3">
                {item.solicitado ? (
                  <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-success" />
                ) : (
                  <XCircle className="mt-0.5 h-5 w-5 shrink-0 text-destructive" />
                )}
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    {item.referencia && (
                      <Badge variant="outline" className="shrink-0 text-xs">
                        {item.referencia}
                      </Badge>
                    )}
                    <p className="text-sm text-foreground">{item.descricao}</p>
                  </div>
                  {item.observacao && (
                    <p className="mt-1 text-xs text-muted-foreground">{item.observacao}</p>
                  )}
                </div>
                {item.envelope && (
                  <span className="shrink-0 text-xs text-muted-foreground">{item.envelope}</span>
                )}
              </div>
            ))}
          </div>
        </motion.div>
      ))}
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
