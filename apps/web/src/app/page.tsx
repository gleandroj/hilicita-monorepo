"use client";

import { useState } from "react";
import { useToast } from "@/hooks/use-toast";
import { useAuth } from "@/hooks/useAuth";
import Header from "@/components/Header";
import PdfUploader from "@/components/PdfUploader";
import ChecklistResult, { type ChecklistData } from "@/components/ChecklistResult";
import ProtectedRoute from "@/components/ProtectedRoute";
import { motion } from "framer-motion";
import { FileText, Zap, Shield, BarChart3 } from "lucide-react";
import { API_URL } from "@/lib/api";

export default function Home() {
  return (
    <ProtectedRoute>
      <HomeContent />
    </ProtectedRoute>
  );
}

function HomeContent() {
  const [isProcessing, setIsProcessing] = useState(false);
  const [checklistData, setChecklistData] = useState<ChecklistData | null>(null);
  const [fileName, setFileName] = useState("");
  const { toast } = useToast();
  const { user } = useAuth();

  const handleExtract = async (file: File) => {
    setIsProcessing(true);
    setFileName(file.name);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const uploadRes = await fetch(`${API_URL}/documents/upload`, {
        method: "POST",
        credentials: "include",
        body: formData,
      });

      if (!uploadRes.ok) {
        const err = await uploadRes.json().catch(() => ({}));
        throw new Error((err as { message?: string }).message ?? "Falha no upload");
      }

      const { documentId } = (await uploadRes.json()) as { documentId: string };

      // Poll status until done
      let status = "pending";
      for (let i = 0; i < 120; i++) {
        const statusRes = await fetch(`${API_URL}/documents/${documentId}/status`, {
          credentials: "include",
        });
        const data = (await statusRes.json()) as { status: string };
        status = data.status;
        if (status === "done") break;
        if (status === "failed") throw new Error("Processamento do documento falhou.");
        await new Promise((r) => setTimeout(r, 2000));
      }

      if (status !== "done") {
        throw new Error("Tempo esgotado aguardando processamento.");
      }

      const genRes = await fetch(`${API_URL}/checklists/generate`, {
        method: "POST",
        credentials: "include",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ documentId, fileName: file.name }),
      });

      if (!genRes.ok) {
        const err = await genRes.json().catch(() => ({}));
        throw new Error((err as { message?: string }).message ?? "Falha ao gerar checklist");
      }

      const { checklist } = (await genRes.json()) as { checklist: ChecklistData };
      setChecklistData(checklist);

      toast({
        title: "Extração concluída!",
        description: user
          ? "O checklist foi gerado e salvo no histórico."
          : "O checklist foi gerado. Faça login para salvar.",
      });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "Não foi possível processar o edital. Tente novamente.";
      toast({
        title: "Erro na extração",
        description: message,
        variant: "destructive",
      });
    } finally {
      setIsProcessing(false);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container mx-auto px-6 py-8">
        {!checklistData ? (
          <div className="mx-auto max-w-2xl space-y-10">
            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              className="text-center space-y-4"
            >
              <h2 className="font-display text-4xl font-bold tracking-tight text-foreground sm:text-5xl">
                Extraia editais em{" "}
                <span className="text-primary">minutos</span>
              </h2>
              <p className="text-lg text-muted-foreground max-w-lg mx-auto">
                Faça upload do PDF ou CSV do edital e a IA irá gerar automaticamente o checklist com todos os requisitos e documentos necessários.
              </p>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 }}
            >
              <PdfUploader onExtract={handleExtract} isProcessing={isProcessing} />
            </motion.div>

            <motion.div
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.2 }}
              className="grid grid-cols-1 gap-4 sm:grid-cols-3"
            >
              <FeatureCard icon={<Zap className="h-5 w-5" />} title="Extração Rápida" description="De horas para minutos com IA" />
              <FeatureCard icon={<Shield className="h-5 w-5" />} title="95% de Precisão" description="Correspondência automática de documentos" />
              <FeatureCard icon={<BarChart3 className="h-5 w-5" />} title="Pontuação" description="Avaliação automática de oportunidades" />
            </motion.div>
          </div>
        ) : (
          <div className="mx-auto max-w-4xl space-y-6">
            <button
              onClick={() => setChecklistData(null)}
              className="inline-flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground transition-colors font-medium"
            >
              <FileText className="h-4 w-4" />
              ← Novo edital
            </button>
            <ChecklistResult data={checklistData} fileName={fileName} />
          </div>
        )}
      </main>
    </div>
  );
}

function FeatureCard({
  icon,
  title,
  description,
}: { icon: React.ReactNode; title: string; description: string }) {
  return (
    <div className="rounded-xl bg-card p-5 shadow-card border border-border text-center space-y-2">
      <div className="mx-auto flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 text-primary">
        {icon}
      </div>
      <h3 className="font-display font-semibold text-foreground text-sm">{title}</h3>
      <p className="text-xs text-muted-foreground">{description}</p>
    </div>
  );
}
