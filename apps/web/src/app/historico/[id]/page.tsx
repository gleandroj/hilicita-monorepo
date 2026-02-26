"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { apiFetch } from "@/lib/api";
import { useAuth } from "@/hooks/useAuth";
import Header from "@/components/Header";
import ChecklistResult, { type ChecklistData } from "@/components/ChecklistResult";
import ProtectedRoute from "@/components/ProtectedRoute";
import { Button } from "@/components/ui/button";
import { ArrowLeft } from "lucide-react";

interface SavedChecklist {
  id: string;
  file_name: string;
  data: ChecklistData;
  pontuacao: number | null;
  orgao: string | null;
  objeto: string | null;
  valor_total: string | null;
  created_at: string;
}

function ChecklistDetailContent() {
  const params = useParams();
  const router = useRouter();
  const id = params?.id as string | undefined;
  const [checklist, setChecklist] = useState<SavedChecklist | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { user } = useAuth();

  useEffect(() => {
    if (!user || !id) {
      if (!id) setError("ID não informado");
      setLoading(false);
      return;
    }
    apiFetch<SavedChecklist>(`/checklists/${id}`)
      .then((data) => setChecklist(data))
      .catch((err) => setError(err instanceof Error ? err.message : "Erro ao carregar checklist"))
      .finally(() => setLoading(false));
  }, [user, id]);

  if (!id) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <main className="container mx-auto px-6 py-8 max-w-4xl">
          <p className="text-muted-foreground">ID não informado.</p>
          <Button variant="link" asChild className="mt-2 pl-0">
            <Link href="/historico">
              <ArrowLeft className="h-4 w-4 mr-2" />
              Voltar ao histórico
            </Link>
          </Button>
        </main>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <main className="container mx-auto px-6 py-8 max-w-4xl">
          <div className="space-y-3">
            <div className="h-8 w-48 rounded bg-muted/50 animate-pulse" />
            <div className="h-64 rounded-xl bg-muted/50 animate-pulse" />
          </div>
        </main>
      </div>
    );
  }

  if (error || !checklist) {
    return (
      <div className="min-h-screen bg-background">
        <Header />
        <main className="container mx-auto px-6 py-8 max-w-4xl space-y-4">
          <p className="text-destructive">{error ?? "Checklist não encontrado."}</p>
          <Button asChild>
            <Link href="/historico">
              <ArrowLeft className="h-4 w-4 mr-2" />
              Voltar ao histórico
            </Link>
          </Button>
        </main>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-6 py-8 max-w-4xl space-y-6">
        <Button variant="ghost" size="sm" asChild className="inline-flex gap-2 text-muted-foreground hover:text-foreground font-medium">
          <Link href="/historico">
            <ArrowLeft className="h-4 w-4" />
            Voltar ao histórico
          </Link>
        </Button>
        <ChecklistResult data={checklist.data} fileName={checklist.file_name} />
      </main>
    </div>
  );
}

export default function ChecklistDetailPage() {
  return (
    <ProtectedRoute>
      <ChecklistDetailContent />
    </ProtectedRoute>
  );
}
