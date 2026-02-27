"use client";

import { useState, useEffect, useMemo } from "react";
import Link from "next/link";
import { apiFetch, apiDelete } from "@/lib/api";
import { useAuth } from "@/hooks/useAuth";
import Header from "@/components/Header";
import { type ChecklistData } from "@/components/ChecklistResult";
import ProtectedRoute from "@/components/ProtectedRoute";
import { motion } from "framer-motion";
import { Calendar, Building2, DollarSign, Eye, Trash2, FileText, Search, SlidersHorizontal, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { Calendar as CalendarPicker } from "@/components/ui/calendar";
import { useToast } from "@/hooks/use-toast";
import { format, isAfter, isBefore, startOfDay, endOfDay } from "date-fns";
import { ptBR } from "date-fns/locale";
import { cn } from "@/lib/utils";

interface SavedChecklist {
  id: string;
  file_name: string;
  data: ChecklistData;
  pontuacao: number | null;
  orgao: string | null;
  objeto: string | null;
  valor_total: string | null;
  processedWithPdfMode?: boolean | null;
  created_at: string;
}

function HistoryContent() {
  const [checklists, setChecklists] = useState<SavedChecklist[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState("");
  const [scoreFilter, setScoreFilter] = useState<string>("all");
  const [dateFrom, setDateFrom] = useState<Date | undefined>();
  const [dateTo, setDateTo] = useState<Date | undefined>();
  const { user } = useAuth();
  const { toast } = useToast();

  const filtered = useMemo(() => {
    return checklists.filter((c) => {
      const term = searchTerm.toLowerCase();
      const matchesSearch =
        !term ||
        c.file_name.toLowerCase().includes(term) ||
        c.orgao?.toLowerCase().includes(term) ||
        c.objeto?.toLowerCase().includes(term);
      const matchesScore =
        scoreFilter === "all" ||
        (scoreFilter === "high" && (c.pontuacao ?? 0) >= 70) ||
        (scoreFilter === "medium" && (c.pontuacao ?? 0) >= 40 && (c.pontuacao ?? 0) < 70) ||
        (scoreFilter === "low" && (c.pontuacao ?? 0) < 40);
      const createdAt = new Date(c.created_at);
      const matchesDateFrom = !dateFrom || !isBefore(createdAt, startOfDay(dateFrom));
      const matchesDateTo = !dateTo || !isAfter(createdAt, endOfDay(dateTo));
      return matchesSearch && matchesScore && matchesDateFrom && matchesDateTo;
    });
  }, [checklists, searchTerm, scoreFilter, dateFrom, dateTo]);

  const hasActiveFilters = searchTerm || scoreFilter !== "all" || dateFrom || dateTo;

  const clearFilters = () => {
    setSearchTerm("");
    setScoreFilter("all");
    setDateFrom(undefined);
    setDateTo(undefined);
  };

  useEffect(() => {
    if (user) {
      apiFetch<SavedChecklist[]>("/checklists")
        .then((data) => setChecklists(Array.isArray(data) ? data : []))
        .catch(() => setChecklists([]))
        .finally(() => setLoading(false));
    }
  }, [user]);

  const handleDelete = async (e: React.MouseEvent, id: string) => {
    e.preventDefault();
    e.stopPropagation();
    try {
      await apiDelete(`/checklists/${id}`);
      setChecklists((prev) => prev.filter((c) => c.id !== id));
      toast({ title: "Checklist excluído" });
    } catch (err) {
      toast({ title: "Erro ao excluir", description: err instanceof Error ? err.message : "Erro", variant: "destructive" });
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-6 py-8 max-w-4xl">
        <motion.div initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} className="space-y-6">
          <div>
            <h2 className="font-display text-3xl font-bold text-foreground">Histórico</h2>
            <p className="text-sm text-muted-foreground">
              {filtered.length} de {checklists.length} {checklists.length === 1 ? "checklist" : "checklists"}
            </p>
          </div>

          <div className="space-y-3">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar por nome, órgão ou objeto..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-9"
              />
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Select value={scoreFilter} onValueChange={setScoreFilter}>
                <SelectTrigger className="w-[160px]">
                  <SlidersHorizontal className="h-3.5 w-3.5 mr-1.5" />
                  <SelectValue placeholder="Pontuação" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">Todas</SelectItem>
                  <SelectItem value="high">Alta (≥70)</SelectItem>
                  <SelectItem value="medium">Média (40-69)</SelectItem>
                  <SelectItem value="low">Baixa (&lt;40)</SelectItem>
                </SelectContent>
              </Select>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn("text-sm", dateFrom && "text-foreground")}>
                    <Calendar className="h-3.5 w-3.5 mr-1.5" />
                    {dateFrom ? format(dateFrom, "dd/MM/yy") : "De"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarPicker mode="single" selected={dateFrom} onSelect={setDateFrom} locale={ptBR} className="p-3 pointer-events-auto" />
                </PopoverContent>
              </Popover>
              <Popover>
                <PopoverTrigger asChild>
                  <Button variant="outline" size="sm" className={cn("text-sm", dateTo && "text-foreground")}>
                    <Calendar className="h-3.5 w-3.5 mr-1.5" />
                    {dateTo ? format(dateTo, "dd/MM/yy") : "Até"}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0" align="start">
                  <CalendarPicker mode="single" selected={dateTo} onSelect={setDateTo} locale={ptBR} className="p-3 pointer-events-auto" />
                </PopoverContent>
              </Popover>
              {hasActiveFilters && (
                <Button variant="ghost" size="sm" onClick={clearFilters} className="text-xs text-muted-foreground">
                  <X className="h-3.5 w-3.5 mr-1" /> Limpar
                </Button>
              )}
            </div>
          </div>

          {loading ? (
            <div className="space-y-3">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-24 rounded-xl bg-muted/50 animate-pulse" />
              ))}
            </div>
          ) : checklists.length === 0 ? (
            <div className="text-center py-16 space-y-3">
              <FileText className="h-12 w-12 mx-auto text-muted-foreground/50" />
              <p className="text-muted-foreground">Nenhum checklist salvo ainda.</p>
              <Button asChild>
                <Link href="/">Extrair primeiro edital</Link>
              </Button>
            </div>
          ) : filtered.length === 0 ? (
            <div className="text-center py-12 space-y-2">
              <Search className="h-10 w-10 mx-auto text-muted-foreground/50" />
              <p className="text-muted-foreground">Nenhum resultado encontrado.</p>
              <Button variant="ghost" size="sm" onClick={clearFilters}>Limpar filtros</Button>
            </div>
          ) : (
            <div className="space-y-3">
              {filtered.map((checklist, i) => (
                <motion.div
                  key={checklist.id}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * 0.05 }}
                  className="rounded-xl bg-card p-5 shadow-card border border-border hover:border-primary/30 transition-colors"
                >
                  <div className="flex items-start justify-between gap-4">
                    <Link href={`/historico/${checklist.id}`} className="min-w-0 flex-1 space-y-2 block">
                      <p className="font-display font-semibold text-foreground truncate">{checklist.file_name}</p>
                      <div className="flex flex-wrap gap-3 text-xs text-muted-foreground">
                        {checklist.orgao && (
                          <span className="flex items-center gap-1">
                            <Building2 className="h-3 w-3" /> {checklist.orgao}
                          </span>
                        )}
                        {checklist.valor_total && (
                          <span className="flex items-center gap-1">
                            <DollarSign className="h-3 w-3" /> {checklist.valor_total}
                          </span>
                        )}
                        <span className="flex items-center gap-1">
                          <Calendar className="h-3 w-3" />
                          {format(new Date(checklist.created_at), "dd MMM yyyy, HH:mm", { locale: ptBR })}
                        </span>
                      </div>
                      <div className="flex flex-wrap items-center gap-2 mt-1">
                        {checklist.pontuacao !== null && (
                          <span
                            className={cn(
                              "inline-block text-xs font-semibold px-2 py-0.5 rounded-full",
                              checklist.pontuacao >= 70 && "bg-success/10 text-success",
                              checklist.pontuacao >= 40 && checklist.pontuacao < 70 && "bg-warning/10 text-warning",
                              checklist.pontuacao < 40 && "bg-destructive/10 text-destructive"
                            )}
                          >
                            Pontuação: {checklist.pontuacao}
                          </span>
                        )}
                        {checklist.processedWithPdfMode === true && (
                          <span className="inline-flex items-center gap-1 text-xs font-medium px-2 py-0.5 rounded-full bg-primary/10 text-primary border border-primary/20">
                            <FileText className="h-3 w-3" />
                            Modo PDF
                          </span>
                        )}
                      </div>
                    </Link>
                    <div className="flex items-center gap-1 shrink-0">
                      <Button size="sm" variant="ghost" asChild>
                        <Link href={`/historico/${checklist.id}`} aria-label="Ver detalhes">
                          <Eye className="h-4 w-4" />
                        </Link>
                      </Button>
                      <Button size="sm" variant="ghost" onClick={(e) => handleDelete(e, checklist.id)} aria-label="Excluir">
                        <Trash2 className="h-4 w-4 text-destructive" />
                      </Button>
                    </div>
                  </div>
                </motion.div>
              ))}
            </div>
          )}
        </motion.div>
      </main>
    </div>
  );
}

export default function HistoricoPage() {
  return (
    <ProtectedRoute>
      <HistoryContent />
    </ProtectedRoute>
  );
}
