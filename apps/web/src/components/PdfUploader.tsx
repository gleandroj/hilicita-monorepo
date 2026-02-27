import { useState, useCallback } from "react";
import { Upload, FileText, X, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { motion, AnimatePresence } from "framer-motion";

interface PdfUploaderProps {
  onExtract: (file: File, options?: { usePdfFile?: boolean }) => void;
  isProcessing: boolean;
}

const PdfUploader = ({ onExtract, isProcessing }: PdfUploaderProps) => {
  const [file, setFile] = useState<File | null>(null);
  const [isDragging, setIsDragging] = useState(false);
  const [usePdfFile, setUsePdfFile] = useState(false);

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    const dropped = e.dataTransfer.files[0];
    if (dropped && (dropped.type === "application/pdf" || dropped.type === "text/csv" || /\.(pdf|csv)$/i.test(dropped.name))) {
      setFile(dropped);
    }
  }, []);

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0];
    if (selected && (selected.type === "application/pdf" || selected.type === "text/csv" || /\.(pdf|csv)$/i.test(selected.name))) {
      setFile(selected);
    }
  };

  const handleExtract = () => {
    if (!file) return;
    onExtract(file, { usePdfFile });
  };

  return (
    <div className="space-y-4">
      <div
        onDragOver={(e) => { e.preventDefault(); setIsDragging(true); }}
        onDragLeave={() => setIsDragging(false)}
        onDrop={handleDrop}
        className={`relative rounded-xl border-2 border-dashed p-10 text-center transition-all duration-200 ${
          isDragging
            ? "border-primary bg-primary/5"
            : file
            ? "border-primary/40 bg-primary/5"
            : "border-border bg-card hover:border-primary/30 hover:bg-muted/50"
        }`}
      >
        <AnimatePresence mode="wait">
          {file ? (
            <motion.div
              key="file"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col items-center gap-3"
            >
              <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-primary/10">
                <FileText className="h-7 w-7 text-primary" />
              </div>
              <div>
                <p className="font-display font-semibold text-foreground">{file.name}</p>
                <p className="text-sm text-muted-foreground">
                  {(file.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
              <button
                onClick={() => setFile(null)}
                className="absolute right-3 top-3 rounded-lg p-1.5 text-muted-foreground hover:bg-muted hover:text-foreground transition-colors"
              >
                <X className="h-4 w-4" />
              </button>
            </motion.div>
          ) : (
            <motion.div
              key="empty"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col items-center gap-3"
            >
              <div className="flex h-14 w-14 items-center justify-center rounded-xl bg-muted">
                <Upload className="h-7 w-7 text-muted-foreground" />
              </div>
              <div>
                <p className="font-display font-semibold text-foreground">
                  Arraste o edital aqui
                </p>
                <p className="text-sm text-muted-foreground">
                  ou clique para selecionar um arquivo PDF ou CSV
                </p>
              </div>
              <input
                type="file"
                accept=".pdf,.csv,application/pdf,text/csv"
                onChange={handleFileSelect}
                className="absolute inset-0 cursor-pointer opacity-0"
              />
            </motion.div>
          )}
        </AnimatePresence>
      </div>

      {file && (
        <motion.div initial={{ opacity: 0, y: 5 }} animate={{ opacity: 1, y: 0 }} className="space-y-3">
          <label className="flex items-center gap-2 text-sm text-muted-foreground cursor-pointer">
            <input
              type="checkbox"
              checked={usePdfFile}
              onChange={(e) => setUsePdfFile(e.target.checked)}
              className="rounded border-border"
            />
            Enviar PDF como arquivo (pode dar melhor resultado que texto extraído)
          </label>
          <Button
            onClick={handleExtract}
            disabled={isProcessing}
            className="w-full gradient-brand text-primary-foreground font-display font-semibold h-12 text-base shadow-glow hover:opacity-90 transition-opacity"
          >
            {isProcessing ? (
              <>
                <Loader2 className="mr-2 h-5 w-5 animate-spin" />
                Extraindo dados com IA...
              </>
            ) : (
              <>
                <FileText className="mr-2 h-5 w-5" />
                Extrair Checklist
              </>
            )}
          </Button>
        </motion.div>
      )}
    </div>
  );
};

export default PdfUploader;
