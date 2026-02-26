-- AlterTable
ALTER TABLE "Checklist" ADD COLUMN IF NOT EXISTS "documentId" TEXT;

-- CreateIndex
CREATE UNIQUE INDEX IF NOT EXISTS "Checklist_documentId_key" ON "Checklist"("documentId");
CREATE INDEX IF NOT EXISTS "Checklist_documentId_idx" ON "Checklist"("documentId");

-- AddForeignKey
ALTER TABLE "Checklist" ADD CONSTRAINT "Checklist_documentId_fkey" FOREIGN KEY ("documentId") REFERENCES "Document"("id") ON DELETE CASCADE ON UPDATE CASCADE;
