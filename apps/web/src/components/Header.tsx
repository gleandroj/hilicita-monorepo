"use client";

import { FileText, History, Upload, LogOut } from "lucide-react";
import { useAuth } from "@/hooks/useAuth";
import { NavLink } from "@/components/NavLink";
import { Button } from "@/components/ui/button";

const Header = () => {
  const { user, signOut } = useAuth();

  return (
    <header className="border-b border-border bg-card">
      <div className="container mx-auto flex items-center justify-between px-6 py-4">
        <div className="flex items-center gap-6">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-lg gradient-brand">
              <FileText className="h-5 w-5 text-primary-foreground" />
            </div>
            <div>
              <h1 className="text-xl font-bold font-display tracking-tight text-foreground">
                Hi Licita
              </h1>
              <p className="text-xs text-muted-foreground hidden sm:block">Extração inteligente de editais</p>
            </div>
          </div>

          {user && (
            <nav className="flex items-center gap-1">
              <NavLink
                href="/"
                className="inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-sm font-medium transition-colors text-muted-foreground hover:text-foreground"
                activeClassName="text-primary bg-primary/10"
              >
                <Upload className="h-4 w-4" />
                <span className="hidden sm:inline">Enviar Licitação</span>
              </NavLink>
              <NavLink
                href="/historico"
                className="inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-sm font-medium transition-colors text-muted-foreground hover:text-foreground"
                activeClassName="text-primary bg-primary/10"
              >
                <History className="h-4 w-4" />
                <span className="hidden sm:inline">Histórico</span>
              </NavLink>
            </nav>
          )}
        </div>

        {user && (
          <div className="flex items-center gap-3">
            <span className="text-xs text-muted-foreground hidden md:block truncate max-w-[160px]">
              {user.email}
            </span>
            <Button variant="ghost" size="sm" onClick={signOut} className="text-muted-foreground hover:text-foreground">
              <LogOut className="h-4 w-4" />
              <span className="hidden sm:inline ml-1.5">Sair</span>
            </Button>
          </div>
        )}
      </div>
    </header>
  );
};

export default Header;
