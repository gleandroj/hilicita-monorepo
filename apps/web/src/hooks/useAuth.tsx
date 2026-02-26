"use client";

import { authClient } from "@/lib/auth-client";

export function useAuth() {
  const { data: session, isPending: loading, refetch } = authClient.useSession();
  const user = session?.user ?? null;

  const signOut = async () => {
    await authClient.signOut();
    refetch();
  };

  return { user, loading, signOut };
}
