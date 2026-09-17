import { useCallback, useEffect, useRef, useState } from "react";
import { containerOf, fetchLog } from "../../shared/api";

export interface StoredOutput {
  data?: string;
  error?: unknown;
  loading: boolean;
  polling: boolean;
  reload: () => void;
}

// Harvest can lag the closing Job write. Follow an open attempt, then allow
// a bounded handoff to storage; a settled page gets only its initial read.
const interval = 5_000;
const harvestGrace = 30_000;

export function useStoredOutput(namespace: string, slug: string, job: string, open: boolean): StoredOutput {
  const phase = useRef({ open, until: 0 });
  const [revision, setRevision] = useState(0);
  const [remote, setRemote] = useState<Omit<StoredOutput, "reload">>({ loading: true, polling: open });
  const reload = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    if (phase.current.open && !open) phase.current.until = Date.now() + harvestGrace;
    phase.current.open = open;
  }, [open]);

  useEffect(() => {
    let current = true;
    let timer: number | undefined;
    const read = async () => {
      let data: string | undefined;
      let error: unknown;
      try { data = await fetchLog(namespace, slug, job, containerOf(job)); }
      catch (failure) { error = failure; }
      if (!current) return;
      const polling = phase.current.open || Date.now() < phase.current.until;
      setRemote((previous) => ({
        data: data ?? previous.data, error, loading: false, polling,
      }));
      if (polling) timer = window.setTimeout(() => void read(), interval);
    };
    void read();
    return () => { current = false; window.clearTimeout(timer); };
  }, [namespace, slug, job, revision]);

  return { ...remote, reload };
}
