import { useCallback, useEffect, useRef, useState } from "react";
import { containerOf, fetchLog, type Job } from "../../shared/api";

export interface Remote<T> { data?: T; error?: unknown; loading: boolean; updatedAt?: number }

export function useRemote<T>(read: () => Promise<T>, poll = 0) {
  const reader = useRef(read);
  reader.current = read;
  const [remote, setRemote] = useState<Remote<T>>({ loading: true });
  const [revision, setRevision] = useState(0);
  const reload = useCallback(() => setRevision(value => value + 1), []);
  useEffect(() => {
    let current = true, reading = false;
    const load = async () => {
      if (reading) return;
      reading = true;
      try {
        const data = await reader.current();
        if (current) setRemote({ data, loading: false, updatedAt: Date.now() });
      } catch (error) {
        if (current) setRemote(previous => ({ ...previous, error, loading: false }));
      } finally { reading = false; }
    };
    void load();
    const timer = poll ? window.setInterval(() => void load(), poll) : undefined;
    return () => { current = false; window.clearInterval(timer); };
  }, [poll, revision]);
  return { ...remote, reload };
}

export function useNow(active: boolean) {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    if (!active) return;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [active]);
  return now;
}

// Retain Two's polling and harvest handoff. A newly completed Job can be
// reached from GitHub before its first stored log is available, too.
export function useStoredOutput(namespace: string, slug: string, job: string, latest?: Job) {
  const open = Boolean(latest && !latest.completed_at);
  const [revision, setRevision] = useState(0);
  const [remote, setRemote] = useState<Remote<string> & { polling: boolean }>({ loading: true, polling: open });
  const phase = useRef({
    open,
    until: latest?.completed_at && Date.now() - Date.parse(latest.completed_at) < 60_000 ? Date.now() + 30_000 : 0,
  });
  useEffect(() => {
    if (phase.current.open && !open) phase.current.until = Date.now() + 30_000;
    phase.current.open = open;
  }, [open]);
  useEffect(() => {
    let current = true;
    let timer: number | undefined;
    const read = async () => {
      let data: string | undefined, error: unknown;
      try { data = await fetchLog(namespace, slug, job, containerOf(job)); }
      catch (failure) { error = failure; }
      if (!current) return;
      const polling = phase.current.open || Date.now() < phase.current.until;
      setRemote(previous => ({ data: data ?? previous.data, error, loading: false, polling, updatedAt: Date.now() }));
      if (polling) timer = window.setTimeout(() => void read(), 5000);
    };
    void read();
    return () => { current = false; window.clearTimeout(timer); };
  }, [namespace, slug, job, revision]);
  return { ...remote, reload: () => setRevision(value => value + 1) };
}
