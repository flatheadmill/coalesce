import { useCallback, useEffect, useRef, useState } from "react";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { ApiError, containerOf, openLogTail, parseStreamEvent, type Job } from "../../shared/api";
import { useStoredOutput } from "./data";

type TailPhase = "idle" | "connecting" | "connected" | "exited" | "unavailable";
interface Observation { text: string; state: TailPhase; note: string; exitCode?: string; observedAt?: number }

function savedObservation(key: string): Observation | undefined {
  try {
    const saved = JSON.parse(sessionStorage.getItem(key) ?? "null") as Observation | null;
    if (!saved || typeof saved.text !== "string") return;
    return { ...saved, state: saved.state === "exited" ? "exited" : "unavailable", note: saved.state === "exited" ? saved.note : "Saved in this tab · disconnected" };
  } catch { return; }
}

function useTail(namespace: string, slug: string, job: string, latest?: Job) {
  const open = Boolean(latest && !latest.completed_at);
  const key = `coalesce:four:tail:${JSON.stringify([namespace, slug, job, latest?.started_at])}`;
  const [saved] = useState(() => savedObservation(key));
  const [enabled, setEnabled] = useState(open && !saved);
  const [revision, setRevision] = useState(0);
  const [observation, setObservation] = useState<Observation>(saved ?? { text: "", state: "idle", note: "Live output has not been requested." });
  const snapshot = useRef(observation);
  const isOpen = useRef(open);
  isOpen.current = open;

  useEffect(() => {
    if (!enabled) return;
    let current = true;
    let socket: WebSocket | undefined, timer: number | undefined;
    let retries = 0;
    const delays = [1200, 2400, 4800, 4800];
    const publish = (update: Partial<Observation>) => {
      if (!current) return;
      snapshot.current = { ...snapshot.current, ...update };
      setObservation(snapshot.current);
    };
    const save = () => {
      if (!snapshot.current.text && snapshot.current.state !== "exited") return;
      try { sessionStorage.setItem(key, JSON.stringify(snapshot.current)); } catch { /* In-memory output remains usable. */ }
    };
    const connect = () => {
      if (!current) return;
      let settled = false, text = "";
      publish({ state: "connecting", note: snapshot.current.text ? "Reconnecting · previous output retained" : "Connecting to output…", exitCode: undefined });
      const tail = openLogTail(namespace, slug, job, containerOf(job));
      socket = tail;
      const fail = () => {
        if (!current || settled) return;
        settled = true;
        tail.close();
        // There is no replay cursor. After receiving bytes, keep them and
        // leave reconnect to the reader; a new connection is a fresh stream.
        if (!snapshot.current.text && isOpen.current && retries < delays.length) {
          publish({ state: "connecting", note: "Connecting to output…" });
          timer = window.setTimeout(() => {
            if (isOpen.current) connect();
            else publish({ state: "unavailable", note: "Live output unavailable" });
          }, delays[retries++]);
        } else {
          publish({ state: "unavailable", note: snapshot.current.text ? "Disconnected · received output retained" : "Live output unavailable" });
          save();
        }
      };
      tail.onopen = () => { if (current && !settled) publish({ state: "connected", note: "Waiting for output…" }); };
      tail.onmessage = message => {
        if (!current || settled) return;
        try {
          const event = parseStreamEvent(String(message.data));
          if (event.kind === "log_line") {
            text += `${String(event.data.line ?? "")}\n`;
            publish({ text, state: "connected", note: "Live output" });
          } else if (event.kind === "log_status") {
            const phase = String(event.data.phase ?? "unknown");
            publish({ state: "connected", note: phase === "Pending" ? "Pod pending" : `Pod ${phase.toLowerCase()}` });
          } else if (event.kind === "log_exit") {
            settled = true;
            const exitCode = event.data.exit_code == null ? undefined : String(event.data.exit_code);
            publish({ text, state: "exited", exitCode, observedAt: Date.now(), note: exitCode === undefined ? "Process exited" : `Process exited · exit ${exitCode} observed` });
            save();
            tail.close();
          } else if (event.kind === "log_error") fail();
        } catch { fail(); }
      };
      tail.onerror = fail;
      tail.onclose = fail;
    };
    connect();
    window.addEventListener("pagehide", save);
    return () => {
      save();
      current = false;
      window.clearTimeout(timer);
      socket?.close();
      window.removeEventListener("pagehide", save);
    };
  }, [enabled, revision, namespace, slug, job, key]);
  const start = useCallback(() => { setEnabled(true); setRevision(value => value + 1); }, []);
  return { ...observation, start };
}

function TerminalSurface({ text, visible, label, atEnd }: { text: string; visible: boolean; label: string; atEnd: boolean }) {
  const mount = useRef<HTMLDivElement>(null);
  const terminal = useRef<Terminal>();
  const fit = useRef<FitAddon>();
  const previous = useRef("");
  const placed = useRef(false);
  const [copyState, setCopyState] = useState("Copy output");
  useEffect(() => {
    if (!mount.current) return;
    const term = new Terminal({
      convertEol: true, disableStdin: true, screenReaderMode: true,
      fontFamily: 'ui-monospace, "SFMono-Regular", Menlo, Consolas, monospace',
      fontSize: 12.5, lineHeight: 1.45, scrollback: 5000,
      theme: { background: "#0c1118", foreground: "#dce4ee", cursor: "#0c1118", selectionBackground: "#354457" },
    });
    const addon = new FitAddon();
    term.loadAddon(addon);
    term.open(mount.current);
    terminal.current = term;
    fit.current = addon;
    const resize = () => { if (mount.current && mount.current.clientWidth > 0) addon.fit(); };
    resize();
    const observer = new ResizeObserver(resize);
    observer.observe(mount.current);
    return () => { observer.disconnect(); terminal.current = undefined; fit.current = undefined; previous.current = ""; placed.current = false; term.dispose(); };
  }, []);
  useEffect(() => { if (visible && mount.current?.clientWidth) fit.current?.fit(); }, [visible]);
  useEffect(() => {
    const term = terminal.current;
    if (!term) return;
    term.options.scrollback = Math.min(500_000, Math.max(5000, text.split("\n").length + term.rows));
    const extendsPrevious = text.startsWith(previous.current);
    const append = extendsPrevious ? text.slice(previous.current.length) : text;
    if (!extendsPrevious) { term.reset(); placed.current = false; }
    previous.current = text;
    if (!append) return;
    term.write(append, () => {
      if (terminal.current !== term || placed.current) return;
      placed.current = true;
      if (atEnd) term.scrollToBottom();
      else term.scrollToTop();
    });
  }, [text, atEnd]);
  const copy = async () => {
    try { await navigator.clipboard.writeText(text); setCopyState("Copied"); }
    catch { setCopyState("Copy unavailable"); }
  };
  useEffect(() => {
    if (copyState === "Copy output") return;
    const timer = window.setTimeout(() => setCopyState("Copy output"), 1500);
    return () => window.clearTimeout(timer);
  }, [copyState]);
  return <div hidden={!visible} className="terminal-surface" aria-label={label}>
    <div className="terminal-tools">
      <span>{label}</span>
      <div><button type="button" onClick={() => terminal.current?.scrollToTop()}>Beginning</button><button type="button" onClick={() => terminal.current?.scrollToBottom()}>End</button><button type="button" disabled={!text} onClick={() => void copy()}>{copyState}</button></div>
    </div>
    <div ref={mount} className="terminal-mount" aria-label={`${label} terminal`} />
  </div>;
}

export function Output({ namespace, slug, job, attempts }: { namespace: string; slug: string; job: string; attempts: Job[] }) {
  const latest = attempts.at(-1);
  const open = Boolean(latest && !latest.completed_at);
  const stored = useStoredOutput(namespace, slug, job, latest);
  const live = useTail(namespace, slug, job, latest);
  // Arrival chooses a source once. A closing write cannot change the
  // terminal or scroll position the reader is working with.
  const [source, setSource] = useState<"live" | "stored">(open ? "live" : "stored");
  const selectSource = (value: "live" | "stored") => {
    setSource(value);
    if (value === "live" && live.state === "idle") live.start();
  };
  const storedNote = stored.data !== undefined
    ? stored.data === "" ? "The response contained no output." : `${(stored.data.split("\n").length - (stored.data.endsWith("\n") ? 1 : 0)).toLocaleString()} lines · ${(new TextEncoder().encode(stored.data).length / 1024).toFixed(1)} KiB`
    : stored.loading ? "Reading stored output…"
    : stored.error instanceof ApiError && stored.error.status === 404
      ? stored.polling ? "Waiting for stored output…" : "No stored output found."
      : "Stored output unavailable";
  return <section className="output-panel" aria-labelledby="output-title">
    <div className="output-heading">
      <div><span className="section-label">Output</span><h2 id="output-title">{job}</h2></div>
      <div className="source-switch" role="group" aria-label="Output source">
        <button type="button" aria-pressed={source === "live"} onClick={() => selectSource("live")}>Live</button>
        <button type="button" aria-pressed={source === "stored"} onClick={() => selectSource("stored")}>Stored{stored.data !== undefined ? <span className="available-dot" aria-label="available" /> : null}</button>
      </div>
    </div>
    <div className="output-note" role="status">
      {source === "live" ? <>
        <span>{live.note}{live.state === "exited" && open ? " · completion not recorded" : ""}</span>
        {live.state === "unavailable" || live.state === "exited" ? <button type="button" onClick={live.start}>{live.text ? "Reconnect" : "Retry"}</button> : null}
      </> : <>
        <span>{storedNote}{stored.data !== undefined && stored.error ? " · refresh failed; previous output retained" : ""}</span>
        {stored.error ? <button type="button" onClick={stored.reload}>Check again</button> : null}
      </>}
    </div>
    <TerminalSurface text={live.text} visible={source === "live"} label="Live output" atEnd />
    <TerminalSurface text={stored.data ?? ""} visible={source === "stored"} label="Stored output" atEnd={latest?.status === "failed"} />
    <div className="output-footnote">
      <span>{source === "live" ? "Received line by line in this tab." : "Copy returns the text received from storage."}</span>
      {attempts.length > 1 ? <span>{attempts.length} attempts. Output cannot be attributed to a specific attempt.</span> : null}
    </div>
  </section>;
}
