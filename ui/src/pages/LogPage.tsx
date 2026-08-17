import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import "@xterm/xterm/css/xterm.css";
import { ApiError, containerOf, fetchLog, fetchRun } from "../api";
import type { LogEvent } from "../events";
import { Trouble } from "../Trouble";

// A running step gets a live terminal. A completed step gets the harvested
// log from storage. The mode is decided from the latest attempt, and a normal
// end to the live stream switches the page to the stored record.
export function LogPage() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const { data: run } = useQuery({
    queryKey: ["run", namespace, slug],
    queryFn: () => fetchRun(namespace, slug),
  });
  const [mode, setMode] = useState<"deciding" | "live" | "harvested">("deciding");
  useEffect(() => {
    if (mode !== "deciding" || !run) return;
    // Render-all accretion: the job's latest attempt decides liveness.
    const attempts = (run.jobs ?? []).filter((row) => row.job === job);
    const latest = attempts[attempts.length - 1];
    setMode(latest && !latest.completed_at ? "live" : "harvested");
  }, [mode, run, job]);
  useEffect(() => {
    document.title = `coalesce · ${namespace} · ${slug} · ${job}`;
  }, [namespace, slug, job]);

  return (
    <>
      <h1 className="nameplate">
        <Link to={`/${namespace}/runs`}>Kerchunkifier 3000</Link>
      </h1>
      <p className="crumb">
        <Link to={`/${namespace}/runs/${slug}`}>← {slug}</Link> · {job}
      </p>

      {mode === "live" && (
        <LiveLog
          namespace={namespace}
          slug={slug}
          job={job}
          onExit={() => setTimeout(() => setMode("harvested"), 2500)}
        />
      )}
      {mode === "harvested" && (
        <HarvestedLog namespace={namespace} slug={slug} job={job} />
      )}
    </>
  );
}

function HarvestedLog({
  namespace,
  slug,
  job,
}: {
  namespace: string;
  slug: string;
  job: string;
}) {
  const { data: log, error } = useQuery({
    queryKey: ["log", namespace, slug, job],
    queryFn: () => fetchLog(namespace, slug, job, containerOf(job)),
    // The harvester can lag the container exit by a watcher cycle.
    retry: (failures, error) =>
      error instanceof ApiError && error.status === 404 && failures < 6,
    retryDelay: 1500,
  });

  if (error instanceof ApiError && error.status === 404) {
    return (
      <p className="error">Not harvested yet — the log lands when the step finishes.</p>
    );
  }
  if (error) {
    return <Trouble error={error} />;
  }
  return log !== undefined ? <pre className="log">{log}</pre> : null;
}

// The live terminal owns one log WebSocket. Its server handler writes each
// Kubernetes log line directly to this connection, so leaving the page closes
// the stream and a slow connection stops the server's log reader.
function LiveLog({
  namespace,
  slug,
  job,
  onExit,
}: {
  namespace: string;
  slug: string;
  job: string;
  onExit: () => void;
}) {
  const mount = useRef<HTMLDivElement>(null);
  const terminal = useRef<Terminal | null>(null);

  useEffect(() => {
    const term = new Terminal({
      convertEol: true,
      disableStdin: true,
      fontFamily: 'ui-monospace, "SF Mono", Menlo, Consolas, monospace',
      fontSize: 13,
      theme: {
        background: "#171B21",
        foreground: "#C9D3DD",
        cursor: "#171B21",
        selectionBackground: "#262D36",
      },
    });
    const fit = new FitAddon();
    term.loadAddon(fit);
    term.open(mount.current!);
    fit.fit();
    terminal.current = term;
    const refit = () => fit.fit();
    window.addEventListener("resize", refit);

    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const path = `/tail/${namespace}/${slug}/${job}?container=${encodeURIComponent(containerOf(job))}`;
    const socket = new WebSocket(`${protocol}//${location.host}${path}`);
    socket.onmessage = (message) => {
      const event = JSON.parse(message.data) as LogEvent;
      switch (event.kind) {
        case "log_line":
          term.writeln(String(event.data.line ?? ""));
          break;
        case "log_status":
          term.writeln(`— ${event.data.phase}`);
          break;
        case "log_exit":
          term.writeln(`— exit ${event.data.exit_code} ${event.data.reason ?? ""}`.trimEnd());
          onExit();
          break;
        case "log_error":
          term.writeln(`— ${event.data.error}`);
          break;
      }
    };
    socket.onerror = () => term.writeln("— log stream failed");

    return () => {
      socket.onmessage = null;
      socket.onerror = null;
      socket.close();
      window.removeEventListener("resize", refit);
      term.dispose();
      terminal.current = null;
    };
  }, [namespace, slug, job]);

  return <div className="terminal" ref={mount} />;
}
