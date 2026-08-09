import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";
import { fetchRun, fetchRuns, type Run } from "../api";
import { useRunEvents } from "../events";
import { countingDuration, recordedDuration, timestamp } from "../format";

// A run is a machine while alive and a record once dead; this page renders
// the phase change. The floor exists only while something runs; below it,
// the ledger. Discovery of new runs is poll-based because the event hub is
// slug-scoped; the floor's own sockets keep the living fresh.
export function RunsPage() {
  const { namespace = "coalesce" } = useParams();
  const [pipeline, setPipeline] = useState("");

  const { data: runs, error, dataUpdatedAt } = useQuery({
    queryKey: ["runs", namespace],
    queryFn: () => fetchRuns(namespace),
    refetchInterval: 10_000,
  });

  useEffect(() => {
    document.title = `coalesce · ${namespace} · runs`;
  }, [namespace]);

  const pipelines = useMemo(
    () => [...new Set((runs ?? []).map((run) => run.pipeline))].sort(),
    [runs],
  );
  const shown = (runs ?? []).filter(
    (run) => pipeline === "" || run.pipeline === pipeline,
  );
  const living = shown.filter((run) => run.status === "running");
  const settled = shown.filter((run) => run.status !== "running");

  return (
    <>
      <h1 className="nameplate">Kerchunkifier 3000</h1>
      <p className="filter-line">
        namespace {namespace} ·{" "}
        <select
          aria-label="Filter by pipeline"
          value={pipeline}
          onChange={(event) => setPipeline(event.target.value)}
        >
          <option value="">all pipelines</option>
          {pipelines.map((name) => (
            <option key={name} value={name}>
              {name}
            </option>
          ))}
        </select>{" "}
        · {shown.length} {shown.length === 1 ? "run" : "runs"} · as of{" "}
        {dataUpdatedAt ? timestamp(new Date(dataUpdatedAt).toISOString()) : "—"}
      </p>

      {error && (
        <p className="error">
          Can't reach the server. Is the deployment up?{" "}
          <code>kubectl --context orbstack get pods -n coalesce</code>
        </p>
      )}

      {living.length > 0 && (
        <div className="floor">
          {living.map((run) => (
            <FloorEntry key={run.slug} namespace={namespace} run={run} />
          ))}
        </div>
      )}

      {settled.length > 0 && <Ledger namespace={namespace} runs={settled} />}

      {runs && shown.length === 0 && !error && (
        <div className="empty">
          <p>No runs in this namespace yet. Stamp one:</p>
          <pre>
            {`export KUBECONFIG=/tmp/orbstack.kubeconfig
export COALESCE_URL=http://coalesce.coalesce.svc.cluster.local
./bin/coalesce run -s hello-1 -N ${namespace} test/one.coalesce.zsh`}
          </pre>
        </div>
      )}
    </>
  );
}

// A working entry: roomy, the current step given space, the duration
// counting — the client's honest estimate, custody not yet transferred.
// No bottom rule: the record is still being made.
function FloorEntry({ namespace, run }: { namespace: string; run: Run }) {
  useRunEvents(namespace, run.slug);
  const { data: detail } = useQuery({
    queryKey: ["run", namespace, run.slug],
    queryFn: () => fetchRun(namespace, run.slug),
    refetchInterval: 5_000,
  });
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);

  const current = detail?.jobs?.filter((job) => !job.completed_at).at(-1);

  return (
    <Link className="floor-entry" to={`/${namespace}/runs/${run.slug}`}>
      <span className="slug">{run.slug}</span>
      <span className="step">
        {current ? <>on <b>{current.job}</b></> : "laying out"}
      </span>
      <span className="counting">{countingDuration(run.started_at, now)}</span>
    </Link>
  );
}

function Ledger({ namespace, runs }: { namespace: string; runs: Run[] }) {
  const navigate = useNavigate();
  return (
    <table>
      <thead>
        <tr>
          <th>Status</th>
          <th>Slug</th>
          <th className="col-pipeline">Pipeline</th>
          <th className="col-started">Started</th>
          <th className="num">Duration</th>
        </tr>
      </thead>
      <tbody>
        {runs.map((run) => (
          <tr
            key={run.slug}
            className="rowlink"
            onClick={() => navigate(`/${namespace}/runs/${run.slug}`)}
          >
            <td className={run.status === "failed" ? "status-failed" : "status-quiet"}>
              {run.status}
            </td>
            <td>
              <Link to={`/${namespace}/runs/${run.slug}`}>{run.slug}</Link>
            </td>
            <td className="dim col-pipeline">{run.pipeline}</td>
            <td className="dim col-started">{timestamp(run.started_at)}</td>
            <td className="num">
              {run.completed_at
                ? recordedDuration(run.started_at, run.completed_at)
                : "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
