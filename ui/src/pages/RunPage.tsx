import { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { fetchRun } from "../api";
import { useRunEvents } from "../events";
import { countingDuration, recordedDuration, timestamp } from "../format";

// The steps view, shallow and honest: rows are the jobs array as returned —
// attempt accretion renders as returned, because the rows are the ledger and
// collapsing is interpretation deferred to the piece we were told not to
// build. The DAG and live tailing stay deferred whole.
export function RunPage() {
  const { namespace = "coalesce", slug = "" } = useParams();
  useRunEvents(namespace, slug);
  const { data: run, error } = useQuery({
    queryKey: ["run", namespace, slug],
    queryFn: () => fetchRun(namespace, slug),
    refetchInterval: 5_000,
  });
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const tick = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(tick);
  }, []);
  useEffect(() => {
    document.title = `coalesce · ${namespace} · ${slug}`;
  }, [namespace, slug]);

  return (
    <>
      <h1 className="nameplate">
        <Link to={`/${namespace}/runs`}>Kerchunkifier 3000</Link>
      </h1>
      <p className="crumb">
        <Link to={`/${namespace}/runs`}>← runs</Link> · namespace {namespace}
      </p>

      {error && (
        <p className="error">
          No run <code>{slug}</code> in this namespace.
        </p>
      )}

      {run && (
        <>
          <div className="detail-head">
            <span className="slug">{run.slug}</span>
            <span className={run.status === "failed" ? "status-failed" : "dim"}>
              {run.status}
            </span>
            <span className="dim">{run.pipeline}</span>
            <span className="dim">{timestamp(run.started_at)}</span>
            <span>
              {run.completed_at
                ? recordedDuration(run.started_at, run.completed_at)
                : countingDuration(run.started_at, now)}
            </span>
          </div>
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>Step</th>
                <th className="col-started">Started</th>
                <th className="num">Duration</th>
                <th className="num">Exit</th>
              </tr>
            </thead>
            <tbody>
              {(run.jobs ?? []).map((job) => (
                <tr key={`${job.job}-${job.started_at}`}>
                  <td className={job.status === "failed" ? "status-failed" : "status-quiet"}>
                    {job.status}
                  </td>
                  <td>
                    <Link to={`/${namespace}/runs/${slug}/logs/${job.job}`}>
                      {job.job}
                    </Link>
                  </td>
                  <td className="dim col-started">{timestamp(job.started_at)}</td>
                  <td className="num">
                    {job.completed_at
                      ? recordedDuration(job.started_at, job.completed_at)
                      : countingDuration(job.started_at, now)}
                  </td>
                  <td className="num dim">{job.exit_code ?? ""}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}
