import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link, Navigate, Route, Routes, useMatch, useParams, useSearchParams } from "react-router-dom";
import { ApiError, fetchDag, fetchRun, fetchRuns, openRunEvents, parseStreamEvent, type Job, type RunDetail } from "../../shared/api";
import { useNow, useRemote } from "./data";
import { duration, fullTime, initialJob, latestJobs, logPath, runPath, runsPath, statusKind, statusLabel } from "./model";
import { Output } from "./Output";
import { buildPrecedenceTopology, type PrecedenceTopology } from "./topology";

function Status({ value }: { value: string }) {
  const mark = value === "completed" ? "✓" : value === "failed" ? "×" : value === "running" ? "●" : value === "pending" ? "◷" : "—";
  return <span className={`status status-${statusKind(value)}`}><span aria-hidden="true">{mark}</span>{statusLabel(value)}</span>;
}

function Shell({ namespace, children }: { namespace: string; children: ReactNode }) {
  return <>
    <a className="skip-link" href="#content">Skip to content</a>
    <header className="site-header"><div className="header-inner"><Link className="brand" to={runsPath(namespace)}>Coalesce</Link><span className="header-divider">/</span><Link to={runsPath(namespace)}>Runs</Link><span className="namespace">{namespace}</span></div></header>
    <main id="content" className="page">{children}</main>
  </>;
}

function Notice({ children, retry }: { children: ReactNode; retry?: () => void }) {
  return <div className="notice" role="status"><span>{children}</span>{retry ? <button type="button" onClick={retry}>Retry</button> : null}</div>;
}

function RunsRoute() {
  const { namespace = "coalesce" } = useParams();
  return <RunsTable key={namespace} namespace={namespace} />;
}

function RunsTable({ namespace }: { namespace: string }) {
  const runs = useRemote(async () => {
    const data = await fetchRuns(namespace);
    if (!Array.isArray(data)) throw new Error("Invalid run list");
    return data;
  }, 15_000);
  const [params, setParams] = useSearchParams();
  const query = params.get("q") ?? "", filter = params.get("status") ?? "all";
  const setFilter = (key: string, value: string) => {
    const next = new URLSearchParams(params);
    if (value && value !== "all") next.set(key, value); else next.delete(key);
    setParams(next, { replace: true });
  };
  const all = runs.data ?? [];
  const now = useNow(all.some(run => !run.completed_at));
  const needle = query.trim().toLowerCase();
  const visible = all.filter(run => {
    const matches = filter === "all" || (filter === "active" ? !run.completed_at && ["running", "pending"].includes(run.status) : run.status === filter);
    return matches && (!needle || [run.pipeline, run.slug, run.status].some(value => value.toLowerCase().includes(needle)));
  });
  const extraStatuses = [...new Set(all.map(run => run.status))].filter(value => !["running", "pending", "completed", "failed"].includes(value));
  useEffect(() => { document.title = "Coalesce · Runs"; }, []);
  return <Shell namespace={namespace}>
    <div className="page-heading"><div><h1>Runs</h1><p>Latest 100 · newest first</p></div><button type="button" onClick={runs.reload}>Refresh</button></div>
    <div className="filters">
      <label className="search"><span className="sr-only">Search latest runs</span><input type="search" value={query} onChange={event => setFilter("q", event.target.value)} placeholder="Search runs…" /></label>
      <label><span className="sr-only">Result filter</span><select value={filter} onChange={event => setFilter("status", event.target.value)}><option value="all">All results</option><option value="active">Active</option><option value="completed">Completed</option><option value="failed">Failed</option>{extraStatuses.map(value => <option key={value} value={value}>{statusLabel(value)}</option>)}</select></label>
      {runs.data ? <span className="filter-count">{query || filter !== "all" ? `${visible.length} of ${all.length}` : `${all.length} runs`}</span> : null}
    </div>
    {runs.error ? <Notice retry={runs.reload}>{runs.data ? "Could not refresh. Showing the last loaded runs." : "Could not load runs."}</Notice> : null}
    {runs.loading ? <Notice>Loading runs…</Notice> : null}
    {runs.data && !visible.length ? <Notice>{all.length ? "No matching runs in the latest 100." : "No runs yet."}</Notice> : null}
    {visible.length ? <div className="runs-table-wrap"><table className="runs-table">
      <thead><tr><th>Result</th><th>Run</th><th>Started</th><th>Duration</th></tr></thead>
      <tbody>{visible.map(run => <tr key={run.slug}>
        <td className="result-cell"><Status value={run.status} /></td>
        <td className="run-cell"><Link to={runPath(namespace, run.slug)} title={run.slug}>{run.pipeline || run.slug}</Link></td>
        <td className="started-cell"><time dateTime={run.started_at} title={fullTime(run.started_at)}>{new Date(run.started_at).toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" })}</time></td>
        <td className="duration-cell"><span title={run.completed_at ? "Completed duration" : "Elapsed since start"}>{duration(run.started_at, run.completed_at ?? now)}</span></td>
      </tr>)}</tbody>
    </table></div> : null}
  </Shell>;
}

function Graph({ topology, latest, selected, namespace, slug, completed, now }: {
  topology: PrecedenceTopology; latest: Job[]; selected?: string; namespace: string; slug: string; completed: boolean; now: number;
}) {
  const stage = useRef<HTMLDivElement>(null);
  const layers = new Map<number, typeof topology.jobs>();
  for (const job of topology.jobs) layers.set(job.rank, [...(layers.get(job.rank) ?? []), job]);
  const rows = Math.max(1, ...[...layers.values()].map(layer => layer.length));
  const columns = Math.max(0, ...topology.jobs.map(job => job.rank)) + 1;
  const height = rows * 86 + 16, width = columns * 224 - 24;
  const positions = new Map(topology.jobs.map(job => {
    const layer = layers.get(job.rank)!;
    return [job.id, { x: job.rank * 224 + 4, y: 8 + (height - 16 - layer.length * 86) / 2 + layer.indexOf(job) * 86 }];
  }));
  useEffect(() => {
    const viewport = stage.current;
    const node = viewport && [...viewport.querySelectorAll<HTMLElement>("[data-job]")].find(element => element.dataset.job === selected);
    if (viewport && node) {
      viewport.scrollLeft = Math.max(0, node.offsetLeft + node.offsetWidth / 2 - viewport.clientWidth / 2);
      viewport.scrollTop = Math.max(0, node.offsetTop + node.offsetHeight / 2 - viewport.clientHeight / 2);
    }
  }, [selected, topology.jobs.length]);
  return <div className="graph-scroll" ref={stage} aria-label="Job dependency graph">
    <div className="graph-canvas" style={{ width, height }}>
      <svg className="graph-lines" width={width} height={height} aria-hidden="true"><defs><marker id="arrow" markerWidth="6" markerHeight="6" refX="5" refY="3" orient="auto"><path d="M0,0 L6,3 L0,6" /></marker></defs>
        {topology.relations.map(edge => {
          const from = positions.get(edge.source)!, to = positions.get(edge.target)!;
          const x = from.x + 192, y = from.y + 33, tx = to.x - 3, ty = to.y + 33;
          return <path key={edge.id} d={`M ${x} ${y} C ${x + 18} ${y}, ${tx - 18} ${ty}, ${tx} ${ty}`} markerEnd="url(#arrow)" />;
        })}
      </svg>
      {topology.jobs.map(node => {
        const job = latest.find(value => value.job === node.id), position = positions.get(node.id)!;
        const contents = <><span className="graph-job-name">{node.label}</span><span className="graph-job-meta">{job ? <><Status value={job.status} /><span>{duration(job.started_at, job.completed_at ?? now)}</span></> : <span>{completed ? "No Job recorded" : "Not started"}</span>}</span></>;
        const props = { className: `graph-job ${selected === node.id ? "selected" : ""} ${job ? "" : "unstarted"}`, style: { left: position.x, top: position.y }, "data-job": node.id, title: node.id };
        return job ? <Link {...props} key={node.id} to={logPath(namespace, slug, node.id)} aria-current={selected === node.id ? "true" : undefined}>{contents}</Link> : <div {...props} key={node.id}>{contents}</div>;
      })}
    </div>
  </div>;
}

function JobList({ jobs, namespace, slug, selected, now }: { jobs: Job[]; namespace: string; slug: string; selected?: string; now: number }) {
  return <div className="job-list">{jobs.map(job => <Link key={job.job} className={selected === job.job ? "selected" : ""} to={logPath(namespace, slug, job.job)} aria-current={selected === job.job ? "true" : undefined}><span>{job.job}</span><Status value={job.status} /><span className="numeric">{duration(job.started_at, job.completed_at ?? now)}</span></Link>)}</div>;
}

function RunDetails({ run, namespace, selected }: { run: RunDetail; namespace: string; selected?: string }) {
  return <details className="run-details"><summary>Run details</summary>
    <dl><div><dt>Run</dt><dd><code>{run.slug}</code></dd></div><div><dt>Namespace</dt><dd>{namespace}</dd></div><div><dt>Started</dt><dd>{fullTime(run.started_at)}</dd></div><div><dt>Completed</dt><dd>{fullTime(run.completed_at)}</dd></div></dl>
    <h3>Job attempts</h3>
    <div className="details-scroll"><table className="attempts-table"><thead><tr><th>Job</th><th>Result</th><th>Started</th><th>Completed</th><th>Recorded exit</th></tr></thead><tbody>{(run.jobs ?? []).map((job, index) => <tr key={`${job.job}:${job.started_at}:${index}`} className={selected === job.job ? "selected-attempt" : ""}><td>{job.job}</td><td>{statusLabel(job.status)}</td><td>{fullTime(job.started_at)}</td><td>{fullTime(job.completed_at)}</td><td>{job.exit_code ?? "Not recorded"}</td></tr>)}</tbody></table></div>
    <p>Arrows show the order declared by the pipeline. Job times and results come from executor records. Live output reports the Pod seen by that connection; stored output is the latest available copy for the Job.</p>
  </details>;
}

function RunRoute() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const explicit = useMatch("/:namespace/runs/:slug/logs/:job");
  return <RunPage key={`${namespace}:${slug}`} namespace={namespace} slug={slug} explicitJob={explicit?.params.job} />;
}

function RunPage({ namespace, slug, explicitJob }: { namespace: string; slug: string; explicitJob?: string }) {
  const run = useRemote(() => fetchRun(namespace, slug), 10_000);
  const dag = useRemote(async () => {
    const value = await fetchDag(namespace, slug);
    if (!Array.isArray(value.dag)) throw new Error("Invalid declaration");
    return value;
  }, 10_000);
  const [picked, setPicked] = useState<string>();
  const topology = useMemo(() => dag.data ? buildPrecedenceTopology(dag.data.dag) : undefined, [dag.data]);
  const jobs = run.data?.jobs ?? [];
  const latest = latestJobs(jobs);
  const selected = explicitJob ?? picked;
  const attempts = jobs.filter(job => job.job === selected).sort((a, b) => Date.parse(a.started_at) - Date.parse(b.started_at));
  const newest = attempts.at(-1);
  const now = useNow(Boolean(run.data && (!run.data.completed_at || latest.some(job => !job.completed_at))));
  useEffect(() => {
    if (!explicitJob && !picked && jobs.length) setPicked(initialJob(jobs, topology?.jobs.map(job => job.id)));
  }, [explicitJob, picked, jobs, topology]);
  useEffect(() => {
    const events = openRunEvents(namespace, slug);
    events.onmessage = message => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (/^(run_|job_|dag_)/.test(event.kind)) { run.reload(); dag.reload(); }
      } catch { /* Ten-second HTTP reads remain available when events drop. */ }
    };
    return () => events.close();
  }, [namespace, slug, run.reload, dag.reload]);
  useEffect(() => { document.title = `Coalesce · ${run.data?.pipeline || slug}`; }, [run.data?.pipeline, slug]);
  const unplaced = topology ? latest.filter(job => !topology.jobs.some(node => node.id === job.job)) : latest;
  return <Shell namespace={namespace}>
    <nav className="breadcrumb"><Link to={runsPath(namespace)}>← Runs</Link></nav>
    <header className="run-heading"><h1>{run.data?.pipeline || slug}</h1>{run.data ? <div className="run-summary"><Status value={run.data.status} /><span className="summary-divider">·</span><span className="run-duration" title={run.data.completed_at ? "Completed duration" : "Elapsed since start"}>{duration(run.data.started_at, run.data.completed_at ?? now)}</span><span className="run-start">Started {new Date(run.data.started_at).toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })}</span></div> : null}</header>
    {run.error ? <Notice retry={run.reload}>{run.data ? "Could not refresh this run. Showing the last loaded state." : run.error instanceof ApiError && run.error.status === 404 ? "Run not found." : "Could not load this run."}</Notice> : null}
    {run.loading ? <Notice>Loading run…</Notice> : null}
    {run.data ? <>
      <section className="jobs-panel" aria-labelledby="jobs-title"><div className="jobs-heading"><h2 id="jobs-title">Jobs</h2><span>Build order · select a Job to view output</span></div>
        {topology?.jobs.length ? <Graph topology={topology} latest={latest} selected={selected} namespace={namespace} slug={slug} completed={Boolean(run.data.completed_at)} now={now} /> : null}
        {unplaced.length ? <>{topology?.jobs.length ? <p className="unplaced-note">Other recorded Jobs</p> : null}<JobList jobs={unplaced} namespace={namespace} slug={slug} selected={selected} now={now} /></> : null}
        {dag.error ? <p className="subtle-note">{dag.error instanceof ApiError && dag.error.status === 404 ? "No diagram available." : "Could not refresh the diagram."} <button type="button" className="text-button" onClick={dag.reload}>Retry</button></p> : null}
        {!jobs.length ? <p className="subtle-note">{run.data.completed_at ? "No Jobs were recorded." : "Waiting for the first Job…"}</p> : null}
      </section>
    </> : null}
    {selected ? <Output key={JSON.stringify([namespace, slug, selected, newest?.started_at])} namespace={namespace} slug={slug} job={selected} attempts={attempts} /> : run.data ? <div className="output-placeholder">Output will appear here when a Job is available.</div> : null}
    {run.data ? <RunDetails run={run.data} namespace={namespace} selected={selected} /> : null}
  </Shell>;
}

export function App() {
  return <Routes><Route path="/" element={<Navigate to="/coalesce/runs" replace />} /><Route path="/:namespace/runs" element={<RunsRoute />} /><Route path="/:namespace/runs/:slug/*" element={<RunRoute />} /><Route path="*" element={<Navigate to="/coalesce/runs" replace />} /></Routes>;
}
