import { useCallback, useEffect, useId, useMemo, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { Link, Navigate, Route, Routes, useParams } from "react-router-dom";
import type { Core, ElementDefinition, StylesheetJson } from "cytoscape";
import {
  ApiError, ShapeError, containerOf, fetchDag, fetchLog, fetchRun, fetchRuns,
  openLogTail, openRunEvents, parseStreamEvent,
  type DagNode, type DagResponse, type Job, type Run, type RunDetail,
} from "../../shared/api";
import { buildPrecedenceTopology } from "./topology";

interface Remote<T> { data?: T; error?: unknown; loading: boolean; updatedAt?: number }

function useRemote<T>(identity: string, read: () => Promise<T>, poll = 0):
  Remote<T> & { reload: () => void } {
  const readRef = useRef(read);
  readRef.current = read;
  const [revision, setRevision] = useState(0);
  const [remote, setRemote] = useState<Remote<T>>({ loading: true });
  const reload = useCallback(() => setRevision((value) => value + 1), []);
  useEffect(() => {
    let current = true;
    const load = async (initial: boolean) => {
      if (initial) setRemote({ loading: true });
      try {
        const data = await readRef.current();
        if (current) setRemote({ data, loading: false, updatedAt: Date.now() });
      } catch (error) {
        if (current) setRemote({ error, loading: false, updatedAt: Date.now() });
      }
    };
    void load(true);
    const timer = poll ? window.setInterval(() => void load(false), poll) : undefined;
    return () => {
      current = false;
      if (timer !== undefined) window.clearInterval(timer);
    };
  }, [identity, poll, revision]);
  return { ...remote, reload };
}

const part = (value: string) => encodeURIComponent(value);
const runsPath = (namespace: string) => `/${part(namespace)}/runs`;
const runPath = (namespace: string, slug: string) => `${runsPath(namespace)}/${part(slug)}`;
const logPath = (namespace: string, slug: string, job: string) =>
  `${runPath(namespace, slug)}/logs/${part(job)}`;

function JobOutputLink({ namespace, slug, job, children, className = "", describedBy }: {
  namespace: string; slug: string; job: string; children?: ReactNode; className?: string; describedBy?: string;
}) {
  return <Link className={`job-output-link ${className}`} to={logPath(namespace, slug, job)} aria-label={`Output for Job ${job}`} aria-describedby={describedBy}>{children ?? job}</Link>;
}

function fullTime(value?: string): string {
  if (!value) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric", month: "short", day: "2-digit", hour: "2-digit",
    minute: "2-digit", second: "2-digit", timeZoneName: "short",
  }).format(new Date(value));
}

function shortTime(value?: string): string {
  if (!value) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(new Date(value));
}

function readTime(value?: number): string {
  if (!value) return "Awaiting response";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit", minute: "2-digit", second: "2-digit",
  }).format(new Date(value));
}

function span(started: string, ended: string | number): string {
  const end = typeof ended === "number" ? ended : Date.parse(ended);
  const total = Math.max(0, Math.floor((end - Date.parse(started)) / 1_000));
  const days = Math.floor(total / 86_400);
  const h = Math.floor((total % 86_400) / 3_600);
  const m = Math.floor((total % 3_600) / 60);
  const s = total % 60;
  const clock = [h, m, s].map((value) => String(value).padStart(2, "0")).join(":");
  return days ? `${days}d ${clock}` : clock;
}

const cssStatus = (value: string) => value.toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
type ClaimKind = "recorded" | "derived" | "observed" | "unavailable" | "failed";

function Claim({ kind, children }: { kind: ClaimKind; children: ReactNode }) {
  return <span className={`claim claim-${kind}`}><i aria-hidden="true" />{children}</span>;
}

function Shell({ namespace, children }: { namespace: string; children: ReactNode }) {
  return (
    <div className="app-shell">
      <a className="skip-link" href="#content">Skip to content</a>
      <header className="site-head">
        <div className="head-inner">
          <div className="brand">
            <Link to={runsPath(namespace)}>Coalesce</Link>
            <span>Bounded run evidence</span>
          </div>
          <p className="namespace"><span>Namespace</span><code>{namespace}</code></p>
          <div className="claim-key" aria-label="Claim key">
            <span>Claim key</span>
            <Claim kind="recorded">Recorded</Claim>
            <Claim kind="observed">Observed now</Claim>
            <Claim kind="unavailable">Unavailable</Claim>
          </div>
        </div>
      </header>
      <main id="content" className="page">{children}</main>
      <footer className="site-foot"><div><span>Exhibit 02</span><span>The record says only what reached it.</span></div></footer>
    </div>
  );
}

function errorAccount(error: unknown) {
  if (error instanceof ApiError) {
    return {
      label: "HTTP answer",
      title: `Coalesce answered ${error.status}.`,
      detail: error.body.split("\n", 1)[0].trim() || "The server supplied no further account.",
    };
  }
  if (error instanceof ShapeError) {
    return { label: "Invalid response", title: "The response was not Coalesce JSON.", detail: error.message };
  }
  return {
    label: "No answer",
    title: "This browser did not receive a Coalesce response.",
    detail: error instanceof Error ? error.message : "The request failed before a response arrived.",
  };
}

function Problem({ error, retry, primary = false }: { error: unknown; retry?: () => void; primary?: boolean }) {
  const value = errorAccount(error);
  const Heading = primary ? "h1" : "h2";
  return (
    <section className="notice notice-problem" role="alert">
      <Claim kind="unavailable">{value.label}</Claim>
      <Heading>{value.title}</Heading><p>{value.detail}</p>
      {retry ? <button className="text-action" type="button" onClick={retry}>Read again</button> : null}
    </section>
  );
}

function Loading({ children }: { children: ReactNode }) {
  return <div className="loading" role="status"><i aria-hidden="true" /><p>{children}</p></div>;
}

function Empty({ label, title, children, primary = false }: { label: string; title: string; children: ReactNode; primary?: boolean }) {
  const Heading = primary ? "h1" : "h2";
  return <section className="notice"><Claim kind="unavailable">{label}</Claim><Heading>{title}</Heading><p>{children}</p></section>;
}

function StatusAccount({ run, compact = false }: { run: Run; compact?: boolean }) {
  const closed = Boolean(run.completed_at);
  const kind: ClaimKind = run.status === "failed" ? "failed" : closed ? "recorded" : "unavailable";
  return (
    <div className={`status-account status-${cssStatus(run.status)}`}>
      <Claim kind={kind}>{closed ? run.status : "Unclosed"}</Claim>
      {closed && compact ? null : <span>{closed ? "Executor outcome · record closed" : compact ? `Stored status: ${run.status}` : `No completion timestamp · stored status: ${run.status}`}</span>}
    </div>
  );
}

function UnclosedJobs({ namespace, run }: { namespace: string; run: Run }) {
  const detail = useRemote(`unclosed:${namespace}:${run.slug}`, () => fetchRun(namespace, run.slug));
  if (detail.loading) return <span className="row-note">Reading Job records…</span>;
  if (detail.error) return <span className="row-note">Job records unavailable</span>;
  const jobs = detail.data?.jobs ?? [];
  const open = jobs.filter((job) => !job.completed_at).length;
  if (!jobs.length) return <span className="row-corroboration"><Claim kind="unavailable">Job snapshot</Claim><span>No Job record has arrived</span></span>;
  if (!open) return <span className="row-corroboration"><Claim kind="recorded">Job snapshot</Claim><span>{jobs.length === 1 ? "The recorded Job is closed; run closure is absent" : `All ${jobs.length} recorded Jobs are closed; run closure is absent`}</span></span>;
  return <span className="row-corroboration"><Claim kind="unavailable">Job snapshot</Claim><span>{open} {open === 1 ? "Job record" : "Job records"} of {jobs.length} {open === 1 ? "has" : "have"} no closure</span></span>;
}

type RunFilter = "all" | "unclosed" | "failed" | "closed";

function RunsRoute() {
  const { namespace = "coalesce" } = useParams();
  const [filter, setFilter] = useState<RunFilter>("all");
  const [query, setQuery] = useState("");
  const runs = useRemote(`runs:${namespace}`, () => fetchRuns(namespace), 15_000);
  const now = runs.updatedAt ?? Date.now();
  useEffect(() => { document.title = `Coalesce — ${namespace} record`; }, [namespace]);
  const all = runs.data ?? [];
  const counts = {
    all: all.length,
    unclosed: all.filter((run) => !run.completed_at).length,
    failed: all.filter((run) => run.status === "failed").length,
    closed: all.filter((run) => run.completed_at).length,
  };
  const needle = query.trim().toLowerCase();
  const visible = all.filter((run) => {
    const selected = filter === "all" ||
      (filter === "unclosed" && !run.completed_at) ||
      (filter === "failed" && run.status === "failed") ||
      (filter === "closed" && Boolean(run.completed_at));
    const closureVocabulary = run.completed_at ? "closed" : "unclosed";
    const closureQuery = needle === "closed" || needle === "unclosed";
    const textMatch = [run.slug, run.pipeline, run.status].some((value) => value.toLowerCase().includes(needle));
    return selected && (!needle || (closureQuery ? closureVocabulary === needle : textMatch));
  });
  const filterLabel: Record<RunFilter, string> = { all: "all available records", unclosed: "records without closure", failed: "failed records", closed: "records with closure" };
  return (
    <Shell namespace={namespace}>
      <header className="page-intro">
        <div><p className="eyebrow">Run register / latest response</p><h1>{runs.error ? "No usable record arrived" : "The record available now"}</h1>
          <p className="lede">Coalesce returns its newest records first. This view describes that bounded response; it is not a claim about the cluster or the full archive.</p>
        </div>
        <aside className="scope-account">
          <span>HTTP read</span><strong>{runs.data ? `${runs.data.length} records` : "No response yet"}</strong>
          <p>{runs.error ? "No usable run list is available, so no limit claim can be made." : runs.data?.length === 100 ? "The endpoint limit was reached. Older records may exist." : runs.data ? "The response is below the 100-record limit." : "Waiting for the run-list response."}</p>
          <time>{readTime(runs.updatedAt)}</time>
        </aside>
      </header>
      {runs.loading ? <Loading>Reading the latest run records…</Loading> : null}
      {runs.error ? <Problem error={runs.error} retry={runs.reload} /> : null}
      {runs.data?.length === 0 ? <Empty label="Empty response" title="No run record was returned.">A successful empty list does not prove whether namespace <code>{namespace}</code> exists; the contract exposes no namespace lookup.</Empty> : null}
      {runs.data && runs.data.length > 0 ? (
        <section className="register" aria-labelledby="register-title">
          <div className="section-heading"><div><p className="eyebrow">Recorded window</p><h2 id="register-title">Newest first</h2></div>
            <button className="read-action" type="button" onClick={runs.reload}>Read HTTP again</button>
          </div>
          <div className="register-controls">
            <div className="scope-filters" aria-label="Filter run records">
              <div className="filter-set"><span className="filter-label">Closure · partitions this response</span><div>
                {(["all", "unclosed", "closed"] as RunFilter[]).map((value) => <button key={value} type="button" className={filter === value ? "selected" : ""} aria-pressed={filter === value} onClick={() => setFilter(value)}><strong>{counts[value]}</strong><span>{value === "all" ? "Available" : value[0].toUpperCase() + value.slice(1)}</span></button>)}
              </div></div>
              <div className="filter-set outcome-filter"><span className="filter-label">Outcome · subset of closed</span><div><button type="button" className={filter === "failed" ? "selected" : ""} aria-pressed={filter === "failed"} onClick={() => setFilter("failed")}><strong>{counts.failed}</strong><span>Failed</span></button></div></div>
            </div>
            <label className="record-search"><span>Search these {all.length} records</span>
              <input type="search" value={query} placeholder="Run, pipeline, status, closed or unclosed" onChange={(event) => setQuery(event.target.value)} />
            </label>
          </div>
          <div className="filter-account" aria-live="polite"><span>Showing {visible.length} of {all.length}{filter === "all" ? needle ? "" : " · complete available window" : ` · ${filterLabel[filter]}`}{needle ? ` · search “${query.trim()}”` : ""}</span>{filter !== "all" ? <button type="button" onClick={() => setFilter("all")}>Clear filter</button> : null}</div>
          {visible.length ? (
            <div className="table-frame"><table className="run-table">
              <caption>Run records returned for namespace {namespace}, filtered to {filter}</caption>
              <thead><tr><th scope="col">Recorded claim</th><th scope="col">Run identity</th><th scope="col">Opened</th><th scope="col">Closure or elapsed</th></tr></thead>
              <tbody>{visible.map((run) => (
                <tr key={run.slug} className={`run-row row-${run.completed_at ? "closed" : "unclosed"} row-${cssStatus(run.status)}`}>
                  <td data-label="Recorded claim"><StatusAccount run={run} compact />{!run.completed_at ? <UnclosedJobs namespace={namespace} run={run} /> : null}</td>
                  <td data-label="Run identity"><Link className="identity-link" to={runPath(namespace, run.slug)}>{run.slug}</Link><span className="pipeline-statement">{run.pipeline}</span></td>
                  <td data-label="Opened"><time dateTime={run.started_at}>{shortTime(run.started_at)}</time></td>
                  <td data-label="Closure or elapsed">{run.completed_at ? <><time dateTime={run.completed_at}>{shortTime(run.completed_at)}</time><span className="row-note">{span(run.started_at, run.completed_at)} recorded span</span></> : <><strong className="age-value">+{span(run.started_at, now)}</strong><span className="row-note">Elapsed at HTTP read; not proof of activity</span></>}</td>
                </tr>
              ))}</tbody>
            </table></div>
          ) : <Empty label="No matching record" title="The available window has no match.">Change the literal search or selected claim class.</Empty>}
        </section>
      ) : null}
    </Shell>
  );
}

interface RunRecord { run: RunDetail; dag: DagResponse | null }

async function readRunRecord(namespace: string, slug: string): Promise<RunRecord> {
  const run = await fetchRun(namespace, slug);
  try {
    return { run, dag: await fetchDag(namespace, slug) };
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return { run, dag: null };
    throw error;
  }
}

function dagCount(nodes: DagNode[]): number {
  return nodes.reduce((sum, node) => sum + 1 + dagCount(node.children ?? []), 0);
}

function DeclarationJobStatus({ attempts, id }: { attempts: Job[]; id: string }) {
  // The run endpoint orders attempts by started_at; status here belongs to
  // the latest record, not to a live Pod or a particular stored output.
  const latest = attempts.at(-1);
  const closed = Boolean(latest?.completed_at);
  const label = !latest ? "No Job record" : !closed ? `Unclosed · recorded ${latest.status}` : latest.status === "completed" ? "Completed" : latest.status === "failed" ? "Failed" : `Recorded ${latest.status}`;
  const kind = latest?.status === "failed" ? "failed" : closed ? "recorded" : "unavailable";
  return <span id={id} className={`dag-job-status job-state-${kind}`}>
    <span>{label}</span>{attempts.length > 1 ? <span className="job-attempt-count">Latest of {attempts.length} attempts</span> : null}
  </span>;
}

function DagList({ nodes, namespace, slug, jobs, prefix = "" }: {
  nodes: DagNode[]; namespace: string; slug: string; jobs: Job[]; prefix?: string;
}) {
  const listId = useId();
  return <ol className={prefix ? "dag-list dag-nested" : "dag-list"}>
    {nodes.map((node, index) => {
      const order = prefix ? `${prefix}.${String(index + 1).padStart(2, "0")}` : String(index + 1).padStart(2, "0");
      const identity = `${node.under}.${node.name}`;
      const attempts = node.kind === "node" ? jobs.filter((job) => job.job === identity) : [];
      const addressable = attempts.length > 0;
      const statusId = `${listId}-${order}`;
      const label = <><strong>{node.name}</strong><code>{identity}</code></>;
      return <li key={`${identity}:${order}`}>
        <div className="dag-node"><span className="dag-order">{order}</span>
          {addressable ? <JobOutputLink namespace={namespace} slug={slug} job={identity} className="dag-identity" describedBy={statusId}>{label}</JobOutputLink> : <div className="dag-identity">{label}</div>}
          {node.kind === "tranche" ? <span className="dag-kind">{node.parallel ? "Parallel tranche" : "Ordered tranche"}</span> : <DeclarationJobStatus attempts={attempts} id={statusId} />}
        </div>
        {node.children?.length ? <DagList nodes={node.children} namespace={namespace} slug={slug} jobs={jobs} prefix={order} /> : null}
      </li>;
    })}
  </ol>;
}

function DagGraph({ nodes, createdAt }: { nodes: DagNode[]; createdAt: string }) {
  const container = useRef<HTMLDivElement>(null);
  const stageContainer = useRef<HTMLDivElement>(null);
  const [state, setState] = useState<"loading" | "ready" | "failed">("loading");
  const topology = useMemo(() => buildPrecedenceTopology(nodes), [createdAt]);
  const graphHeight = Math.max(18, Math.min(44, 13 + (Math.max(0, ...topology.jobs.map((job) => job.rank)) + 1) * 5));
  const positions = useMemo(() => {
    const layers = new Map<number, typeof topology.jobs>();
    for (const job of topology.jobs) layers.set(job.rank, [...(layers.get(job.rank) ?? []), job]);
    const predecessors = new Map<string, string[]>();
    for (const relation of topology.relations) predecessors.set(relation.target, [...(predecessors.get(relation.target) ?? []), relation.source]);
    const unitX = new Map<string, number>();
    const maximumLayer = Math.max(1, ...[...layers.values()].map((layer) => layer.length));
    const width = Math.max(760, maximumLayer * 150);
    for (const [, layer] of [...layers.entries()].sort(([a], [b]) => a - b)) {
      const ordered = [...layer].sort((a, b) => a.order - b.order);
      ordered.forEach((job, index) => {
        const incoming = predecessors.get(job.id) ?? [];
        const predecessorPositions = incoming.map((id) => unitX.get(id)).filter((value): value is number => value !== undefined);
        const distributed = ordered.length === 1 ? 0.5 : (index + 0.5) / ordered.length;
        unitX.set(job.id, ordered.length === 1 || incoming.length > 1
          ? predecessorPositions.length ? predecessorPositions.reduce((sum, value) => sum + value, 0) / predecessorPositions.length : distributed
          : distributed);
      });
    }
    return new Map(topology.jobs.map((job) => [job.id, { x: 70 + (unitX.get(job.id) ?? 0.5) * width, y: 70 + job.rank * 104 }]));
  }, [topology]);

  useEffect(() => {
    let active = true;
    let graph: Core | undefined;
    let observer: ResizeObserver | undefined;
    let frame = 0;
    setState("loading");

    void import("cytoscape").then(({ default: cytoscape }) => {
      if (!active || !container.current) return;
      const elements: ElementDefinition[] = [
        ...topology.jobs.map((job) => ({
          data: { id: job.id, label: job.label, order: job.order },
          classes: "topology-job",
        })),
        ...topology.relations.map((relation) => ({
          data: { id: relation.id, source: relation.source, target: relation.target },
          classes: "precedence-relation",
        })),
      ];
      const style: StylesheetJson = [
        {
          selector: "node.topology-job",
          style: {
            "background-color": "#fbfcf9", "border-color": "#26383d", "border-width": 2,
            color: "#152024", label: "data(label)", shape: "round-rectangle", width: 132, height: 48,
            "font-family": "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace", "font-size": 11,
            "font-weight": 700, "text-halign": "center", "text-valign": "center",
            "text-max-width": "116px", "text-wrap": "wrap",
          },
        },
        {
          selector: "edge.precedence-relation",
          style: {
            width: 1.5, "line-color": "#59676b", "target-arrow-color": "#59676b",
            "target-arrow-shape": "triangle", "arrow-scale": 0.8, "curve-style": "taxi",
            "taxi-direction": "downward", "taxi-turn": 24, "taxi-turn-min-distance": 8,
          },
        },
      ];
      graph = cytoscape({
        container: container.current,
        elements,
        style,
        layout: { name: "preset", positions: Object.fromEntries(positions), fit: true, padding: 30 },
        autoungrabify: true,
        autounselectify: true,
        boxSelectionEnabled: false,
        userPanningEnabled: false,
        userZoomingEnabled: false,
      });
      const refit = () => {
        window.cancelAnimationFrame(frame);
        frame = window.requestAnimationFrame(() => {
          graph?.resize();
          graph?.fit(graph.elements(), 30);
        });
      };
      observer = new ResizeObserver(refit);
      observer.observe(container.current);
      refit();
      setState("ready");
    }).catch(() => {
      if (active) setState("failed");
    });

    return () => {
      active = false;
      window.cancelAnimationFrame(frame);
      observer?.disconnect();
      graph?.destroy();
    };
  }, [topology]);

  useEffect(() => {
    if (state !== "ready") return;
    const timer = window.setTimeout(() => {
      const stage = stageContainer.current;
      if (stage && stage.scrollWidth > stage.clientWidth) {
        stage.scrollLeft = (stage.scrollWidth - stage.clientWidth) / 2;
      }
    }, 100);
    return () => window.clearTimeout(timer);
  }, [state, topology]);

  return <section className="topology-panel" aria-labelledby="topology-title">
    <header className="topology-heading"><div><Claim kind="derived">Derived view</Claim><h4 id="topology-title">Inferred Job precedence</h4></div>
      <p><strong>{topology.jobs.length}</strong> {topology.jobs.length === 1 ? "Job identity" : "Job identities"} · <strong>{topology.relations.length}</strong> inferred precedence {topology.relations.length === 1 ? "relation" : "relations"}</p>
    </header>
    <p className="topology-caption">Each arrow means only that its source Job must precede its target Job under the recorded ordered/parallel tranche semantics. It is not observed timing, an execution trace, or dataflow. Parallel permits fan-out; it does not prove simultaneity.</p>
    <p className="topology-version">Derived from the declaration recorded <time dateTime={createdAt}>{fullTime(createdAt)}</time>.</p>
    <div ref={stageContainer} className="topology-stage" style={{ "--topology-height": `${graphHeight}rem` } as CSSProperties}>
      <div ref={container} className="topology-canvas" aria-hidden="true" />
      {state === "loading" ? <div className="topology-state" role="status">Drawing the inferred precedence view…</div> : null}
      {state === "failed" ? <div className="topology-state topology-state-failed" role="status">The inferred drawing is unavailable. The recorded declaration remains above.</div> : null}
    </div>
  </section>;
}

type SequenceEvent =
  | { kind: "opened"; at: string }
  | { kind: "declaration"; at: string; dag: DagResponse }
  | { kind: "job"; at: string; job: Job; ordinal: number; total: number }
  | { kind: "closed"; at: string };

function sequenceFor(record: RunRecord): SequenceEvent[] {
  const counts = new Map<string, number>();
  for (const job of record.run.jobs ?? []) counts.set(job.job, (counts.get(job.job) ?? 0) + 1);
  const seen = new Map<string, number>();
  const events: SequenceEvent[] = [{ kind: "opened", at: record.run.started_at }];
  if (record.dag) events.push({ kind: "declaration", at: record.dag.created_at, dag: record.dag });
  const jobs = record.run.jobs ?? [];
  jobs.forEach((job) => {
    const ordinal = (seen.get(job.job) ?? 0) + 1;
    seen.set(job.job, ordinal);
    events.push({
      kind: "job", at: job.started_at, job, ordinal, total: counts.get(job.job) ?? 1,
    });
  });
  if (record.run.completed_at) events.push({ kind: "closed", at: record.run.completed_at });
  const priority: Record<SequenceEvent["kind"], number> = { opened: 0, declaration: 1, job: 2, closed: 3 };
  return events.sort((a, b) => Date.parse(a.at) - Date.parse(b.at) || priority[a.kind] - priority[b.kind]);
}

function Fact({ label, children, missing = false }: { label: string; children: ReactNode; missing?: boolean }) {
  return <div className={missing ? "fact fact-missing" : "fact"}><dt>{label}</dt><dd>{children}</dd></div>;
}

function SequenceItem({ event, namespace, slug, now, runStatus, jobs }: {
  event: SequenceEvent; namespace: string; slug: string; now: number; runStatus: string; jobs: Job[];
}) {
  if (event.kind === "opened") return <li className="sequence-item event-recorded">
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body"><Claim kind="recorded">Executor record</Claim><h3>Run opened</h3><p>The original start remains attached to this run identity across resume.</p></div>
  </li>;
  if (event.kind === "declaration") return <li className="sequence-item event-recorded">
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body"><Claim kind="recorded">Latest declaration</Claim><h3>{dagCount(event.dag.dag)} declared {dagCount(event.dag.dag) === 1 ? "object" : "objects"}</h3>
      <p>The endpoint returns the newest stored DAG version. It records names, parents, kinds, and nesting—not literal edges.</p>
      <section className="declaration-record" aria-labelledby="declaration-record-title"><div className="declaration-heading"><Claim kind="recorded">Recorded declaration</Claim><h4 id="declaration-record-title">Nested objects as returned</h4></div><DagList nodes={event.dag.dag} namespace={namespace} slug={slug} jobs={jobs} /></section>
      <DagGraph nodes={event.dag.dag} createdAt={event.dag.created_at} />
    </div>
  </li>;
  if (event.kind === "closed") return <li className={`sequence-item event-${runStatus === "failed" ? "failed" : "recorded"}`}>
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body"><Claim kind={runStatus === "failed" ? "failed" : "recorded"}>Executor outcome</Claim><h3>Run closed as {runStatus}</h3><p>Coalesce received the closing write and recorded this timestamp.</p></div>
  </li>;

  const job = event.job;
  const closed = Boolean(job.completed_at);
  const failed = job.status === "failed";
  return <li className={`sequence-item event-${failed ? "failed" : closed ? "recorded" : "unavailable"}`}>
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body job-event">
      <Claim kind={failed ? "failed" : closed ? "recorded" : "unavailable"}>{closed ? "Job attempt" : "Unclosed Job"}</Claim>
      <div className="event-title-line"><div><h3><JobOutputLink namespace={namespace} slug={slug} job={job.job} /></h3><p>{event.total > 1 ? `Attempt ${event.ordinal} of ${event.total} for this Job identity` : "One recorded attempt for this Job identity"}</p></div><span className={`job-status status-${cssStatus(job.status)}`}>Stored · {job.status}</span></div>
      <dl className="event-facts">
        <Fact label="Opened"><time dateTime={job.started_at}>{fullTime(job.started_at)}</time></Fact>
        <Fact label="Closure" missing={!job.completed_at}>{job.completed_at ? <time dateTime={job.completed_at}>{fullTime(job.completed_at)}</time> : "Not recorded"}</Fact>
        <Fact label={closed ? "Recorded span" : "Elapsed at latest HTTP read"}><span className="numeric">{closed ? "" : "+"}{span(job.started_at, job.completed_at ?? now)}</span></Fact>
        <Fact label="Exit code" missing={job.exit_code == null}>{job.exit_code ?? "Not recorded"}</Fact>
      </dl>
      {event.total > 1 ? <p className="output-address-note">Output is shared by this Job identity. The latest stored output cannot be selected by attempt.</p> : null}
    </div>
  </li>;
}

function RunJobAccount({ run, namespace }: { run: RunDetail; namespace: string }) {
  const jobs = run.jobs ?? [];
  if (!jobs.length) return <aside className="job-account"><Claim kind="unavailable">Job snapshot</Claim><div><h2>No Job record has arrived.</h2><p>The run response supplies no attempt facts to compare with its status.</p></div></aside>;
  const failed = jobs.filter((job) => job.status === "failed");
  const unclosed = jobs.filter((job) => !job.completed_at);
  if (failed.length) {
    const latest = failed.at(-1)!;
    return <aside className="job-account job-account-failed"><Claim kind="failed">Job snapshot</Claim><div><h2><JobOutputLink namespace={namespace} slug={run.slug} job={latest.job} /> failed.</h2><p>{failed.length} failed {failed.length === 1 ? "attempt appears" : "attempts appear"} in this snapshot. Output uses the latest stored address for the Job.</p></div></aside>;
  }
  if (unclosed.length) return <aside className="job-account"><Claim kind="unavailable">Job snapshot</Claim><div><h2>{unclosed.length} {unclosed.length === 1 ? "Job record has" : "Job records have"} no closure.</h2><p>{unclosed.map((job, index) => <span key={`${job.job}:${job.started_at}`}>{index ? ", " : ""}<JobOutputLink namespace={namespace} slug={run.slug} job={job.job} /></span>)}</p></div></aside>;
  return <aside className="job-account"><Claim kind="recorded">Job snapshot</Claim><div><h2>Every recorded Job is closed.</h2><p>{run.completed_at ? `${jobs.length} closed ${jobs.length === 1 ? "Job appears" : "Jobs appear"} in this snapshot.` : "Run closure remains absent; this corroboration does not supply it."}</p></div></aside>;
}

function RunRoute() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const record = useRemote(`run:${namespace}:${slug}`, () => readRunRecord(namespace, slug), 10_000);
  const unclosed = Boolean(record.data && !record.data.run.completed_at);
  const now = record.updatedAt ?? Date.now();
  useEffect(() => { document.title = `Coalesce — ${slug}`; }, [slug]);
  useEffect(() => {
    const socket = openRunEvents(namespace, slug);
    socket.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (/^(run_|job_|dag_)/.test(event.kind)) record.reload();
      } catch { /* HTTP polling remains the account when an advisory hint is malformed. */ }
    };
    return () => socket.close();
  }, [namespace, slug, record.reload]);
  const events = useMemo(() => record.data ? sequenceFor(record.data) : [], [record.data]);
  return <Shell namespace={namespace}>
    <nav className="breadcrumb" aria-label="Breadcrumb"><Link to={runsPath(namespace)}>← Latest run records</Link></nav>
    {record.loading ? <Loading>Reading run record {slug}…</Loading> : null}
    {record.error instanceof ApiError && record.error.status === 404 ? <Empty primary label="Missing record" title="Coalesce has no run at this address.">No run named <code>{slug}</code> was returned for namespace <code>{namespace}</code>.</Empty> : record.error ? <Problem primary error={record.error} retry={record.reload} /> : null}
    {record.data ? <article className="run-record">
      <header className="record-title"><div><p className="eyebrow">Pipeline statement / executor record</p><h1 className="pipeline-heading">{record.data.run.pipeline}</h1><p className="run-identity">Run identity <code>{record.data.run.slug}</code> · namespace <code>{namespace}</code></p></div><StatusAccount run={record.data.run} /></header>
      <section className={unclosed ? "claim-account account-unclosed" : "claim-account"}>
        <Claim kind={record.data.run.status === "failed" ? "failed" : unclosed ? "unavailable" : "recorded"}>{unclosed ? "Closure unavailable" : "Recorded run outcome"}</Claim>
        <h2>{unclosed ? "No closing write has reached this record." : `The executor closed this run as ${record.data.run.status}.`}</h2>
        <p>{unclosed ? `The stored status is “${record.data.run.status}.” That value and the age below do not establish current cluster activity.` : `Coalesce received completion at ${fullTime(record.data.run.completed_at)}.`}</p>
      </section>
      <RunJobAccount run={record.data.run} namespace={namespace} />
      <dl className="record-facts">
        <Fact label="Opened"><time dateTime={record.data.run.started_at}>{fullTime(record.data.run.started_at)}</time></Fact>
        <Fact label="Closure" missing={!record.data.run.completed_at}>{record.data.run.completed_at ? <time dateTime={record.data.run.completed_at}>{fullTime(record.data.run.completed_at)}</time> : "Not recorded"}</Fact>
        <Fact label={unclosed ? "Elapsed at latest HTTP read" : "Recorded span"}><span className="numeric numeric-large">{unclosed ? "+" : ""}{span(record.data.run.started_at, record.data.run.completed_at ?? now)}</span></Fact>
        <Fact label="Latest HTTP read">{readTime(record.updatedAt)}</Fact>
      </dl>
      <section className="sequence" aria-labelledby="sequence-title">
        <div className="section-heading sequence-heading"><div><p className="eyebrow">Accumulated record</p><h2 id="sequence-title">Assertions in time</h2></div><p>{events.length} {events.length === 1 ? "entry" : "entries"} · attempts remain separate</p></div>
        <ol className="sequence-list">{events.map((event, index) => <SequenceItem key={`${event.kind}:${event.at}:${index}`} event={event} namespace={namespace} slug={slug} now={now} runStatus={record.data!.run.status} jobs={record.data!.run.jobs ?? []} />)}
          {unclosed ? <li className="sequence-item sequence-open-end"><i className="sequence-marker" aria-hidden="true" /><span className="open-time">No timestamp</span><div className="event-body"><Claim kind="unavailable">Open boundary</Claim><h3>No run closure is recorded</h3><p>The sequence ends where the available evidence ends.</p></div></li> : null}
        </ol>
      </section>
    </article> : null}
  </Shell>;
}

interface RouteIdentity { namespace: string; slug: string; job: string }

function bytesLabel(value: number): string {
  return value < 1_024 ? `${value} B` : `${(value / 1_024).toFixed(value >= 10_240 ? 0 : 1)} KiB`;
}

function LogSurface({ text, label, caption, annotation, startAtEnd = false }: {
  text: string; label: string; caption?: ReactNode; annotation?: ReactNode; startAtEnd?: boolean;
}) {
  const [copyState, setCopyState] = useState("Copy exact text");
  const [wrap, setWrap] = useState(true);
  const frame = useRef<HTMLDivElement>(null);
  const end = useRef<HTMLSpanElement>(null);
  const placed = useRef(false);
  const bytes = useMemo(() => new TextEncoder().encode(text).length, [text]);
  const lines = useMemo(() => text ? text.split("\n").length - (text.endsWith("\n") ? 1 : 0) : 0, [text]);
  useEffect(() => {
    if (startAtEnd && text && !placed.current) {
      const timer = window.setTimeout(() => {
        placed.current = true;
        end.current?.scrollIntoView({ block: "center" });
      }, 100);
      return () => window.clearTimeout(timer);
    }
  }, [startAtEnd, text]);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setCopyState("Copied");
      window.setTimeout(() => setCopyState("Copy exact text"), 1_500);
    } catch { setCopyState("Copy unavailable"); }
  };
  return <div className="log-frame" ref={frame}>
    <div className="log-toolbar"><div><span>{label}</span>{caption ? <small aria-live="polite">{caption}</small> : null}<small>{lines} lines · {bytesLabel(bytes)}{startAtEnd ? " · opened at end" : ""}</small></div>
      <div className="log-actions"><button type="button" onClick={() => frame.current?.scrollIntoView({ block: "start" })}>Beginning</button><button type="button" onClick={() => end.current?.scrollIntoView({ block: "center" })}>End</button><button type="button" aria-pressed={!wrap} onClick={() => setWrap((value) => !value)}>{wrap ? "Preserve columns" : "Wrap lines"}</button><button type="button" onClick={() => void copy()}>{copyState}</button></div>
    </div>
    <pre className={`log-output ${wrap ? "" : "preserve-columns"}`} tabIndex={0} aria-label={label}>{text}</pre><span ref={end} className="log-end-marker" aria-hidden="true" />
    {text === "" ? <p className="log-empty" role="status">The response contained no output.</p> : null}
    {annotation ? <div className="log-annotation">{annotation}</div> : null}
  </div>;
}

function StoredLog({ log, job, attempts, primary }: {
  log: Remote<string> & { reload: () => void }; job: string; attempts: Job[]; primary: boolean;
}) {
  const latest = attempts.at(-1);
  if (log.loading) return <Loading>Reading stored output…</Loading>;
  if (log.error instanceof ApiError && log.error.status === 404) return <section className="notice artifact-missing" role="status">
    <Claim kind="unavailable">Stored output unavailable</Claim><h2>No stored output was found.</h2>
    <p>The server returned 404 for this Job and container. Output may still be collected later.</p>
    <button className="text-action" type="button" onClick={log.reload}>Check again</button>
  </section>;
  if (log.error) return <div className="output-problem"><p className="eyebrow">Stored output unavailable</p><Problem error={log.error} retry={log.reload} /></div>;
  return <LogSurface text={log.data ?? ""} label="Stored output" caption={attempts.length > 1 ? `${attempts.length} attempts · output attempt unspecified` : "Latest stored response"} startAtEnd={primary && latest?.status === "failed"} annotation={<>
    <p>Container <code>{containerOf(job)}</code>, from the Job name. Latest stored output; its attempt is unspecified.</p>
    {attempts.length > 1 ? <p>{attempts.length} attempts share this address. Output cannot be selected or attributed by attempt.</p> : null}
    {latest && !latest.completed_at ? <p>Job completion is not recorded. Stored output does not establish whether a process is still running.</p> : null}
  </>} />;
}

interface TailObservation { exitCode?: string; reason?: string; observedAt: number; text: string }
type TailState = "connecting" | "observed" | "error" | "exited" | "closed";

function StreamingLog({ namespace, slug, job, finished }: RouteIdentity & { finished: (value: TailObservation) => void }) {
  const [lines, setLines] = useState<string[]>([]);
  const linesRef = useRef<string[]>([]);
  const [state, setState] = useState<TailState>("connecting");
  const [note, setNote] = useState("Connecting to Pod output.");
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let current = true;
    let exited = false;
    let failed = false;
    linesRef.current = [];
    setLines([]);
    setState("connecting");
    setNote("Connecting to Pod output.");
    const tail = openLogTail(namespace, slug, job, containerOf(job));
    tail.onopen = () => { if (current) { setState("observed"); setNote("Connected to Pod output."); } };
    tail.onmessage = (message) => {
      if (!current) return;
      try {
        const event = parseStreamEvent(String(message.data));
        if (event.kind === "log_line") {
          linesRef.current = [...linesRef.current, String(event.data.line ?? "")];
          setLines(linesRef.current);
        }
        else if (event.kind === "log_status") { setState("observed"); setNote(`Pod phase observed: ${String(event.data.phase ?? "not supplied")}.`); }
        else if (event.kind === "log_exit") {
          exited = true;
          const value = { exitCode: String(event.data.exit_code ?? "not supplied"), reason: String(event.data.reason ?? "not supplied"), observedAt: Date.now(), text: linesRef.current.length ? `${linesRef.current.join("\n")}\n` : "" };
          setState("exited");
          setNote(`Process exit ${value.exitCode} observed; ${value.reason}.`);
          finished(value);
        } else if (event.kind === "log_error") { failed = true; setState("error"); setNote(String(event.data.error ?? "The output stream failed.")); }
      } catch { failed = true; setState("error"); setNote("The output stream sent an unreadable message."); }
    };
    tail.onerror = () => { if (current && !exited) { failed = true; setState("error"); setNote("The Pod output connection failed."); } };
    tail.onclose = () => { if (current && !exited && !failed) { setState("closed"); setNote("The connection closed without reporting process exit."); } };
    return () => { current = false; tail.close(); };
  }, [namespace, slug, job, finished, revision]);
  const output = lines.length ? `${lines.join("\n")}\n` : "";
  const account = <div className={`tail-account tail-${state}`} aria-live="polite">
    <p>Observed in this tab. Container <code>{containerOf(job)}</code> is taken from the Job name; the server selects a Pod without returning its name or identifying an attempt.</p>
    {state === "error" || state === "closed" ? <button className="text-action" type="button" onClick={() => setRevision((value) => value + 1)}>Reconnect</button> : null}
  </div>;
  return output ? <LogSurface text={output} label="Observed Pod output" caption={note} annotation={account} /> : <section className="output-wait">
    <p className="eyebrow">Pod output</p><p role="status">{state === "error" || state === "closed" ? "No output was received from this connection." : "Waiting for output…"}</p><p aria-live="polite">{note}</p>{account}
  </section>;
}

function EndedObservation({ value, primary, again }: { value: TailObservation; primary: boolean; again?: () => void }) {
  return <LogSurface text={value.text} label="Observed Pod output" caption={`Process exit ${value.exitCode ?? "not supplied"} observed · saved in this tab`} startAtEnd={primary} annotation={<div className="ended-observation">
    <p><Claim kind="observed">Process exit observed</Claim> Exit <strong>{value.exitCode ?? "not supplied"}</strong> · {value.reason ?? "reason not supplied"} · {fullTime(new Date(value.observedAt).toISOString())}.</p>
    <p>Saved in this tab across reloads, not in the stored Job record. The selected Pod and output attempt are unspecified.</p>
    {again ? <button className="text-action" type="button" onClick={again}>Reconnect</button> : null}
  </div>} />;
}

function LogRoute() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const run = useRemote(`log-run:${namespace}:${slug}`, () => fetchRun(namespace, slug), 5_000);
  const stored = useRemote(`stored:${namespace}:${slug}:${job}`, () => fetchLog(namespace, slug, job, containerOf(job)));
  const observationKey = `coalesce:tail:${namespace}:${slug}:${job}`;
  const [observation, setObservation] = useState<TailObservation>();
  useEffect(() => {
    try {
      const saved = window.sessionStorage.getItem(observationKey);
      setObservation(saved ? JSON.parse(saved) as TailObservation : undefined);
    } catch { setObservation(undefined); }
  }, [observationKey]);
  const finished = useCallback((value: TailObservation) => {
    setObservation(value);
    try { window.sessionStorage.setItem(observationKey, JSON.stringify(value)); } catch { /* The in-memory observation remains available. */ }
  }, [observationKey]);
  const observeAgain = useCallback(() => {
    try { window.sessionStorage.removeItem(observationKey); } catch { /* In-memory clearing still works. */ }
    setObservation(undefined);
  }, [observationKey]);
  const attempts = (run.data?.jobs ?? []).filter((candidate) => candidate.job === job);
  const latest = attempts.at(-1);
  const storedAvailable = stored.data !== undefined;
  const canTail = Boolean(latest && !latest.completed_at);
  const observedAvailable = Boolean(observation || canTail);
  const storedFirst = storedAvailable || !observedAvailable;
  const now = run.updatedAt ?? Date.now();
  useEffect(() => { document.title = `Coalesce — ${job} output`; window.scrollTo(0, 0); }, [namespace, slug, job]);
  const storedOutput = <section key="stored" className="job-output" aria-label="Stored output">
    <StoredLog log={stored} job={job} attempts={attempts} primary={storedFirst} />
  </section>;
  const observedOutput = observedAvailable ? <section key="observed" className="job-output" aria-label="Observed Pod output">
    {observation ? <EndedObservation value={observation} primary={!storedFirst} again={canTail ? observeAgain : undefined} /> : <StreamingLog namespace={namespace} slug={slug} job={job} finished={finished} />}
  </section> : null;
  return <Shell namespace={namespace}>
    <article className="log-record">
      <nav className="breadcrumb" aria-label="Breadcrumb"><Link to={runPath(namespace, slug)}>← Run {slug}</Link></nav>
      <header className="log-title"><p className="eyebrow">Job output</p><h1>{job}</h1></header>
      {storedFirst ? [storedOutput, observedOutput] : [observedOutput, storedOutput]}
      <section className="output-details" aria-labelledby="job-record-title">
        <h2 id="job-record-title">{attempts.length > 1 ? "Latest Job attempt" : "Job record"}</h2>
        {run.loading ? <Loading>Reading the Job record…</Loading> : null}
        {run.error instanceof ApiError && run.error.status === 404 ? <Empty label="Run unavailable" title="The run record was not found.">No run named <code>{slug}</code> was returned for namespace <code>{namespace}</code>.</Empty> : run.error ? <Problem error={run.error} retry={run.reload} /> : null}
        {run.data && !latest ? <Empty label="Job record unavailable" title="No matching Job attempt was returned.">The run response has no entry for <code>{job}</code>. Stored output is looked up separately.</Empty> : null}
        {latest ? <>
          {attempts.length > 1 ? <p>{attempts.length} attempts share this Job identity. These facts describe the latest; the output lookup cannot select an attempt.</p> : null}
          {!latest.completed_at ? <p className="unclosed-note">Job completion is not recorded. Stored status “{latest.status}” does not establish whether a process is running.</p> : null}
          <dl className="log-facts">
            <Fact label="Started"><time dateTime={latest.started_at}>{fullTime(latest.started_at)}</time></Fact>
            <Fact label="Completed" missing={!latest.completed_at}>{latest.completed_at ? <time dateTime={latest.completed_at}>{fullTime(latest.completed_at)}</time> : "Not recorded"}</Fact>
            <Fact label={latest.completed_at ? "Recorded duration" : "Elapsed at latest read"}><span className="numeric">{latest.completed_at ? "" : "+"}{span(latest.started_at, latest.completed_at ?? now)}</span></Fact>
            <Fact label="Recorded status">{latest.status}</Fact>
            <Fact label="Recorded exit status" missing={latest.exit_code == null}>{latest.exit_code ?? "Not recorded"}</Fact>
            <Fact label="Container from Job name"><code>{containerOf(job)}</code></Fact>
          </dl>
        </> : null}
        {run.data ? <p className="pipeline-title">{run.data.pipeline}</p> : null}
      </section>
    </article>
  </Shell>;
}

function MissingRoute() {
  return <Shell namespace="coalesce"><Empty primary label="Unknown address" title="No Coalesce route matches this location.">Return to <Link to={runsPath("coalesce")}>the latest run records</Link>.</Empty></Shell>;
}

export function App() {
  return <Routes>
    <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
    <Route path="/:namespace/runs" element={<RunsRoute />} />
    <Route path="/:namespace/runs/:slug" element={<RunRoute />} />
    <Route path="/:namespace/runs/:slug/logs/:job" element={<LogRoute />} />
    <Route path="*" element={<MissingRoute />} />
  </Routes>;
}
