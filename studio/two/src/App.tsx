import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link, Navigate, Route, Routes, useParams } from "react-router-dom";
import {
  ApiError, ShapeError, containerOf, fetchDag, fetchLog, fetchRun, fetchRuns,
  openLogTail, openRunEvents, parseStreamEvent,
  type DagNode, type DagResponse, type Job, type Run, type RunDetail,
} from "../../shared/api";

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
type ClaimKind = "recorded" | "observed" | "unavailable" | "failed";

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

function DagList({ nodes, prefix = "" }: { nodes: DagNode[]; prefix?: string }) {
  return <ol className={prefix ? "dag-list dag-nested" : "dag-list"}>
    {nodes.map((node, index) => {
      const order = prefix ? `${prefix}.${String(index + 1).padStart(2, "0")}` : String(index + 1).padStart(2, "0");
      const identity = `${node.under}.${node.name}`;
      return <li key={`${identity}:${order}`}>
        <div className="dag-node"><span className="dag-order">{order}</span><div><strong>{node.name}</strong><code>{identity}</code></div>
          <span className="dag-kind">{node.kind === "tranche" ? node.parallel ? "Parallel tranche" : "Ordered tranche" : "Node"}</span>
        </div>
        {node.children?.length ? <DagList nodes={node.children} prefix={order} /> : null}
      </li>;
    })}
  </ol>;
}

type SequenceEvent =
  | { kind: "opened"; at: string }
  | { kind: "declaration"; at: string; dag: DagResponse }
  | { kind: "job"; at: string; job: Job; ordinal: number; total: number; latest: boolean }
  | { kind: "closed"; at: string };

function sequenceFor(record: RunRecord): SequenceEvent[] {
  const counts = new Map<string, number>();
  for (const job of record.run.jobs ?? []) counts.set(job.job, (counts.get(job.job) ?? 0) + 1);
  const seen = new Map<string, number>();
  const events: SequenceEvent[] = [{ kind: "opened", at: record.run.started_at }];
  if (record.dag) events.push({ kind: "declaration", at: record.dag.created_at, dag: record.dag });
  const jobs = record.run.jobs ?? [];
  jobs.forEach((job, index) => {
    const ordinal = (seen.get(job.job) ?? 0) + 1;
    seen.set(job.job, ordinal);
    events.push({
      kind: "job", at: job.started_at, job, ordinal, total: counts.get(job.job) ?? 1,
      latest: !jobs.slice(index + 1).some((candidate) => candidate.job === job.job),
    });
  });
  if (record.run.completed_at) events.push({ kind: "closed", at: record.run.completed_at });
  const priority: Record<SequenceEvent["kind"], number> = { opened: 0, declaration: 1, job: 2, closed: 3 };
  return events.sort((a, b) => Date.parse(a.at) - Date.parse(b.at) || priority[a.kind] - priority[b.kind]);
}

function Fact({ label, children, missing = false }: { label: string; children: ReactNode; missing?: boolean }) {
  return <div className={missing ? "fact fact-missing" : "fact"}><dt>{label}</dt><dd>{children}</dd></div>;
}

function SequenceItem({ event, namespace, slug, now, runStatus }: {
  event: SequenceEvent; namespace: string; slug: string; now: number; runStatus: string;
}) {
  if (event.kind === "opened") return <li className="sequence-item event-recorded">
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body"><Claim kind="recorded">Executor record</Claim><h3>Run opened</h3><p>The original start remains attached to this run identity across resume.</p></div>
  </li>;
  if (event.kind === "declaration") return <li className="sequence-item event-recorded">
    <i className="sequence-marker" aria-hidden="true" /><time dateTime={event.at}>{fullTime(event.at)}</time>
    <div className="event-body"><Claim kind="recorded">Latest declaration</Claim><h3>{dagCount(event.dag.dag)} declared {dagCount(event.dag.dag) === 1 ? "node" : "nodes"}</h3>
      <p>The endpoint returns the newest stored DAG version. Names, parents, kinds, and nesting are preserved below; no edges are inferred.</p><DagList nodes={event.dag.dag} />
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
      <div className="event-title-line"><div><h3>{job.job}</h3><p>{event.total > 1 ? `Attempt ${event.ordinal} of ${event.total} for this Job identity` : "One recorded attempt for this Job identity"}</p></div><span className={`job-status status-${cssStatus(job.status)}`}>Stored · {job.status}</span></div>
      <dl className="event-facts">
        <Fact label="Opened"><time dateTime={job.started_at}>{fullTime(job.started_at)}</time></Fact>
        <Fact label="Closure" missing={!job.completed_at}>{job.completed_at ? <time dateTime={job.completed_at}>{fullTime(job.completed_at)}</time> : "Not recorded"}</Fact>
        <Fact label={closed ? "Recorded span" : "Elapsed at latest HTTP read"}><span className="numeric">{closed ? "" : "+"}{span(job.started_at, job.completed_at ?? now)}</span></Fact>
        <Fact label="Exit code" missing={job.exit_code == null}>{job.exit_code ?? "Not recorded"}</Fact>
      </dl>
      <div className="evidence-address">{event.latest ? <>
        <Link to={logPath(namespace, slug, job.job)}>{closed ? "Read latest stored log" : "Open cluster observation"}<span aria-hidden="true"> ↗</span></Link>
        <p>Addressed by run, Job, and derived container identity—not by this attempt timestamp.</p>
      </> : <><span>No attempt-specific log address</span><p>The contract cannot retrieve this historical attempt directly.</p></>}</div>
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
    return <aside className="job-account job-account-failed"><Claim kind="failed">Job snapshot</Claim><div><h2>{latest.job} failed.</h2><p>{failed.length} failed {failed.length === 1 ? "attempt appears" : "attempts appear"} in this snapshot. <Link to={logPath(namespace, run.slug, latest.job)}>Read the latest addressable log.</Link></p></div></aside>;
  }
  if (unclosed.length) return <aside className="job-account"><Claim kind="unavailable">Job snapshot</Claim><div><h2>{unclosed.length} {unclosed.length === 1 ? "Job record has" : "Job records have"} no closure.</h2><p>{unclosed.map((job) => job.job).join(", ")}</p></div></aside>;
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
        <ol className="sequence-list">{events.map((event, index) => <SequenceItem key={`${event.kind}:${event.at}:${index}`} event={event} namespace={namespace} slug={slug} now={now} runStatus={record.data!.run.status} />)}
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

function LogSurface({ text, label, startAtEnd = false }: { text: string; label: string; startAtEnd?: boolean }) {
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
    <div className="log-toolbar"><div><span>{label}</span><small>{lines} lines · {bytesLabel(bytes)}{startAtEnd ? " · opened at end" : ""}</small></div>
      <div className="log-actions"><button type="button" onClick={() => frame.current?.scrollIntoView({ block: "start" })}>Beginning</button><button type="button" onClick={() => end.current?.scrollIntoView({ block: "center" })}>End</button><button type="button" aria-pressed={!wrap} onClick={() => setWrap((value) => !value)}>{wrap ? "Preserve columns" : "Wrap lines"}</button><button type="button" onClick={() => void copy()}>{copyState}</button></div>
    </div>
    <pre className={`log-output ${wrap ? "" : "preserve-columns"}`} tabIndex={0} aria-label={label}>{text || "No log text was returned.\n"}</pre><span ref={end} className="log-end-marker" aria-hidden="true" />
  </div>;
}

function StoredLog({ log, failed = false }: { log: Remote<string> & { reload: () => void }; failed?: boolean }) {
  if (log.loading) return <Loading>Reading the latest stored artifact…</Loading>;
  if (log.error instanceof ApiError && log.error.status === 404) return <section className="notice artifact-missing" role="status">
    <Claim kind="unavailable">Stored artifact unavailable</Claim><h2>Coalesce answered 404 for this log identity.</h2>
    <p>No artifact is addressable at the latest-only stored endpoint. It may still arrive, or harvest may never have completed.</p>
    <button className="text-action" type="button" onClick={log.reload}>Check the stored address again</button>
  </section>;
  if (log.error) return <Problem error={log.error} retry={log.reload} />;
  return <LogSurface text={log.data ?? ""} label="Latest stored log artifact" startAtEnd={failed} />;
}

interface TailObservation { exitCode?: string; reason?: string; observedAt: number; text: string }
type TailState = "connecting" | "observed" | "error" | "exited";

function StreamingLog({ namespace, slug, job, finished }: RouteIdentity & { finished: (value: TailObservation) => void }) {
  const [lines, setLines] = useState<string[]>([]);
  const linesRef = useRef<string[]>([]);
  const [state, setState] = useState<TailState>("connecting");
  const [note, setNote] = useState("Opening a WebSocket to a pod selected by the server.");
  const [revision, setRevision] = useState(0);
  useEffect(() => {
    let exited = false;
    linesRef.current = [];
    setLines([]);
    setState("connecting");
    setNote("Opening a WebSocket to a pod selected by the server.");
    const tail = openLogTail(namespace, slug, job, containerOf(job));
    tail.onopen = () => { setState("observed"); setNote("This browser is receiving a current cluster observation."); };
    tail.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (event.kind === "log_line") {
          linesRef.current = [...linesRef.current, String(event.data.line ?? "")];
          setLines(linesRef.current);
        }
        else if (event.kind === "log_status") { setState("observed"); setNote(`The selected pod reported phase ${String(event.data.phase ?? "unknown")}.`); }
        else if (event.kind === "log_exit") {
          exited = true;
          const value = { exitCode: String(event.data.exit_code ?? "not supplied"), reason: String(event.data.reason ?? "not supplied"), observedAt: Date.now(), text: linesRef.current.length ? `${linesRef.current.join("\n")}\n` : "" };
          setState("exited");
          setNote(`This connection observed process exit ${value.exitCode}; termination reason ${value.reason}.`);
          finished(value);
        } else if (event.kind === "log_error") { setState("error"); setNote(String(event.data.error ?? "The tail reported an unspecified error.")); }
      } catch { setState("error"); setNote("A WebSocket event arrived, but this browser could not parse it."); }
    };
    tail.onerror = () => { if (!exited) { setState("error"); setNote("No pod accepted the requested live tail connection."); } };
    return () => tail.close();
  }, [namespace, slug, job, finished, revision]);
  const output = lines.length ? `${lines.join("\n")}\n` : "";
  return <>
    <div className={`tail-account tail-${state}`} aria-live="polite">
      <Claim kind={state === "observed" || state === "exited" ? "observed" : "unavailable"}>{state === "connecting" ? "Connecting" : state === "observed" ? "Observed now" : state === "exited" ? "Observed exit" : "Observation unavailable"}</Claim>
      <p>{note}</p><small>This account belongs to this browser session and is not the stored run record.</small>{state === "error" ? <button className="text-action" type="button" onClick={() => setRevision((value) => value + 1)}>Try observation again</button> : null}
    </div>
    {output ? <LogSurface text={output} label="Current browser observation" /> : <div className="observation-empty" role="status"><p>{state === "error" ? "No log lines were received from this request." : "No log lines have been received yet."}</p><small>This interface message is not counted or copyable as observed evidence.</small></div>}
  </>;
}

function EndedObservation({ value, again }: { value: TailObservation; again: () => void }) {
  return <div className="ended-observation"><aside className="observed-exit"><Claim kind="observed">Session observation</Claim><div>
    <p>This browser observed exit <strong>{value.exitCode ?? "not supplied"}</strong> with reason <strong>{value.reason ?? "not supplied"}</strong> at {readTime(value.observedAt)}.</p>
    <small>Kept in sessionStorage for this tab: it survives reload and same-tab navigation, is normally cleared when the tab closes, and is not stored by Coalesce.</small></div><button className="text-action" type="button" onClick={again}>Observe again</button>
  </aside><LogSurface text={value.text} label="Ended browser observation" startAtEnd /></div>;
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
    try { window.sessionStorage.setItem(observationKey, JSON.stringify(value)); } catch { /* The in-memory session account remains available. */ }
  }, [observationKey]);
  const observeAgain = useCallback(() => {
    try { window.sessionStorage.removeItem(observationKey); } catch { /* In-memory clearing still works. */ }
    setObservation(undefined);
  }, [observationKey]);
  const attempts = (run.data?.jobs ?? []).filter((candidate) => candidate.job === job);
  const latest = attempts.at(-1);
  const storedAvailable = stored.data !== undefined;
  const now = run.updatedAt ?? Date.now();
  useEffect(() => { document.title = `Coalesce — ${job} evidence`; }, [job]);
  return <Shell namespace={namespace}>
    <nav className="breadcrumb" aria-label="Breadcrumb"><Link to={runPath(namespace, slug)}>← Run {slug}</Link></nav>
    <header className="log-title"><div><p className="eyebrow">Job identity / latest addressable evidence</p><h1>{job}</h1><p className="pipeline-title">{run.data?.pipeline ?? "Pipeline statement unavailable"}</p><p className="run-identity">Run <code>{slug}</code></p></div>
      {latest ? <Claim kind={storedAvailable ? "recorded" : observation ? "observed" : "unavailable"}>{storedAvailable ? "Stored artifact" : observation ? "Observed exit" : "Evidence resolving"}</Claim> : null}
    </header>
    {run.loading ? <Loading>Reading the latest Job attempt…</Loading> : null}
    {run.error instanceof ApiError && run.error.status === 404 ? <Empty label="Missing parent" title="The run record is unavailable.">Coalesce has no run named <code>{slug}</code> in namespace <code>{namespace}</code>.</Empty> : run.error ? <Problem error={run.error} retry={run.reload} /> : null}
    {run.data && !latest ? <Empty label="Missing Job identity" title="This run has no matching attempt.">No Job named <code>{job}</code> appears in the available run snapshot.</Empty> : null}
    {run.data && latest ? <article className="log-record">
      <section className={`log-custody ${storedAvailable ? "custody-stored" : observation ? "custody-observed" : "custody-unavailable"}`}><Claim kind={storedAvailable ? "recorded" : observation ? "observed" : "unavailable"}>{storedAvailable && !latest.completed_at ? "Stored artifact · closure absent" : storedAvailable ? "Bucket custody" : observation ? "Browser custody" : "Evidence boundary"}</Claim>
        <h2>{storedAvailable && !latest.completed_at ? "A stored artifact exists while the Job record remains unclosed." : storedAvailable ? "Newest stored artifact for this identity" : observation ? "This browser observed the tail end; durable closure is absent" : "Stored and observed evidence are resolved independently."}</h2>
        <p>{storedAvailable ? "The latest-only endpoint returned an artifact matching run, Job, and derived container. That lookup does not prove which attempt deposited it, and it does not supply the absent Job closure." : observation ? "The browser observation is separate from both the stored Job response and the latest-only artifact lookup." : "Job closure does not decide whether an artifact exists. The stored endpoint and any browser observation are accounted for separately below."}</p>
      </section>
      {attempts.length > 1 ? <aside className="attempt-boundary"><Claim kind="unavailable">Selection boundary</Claim><p>{attempts.length} attempts share this Job identity. This route cannot select one by timestamp; the facts below describe the latest attempt.</p></aside> : null}
      <dl className="log-facts">
        <Fact label="Latest attempt opened"><time dateTime={latest.started_at}>{fullTime(latest.started_at)}</time></Fact>
        <Fact label="Latest attempt closure" missing={!latest.completed_at}>{latest.completed_at ? <time dateTime={latest.completed_at}>{fullTime(latest.completed_at)}</time> : "Not recorded"}</Fact>
        <Fact label={latest.completed_at ? "Recorded span" : "Elapsed at latest HTTP read"}><span className="numeric">{latest.completed_at ? "" : "+"}{span(latest.started_at, latest.completed_at ?? now)}</span></Fact>
        <Fact label="Stored status">{latest.status}</Fact>
        <Fact label="Stored exit code" missing={latest.exit_code == null}>{latest.exit_code ?? "Not recorded"}</Fact>
        <Fact label="Derived container"><code>{containerOf(job)}</code></Fact>
      </dl>
      {observation && latest.completed_at ? <aside className="observed-exit compact-observation"><Claim kind="observed">Earlier session observation</Claim><div><p>This tab observed exit <strong>{observation.exitCode ?? "not supplied"}</strong> with reason <strong>{observation.reason ?? "not supplied"}</strong> at {readTime(observation.observedAt)}.</p><small>The durable record is now shown below; this session account is not its source.</small></div></aside> : null}
      <section className="log-evidence" aria-labelledby="stored-evidence-title">
        <div className="section-heading"><div><p className="eyebrow">Bucket custody / latest-only lookup</p><h2 id="stored-evidence-title">Stored evidence</h2></div><p>Exact text returned for run, Job, and derived container</p></div>
        <StoredLog log={stored} failed={latest.status === "failed"} />
      </section>
      {!latest.completed_at ? <section className="log-evidence observation-evidence" aria-labelledby="observed-evidence-title">
        <div className="section-heading"><div><p className="eyebrow">Browser custody / separate request</p><h2 id="observed-evidence-title">Browser observation</h2></div><p>Lines, status, and exit seen only by this browser</p></div>
        {observation ? <EndedObservation value={observation} again={observeAgain} /> : <StreamingLog namespace={namespace} slug={slug} job={job} finished={finished} />}
      </section> : null}
    </article> : null}
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
