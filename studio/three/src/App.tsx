import {
  useCallback,
  useEffect,
  useRef,
  useState,
  type ReactNode,
} from "react";
import {
  Link,
  Navigate,
  Route,
  Routes,
  useParams,
} from "react-router-dom";
import {
  ApiError,
  containerOf,
  fetchDag,
  fetchLog,
  fetchRun,
  fetchRuns,
  openLogTail,
  openRunEvents,
  parseStreamEvent,
  type DagNode,
  type DagResponse,
  type RunDetail,
} from "../../shared/api";

interface Query<T> {
  result?: T;
  issue?: unknown;
  waiting: boolean;
}

function useQuery<T>(
  address: string,
  query: () => Promise<T>,
  frequency = 0,
): Query<T> & { invalidate: () => void } {
  const queryRef = useRef(query);
  queryRef.current = query;
  const [version, setVersion] = useState(0);
  const [state, setState] = useState<Query<T>>({ waiting: true });
  const invalidate = useCallback(
    () => setVersion((current) => current + 1),
    [],
  );

  useEffect(() => {
    let valid = true;
    async function execute(showWait: boolean) {
      if (showWait) setState({ waiting: true });
      try {
        const result = await queryRef.current();
        if (valid) setState({ result, waiting: false });
      } catch (issue) {
        if (valid) setState({ issue, waiting: false });
      }
    }
    void execute(true);
    const clock = frequency
      ? window.setInterval(() => void execute(false), frequency)
      : undefined;
    return () => {
      valid = false;
      if (clock !== undefined) window.clearInterval(clock);
    };
  }, [address, frequency, version]);

  return { ...state, invalidate };
}

function Page({ children }: { children: ReactNode }) {
  return (
    <div className="page">
      <header className="topline">
        <Link to="/coalesce/runs">Coalesce</Link>
        <span>Exhibit three</span>
      </header>
      <main>{children}</main>
      <aside className="provenance">
        Neutral scaffold · presentation code local to studio/three
      </aside>
    </div>
  );
}

function readableIssue(issue: unknown): string {
  if (issue instanceof ApiError) {
    return `${issue.message}${issue.body.trim() ? ` — ${issue.body.trim()}` : ""}`;
  }
  return issue instanceof Error ? issue.message : "The request failed unexpectedly.";
}

function Alert({ issue }: { issue: unknown }) {
  return (
    <section className="alert" role="alert">
      <b>Coalesce could not answer.</b>
      <p>{readableIssue(issue)}</p>
    </section>
  );
}

function Wait({ children }: { children: ReactNode }) {
  return (
    <p className="wait" role="status">
      {children}
    </p>
  );
}

function time(value?: string): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "short",
    timeStyle: "medium",
  }).format(new Date(value));
}

function totalDagNodes(nodes: DagNode[]): number {
  return nodes.reduce(
    (sum, node) => sum + 1 + totalDagNodes(node.children ?? []),
    0,
  );
}

function RunIndex() {
  const { namespace = "coalesce" } = useParams();
  const query = useQuery(
    `index:${namespace}`,
    () => fetchRuns(namespace),
    10_000,
  );

  useEffect(() => {
    document.title = `Coalesce / ${namespace} / studio three`;
  }, [namespace]);

  return (
    <Page>
      <section className="intro">
        <div>
          <p>Namespace {namespace}</p>
          <h1>Runs</h1>
        </div>
        <button type="button" onClick={query.invalidate}>
          Refresh record
        </button>
      </section>
      {query.waiting ? <Wait>Contacting the run API…</Wait> : null}
      {query.issue ? <Alert issue={query.issue} /> : null}
      {query.result?.length === 0 ? (
        <section className="nothing">
          <b>No runs.</b>
          <p>The response for this namespace contains no run records.</p>
        </section>
      ) : null}
      {query.result && query.result.length > 0 ? (
        <div className="records">
          {query.result.map((run) => (
            <article className="record" key={run.slug}>
              <div className="record-primary">
                <span className="status">{run.status}</span>
                <h2>
                  <Link to={`/${namespace}/runs/${run.slug}`}>{run.slug}</Link>
                </h2>
              </div>
              <dl>
                <div>
                  <dt>Pipeline</dt>
                  <dd>{run.pipeline}</dd>
                </div>
                <div>
                  <dt>Started</dt>
                  <dd>{time(run.started_at)}</dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      ) : null}
    </Page>
  );
}

interface Detail {
  run: RunDetail;
  dag: DagResponse | null;
}

async function getDetail(namespace: string, slug: string): Promise<Detail> {
  const run = await fetchRun(namespace, slug);
  try {
    return { run, dag: await fetchDag(namespace, slug) };
  } catch (issue) {
    if (issue instanceof ApiError && issue.status === 404) {
      return { run, dag: null };
    }
    throw issue;
  }
}

function RunDetailPage() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const query = useQuery(`detail:${namespace}:${slug}`, () =>
    getDetail(namespace, slug),
  );

  useEffect(() => {
    const events = openRunEvents(namespace, slug);
    events.onmessage = () => query.invalidate();
    return () => events.close();
  }, [namespace, slug, query.invalidate]);
  useEffect(() => {
    document.title = `Coalesce / ${slug} / studio three`;
  }, [slug]);

  return (
    <Page>
      <nav className="back">
        <Link to={`/${namespace}/runs`}>← Runs in {namespace}</Link>
      </nav>
      {query.waiting ? <Wait>Reading run {slug}…</Wait> : null}
      {query.issue instanceof ApiError && query.issue.status === 404 ? (
        <section className="nothing">
          <b>Unknown run.</b>
          <p>{slug} does not exist in this namespace.</p>
        </section>
      ) : query.issue ? (
        <Alert issue={query.issue} />
      ) : null}
      {query.result ? (
        <>
          <section className="intro detail-intro">
            <div>
              <p>{query.result.run.pipeline}</p>
              <h1>{query.result.run.slug}</h1>
            </div>
            <span className="status">{query.result.run.status}</span>
          </section>
          <dl className="summary">
            <div>
              <dt>Started</dt>
              <dd>{time(query.result.run.started_at)}</dd>
            </div>
            <div>
              <dt>Completed</dt>
              <dd>{time(query.result.run.completed_at)}</dd>
            </div>
            <div>
              <dt>DAG</dt>
              <dd>
                {query.result.dag
                  ? `${totalDagNodes(query.result.dag.dag)} nodes received; no visualization selected`
                  : "No current DAG"}
              </dd>
            </div>
          </dl>
          {(query.result.run.jobs ?? []).length === 0 ? (
            <section className="nothing">
              <b>No jobs.</b>
              <p>The run record has no job attempts.</p>
            </section>
          ) : (
            <section className="job-list" aria-label="Job attempts">
              {(query.result.run.jobs ?? []).map((job) => (
                <article key={`${job.job}:${job.started_at}`}>
                  <span className="status">{job.status}</span>
                  <h2>
                    <Link to={`/${namespace}/runs/${slug}/logs/${job.job}`}>
                      {job.job}
                    </Link>
                  </h2>
                  <p>
                    {time(job.started_at)} → {time(job.completed_at)} · exit {job.exit_code ?? "—"}
                  </p>
                </article>
              ))}
            </section>
          )}
        </>
      ) : null}
    </Page>
  );
}

interface LogIdentity {
  namespace: string;
  slug: string;
  job: string;
}

function Archive({ namespace, slug, job }: LogIdentity) {
  const query = useQuery(`archive:${namespace}:${slug}:${job}`, () =>
    fetchLog(namespace, slug, job, containerOf(job)),
  );
  if (query.waiting) return <Wait>Retrieving harvested evidence…</Wait>;
  if (query.issue instanceof ApiError && query.issue.status === 404) {
    return (
      <section className="nothing">
        <b>Evidence not deposited.</b>
        <p>The expected harvested log does not exist yet.</p>
      </section>
    );
  }
  if (query.issue) return <Alert issue={query.issue} />;
  return (
    <pre className="console" tabIndex={0}>
      {query.result || "Log is empty.\n"}
    </pre>
  );
}

function Tail({
  namespace,
  slug,
  job,
  complete,
}: LogIdentity & { complete: () => void }) {
  const [lines, setLines] = useState<string[]>([]);
  const [state, setState] = useState("Connecting…");

  useEffect(() => {
    const socket = openLogTail(namespace, slug, job, containerOf(job));
    socket.onopen = () => setState("Connected to running container.");
    socket.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (event.kind === "log_line") {
          setLines((current) => [...current, String(event.data.line ?? "")]);
        }
        if (event.kind === "log_status") {
          setState(`Container phase ${String(event.data.phase ?? "unknown")}.`);
        }
        if (event.kind === "log_exit") {
          setState(`Container exit ${String(event.data.exit_code ?? "unknown")}.`);
          complete();
        }
        if (event.kind === "log_error") {
          setState(String(event.data.error ?? "The log stream failed."));
        }
      } catch {
        setState("The stream sent an unreadable event.");
      }
    };
    socket.onerror = () => setState("The log stream could not connect.");
    return () => socket.close();
  }, [namespace, slug, job, complete]);

  return (
    <section aria-live="polite">
      <p className="stream">{state}</p>
      <pre className="console" tabIndex={0}>
        {lines.length ? `${lines.join("\n")}\n` : "Waiting for output…\n"}
      </pre>
    </section>
  );
}

function LogPage() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const query = useQuery(`log-parent:${namespace}:${slug}`, () =>
    fetchRun(namespace, slug),
  );
  const complete = useCallback(
    () => window.setTimeout(query.invalidate, 1_500),
    [query.invalidate],
  );
  const matches = (query.result?.jobs ?? []).filter(
    (candidate) => candidate.job === job,
  );
  const current = matches.at(-1);

  useEffect(() => {
    document.title = `Coalesce / ${job} log / studio three`;
  }, [job]);

  return (
    <Page>
      <nav className="back">
        <Link to={`/${namespace}/runs/${slug}`}>← Run {slug}</Link>
      </nav>
      <section className="intro">
        <div>
          <p>Job evidence</p>
          <h1>{job}</h1>
        </div>
      </section>
      {query.waiting ? <Wait>Locating the latest job attempt…</Wait> : null}
      {query.issue ? <Alert issue={query.issue} /> : null}
      {query.result && !current ? (
        <section className="nothing">
          <b>Unknown job.</b>
          <p>No attempt named {job} belongs to {slug}.</p>
        </section>
      ) : null}
      {current && !current.completed_at ? (
        <Tail
          namespace={namespace}
          slug={slug}
          job={job}
          complete={complete}
        />
      ) : null}
      {current?.completed_at ? (
        <Archive namespace={namespace} slug={slug} job={job} />
      ) : null}
    </Page>
  );
}

function Lost() {
  return (
    <Page>
      <section className="nothing">
        <b>Route not found.</b>
        <p>
          Continue at <Link to="/coalesce/runs">the run index</Link>.
        </p>
      </section>
    </Page>
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
      <Route path="/:namespace/runs" element={<RunIndex />} />
      <Route path="/:namespace/runs/:slug" element={<RunDetailPage />} />
      <Route
        path="/:namespace/runs/:slug/logs/:job"
        element={<LogPage />}
      />
      <Route path="*" element={<Lost />} />
    </Routes>
  );
}
