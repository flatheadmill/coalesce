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

interface Remote<T> {
  data?: T;
  error?: unknown;
  loading: boolean;
}

function useRemote<T>(
  key: string,
  read: () => Promise<T>,
  pollMilliseconds = 0,
): Remote<T> & { reload: () => void } {
  const readRef = useRef(read);
  readRef.current = read;
  const [revision, setRevision] = useState(0);
  const [remote, setRemote] = useState<Remote<T>>({ loading: true });
  const reload = useCallback(() => setRevision((value) => value + 1), []);

  useEffect(() => {
    let current = true;
    const load = async (showLoading: boolean) => {
      if (showLoading) setRemote({ loading: true });
      try {
        const data = await readRef.current();
        if (current) setRemote({ data, loading: false });
      } catch (error) {
        if (current) setRemote({ error, loading: false });
      }
    };

    void load(true);
    const timer = pollMilliseconds
      ? window.setInterval(() => void load(false), pollMilliseconds)
      : undefined;
    return () => {
      current = false;
      if (timer !== undefined) window.clearInterval(timer);
    };
  }, [key, pollMilliseconds, revision]);

  return { ...remote, reload };
}

function frame(content: ReactNode): ReactNode {
  return (
    <div className="frame">
      <header className="masthead">
        <Link className="product" to="/coalesce/runs">
          Coalesce
        </Link>
        <span>design studio · exhibit one</span>
      </header>
      <main>{content}</main>
      <footer>Independent scaffold one · live Coalesce contract</footer>
    </div>
  );
}

function describeError(error: unknown): string {
  if (error instanceof ApiError) {
    return `${error.message}${error.body ? ` — ${error.body.trim()}` : ""}`;
  }
  return error instanceof Error ? error.message : "Unknown failure";
}

function Problem({ error }: { error: unknown }) {
  return (
    <section className="problem" role="alert">
      <strong>Unable to read Coalesce.</strong>
      <p>{describeError(error)}</p>
    </section>
  );
}

function Loading({ children }: { children: ReactNode }) {
  return (
    <p className="loading" role="status">
      {children}
    </p>
  );
}

function formatTime(value: string | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(new Date(value));
}

function countNodes(nodes: DagNode[]): number {
  return nodes.reduce(
    (total, node) => total + 1 + countNodes(node.children ?? []),
    0,
  );
}

function RunsRoute() {
  const { namespace = "coalesce" } = useParams();
  const runs = useRemote(
    `runs:${namespace}`,
    () => fetchRuns(namespace),
    10_000,
  );

  useEffect(() => {
    document.title = `Coalesce · ${namespace} runs · one`;
  }, [namespace]);

  return frame(
    <>
      <div className="heading">
        <div>
          <p className="eyebrow">Namespace {namespace}</p>
          <h1>Runs</h1>
        </div>
        <button type="button" onClick={runs.reload}>
          Refresh
        </button>
      </div>
      {runs.loading ? <Loading>Reading runs…</Loading> : null}
      {runs.error ? <Problem error={runs.error} /> : null}
      {runs.data?.length === 0 ? (
        <section className="empty">
          <strong>No runs recorded.</strong>
          <p>The server returned an empty run collection for this namespace.</p>
        </section>
      ) : null}
      {runs.data && runs.data.length > 0 ? (
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Status</th>
                <th>Run</th>
                <th>Pipeline</th>
                <th>Started</th>
              </tr>
            </thead>
            <tbody>
              {runs.data.map((run) => (
                <tr key={run.slug}>
                  <td>{run.status}</td>
                  <td>
                    <Link to={`/${namespace}/runs/${run.slug}`}>
                      {run.slug}
                    </Link>
                  </td>
                  <td>{run.pipeline}</td>
                  <td>{formatTime(run.started_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </>,
  );
}

interface RunRecord {
  run: RunDetail;
  dag: DagResponse | null;
}

async function readRunRecord(
  namespace: string,
  slug: string,
): Promise<RunRecord> {
  const run = await fetchRun(namespace, slug);
  let dag: DagResponse | null = null;
  try {
    dag = await fetchDag(namespace, slug);
  } catch (error) {
    if (!(error instanceof ApiError) || error.status !== 404) throw error;
  }
  return { run, dag };
}

function RunRoute() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const record = useRemote(`run:${namespace}:${slug}`, () =>
    readRunRecord(namespace, slug),
  );

  useEffect(() => {
    document.title = `Coalesce · ${slug} · one`;
  }, [slug]);
  useEffect(() => {
    const socket = openRunEvents(namespace, slug);
    socket.onmessage = () => record.reload();
    return () => socket.close();
  }, [namespace, slug, record.reload]);

  return frame(
    <>
      <p className="back">
        <Link to={`/${namespace}/runs`}>← Runs</Link>
      </p>
      {record.loading ? <Loading>Reading run {slug}…</Loading> : null}
      {record.error instanceof ApiError && record.error.status === 404 ? (
        <section className="empty" role="status">
          <strong>Run not found.</strong>
          <p>No run named {slug} is recorded in {namespace}.</p>
        </section>
      ) : record.error ? (
        <Problem error={record.error} />
      ) : null}
      {record.data ? (
        <>
          <div className="heading run-heading">
            <div>
              <p className="eyebrow">{record.data.run.pipeline}</p>
              <h1>{record.data.run.slug}</h1>
            </div>
            <span className="state">{record.data.run.status}</span>
          </div>
          <dl className="facts">
            <div>
              <dt>Started</dt>
              <dd>{formatTime(record.data.run.started_at)}</dd>
            </div>
            <div>
              <dt>Completed</dt>
              <dd>{formatTime(record.data.run.completed_at)}</dd>
            </div>
            <div>
              <dt>DAG</dt>
              <dd>
                {record.data.dag
                  ? `${countNodes(record.data.dag.dag)} nodes recorded ${formatTime(record.data.dag.created_at)}; not visualized`
                  : "No DAG recorded"}
              </dd>
            </div>
          </dl>
          {(record.data.run.jobs ?? []).length === 0 ? (
            <section className="empty">
              <strong>No jobs recorded.</strong>
              <p>The run exists, but its job ledger is empty.</p>
            </section>
          ) : (
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Status</th>
                    <th>Job</th>
                    <th>Started</th>
                    <th>Completed</th>
                    <th>Exit</th>
                  </tr>
                </thead>
                <tbody>
                  {(record.data.run.jobs ?? []).map((job) => (
                    <tr key={`${job.job}:${job.started_at}`}>
                      <td>{job.status}</td>
                      <td>
                        <Link
                          to={`/${namespace}/runs/${slug}/logs/${job.job}`}
                        >
                          {job.job}
                        </Link>
                      </td>
                      <td>{formatTime(job.started_at)}</td>
                      <td>{formatTime(job.completed_at)}</td>
                      <td>{job.exit_code ?? "—"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      ) : null}
    </>,
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
  const log = useRemote(`log:${namespace}:${slug}:${job}`, () =>
    fetchLog(namespace, slug, job, containerOf(job)),
  );

  if (log.loading) return <Loading>Reading harvested log…</Loading>;
  if (log.error instanceof ApiError && log.error.status === 404) {
    return (
      <section className="empty" role="status">
        <strong>Log not harvested.</strong>
        <p>The container has no stored log at the expected path yet.</p>
      </section>
    );
  }
  if (log.error) return <Problem error={log.error} />;
  return (
    <pre className="log" tabIndex={0}>
      {log.data || "Log is empty.\n"}
    </pre>
  );
}

function LiveLog({
  namespace,
  slug,
  job,
  onFinished,
}: {
  namespace: string;
  slug: string;
  job: string;
  onFinished: () => void;
}) {
  const [lines, setLines] = useState<string[]>([]);
  const [status, setStatus] = useState("Connecting to live log…");

  useEffect(() => {
    const socket = openLogTail(namespace, slug, job, containerOf(job));
    socket.onopen = () => setStatus("Live log connected.");
    socket.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (event.kind === "log_line") {
          setLines((current) => [...current, String(event.data.line ?? "")]);
        } else if (event.kind === "log_status") {
          setStatus(`Container ${String(event.data.phase ?? "state changed")}.`);
        } else if (event.kind === "log_exit") {
          setStatus(`Container exited with ${String(event.data.exit_code ?? "unknown status")}.`);
          onFinished();
        } else if (event.kind === "log_error") {
          setStatus(String(event.data.error ?? "Live log failed."));
        }
      } catch {
        setStatus("The live log returned an unreadable event.");
      }
    };
    socket.onerror = () => setStatus("The live log connection failed.");
    return () => socket.close();
  }, [namespace, slug, job, onFinished]);

  return (
    <section aria-live="polite">
      <p className="stream-state">{status}</p>
      <pre className="log" tabIndex={0}>
        {lines.length ? `${lines.join("\n")}\n` : "Waiting for output…\n"}
      </pre>
    </section>
  );
}

function LogRoute() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const run = useRemote(`log-run:${namespace}:${slug}`, () =>
    fetchRun(namespace, slug),
  );
  const finish = useCallback(
    () => window.setTimeout(run.reload, 1_500),
    [run.reload],
  );
  const attempts = (run.data?.jobs ?? []).filter(
    (candidate) => candidate.job === job,
  );
  const latest = attempts.at(-1);

  useEffect(() => {
    document.title = `Coalesce · ${job} log · one`;
  }, [job]);

  return frame(
    <>
      <p className="back">
        <Link to={`/${namespace}/runs/${slug}`}>← {slug}</Link>
      </p>
      <div className="heading">
        <div>
          <p className="eyebrow">Job log</p>
          <h1>{job}</h1>
        </div>
      </div>
      {run.loading ? <Loading>Finding the latest job attempt…</Loading> : null}
      {run.error ? <Problem error={run.error} /> : null}
      {run.data && !latest ? (
        <section className="empty">
          <strong>Job not found.</strong>
          <p>This run has no recorded attempt named {job}.</p>
        </section>
      ) : null}
      {latest && !latest.completed_at ? (
        <LiveLog
          namespace={namespace}
          slug={slug}
          job={job}
          onFinished={finish}
        />
      ) : null}
      {latest?.completed_at ? (
        <HarvestedLog namespace={namespace} slug={slug} job={job} />
      ) : null}
    </>,
  );
}

function MissingRoute() {
  return frame(
    <section className="empty">
      <strong>Route not found.</strong>
      <p>
        Return to <Link to="/coalesce/runs">the Coalesce runs</Link>.
      </p>
    </section>,
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
      <Route path="/:namespace/runs" element={<RunsRoute />} />
      <Route path="/:namespace/runs/:slug" element={<RunRoute />} />
      <Route
        path="/:namespace/runs/:slug/logs/:job"
        element={<LogRoute />}
      />
      <Route path="*" element={<MissingRoute />} />
    </Routes>
  );
}
