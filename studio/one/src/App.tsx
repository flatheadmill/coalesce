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
  ShapeError,
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
  type Job,
  type Run,
  type RunDetail,
} from "../../shared/api";

interface Remote<T> {
  data?: T;
  error?: unknown;
  loading: boolean;
  updatedAt?: number;
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
      if (showLoading) {
        setRemote({ loading: true });
      }
      try {
        const data = await readRef.current();
        if (current) {
          setRemote({ data, loading: false, updatedAt: Date.now() });
        }
      } catch (error) {
        if (current) {
          setRemote({ error, loading: false, updatedAt: Date.now() });
        }
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

function useClock(enabled: boolean): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!enabled) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1_000);
    return () => window.clearInterval(timer);
  }, [enabled]);
  return now;
}

function segment(value: string): string {
  return encodeURIComponent(value);
}

function runsPath(namespace: string): string {
  return `/${segment(namespace)}/runs`;
}

function runPath(namespace: string, slug: string): string {
  return `${runsPath(namespace)}/${segment(slug)}`;
}

function logPath(namespace: string, slug: string, job: string): string {
  return `${runPath(namespace, slug)}/logs/${segment(job)}`;
}

function formatTime(value: string | undefined): string {
  if (!value) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  }).format(new Date(value));
}

function formatShortTime(value: string | undefined): string {
  if (!value) return "—";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatRefresh(value: number | undefined): string {
  if (!value) return "not yet read";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  }).format(new Date(value));
}

function formatDuration(started: string, completed: string | number): string {
  const end = typeof completed === "number" ? completed : Date.parse(completed);
  const total = Math.max(0, Math.floor((end - Date.parse(started)) / 1_000));
  const hours = Math.floor(total / 3_600);
  const minutes = Math.floor((total % 3_600) / 60);
  const seconds = total % 60;
  const mm = String(minutes).padStart(2, "0");
  const ss = String(seconds).padStart(2, "0");
  return hours ? `${hours}:${mm}:${ss}` : `${mm}:${ss}`;
}

function Shell({
  namespace,
  children,
}: {
  namespace: string;
  children: ReactNode;
}) {
  return (
    <div className="shell">
      <a className="skip-link" href="#content">
        Skip to content
      </a>
      <header className="site-head">
        <Link className="wordmark" to={runsPath(namespace)}>
          Coalesce
        </Link>
        <p className="site-purpose">Kubernetes run evidence</p>
        <p className="site-context">
          <span>Namespace</span>
          <code>{namespace}</code>
        </p>
      </header>
      <main id="content">{children}</main>
      <footer className="site-foot">
        <span>Exhibit 01</span>
        <span>Kubernetes executes. Coalesce records.</span>
      </footer>
    </div>
  );
}

function StatusMark({ status }: { status: string }) {
  return (
    <span className={`status status-${status.toLowerCase()}`}>
      <span aria-hidden="true" className="status-glyph" />
      {status}
    </span>
  );
}

function describeError(error: unknown): { title: string; detail: string } {
  if (error instanceof ApiError) {
    const body = error.body.split("\n", 1)[0].trim();
    return {
      title: `The server answered ${error.status}.`,
      detail: body || "Coalesce did not provide more detail.",
    };
  }
  if (error instanceof ShapeError) {
    return {
      title: "The address answered with the wrong shape.",
      detail: "Coalesce expected JSON, but another response arrived.",
    };
  }
  return {
    title: "The request never reached Coalesce.",
    detail: error instanceof Error ? error.message : "The connection failed.",
  };
}

function Problem({ error }: { error: unknown }) {
  const message = describeError(error);
  return (
    <section className="notice notice-problem" role="alert">
      <p className="notice-label">Record unavailable</p>
      <h2>{message.title}</h2>
      <p>{message.detail}</p>
    </section>
  );
}

function Loading({ children }: { children: ReactNode }) {
  return (
    <div className="loading" role="status">
      <span aria-hidden="true" />
      <p>{children}</p>
    </div>
  );
}

function Empty({
  label,
  title,
  children,
}: {
  label: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <section className="notice notice-empty">
      <p className="notice-label">{label}</p>
      <h2>{title}</h2>
      <p>{children}</p>
    </section>
  );
}

function ActiveRun({ namespace, run }: { namespace: string; run: Run }) {
  const detail = useRemote(
    `active:${namespace}:${run.slug}`,
    () => fetchRun(namespace, run.slug),
    5_000,
  );
  const now = useClock(true);

  useEffect(() => {
    const socket = openRunEvents(namespace, run.slug);
    socket.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (/^(job_|run_|dag_)/.test(event.kind)) detail.reload();
      } catch {
        // HTTP polling remains the record when an advisory event is malformed.
      }
    };
    return () => socket.close();
  }, [namespace, run.slug, detail.reload]);

  const current = detail.data?.jobs?.filter((job) => !job.completed_at).at(-1);
  const completed = detail.data?.jobs?.filter((job) => job.completed_at).length ?? 0;
  const total = detail.data?.jobs?.length ?? 0;

  return (
    <article className="active-run">
      <div className="active-run-main">
        <p className="active-pipeline">{run.pipeline}</p>
        <h3>
          <Link to={runPath(namespace, run.slug)}>{run.slug}</Link>
        </h3>
        <p className="active-step">
          {detail.loading
            ? "Reading current step"
            : current
              ? <>On <code>{current.job}</code></>
              : "Waiting for the next recorded job"}
        </p>
      </div>
      <div className="active-run-progress">
        <p className="active-duration">
          <span>Elapsed</span>
          <strong>{formatDuration(run.started_at, now)}</strong>
        </p>
        <p className="active-count">
          {total ? `${completed} of ${total} jobs settled` : "Run record opened"}
        </p>
      </div>
    </article>
  );
}

type ArchiveFilter = "all" | "completed" | "failed";

function RunsRoute() {
  const { namespace = "coalesce" } = useParams();
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState<ArchiveFilter>("all");
  const runs = useRemote(
    `runs:${namespace}`,
    () => fetchRuns(namespace),
    10_000,
  );

  useEffect(() => {
    document.title = `Coalesce — ${namespace} run evidence`;
  }, [namespace]);

  const active = (runs.data ?? []).filter((run) => run.status === "running");
  const archive = (runs.data ?? []).filter((run) => run.status !== "running");
  const needle = query.trim().toLowerCase();
  const shown = archive.filter((run) => {
    const matchesStatus = filter === "all" || run.status === filter;
    const matchesQuery =
      !needle ||
      run.slug.toLowerCase().includes(needle) ||
      run.pipeline.toLowerCase().includes(needle);
    return matchesStatus && matchesQuery;
  });
  const failed = archive.filter((run) => run.status === "failed").length;
  const completed = archive.filter((run) => run.status === "completed").length;

  return (
    <Shell namespace={namespace}>
      <section className="page-intro runs-intro">
        <div>
          <p className="overline">Run record</p>
          <h1>What is moving. What remains.</h1>
        </div>
        <div className="read-state">
          <strong>{runs.data?.length ?? "—"}</strong>
          <span>records read</span>
          <small>as of {formatRefresh(runs.updatedAt)}</small>
        </div>
      </section>

      {runs.loading ? <Loading>Reading the run ledger…</Loading> : null}
      {runs.error ? <Problem error={runs.error} /> : null}
      {runs.data?.length === 0 ? (
        <Empty label="Empty ledger" title="No runs are recorded.">
          Coalesce has no evidence for namespace <code>{namespace}</code>.
        </Empty>
      ) : null}

      {runs.data && runs.data.length > 0 ? (
        <>
          <section className="working-field" aria-labelledby="working-title">
            <div className="section-head working-head">
              <div>
                <p className="section-index">01 / In flight</p>
                <h2 id="working-title">
                  {active.length
                    ? `${active.length} ${active.length === 1 ? "run is" : "runs are"} still open`
                    : "No runs are in flight"}
                </h2>
              </div>
              <p>
                Live state is advisory. Elapsed time continues until the run
                record settles.
              </p>
            </div>
            {active.length ? (
              <div className="active-runs">
                {active.map((run) => (
                  <ActiveRun key={run.slug} namespace={namespace} run={run} />
                ))}
              </div>
            ) : (
              <p className="quiet-line">Every run in the current window has settled.</p>
            )}
          </section>

          <section className="archive" aria-labelledby="archive-title">
            <div className="section-head archive-head">
              <div>
                <p className="section-index">02 / Ledger</p>
                <h2 id="archive-title">Settled run evidence</h2>
              </div>
              <p>{archive.length} records, newest first.</p>
            </div>

            <div className="ledger-tools">
              <div className="filter-group" aria-label="Filter by outcome">
                <button
                  className={filter === "all" ? "selected" : ""}
                  type="button"
                  aria-pressed={filter === "all"}
                  onClick={() => setFilter("all")}
                >
                  All <span>{archive.length}</span>
                </button>
                <button
                  className={filter === "failed" ? "selected" : ""}
                  type="button"
                  aria-pressed={filter === "failed"}
                  onClick={() => setFilter("failed")}
                >
                  Failed <span>{failed}</span>
                </button>
                <button
                  className={filter === "completed" ? "selected" : ""}
                  type="button"
                  aria-pressed={filter === "completed"}
                  onClick={() => setFilter("completed")}
                >
                  Completed <span>{completed}</span>
                </button>
              </div>
              <label className="ledger-search">
                <span>Find a run or pipeline</span>
                <input
                  type="search"
                  value={query}
                  placeholder="Find in this ledger"
                  onChange={(event) => setQuery(event.target.value)}
                />
              </label>
              <button className="refresh" type="button" onClick={runs.reload}>
                Refresh record
              </button>
            </div>

            {shown.length ? (
              <div className="table-scroll">
                <table className="ledger-table">
                  <caption>
                    Settled runs in namespace {namespace}, filtered to {filter}
                  </caption>
                  <thead>
                    <tr>
                      <th scope="col">Outcome</th>
                      <th scope="col">Run and pipeline</th>
                      <th scope="col">Started</th>
                      <th scope="col" className="numeric">Duration</th>
                    </tr>
                  </thead>
                  <tbody>
                    {shown.map((run) => (
                      <tr key={run.slug} className={`record-${run.status}`}>
                        <td data-label="Outcome">
                          <StatusMark status={run.status} />
                        </td>
                        <td data-label="Run">
                          <Link className="run-link" to={runPath(namespace, run.slug)}>
                            {run.slug}
                          </Link>
                          <span className="pipeline">{run.pipeline}</span>
                        </td>
                        <td data-label="Started">
                          <time dateTime={run.started_at}>
                            {formatShortTime(run.started_at)}
                          </time>
                        </td>
                        <td data-label="Duration" className="numeric duration">
                          {run.completed_at
                            ? formatDuration(run.started_at, run.completed_at)
                            : "—"}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty label="No match" title="No ledger rows meet this filter.">
                Change the outcome or search phrase to return to the record.
              </Empty>
            )}
          </section>
        </>
      ) : null}
    </Shell>
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

function dagNodeCount(nodes: DagNode[]): number {
  return nodes.reduce(
    (total, node) => total + 1 + dagNodeCount(node.children ?? []),
    0,
  );
}

function DagEntry({
  node,
  index,
  jobs,
}: {
  node: DagNode;
  index: number;
  jobs: Job[];
}) {
  const identity = `${node.under}.${node.name}`;
  const attempts = jobs.filter((job) => job.job === identity);
  const latest = attempts.at(-1);
  return (
    <li className={`dag-entry dag-${node.kind}`}>
      <div className="dag-line">
        <span className="dag-order">{String(index + 1).padStart(2, "0")}</span>
        <div className="dag-identity">
          <strong>{node.name}</strong>
          <code>{identity}</code>
        </div>
        {node.kind === "tranche" ? (
          <span className="dag-mode">
            {node.parallel ? "Parallel group" : "Ordered group"}
          </span>
        ) : null}
        <StatusMark status={latest?.status ?? "declared"} />
      </div>
      {node.children?.length ? (
        <ol className="dag-children">
          {node.children.map((child, childIndex) => (
            <DagEntry
              key={`${child.under}:${child.name}`}
              node={child}
              index={childIndex}
              jobs={jobs}
            />
          ))}
        </ol>
      ) : null}
    </li>
  );
}

function DagDeclaration({ dag, jobs }: { dag: DagResponse; jobs: Job[] }) {
  return (
    <ol className="dag-declaration">
      {dag.dag.map((node, index) => (
        <DagEntry
          key={`${node.under}:${node.name}`}
          node={node}
          index={index}
          jobs={jobs}
        />
      ))}
    </ol>
  );
}

function RunRoute() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const record = useRemote(`run:${namespace}:${slug}`, () =>
    readRunRecord(namespace, slug),
  );
  const running = record.data?.run.status === "running";
  const now = useClock(running);

  useEffect(() => {
    document.title = `Coalesce — ${slug}`;
  }, [slug]);
  useEffect(() => {
    const socket = openRunEvents(namespace, slug);
    socket.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        if (/^(job_|run_|dag_)/.test(event.kind)) record.reload();
      } catch {
        // The corresponding HTTP reads remain authoritative.
      }
    };
    return () => socket.close();
  }, [namespace, slug, record.reload]);

  const jobs = record.data?.run.jobs ?? [];

  return (
    <Shell namespace={namespace}>
      <nav className="back-link" aria-label="Breadcrumb">
        <Link to={runsPath(namespace)}>← Run ledger</Link>
      </nav>
      {record.loading ? <Loading>Opening run record {slug}…</Loading> : null}
      {record.error instanceof ApiError && record.error.status === 404 ? (
        <Empty label="Missing record" title="This run is not in the ledger.">
          No run named <code>{slug}</code> is recorded in namespace{" "}
          <code>{namespace}</code>.
        </Empty>
      ) : record.error ? (
        <Problem error={record.error} />
      ) : null}
      {record.data ? (
        <article className="run-sheet">
          <header className="run-title">
            <div>
              <p className="overline">Run record / {record.data.run.pipeline}</p>
              <h1>{record.data.run.slug}</h1>
            </div>
            <StatusMark status={record.data.run.status} />
          </header>

          <dl className="run-facts">
            <div>
              <dt>Started</dt>
              <dd>
                <time dateTime={record.data.run.started_at}>
                  {formatTime(record.data.run.started_at)}
                </time>
              </dd>
            </div>
            <div>
              <dt>{running ? "Elapsed" : "Duration"}</dt>
              <dd className="fact-figure">
                {formatDuration(
                  record.data.run.started_at,
                  record.data.run.completed_at ?? now,
                )}
              </dd>
            </div>
            <div>
              <dt>Completed</dt>
              <dd>
                {record.data.run.completed_at ? (
                  <time dateTime={record.data.run.completed_at}>
                    {formatTime(record.data.run.completed_at)}
                  </time>
                ) : (
                  "Run remains open"
                )}
              </dd>
            </div>
          </dl>

          <section className="run-section declaration" aria-labelledby="declaration-title">
            <div className="section-head">
              <div>
                <p className="section-index">01 / Declaration</p>
                <h2 id="declaration-title">The path this run declared</h2>
              </div>
              <p>
                {record.data.dag
                  ? `${dagNodeCount(record.data.dag.dag)} nodes · recorded ${formatShortTime(record.data.dag.created_at)}`
                  : "No DAG version was recorded"}
              </p>
            </div>
            {record.data.dag ? (
              <DagDeclaration dag={record.data.dag} jobs={jobs} />
            ) : (
              <p className="quiet-line">The run exists without a stored declaration.</p>
            )}
          </section>

          <section className="run-section attempts" aria-labelledby="attempts-title">
            <div className="section-head">
              <div>
                <p className="section-index">02 / Evidence</p>
                <h2 id="attempts-title">Job attempts</h2>
              </div>
              <p>{jobs.length} immutable {jobs.length === 1 ? "entry" : "entries"}.</p>
            </div>
            {jobs.length ? (
              <div className="table-scroll">
                <table className="jobs-table">
                  <caption>Job attempts recorded for run {slug}</caption>
                  <thead>
                    <tr>
                      <th scope="col">State</th>
                      <th scope="col">Job</th>
                      <th scope="col">Started</th>
                      <th scope="col" className="numeric">Duration</th>
                      <th scope="col" className="numeric">Exit</th>
                      <th scope="col"><span className="visually-hidden">Evidence</span></th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobs.map((job, jobIndex) => {
                      const attempt = jobs
                        .slice(0, jobIndex + 1)
                        .filter((candidate) => candidate.job === job.job).length;
                      const repeated = jobs.filter(
                        (candidate) => candidate.job === job.job,
                      ).length > 1;
                      return (
                        <tr key={`${job.job}:${job.started_at}`}>
                          <td data-label="State">
                            <StatusMark status={job.status} />
                          </td>
                          <td data-label="Job">
                            <code>{job.job}</code>
                            {repeated ? (
                              <span className="attempt-number">Attempt {attempt}</span>
                            ) : null}
                          </td>
                          <td data-label="Started">
                            <time dateTime={job.started_at}>
                              {formatShortTime(job.started_at)}
                            </time>
                          </td>
                          <td data-label="Duration" className="numeric duration">
                            {formatDuration(job.started_at, job.completed_at ?? now)}
                          </td>
                          <td data-label="Exit" className="numeric">
                            {job.exit_code ?? "—"}
                          </td>
                          <td data-label="Evidence" className="evidence-link">
                            <Link to={logPath(namespace, slug, job.job)}>
                              Open log <span aria-hidden="true">→</span>
                            </Link>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            ) : (
              <Empty label="No attempts" title="This run has not recorded a Job.">
                The declaration exists, but no Job evidence has arrived.
              </Empty>
            )}
          </section>
        </article>
      ) : null}
    </Shell>
  );
}

function CopyLog({ text }: { text: string }) {
  const [state, setState] = useState("Copy log");
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text);
      setState("Copied");
      window.setTimeout(() => setState("Copy log"), 1_500);
    } catch {
      setState("Copy failed");
    }
  };
  return (
    <button className="copy-log" type="button" onClick={() => void copy()}>
      {state}
    </button>
  );
}

function LogSurface({ text, label }: { text: string; label: string }) {
  return (
    <div className="log-frame">
      <div className="log-toolbar">
        <span>{label}</span>
        <CopyLog text={text} />
      </div>
      <pre className="log" tabIndex={0} aria-label={label}>
        {text || "Log is empty.\n"}
      </pre>
    </div>
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

  if (log.loading) return <Loading>Retrieving harvested evidence…</Loading>;
  if (log.error instanceof ApiError && log.error.status === 404) {
    return (
      <section className="notice notice-pending" role="status">
        <p className="notice-label">Harvest pending</p>
        <h2>The job settled before its log arrived.</h2>
        <p>Coalesce has not deposited the expected container log yet.</p>
        <button type="button" onClick={log.reload}>Check again</button>
      </section>
    );
  }
  if (log.error) return <Problem error={log.error} />;
  return <LogSurface text={log.data ?? ""} label="Harvested container log" />;
}

type TailState = "connecting" | "live" | "error" | "exited";

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
  const [output, setOutput] = useState("");
  const [tailState, setTailState] = useState<TailState>("connecting");
  const [message, setMessage] = useState("Opening a stream to the running container.");

  useEffect(() => {
    const socket = openLogTail(namespace, slug, job, containerOf(job));
    socket.onopen = () => {
      setTailState("live");
      setMessage("Connected. New lines will appear as the container writes them.");
    };
    socket.onmessage = (eventMessage) => {
      try {
        const event = parseStreamEvent(String(eventMessage.data));
        if (event.kind === "log_line") {
          setOutput((current) => `${current}${String(event.data.line ?? "")}\n`);
        } else if (event.kind === "log_status") {
          setMessage(`Container phase: ${String(event.data.phase ?? "unknown")}.`);
        } else if (event.kind === "log_exit") {
          setTailState("exited");
          setMessage(`Container exited ${String(event.data.exit_code ?? "without a recorded code")}. Waiting for the harvested record.`);
          onFinished();
        } else if (event.kind === "log_error") {
          setTailState("error");
          setMessage(String(event.data.error ?? "The live stream failed."));
        }
      } catch {
        setTailState("error");
        setMessage("The stream returned an event Coalesce could not read.");
      }
    };
    socket.onerror = () => {
      setTailState("error");
      setMessage("The run is marked running, but no pod accepted the tail connection.");
    };
    return () => socket.close();
  }, [namespace, slug, job, onFinished]);

  return (
    <>
      <div className={`stream-note stream-${tailState}`} aria-live="polite">
        <StatusMark status={tailState === "live" ? "live" : tailState} />
        <p>{message}</p>
      </div>
      <LogSurface
        text={output || (tailState === "error" ? "No live output was received.\n" : "Waiting for output…\n")}
        label="Live container log"
      />
    </>
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
  const now = useClock(Boolean(latest && !latest.completed_at));

  useEffect(() => {
    document.title = `Coalesce — ${job} evidence`;
  }, [job]);

  return (
    <Shell namespace={namespace}>
      <nav className="back-link" aria-label="Breadcrumb">
        <Link to={runPath(namespace, slug)}>← Run {slug}</Link>
      </nav>
      {run.loading ? <Loading>Locating job evidence…</Loading> : null}
      {run.error instanceof ApiError && run.error.status === 404 ? (
        <Empty label="Missing run" title="The parent run is not in the ledger.">
          No run named <code>{slug}</code> is recorded in namespace{" "}
          <code>{namespace}</code>.
        </Empty>
      ) : run.error ? (
        <Problem error={run.error} />
      ) : null}
      {run.data && !latest ? (
        <Empty label="Missing job" title="This attempt is not in the run record.">
          Run <code>{slug}</code> has no job named <code>{job}</code>.
        </Empty>
      ) : null}
      {run.data && latest ? (
        <article className="log-sheet">
          <header className="log-title">
            <div>
              <p className="overline">
                {latest.completed_at ? "Harvested evidence" : "Live evidence"} / {run.data.pipeline}
              </p>
              <h1>{job}</h1>
              <p className="run-reference">Run {slug}</p>
            </div>
            <StatusMark status={latest.status} />
          </header>

          <dl className="log-facts">
            <div>
              <dt>Started</dt>
              <dd>{formatTime(latest.started_at)}</dd>
            </div>
            <div>
              <dt>{latest.completed_at ? "Duration" : "Elapsed"}</dt>
              <dd className="fact-figure">
                {formatDuration(latest.started_at, latest.completed_at ?? now)}
              </dd>
            </div>
            <div>
              <dt>Custody</dt>
              <dd>{latest.completed_at ? "Stored by Coalesce" : "Streaming from the pod"}</dd>
            </div>
            <div>
              <dt>Container</dt>
              <dd><code>{containerOf(job)}</code></dd>
            </div>
          </dl>

          <section className="log-evidence" aria-labelledby="log-evidence-title">
            <div className="section-head">
              <div>
                <p className="section-index">01 / Container record</p>
                <h2 id="log-evidence-title">
                  {latest.completed_at ? "Deposited log" : "Open stream"}
                </h2>
              </div>
              <p>
                {latest.completed_at
                  ? `Completed ${formatShortTime(latest.completed_at)}`
                  : "The HTTP run record still marks this Job open"}
              </p>
            </div>
            {latest.completed_at ? (
              <HarvestedLog namespace={namespace} slug={slug} job={job} />
            ) : (
              <LiveLog
                namespace={namespace}
                slug={slug}
                job={job}
                onFinished={finish}
              />
            )}
          </section>
        </article>
      ) : null}
    </Shell>
  );
}

function MissingRoute() {
  return (
    <Shell namespace="coalesce">
      <Empty label="Unknown route" title="There is no evidence at this address.">
        Return to <Link to={runsPath("coalesce")}>the Coalesce run ledger</Link>.
      </Empty>
    </Shell>
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
      <Route path="/:namespace/runs" element={<RunsRoute />} />
      <Route path="/:namespace/runs/:slug" element={<RunRoute />} />
      <Route path="/:namespace/runs/:slug/logs/:job" element={<LogRoute />} />
      <Route path="*" element={<MissingRoute />} />
    </Routes>
  );
}
