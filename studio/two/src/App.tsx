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

type Load<T> = {
  value?: T;
  failure?: unknown;
  pending: boolean;
};

function useLoad<T>(
  identity: string,
  request: () => Promise<T>,
  interval = 0,
): Load<T> & { refresh: () => void } {
  const requestRef = useRef(request);
  requestRef.current = request;
  const [turn, setTurn] = useState(0);
  const [load, setLoad] = useState<Load<T>>({ pending: true });
  const refresh = useCallback(() => setTurn((current) => current + 1), []);

  useEffect(() => {
    let mounted = true;
    const perform = async (pending: boolean) => {
      if (pending) setLoad({ pending: true });
      try {
        const value = await requestRef.current();
        if (mounted) setLoad({ value, pending: false });
      } catch (failure) {
        if (mounted) setLoad({ failure, pending: false });
      }
    };
    void perform(true);
    const timer = interval
      ? window.setInterval(() => void perform(false), interval)
      : undefined;
    return () => {
      mounted = false;
      if (timer !== undefined) window.clearInterval(timer);
    };
  }, [identity, interval, turn]);

  return { ...load, refresh };
}

function Shell({ children }: { children: ReactNode }) {
  return (
    <div className="shell">
      <header>
        <div>
          <Link className="wordmark" to="/coalesce/runs">
            Coalesce
          </Link>
          <span className="studio-mark">Studio two</span>
        </div>
        <span className="neutral-mark">neutral contract scaffold</span>
      </header>
      <main>{children}</main>
    </div>
  );
}

function errorText(failure: unknown): string {
  if (failure instanceof ApiError) {
    const body = failure.body.trim();
    return body ? `${failure.message}: ${body}` : failure.message;
  }
  return failure instanceof Error ? failure.message : "An unknown error occurred.";
}

function Failure({ value }: { value: unknown }) {
  return (
    <div className="failure" role="alert">
      <b>Request failed</b>
      <span>{errorText(value)}</span>
    </div>
  );
}

function Pending({ text }: { text: string }) {
  return (
    <div className="pending" role="status">
      {text}
    </div>
  );
}

function when(value?: string): string {
  if (!value) return "Not recorded";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function dagSize(nodes: DagNode[]): number {
  let size = 0;
  for (const node of nodes) size += 1 + dagSize(node.children ?? []);
  return size;
}

function Runs() {
  const { namespace = "coalesce" } = useParams();
  const result = useLoad(
    `runs/${namespace}`,
    () => fetchRuns(namespace),
    10_000,
  );

  useEffect(() => {
    document.title = `${namespace} runs · Coalesce studio two`;
  }, [namespace]);

  return (
    <Shell>
      <div className="title-row">
        <div>
          <span className="context">Namespace / {namespace}</span>
          <h1>Pipeline runs</h1>
        </div>
        <button type="button" onClick={result.refresh}>
          Read again
        </button>
      </div>
      {result.pending ? <Pending text="Loading the run record…" /> : null}
      {result.failure ? <Failure value={result.failure} /> : null}
      {result.value?.length === 0 ? (
        <div className="blank">
          <b>Nothing has run here.</b>
          <span>Coalesce returned no runs for {namespace}.</span>
        </div>
      ) : null}
      {result.value && result.value.length > 0 ? (
        <ol className="runs">
          {result.value.map((run) => (
            <li key={run.slug}>
              <span className="run-state">{run.status}</span>
              <div>
                <Link to={`/${namespace}/runs/${run.slug}`}>{run.slug}</Link>
                <span>{run.pipeline}</span>
              </div>
              <time dateTime={run.started_at}>{when(run.started_at)}</time>
            </li>
          ))}
        </ol>
      ) : null}
    </Shell>
  );
}

type RunEvidence = { run: RunDetail; dag: DagResponse | null };

async function getRunEvidence(
  namespace: string,
  slug: string,
): Promise<RunEvidence> {
  const run = await fetchRun(namespace, slug);
  try {
    return { run, dag: await fetchDag(namespace, slug) };
  } catch (failure) {
    if (failure instanceof ApiError && failure.status === 404) {
      return { run, dag: null };
    }
    throw failure;
  }
}

function Run() {
  const { namespace = "coalesce", slug = "" } = useParams();
  const evidence = useLoad(`run/${namespace}/${slug}`, () =>
    getRunEvidence(namespace, slug),
  );

  useEffect(() => {
    const events = openRunEvents(namespace, slug);
    events.onmessage = () => evidence.refresh();
    return () => events.close();
  }, [namespace, slug, evidence.refresh]);
  useEffect(() => {
    document.title = `${slug} · Coalesce studio two`;
  }, [slug]);

  return (
    <Shell>
      <nav className="return">
        <Link to={`/${namespace}/runs`}>← All runs in {namespace}</Link>
      </nav>
      {evidence.pending ? <Pending text={`Loading ${slug}…`} /> : null}
      {evidence.failure instanceof ApiError &&
      evidence.failure.status === 404 ? (
        <div className="blank">
          <b>Run not found.</b>
          <span>{slug} is not recorded in {namespace}.</span>
        </div>
      ) : evidence.failure ? (
        <Failure value={evidence.failure} />
      ) : null}
      {evidence.value ? (
        <article>
          <div className="title-row">
            <div>
              <span className="context">{evidence.value.run.pipeline}</span>
              <h1>{evidence.value.run.slug}</h1>
            </div>
            <b className="run-state">{evidence.value.run.status}</b>
          </div>
          <div className="evidence-strip">
            <p>
              <span>Started</span>
              {when(evidence.value.run.started_at)}
            </p>
            <p>
              <span>Completed</span>
              {when(evidence.value.run.completed_at)}
            </p>
            <p>
              <span>DAG response</span>
              {evidence.value.dag
                ? `${dagSize(evidence.value.dag.dag)} nodes; structure not pictured`
                : "Not recorded"}
            </p>
          </div>
          {(evidence.value.run.jobs ?? []).length === 0 ? (
            <div className="blank">
              <b>No jobs recorded.</b>
              <span>The run exists without a job attempt.</span>
            </div>
          ) : (
            <ol className="jobs">
              {(evidence.value.run.jobs ?? []).map((job) => (
                <li key={`${job.job}/${job.started_at}`}>
                  <span className="run-state">{job.status}</span>
                  <div>
                    <Link to={`/${namespace}/runs/${slug}/logs/${job.job}`}>
                      {job.job}
                    </Link>
                    <span>
                      {when(job.started_at)} → {when(job.completed_at)}
                    </span>
                  </div>
                  <span>exit {job.exit_code ?? "—"}</span>
                </li>
              ))}
            </ol>
          )}
        </article>
      ) : null}
    </Shell>
  );
}

function StoredLog({ namespace, slug, job }: RouteIdentity) {
  const result = useLoad(`stored/${namespace}/${slug}/${job}`, () =>
    fetchLog(namespace, slug, job, containerOf(job)),
  );
  if (result.pending) return <Pending text="Loading stored output…" />;
  if (result.failure instanceof ApiError && result.failure.status === 404) {
    return (
      <div className="blank">
        <b>Stored output is not ready.</b>
        <span>The harvester has not deposited this log.</span>
      </div>
    );
  }
  if (result.failure) return <Failure value={result.failure} />;
  return (
    <pre className="output" tabIndex={0}>
      {result.value || "Log is empty.\n"}
    </pre>
  );
}

interface RouteIdentity {
  namespace: string;
  slug: string;
  job: string;
}

function StreamingLog({
  namespace,
  slug,
  job,
  finished,
}: RouteIdentity & { finished: () => void }) {
  const [output, setOutput] = useState<string[]>([]);
  const [note, setNote] = useState("Opening live output…");

  useEffect(() => {
    const tail = openLogTail(namespace, slug, job, containerOf(job));
    tail.onopen = () => setNote("Live output connected.");
    tail.onmessage = (message) => {
      try {
        const event = parseStreamEvent(String(message.data));
        switch (event.kind) {
          case "log_line":
            setOutput((current) => [
              ...current,
              String(event.data.line ?? ""),
            ]);
            break;
          case "log_status":
            setNote(`Pod phase: ${String(event.data.phase ?? "unknown")}.`);
            break;
          case "log_exit":
            setNote(`Process exited ${String(event.data.exit_code ?? "")}.`);
            finished();
            break;
          case "log_error":
            setNote(String(event.data.error ?? "Live output failed."));
            break;
        }
      } catch {
        setNote("The live event could not be read.");
      }
    };
    tail.onerror = () => setNote("The live output connection failed.");
    return () => tail.close();
  }, [namespace, slug, job, finished]);

  return (
    <div aria-live="polite">
      <p className="tail-note">{note}</p>
      <pre className="output" tabIndex={0}>
        {output.length ? `${output.join("\n")}\n` : "Waiting for output…\n"}
      </pre>
    </div>
  );
}

function Log() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const run = useLoad(`job/${namespace}/${slug}`, () =>
    fetchRun(namespace, slug),
  );
  const finished = useCallback(
    () => window.setTimeout(run.refresh, 1_500),
    [run.refresh],
  );
  const attempts = (run.value?.jobs ?? []).filter(
    (candidate) => candidate.job === job,
  );
  const current = attempts.at(-1);

  useEffect(() => {
    document.title = `${job} output · Coalesce studio two`;
  }, [job]);

  return (
    <Shell>
      <nav className="return">
        <Link to={`/${namespace}/runs/${slug}`}>← Run {slug}</Link>
      </nav>
      <span className="context">Job output</span>
      <h1>{job}</h1>
      {run.pending ? <Pending text="Finding the current attempt…" /> : null}
      {run.failure ? <Failure value={run.failure} /> : null}
      {run.value && !current ? (
        <div className="blank">
          <b>Job not found.</b>
          <span>No attempt named {job} belongs to this run.</span>
        </div>
      ) : null}
      {current && !current.completed_at ? (
        <StreamingLog
          namespace={namespace}
          slug={slug}
          job={job}
          finished={finished}
        />
      ) : null}
      {current?.completed_at ? (
        <StoredLog namespace={namespace} slug={slug} job={job} />
      ) : null}
    </Shell>
  );
}

function NotFound() {
  return (
    <Shell>
      <div className="blank">
        <b>This route does not exist.</b>
        <span>
          Open <Link to="/coalesce/runs">the run collection</Link>.
        </span>
      </div>
    </Shell>
  );
}

export function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
      <Route path="/:namespace/runs" element={<Runs />} />
      <Route path="/:namespace/runs/:slug" element={<Run />} />
      <Route path="/:namespace/runs/:slug/logs/:job" element={<Log />} />
      <Route path="*" element={<NotFound />} />
    </Routes>
  );
}
