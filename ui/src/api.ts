// Types mirrored by hand from cmd/web/main.go's response structs — five
// shapes; codegen for five shapes is Jenkins.

export interface Run {
  slug: string;
  pipeline: string;
  started_at: string;
  completed_at?: string;
  status: string;
}

export interface Job {
  job: string;
  started_at: string;
  completed_at?: string;
  status: string;
  exit_code?: number;
}

export interface RunDetail extends Run {
  jobs?: Job[];
}

// The error vocabulary, so the interface can tell the truth: a real status
// carries its status and body; JSON that fails to parse (usually the SPA
// fallback answering a mistyped API path with HTML) is a shape problem, not
// a transport problem; anything else never reached the server at all.
export class ApiError extends Error {
  constructor(
    public status: number,
    public body: string,
  ) {
    super(`server answered ${status}`);
  }
}

export class ShapeError extends Error {}

async function get<T>(path: string): Promise<T> {
  const response = await fetch(path);
  const text = await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, text.slice(0, 300));
  }
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ShapeError(`not JSON from ${path}`);
  }
}

// The recorded seam: GET runs returns JSON null when the table is empty —
// Go's nil slice through json.Marshal — absorbed here so no view meets it.
export async function fetchRuns(namespace: string): Promise<Run[]> {
  return (await get<Run[] | null>(`/api/${namespace}/runs`)) ?? [];
}

export async function fetchRun(namespace: string, slug: string): Promise<RunDetail> {
  return get<RunDetail>(`/api/${namespace}/runs/${slug}`);
}

export async function fetchLog(
  namespace: string,
  slug: string,
  job: string,
  container: string,
): Promise<string> {
  const response = await fetch(`/api/${namespace}/logs/${slug}/${job}/${container}`);
  const text = await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, text.slice(0, 300));
  }
  return text;
}

// Convention dependency, recorded: the executor names a step's container
// after the job's leaf — coalesce.press runs in container "press". Holds for
// every current pipeline; the honest fix when a sidecar breaks it is one
// JOIN in the run detail response.
export function containerOf(job: string): string {
  return job.split(".").pop() ?? job;
}
