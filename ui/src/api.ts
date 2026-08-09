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

async function get<T>(path: string): Promise<T> {
  const response = await fetch(path);
  if (!response.ok) {
    throw new ApiError(response.status, `${response.status} from ${path}`);
  }
  return response.json();
}

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
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
  if (!response.ok) {
    throw new ApiError(response.status, `${response.status} from logs`);
  }
  return response.text();
}

// Convention dependency, recorded: the executor names a step's container
// after the job's leaf — coalesce.press runs in container "press". Holds for
// every current pipeline; the honest fix when a sidecar breaks it is one
// JOIN in the run detail response.
export function containerOf(job: string): string {
  return job.split(".").pop() ?? job;
}
