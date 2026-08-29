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

export interface DagNode {
  name: string;
  under: string;
  kind: "node" | "tranche";
  parallel?: boolean;
  children: DagNode[];
}

export interface DagResponse {
  dag: DagNode[];
  created_at: string;
}

export interface StreamEvent {
  kind: string;
  data: Record<string, unknown>;
}

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly body: string,
  ) {
    super(`Coalesce answered ${status}`);
    this.name = "ApiError";
  }
}

export class ShapeError extends Error {
  constructor(path: string) {
    super(`Coalesce returned non-JSON content from ${path}`);
    this.name = "ShapeError";
  }
}

function segment(value: string): string {
  return encodeURIComponent(value);
}

async function getJSON<T>(path: string): Promise<T> {
  const response = await fetch(path);
  const body = await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, body.slice(0, 300));
  }
  try {
    return JSON.parse(body) as T;
  } catch {
    throw new ShapeError(path);
  }
}

export async function fetchRuns(namespace: string): Promise<Run[]> {
  const runs = await getJSON<Run[] | null>(`/api/${segment(namespace)}/runs`);
  return runs ?? [];
}

export function fetchRun(namespace: string, slug: string): Promise<RunDetail> {
  return getJSON<RunDetail>(`/api/${segment(namespace)}/runs/${segment(slug)}`);
}

export function fetchDag(namespace: string, slug: string): Promise<DagResponse> {
  return getJSON<DagResponse>(`/api/${segment(namespace)}/dags/${segment(slug)}`);
}

export async function fetchLog(
  namespace: string,
  slug: string,
  job: string,
  container: string,
): Promise<string> {
  const path = `/api/${segment(namespace)}/logs/${segment(slug)}/${segment(job)}/${segment(container)}`;
  const response = await fetch(path);
  const body = await response.text();
  if (!response.ok) {
    throw new ApiError(response.status, body.slice(0, 300));
  }
  return body;
}

export function containerOf(job: string): string {
  return job.split(".").pop() ?? job;
}

function socketURL(path: string): string {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${window.location.host}${path}`;
}

export function openRunEvents(namespace: string, slug: string): WebSocket {
  return new WebSocket(
    socketURL(`/events/${segment(namespace)}/${segment(slug)}`),
  );
}

export function openLogTail(
  namespace: string,
  slug: string,
  job: string,
  container: string,
): WebSocket {
  const path = `/tail/${segment(namespace)}/${segment(slug)}/${segment(job)}?container=${segment(container)}`;
  return new WebSocket(socketURL(path));
}

export function parseStreamEvent(payload: string): StreamEvent {
  return JSON.parse(payload) as StreamEvent;
}
