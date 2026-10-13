import type { Job } from "../../shared/api";

export const runsPath = (namespace: string) => `/${encodeURIComponent(namespace)}/runs`;
export const runPath = (namespace: string, slug: string) => `${runsPath(namespace)}/${encodeURIComponent(slug)}`;
export const logPath = (namespace: string, slug: string, job: string) => `${runPath(namespace, slug)}/logs/${encodeURIComponent(job)}`;

export function latestJobs(jobs: Job[]): Job[] {
  const latest = new Map<string, Job>();
  for (const job of jobs) {
    const previous = latest.get(job.job);
    if (!previous || Date.parse(job.started_at) >= Date.parse(previous.started_at)) latest.set(job.job, job);
  }
  return [...latest.values()].sort((a, b) => Date.parse(a.started_at) - Date.parse(b.started_at));
}

export function initialJob(jobs: Job[], declarationOrder: string[] = []): string | undefined {
  const latest = latestJobs(jobs);
  const failed = latest.filter(job => job.status === "failed").sort((a, b) => Date.parse(b.started_at) - Date.parse(a.started_at));
  if (failed.length) return failed[0].job;
  const active = latest.filter(job => !job.completed_at && (job.status === "running" || job.status === "pending"));
  // A slow or absent declaration must not delay output. Once chosen, its
  // arrival is not permission to change the reader's selection.
  for (const id of declarationOrder) if (active.some(job => job.job === id)) return id;
  if (active.length) return active[0].job;
  const closed = latest.filter(job => job.completed_at).sort((a, b) => Date.parse(b.completed_at!) - Date.parse(a.completed_at!));
  return closed[0]?.job ?? latest.at(-1)?.job;
}

export function duration(start: string, end: string | number): string {
  const elapsed = Math.max(0, Math.floor(((typeof end === "number" ? end : Date.parse(end)) - Date.parse(start)) / 1000));
  if (!Number.isFinite(elapsed)) return "—";
  const days = Math.floor(elapsed / 86400), hours = Math.floor(elapsed % 86400 / 3600);
  const minutes = Math.floor(elapsed % 3600 / 60), seconds = elapsed % 60;
  if (days) return `${days}d ${hours}h`;
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${seconds.toString().padStart(2, "0")}s`;
  return `${seconds}s`;
}

export function statusLabel(value: string): string {
  return ({ running: "Running", completed: "Completed", failed: "Failed", pending: "Waiting" } as Record<string, string>)[value] ?? value;
}
export function statusKind(value: string): string {
  return ["running", "completed", "failed", "pending"].includes(value) ? value : "other";
}
export function fullTime(value?: string): string {
  return value ? new Date(value).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "long" }) : "Not recorded";
}
