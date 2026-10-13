import test from "node:test";
import assert from "node:assert/strict";
import { initialJob, latestJobs, statusLabel } from "../src/model.ts";

const started = (minute: number) => `2026-10-05T12:${String(minute).padStart(2, "0")}:00Z`;
const job = (name: string, minute: number, status: string, end?: number) => ({ job: name, started_at: started(minute), status, ...(end === undefined ? {} : { completed_at: started(end) }) });

test("an active retry displaces its older failure", () => {
  const jobs = [job("build", 1, "failed", 2), job("build", 4, "running"), job("lint", 3, "completed", 5)];
  assert.equal(initialJob(jobs), "build");
  assert.equal(latestJobs(jobs).find(value => value.job === "build")?.status, "running");
});

test("selection uses the most recent failure among latest attempts", () => {
  const jobs = [job("old", 1, "failed", 2), job("tests", 4, "failed", 5), job("lint", 3, "failed", 6), job("old", 7, "completed", 8)];
  assert.equal(initialJob(jobs), "tests");
});

test("parallel active Jobs follow declaration order, with arrival order as fallback", () => {
  const jobs = [job("second", 2, "running"), job("first", 3, "running")];
  assert.equal(initialJob(jobs, ["first", "second"]), "first");
  assert.equal(initialJob(jobs), "second");
  assert.equal(initialJob([]), undefined);
});

test("a settled run opens its last completion, independent of start order", () => {
  assert.equal(initialJob([job("slow", 1, "completed", 8), job("fast", 2, "completed", 4)]), "slow");
});

test("unknown result words remain visible", () => {
  assert.equal(statusLabel("interrupted"), "interrupted");
  assert.equal(statusLabel("future-result"), "future-result");
});
