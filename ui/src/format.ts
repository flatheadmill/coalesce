// The custody rule lives here. While a run lives, its duration is the
// client's honest estimate ticking against local time. Once dead, the figure
// is the cubbyhole's own arithmetic — completed_at minus started_at — and
// the swap is the settle.

export function recordedDuration(started: string, completed: string): string {
  return spell((Date.parse(completed) - Date.parse(started)) / 1000);
}

export function countingDuration(started: string, now: number): string {
  return spell((now - Date.parse(started)) / 1000);
}

function spell(total: number): string {
  const seconds = Math.max(0, Math.floor(total));
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = seconds % 60;
  const mm = String(m).padStart(2, "0");
  const ss = String(s).padStart(2, "0");
  return h > 0 ? `${h}:${mm}:${ss}` : `${mm}:${ss}`;
}

// Absolute timestamps — evidence, not vibes. Local time, spelled fully.
export function timestamp(iso: string): string {
  const at = new Date(iso);
  const date = [
    at.getFullYear(),
    String(at.getMonth() + 1).padStart(2, "0"),
    String(at.getDate()).padStart(2, "0"),
  ].join("-");
  const time = [
    String(at.getHours()).padStart(2, "0"),
    String(at.getMinutes()).padStart(2, "0"),
    String(at.getSeconds()).padStart(2, "0"),
  ].join(":");
  return `${date} ${time}`;
}
