import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

export interface LogEvent {
  kind: string;
  data: Record<string, unknown>;
}

// One WebSocket per (namespace, slug), dispatching invalidations. These events
// are advisory and GETs are truth; log content uses its own direct stream.
export function useRunEvents(namespace: string, slug: string | undefined): void {
  const client = useQueryClient();
  useEffect(() => {
    if (!slug) return;
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(`${protocol}//${location.host}/events/${namespace}/${slug}`);
    socket.onmessage = (message) => {
      const { kind } = JSON.parse(message.data) as LogEvent;
      if (kind.startsWith("job_") || kind.startsWith("run_")) {
        client.invalidateQueries({ queryKey: ["runs", namespace] });
        client.invalidateQueries({ queryKey: ["run", namespace, slug] });
      }
    };
    return () => socket.close();
  }, [namespace, slug, client]);
}
