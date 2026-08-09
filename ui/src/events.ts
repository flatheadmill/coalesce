import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";

// One WebSocket per (namespace, slug), dispatching invalidations — events
// are advisory, GETs are truth. The hub is slug-scoped and forgetful; a
// dropped event degrades to stillness until the next reconcile, never to
// wrongness. Log events are ignored here: harvested logs are fetched, and
// live tailing is deferred whole.
export function useRunEvents(namespace: string, slug: string | undefined): void {
  const client = useQueryClient();
  useEffect(() => {
    if (!slug) return;
    const protocol = location.protocol === "https:" ? "wss:" : "ws:";
    const socket = new WebSocket(`${protocol}//${location.host}/events/${namespace}/${slug}`);
    socket.onmessage = (message) => {
      const { kind } = JSON.parse(message.data) as { kind: string };
      if (kind.startsWith("job_") || kind.startsWith("run_")) {
        client.invalidateQueries({ queryKey: ["runs", namespace] });
        client.invalidateQueries({ queryKey: ["run", namespace, slug] });
      }
    };
    return () => socket.close();
  }, [namespace, slug, client]);
}
