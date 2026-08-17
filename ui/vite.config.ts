import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The dev loop: Vite on the Mac, proxying to the OrbStack Coalesce server —
// OrbStack resolves cluster DNS from the Mac directly, so the target is the
// executor's own default address. /events carries WebSockets.
export default defineConfig({
  plugins: [react()],
  // Built output lands where the server's go:embed expects it — the UI is
  // embedded and versioned with the server. emptyOutDir stays off so the
  // tracked .gitkeep that satisfies the embed pattern on a clean clone
  // survives local builds.
  build: {
    outDir: "../cmd/web/dist",
    emptyOutDir: false,
  },
  server: {
    proxy: {
      "/api": { target: "http://coalesce.coalesce.svc.cluster.local" },
      "/events": {
        target: "http://coalesce.coalesce.svc.cluster.local",
        ws: true,
      },
    },
  },
});
