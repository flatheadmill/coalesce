import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target =
    env.COALESCE_DEV_UPSTREAM ??
    "http://coalesce.coalesce.svc.cluster.local";

  return {
    root: new URL(".", import.meta.url).pathname,
    plugins: [react()],
    build: {
      outDir: "../dist/three",
      emptyOutDir: true,
    },
    server: {
      port: 5173,
      // An HTTPS ingress selects its certificate from the upstream Host/SNI,
      // not the localhost Host the browser sent to Vite.
      proxy: {
        "/api": { target, changeOrigin: true },
        "/events": { target, changeOrigin: true, ws: true },
        "/tail": { target, changeOrigin: true, ws: true },
      },
    },
  };
});
