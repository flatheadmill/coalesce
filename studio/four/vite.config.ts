import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const target = env.COALESCE_DEV_UPSTREAM ?? "http://coalesce.coalesce.svc.cluster.local";
  return {
    root: new URL(".", import.meta.url).pathname,
    plugins: [react()],
    build: { outDir: "../dist/four", emptyOutDir: true },
    server: {
      port: 5174,
      strictPort: true,
      proxy: {
        "/api": { target, changeOrigin: true },
        "/events": { target, changeOrigin: true, ws: true },
        "/tail": { target, changeOrigin: true, ws: true },
      },
    },
    preview: { port: 4174, strictPort: true },
  };
});
