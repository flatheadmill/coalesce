import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { RunsPage } from "./pages/RunsPage";
import { RunPage } from "./pages/RunPage";
import { LogPage } from "./pages/LogPage";

const client = new QueryClient();

// Routes are the identity model verbatim: namespace, then the run's slug,
// then a step's logs. The default namespace is the local world's.
export function App() {
  return (
    <QueryClientProvider client={client}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Navigate to="/coalesce/runs" replace />} />
          <Route path="/:namespace/runs" element={<RunsPage />} />
          <Route path="/:namespace/runs/:slug" element={<RunPage />} />
          <Route path="/:namespace/runs/:slug/logs/:job" element={<LogPage />} />
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
