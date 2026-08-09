import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router-dom";
import { ApiError, containerOf, fetchLog } from "../api";

// Harvested logs as honest preformatted text — the log that says why. No
// typeset-document ambitions yet; live tailing deferred whole. A 404 before
// harvest means the step has not finished: quiet Voice, no apology.
export function LogPage() {
  const { namespace = "coalesce", slug = "", job = "" } = useParams();
  const { data: log, error } = useQuery({
    queryKey: ["log", namespace, slug, job],
    queryFn: () => fetchLog(namespace, slug, job, containerOf(job)),
    retry: false,
  });
  useEffect(() => {
    document.title = `coalesce · ${namespace} · ${slug} · ${job}`;
  }, [namespace, slug, job]);

  return (
    <>
      <h1 className="nameplate">
        <Link to={`/${namespace}/runs`}>Kerchunkifier 3000</Link>
      </h1>
      <p className="crumb">
        <Link to={`/${namespace}/runs/${slug}`}>← {slug}</Link> · {job}
      </p>

      {error instanceof ApiError && error.status === 404 ? (
        <p className="error">
          Not harvested yet — the log lands when the step finishes.
        </p>
      ) : error ? (
        <p className="error">
          Can't reach the server. Is the deployment up?{" "}
          <code>kubectl --context orbstack get pods -n coalesce</code>
        </p>
      ) : null}

      {log !== undefined && <pre className="log">{log}</pre>}
    </>
  );
}
