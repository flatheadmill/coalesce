import { ApiError, ShapeError } from "./api";

// Errors say what happened, in the interface's voice — facts, no apology,
// and no coached commands: a production page has no business suggesting
// laptop kubectl.
export function Trouble({ error }: { error: unknown }) {
  if (error instanceof ApiError) {
    const body = error.body.split("\n", 1)[0].trim();
    return (
      <p className="error">
        The server answered {error.status}
        {body ? <>: <code>{body}</code></> : "."}
      </p>
    );
  }
  if (error instanceof ShapeError) {
    return (
      <p className="error">
        The server answered, but not with JSON — something else may be serving
        this address.
      </p>
    );
  }
  return <p className="error">Can't reach the server — the request never arrived.</p>;
}
