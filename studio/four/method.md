# Four: the daily run page

Four begins with the developer arriving from a GitHub check. The run's result,
its Jobs, their durations, and the selected output belong on the first screen.
The dashboard register follows Nascent: a modest heading, ordinary controls,
and the information the reader came for. Resource telemetry is outside this cut.

The runs page is a newest-first table: Result, Run, Started, Duration. Each
invocation keeps its own row. Search and status filtering cover the API's latest
100 records. Pipeline statements are the links; slugs remain the addresses.
There are no per-row Job reads, inferred PR families, or parsed branch names.

Run detail keeps a compact dependency diagram above a full-width xterm surface.
Four carries forward Two's precedence derivation as its own source file. Diagram
nodes are ordinary links, usable by keyboard as well as pointer. Job rows remain
available when the declaration is missing or cannot place a recorded identity.
The header and output load independently of the declaration.

Initial selection uses the latest attempt for each Job: most recent failure,
otherwise the first active Job in declaration order, otherwise last completion.
Before the declaration arrives, active Jobs follow response order. An explicit
Job URL takes priority. Once selected, refreshes and arriving failures do not
move the selection. An empty run selects the first Job it receives.

Running, Completed and Failed describe the executor record. Other values remain
visible verbatim. Elapsed time measures time since start; it does not diagnose
orphaned runs. Pending is reported locally only when the tail supplies that Pod
phase. Exact timestamps, attempts and recorded exit codes remain in Run details.
The API has no source revision field or attempt-selectable output endpoint;
Four does not manufacture either.

Live and stored output have separate mounted terminals so each keeps its scroll.
Arrival during an open Job starts a tail; arrival after completion starts with
storage. A closing write does not change the source being read. Existing stored
polling and the thirty-second harvest grace are retained. A Job completed within
the preceding minute also receives that grace when the page first opens.

The browser cannot distinguish a WebSocket handshake's 404 from other connection
failures. Quiet retries describe connecting, then output unavailable; they do
not diagnose a missing Pod. Four makes four retries over roughly thirteen seconds
while the record is open and the connection has received no bytes or exit.
After bytes arrive, a disconnect preserves them. An explicit reconnect begins a
fresh stream, because the server has no replay cursor. Observations are saved in
the tab session under namespace, run, Job and latest attempt start.

Copy returns the text received for that source. Live output is received line by
line; it is not a claim about raw container bytes. An empty stored response is
distinct from a missing artifact. A failed refresh preserves already received
text. Repeated attempts get a short local attribution caveat.

This is a later brief, agreed with coalesce and discussed with coalesce-fable in
three exchanges on October 5, 2026. It deliberately carries forward useful work
from the earlier cuts. One, Two and Three remain intact at the base ea19232.
Four imports only the shared transport; it owns its presentation and behavior.

## Local preview

From `studio/`:

```sh
COALESCE_DEV_UPSTREAM=https://coalesce.acreops.org npm run dev:four -- --host 127.0.0.1
```

Open `http://localhost:5174/coalesce/runs`. The proxy reads the existing API and
streams. It does not deploy Four or replace the embedded product UI.

```sh
npm run build:four
node --test four/test/model.test.ts
```

The selector tests use Node's TypeScript stripping (Node 22.18+ or Node 24).
