# Four: first-cut receipt

October 5, 2026. Local preview on `topic/main/ui-one`, based on `ea19232`.

Four implements the daily-developer brief in [method.md](method.md): an ordinary
newest-first runs table, a compact run header and navigable dependency diagram,
then full-width output. The failed Job is selected on arrival; explicit Job
links win, and later updates leave the reader's selection and scroll alone.

Four imports the shared API and owns the rest of its source. The studio container
still serves the original three; the product image embeds Four.

## Preview

From `studio/`:

```sh
COALESCE_DEV_UPSTREAM=http://coalesce.coalesce.svc.cluster.local npm run dev:four -- --host 127.0.0.1
```

- Runs: <http://localhost:5174/coalesce/runs>

This preview reads the configured API and streams. Browser verification covered
a failed run with its failed Job selected and the terminal visible on arrival.
Copying stored output matched the API response exactly.

## Verification

- `npm run build`: all four exhibits passed type checking and production builds.
  Two and Four report the existing xterm-sized bundle warning above 500 kB.
- `node --test four/test/model.test.ts`: all five selector and status tests passed.
- A controlled HTTP/WebSocket fixture exercised active arrival, an active retry
  after an earlier failed attempt, explicit Job links, missing declarations,
  first Job arrival, and later failures without moving the selection.
- Separate Live and Stored terminals retained their scroll through source
  switches. A closing record, appended output, and disconnection retained the
  current source, received text, and reading position. Live copy matched all
  241 received lines, including the line appended after closure.
- Quiet connection retry recovered after two failed handshakes. An empty stored
  200 remained distinct from a missing artifact and opened no live connection.
  Recently completed arrival received the harvest grace; delayed stored output
  appeared without a manual retry. Unknown status words stayed visible.
- The real latest-100 runs table filtered locally without run-detail requests.
  Desktop and 390-pixel mobile runs and detail views had no page overflow. The
  dependency diagram scrolls inside its own panel.

Browser checks used Shotgun.

## October 6: resizable diagram pane

Studio-only follow-up to the promoted first cut: the DAG viewport now has a
native vertical resize grip and no fixed maximum height. Its compact initial
height remains at most 270px, with a 123px minimum. Mounting the pane starts at
the top; selecting a Job no longer recenters it vertically.

`npm run build:four` and `git diff --check` passed. In Shotgun, a fourteen-row
diagram opened at scrollTop 0 with its last Job explicitly selected. Browser
checks set the viewport to 520px and then 170px, confirmed native
`resize: vertical` with no maximum, and verified that polling and Job selection
preserved the adjusted height and vertical scroll. Returning from the runs list
opened the diagram at the top again. The change remains in the local preview
for review, without a deployment.

## Contract limits retained

Recorded Running does not prove a Pod is still alive. Live output is the server's
line-by-line observation; reconnect starts a fresh observation because there is
no replay cursor. Output endpoints cannot select historical attempts, so repeat
attempts carry a short local qualification. Search covers the latest 100 runs
returned by the API. Four adds no resource telemetry or backend contract.
