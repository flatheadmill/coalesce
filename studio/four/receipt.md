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
COALESCE_DEV_UPSTREAM=https://coalesce.acreops.org npm run dev:four -- --host 127.0.0.1
```

- Runs: <http://localhost:5174/coalesce/runs>
- Failed specimen: <http://localhost:5174/coalesce/runs/itasca-build-9357-tmgc95>

This preview reads the existing API and streams. The failed specimen selected
`tests` and showed Failed, 1m 43s, with the terminal visible on arrival. Copying
stored output matched the API response exactly: 20,323 characters.

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

Browser checks used Shotgun. Local screenshot evidence is in
`/Users/alan/pane/coalesce/previews/2026-10-05-four/`.

## Contract limits retained

Recorded Running does not prove a Pod is still alive. Live output is the server's
line-by-line observation; reconnect starts a fresh observation because there is
no replay cursor. Output endpoints cannot select historical attempts, so repeat
attempts carry a short local qualification. Search covers the latest 100 runs
returned by the API. Four adds no resource telemetry or backend contract.
