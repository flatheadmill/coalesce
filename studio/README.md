**Coalesce Design Studio**

The studio holds three presentations of one Coalesce specimen. The backend and its data contract stay fixed while exhibits `one`, `two`, and `three` are allowed to reach independent conclusions about the interface.

This is scaffolding, not three finished designs. Each app deliberately proves only the seam: the runs, run, and log routes; live reads from the current API; run events and log tails; DAG response recognition without a chosen visualization; and honest loading, empty, and error states. Representative fixtures live beside the transport for later visual work, but the apps do not silently fall back to them when the backend fails.

The only shared browser code is `shared/api.ts` and `shared/fixtures.ts`. An exhibit does not import another exhibit, and the studio has no shared components, CSS, tokens, layout shell, or aesthetic vocabulary. Repetition across the three apps is the cost of preserving their freedom to diverge.

**Working With An Exhibit**

Install once in `studio/` with Node 22.12 or newer, then choose an exhibit. Each Vite server proxies `/api`, `/events`, and `/tail` to `http://coalesce.coalesce.svc.cluster.local` by default. Set `COALESCE_DEV_UPSTREAM` to use another backend.

```bash
npm install
npm run dev:one
npm run dev:two
npm run dev:three
```

The development ports are 5171, 5172, and 5173. Every app starts at `/coalesce/runs`; the namespace remains part of the browser route and can be changed directly in the URL.

**Building**

Each build has its own type check and output identity under `dist/`. The aggregate command runs all three without rebuilding the existing product UI.

```bash
npm run build:one
npm run build:two
npm run build:three
npm run build
```

**Production Container**

The studio container is separate from the repository's Coalesce server image and does not replace the product UI. Build it from the repository root so the Dockerfile receives the `studio/` directory as its context.

```bash
docker build -f studio/Dockerfile -t coalesce-studio .
docker run --rm -p 8080:80 \
  -e COALESCE_UPSTREAM=http://coalesce.coalesce.svc.cluster.local \
  -e COALESCE_ONE_HOST=one.localhost \
  -e COALESCE_TWO_HOST=two.localhost \
  -e COALESCE_THREE_HOST=three.localhost \
  coalesce-studio
```

Caddy chooses a static build from the request Host, falls back to that build's `index.html` for browser routes, and reverse-proxies the three backend path families before static routing. Caddy carries WebSocket upgrades for `/events` and `/tail` without a separate rule.

**Finish And Preserve Cuts**

An exhibit begins from the frozen brief and its own `method.md`. Once a cut is judged finished, record the result in `receipt.md` and preserve it as a complete artifact. Do not improve one cut by importing the successful parts of another; that would turn three independent methods into one design system wearing three themes. A later experiment gets a new exhibit or a deliberate reset whose loss is recorded.

The frozen brief is the common starting line, not a component specification. Shared transport may follow backend changes because there is only one Coalesce contract. Presentation changes remain local to the exhibit that earned them.
