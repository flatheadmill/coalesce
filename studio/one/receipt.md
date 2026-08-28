**Receipt**

Exhibit one currently contains an intentionally neutral scaffold, not a finished design cut. It has an independent HTML entry, Vite build, React router, remote-state implementation, event handling, log tail, and stylesheet.

**Contract**

The runs route reads the live run collection and represents loading, failure, and no-run responses. The run route reads job attempts and the latest DAG response, reports DAG availability without selecting a visualization, and refreshes from run events. The log route chooses the latest attempt, follows a running container through `/tail`, and reads harvested text through `/api` after completion.

**Verification**

`npm run build:one` passed its TypeScript check and Vite production build on August 28, 2026. The aggregate `npm run build` and the production Docker build repeated the same check successfully, producing a distinct exhibit-one HTML title and asset pair.

The Caddy image served exhibit one for Host `one.localhost` during the in-container smoke test.
