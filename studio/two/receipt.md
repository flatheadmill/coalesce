**Receipt**

Exhibit two is an independent neutral scaffold rather than a finished cut. Its build identity, source tree, state machinery, semantic markup, live streams, and CSS are local to `two/`.

**Contract**

The collection route polls live runs and exposes loading, failure, and empty answers. The detail route refreshes on `/events`, reads job attempts, and reports the current DAG as recorded data without drawing it. The log route distinguishes a running tail from harvested evidence and gives each failure its own account.

**Verification**

`npm run build:two` passed its TypeScript check and Vite production build on August 28, 2026. The aggregate `npm run build` and the production Docker build repeated the same check successfully, producing a distinct exhibit-two HTML title and asset pair.

The Caddy image served exhibit two's `index.html` for a deep browser route under Host `two.localhost`, proving both Host selection and SPA fallback.
