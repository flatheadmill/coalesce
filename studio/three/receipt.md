**Receipt**

Exhibit three is presently an independent, intentionally neutral scaffold. Its HTML identity, Vite output, React routes, asynchronous state, stream readers, semantic structure, and CSS all live within `three/`.

**Contract**

The run index names loading, empty, and failed requests before rendering live rows. Run detail loads the ledger and optional DAG response, updates after state events, and links each attempt to evidence. The log route follows unfinished work over `/tail` and reads completed work from harvested storage, retaining distinct absent and failed states.

**Verification**

`npm run build:three` passed its TypeScript check and Vite production build on August 28, 2026. The aggregate `npm run build` and the production Docker build repeated the same check successfully, producing a distinct exhibit-three HTML title and asset pair.

The Caddy image served exhibit three for Host `three.localhost`, while an unconfigured Host returned HTTP 404.
