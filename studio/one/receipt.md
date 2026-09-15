**Receipt**

Exhibit one is a finished, independent design cut for Coalesce run evidence. Its organizing claim is visible in the page rather than added as decoration: open runs behave as an instrument in a dark working field, while settled runs become a quieter paper ledger beneath it.

**Run Ledger**

The production specimen contained 100 runs: three open, one failed, and 96 completed. The exhibit hoists the three open records even though they arrived deep in the chronological response, names the current Job, keeps elapsed time moving, and reports how many attempts have settled. Search and outcome filters operate only on the 97-record archive, where failure receives the sole reserved color and every row retains its run, pipeline, start, duration, and outcome.

**Run Record**

Run detail is an evidence sheet rather than a dashboard drill-down. Full timestamps and duration establish identity and custody first; the stored DAG follows as a semantic, recursively nested declaration whose nodes show their matching latest Job state; immutable Job attempts remain separate rows with repeat number, duration, exit code, and a direct log address.

**Log Evidence**

The log route keeps the parent run, Job, status, start, duration, container, and custody visible above the largest uninterrupted surface in the exhibit. Completed Jobs read deposited text from Coalesce, running Jobs open the advisory tail, and both preserve long lines with local horizontal scrolling and a copy action. A current open production record had no reachable pod, so the rendered interface states that the run remains marked running while no pod accepted the connection instead of presenting a blank terminal as live.

**State Coverage**

Loading, empty namespace, no filter match, missing run, missing Job, absent DAG, pending harvest, malformed response, HTTP error, transport failure, and unknown route have distinct language and structure. The empty namespace and missing-run states were rendered against the live development proxy. An intentionally unreachable local upstream exposed a Vite proxy request that did not close, so the general transport-failure presentation was verified in implementation but not accepted as a rendered proof.

**Rendered Inspection**

Shotgun inspection covered the live upstream at a 1439 by 1037 desktop viewport and a 390 by 844 narrow viewport, including the ledger, failed and running run sheets, failed harvested log, stale running tail, empty namespace, and missing run. At narrow width, the document client and scroll widths both measured 375 pixels with no overflowing element; a 1,091-pixel log line remained contained by a 338-pixel scroll surface. Filter targets measured 63.6 pixels high, buttons measured at least 44 pixels, and breadcrumb and evidence links have a 44-pixel minimum target.

**Visual Measurements**

The primary ink on paper contrast is 13.62:1, muted text on paper is 5.08:1, failure on paper is 5.84:1, light text on the dark working field is 14.84:1, terminal text is 15.05:1, and terminal secondary text is 8.89:1. Status always retains text and shape, so color is not the sole carrier of outcome.

**Verification**

`npm run build:one` passed the exhibit's TypeScript check and Vite production build on August 29, 2026, producing a 0.52 kB HTML entry, 15.98 kB CSS asset, and 201.84 kB JavaScript asset before gzip. `git diff --check` passed, and the final path audit found changes only under `studio/one`.

**Limits**

The live production specimen exposed only one- and two-node DAGs, so nested tranche presentation is supported by the recursive renderer but was not judged against a live nested run. No current open specimen produced a successful live log line during inspection. The private upstream origin and production identifiers were supplied only at runtime and are not persisted in the exhibit.
