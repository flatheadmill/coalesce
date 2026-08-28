**Frozen Brief**

Frozen on August 28, 2026, for the first Coalesce design-studio scaffold. Amendments belong in a later brief; this file preserves the specimen all three exhibits received.

**Subject**

Coalesce runs pipelines on Kubernetes and records their evidence. The interface gives an operator a run table, a run's jobs, stored logs, and live state while work is still moving. The server also records the current DAG for a run, but this brief does not choose how that structure should be visualized.

**Audience**

The reader already operates Kubernetes and wants proof that work ran, a quick account of what is running now, and a direct path to the evidence when something failed. Coalesce should not put a new platform between that reader and the Jobs, Pods, timestamps, exit codes, and logs they already understand.

**Fixed Specimen**

All exhibits use the same Coalesce HTTP and WebSocket contract. They support runs at `/:namespace/runs`, a run at `/:namespace/runs/:slug`, and a job log at `/:namespace/runs/:slug/logs/:job`. The default namespace is `coalesce`.

The backend can return no runs as JSON `null`; the transport normalizes that response to an empty list. A missing run, DAG, or harvested log is distinct from a network failure or a response with the wrong shape. Live events are advisory and a subsequent HTTP read remains the record.

**Studio Discipline**

Exhibits one, two, and three share API types, transport, and representative fixtures only. They do not share CSS, components, tokens, shells, or aesthetic decisions. Each method must be able to succeed or fail on its own terms.

The initial apps stay neutral and minimal. They prove the routes, backend contact, WebSocket paths, independent build identity, and loading, empty, and error states. They are not the actual design cuts.

**Delivery**

Local Vite servers proxy `/api`, `/events`, and `/tail` to the Coalesce service. A small production container carries all three static builds, selects one by Host, provides SPA fallback, and proxies those same backend paths to a configurable upstream. The existing product UI and its deployment remain outside this work.

**Finish**

A finished cut gets a receipt and then stays intact. Comparison is useful only while each exhibit remains the result of its own method rather than a collection of parts borrowed from its siblings.
