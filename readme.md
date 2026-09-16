Coalesce is a pipeline executor for Kubernetes. The Zsh executor orchestrates — it builds DAGs, creates Jobs, watches completion, propagates state. The Go server observes and records — metadata to PostgreSQL, logs to buckets, events to UI via WebSocket. The receiver verifies GitHub deliveries and dispatches them through pipeline ConfigMaps. The executor doesn't need the server to run a pipeline. The server doesn't orchestrate.

---

Local Development

HACKING is the doorway and the sole command authority — the full local
environment on OrbStack's single-node Kubernetes, from `orb start k8s`
through deploying, running pipelines, synthetic webhook deliveries, and
resetting. Its commands carry the kubectl context and environment guards on
purpose; on a machine whose default context is a real cluster, an unguarded
command is aimed at production. Copy commands from HACKING, not from memory.

The server image builds and embeds Two from `studio/two`. The same source
runs under Vite for local UI development; there is no separate production
copy. `studio/README.md` describes the exhibits and their separate container.

---

Layout

```
bin/coalesce                    Executor entry point (zshctl shebang)
share/coalesce/commands/        Executor commands (run/command.zsh is the event loop)
cmd/web/main.go                 Go server
cmd/web/migrations/             Database schema
cmd/receiver/                   Webhook receiver
studio/two/                     Product UI (Vite, React, TypeScript)
studio/                         UI package, shared transport, and exhibits
run.html                        Old UI prototype (served at /run.html)
test/                           Pipeline test definitions
manifests/local/                The local environment
Dockerfile                      Server image (builds and embeds the UI)
Dockerfile.receiver             Receiver image
Dockerfile.dispatch             Local dispatch image (zsh, tini, zshctl)
```
