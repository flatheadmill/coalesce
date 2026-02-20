Coalesce is a pipeline executor for Kubernetes. The Zsh executor orchestrates — it builds DAGs, creates Jobs, watches completion, propagates state. The Go server observes and records — metadata to PostgreSQL, logs to buckets, events to UI via WebSocket. The executor doesn't need the server to run a pipeline. The server doesn't orchestrate.

---

Local Development

OrbStack provides the local environment. `orb start k8s` brings up a single-node cluster. The executor runs against it without modification.

PostgreSQL runs as a container:

```
docker run -d --name coalesce-pg \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -e POSTGRES_DB=coalesce \
    -p 5432:5432 \
    postgres:17
```

Stop and start with `docker stop coalesce-pg` / `docker start coalesce-pg`. Data persists across restarts.

The Go server defaults match this container: localhost:5432, user postgres, password postgres, database coalesce, SSL disabled.

The Go server runs inside the OrbStack Kubernetes cluster. See HACKING for the full development setup.

---

Layout

```
bin/coalesce                    Executor entry point (zshctl shebang)
share/coalesce/commands/        Executor commands (run/command.zsh is the event loop)
cmd/web/main.go                 Go server
cmd/web/migrations/             Database schema
run.html                        UI prototype (Cytoscape + xterm)
test/                           Pipeline test definitions
```

---

Running a Pipeline

```
./bin/coalesce -s test-one run test/one.coalesce.zsh
./bin/coalesce -s test-fanout run test/fanout.coalesce.zsh
```

The `-s` flag sets the slug and goes before the subcommand.

---

Current State

The executor orchestrates but doesn't persist. The Go server has endpoints wired for both executor (POST/PUT dags, runs, jobs, containers) and UI (GET runs, dags, logs; WebSocket events). Migrations run on startup. Stow handles log storage with local backend for development. Priority: wire the executor to curl the server, then failure propagation, then pre-flight hooks.
