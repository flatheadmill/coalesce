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

The executor orchestrates but doesn't persist. The Go server has scaffolding — database connection, migrations, WebSocket stub — but needs endpoints wired. Priority: persistence first (SOC 2 evidence), failure propagation second, pre-flight hooks third.
