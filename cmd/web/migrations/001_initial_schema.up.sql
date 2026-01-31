-- DAGs table (versioned pipeline structure)
CREATE TABLE IF NOT EXISTS dags (
    namespace text NOT NULL,
    slug text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    dag jsonb NOT NULL,
    PRIMARY KEY (namespace, slug, created_at)
);

-- Runs table (one per pipeline execution)
CREATE TABLE IF NOT EXISTS runs (
    namespace text NOT NULL,
    slug text NOT NULL,
    pipeline text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    status text DEFAULT 'running', -- running, completed, failed, cancelled
    PRIMARY KEY (namespace, slug)
);

-- Jobs table (individual jobs within a run)
CREATE TABLE IF NOT EXISTS jobs (
    namespace text NOT NULL,
    slug text NOT NULL,
    job text NOT NULL,  -- The full path like 'coalesce.fanout.baz.frobinate.a'
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    status text DEFAULT 'pending', -- pending, running, completed, failed
    exit_code integer,
    PRIMARY KEY (namespace, slug, job, started_at),
    FOREIGN KEY (namespace, slug) REFERENCES runs(namespace, slug) ON DELETE CASCADE
);

-- Containers table (logs per container within a job)
CREATE TABLE IF NOT EXISTS containers (
    namespace text NOT NULL,
    slug text NOT NULL,
    job text NOT NULL,
    started_at timestamptz NOT NULL,
    container text NOT NULL,
    log_path text,
    PRIMARY KEY (namespace, slug, job, started_at, container),
    FOREIGN KEY (namespace, slug, job, started_at) REFERENCES jobs(namespace, slug, job, started_at) ON DELETE CASCADE
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(namespace, status);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(namespace, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(namespace, slug, status);
