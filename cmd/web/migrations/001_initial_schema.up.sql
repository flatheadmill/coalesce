-- Runs table (one per pipeline execution)
CREATE TABLE IF NOT EXISTS runs (
    slug text PRIMARY KEY,
    pipeline text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    status text DEFAULT 'running' -- running, completed, failed, cancelled
);

-- Jobs table (individual jobs within a run)
CREATE TABLE IF NOT EXISTS jobs (
    slug text NOT NULL,
    job text NOT NULL,  -- The full path like 'coalesce.fanout.baz.frobinate.a'
    k8s_name text,      -- The generated k8s job name 'run-1758331248-32f64f69-cdl'
    started_at timestamptz DEFAULT now(),
    completed_at timestamptz,
    status text DEFAULT 'pending', -- pending, running, completed, failed
    exit_code integer,
    PRIMARY KEY (slug, job),
    FOREIGN KEY (slug) REFERENCES runs(slug) ON DELETE CASCADE
);

-- Logs table (blob storage for now, can migrate to line-by-line later)
CREATE TABLE IF NOT EXISTS logs (
    slug text NOT NULL,
    job text NOT NULL,
    content text,
    created_at timestamptz DEFAULT now(),
    PRIMARY KEY (slug, job),
    FOREIGN KEY (slug, job) REFERENCES jobs(slug, job) ON DELETE CASCADE
);

-- Artifacts for passing data between jobs
CREATE TABLE IF NOT EXISTS artifacts (
    slug text NOT NULL,
    job text NOT NULL,
    key text NOT NULL,
    value text,
    created_at timestamptz DEFAULT now(),
    PRIMARY KEY (slug, job, key),
    FOREIGN KEY (slug, job) REFERENCES jobs(slug, job) ON DELETE CASCADE
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(slug, status);
CREATE INDEX IF NOT EXISTS idx_jobs_k8s_name ON jobs(k8s_name);