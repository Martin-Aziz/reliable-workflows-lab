-- Reliable Workflows Lab Phase 2. Apply to a dedicated application database.
CREATE TABLE IF NOT EXISTS workflow_jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    payload JSONB NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued','running','succeeded','dead')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL CHECK (max_attempts BETWEEN 1 AND 8),
    available_at TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ,
    lease_token TEXT,
    worker_id TEXT,
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT workflow_job_lease_consistency CHECK (
        (state = 'running' AND lease_until IS NOT NULL AND lease_token IS NOT NULL AND worker_id IS NOT NULL)
        OR (state <> 'running' AND lease_until IS NULL AND lease_token IS NULL AND worker_id IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS workflow_jobs_due_idx ON workflow_jobs (available_at, created_at, id) WHERE state = 'queued';
CREATE INDEX IF NOT EXISTS workflow_jobs_lease_idx ON workflow_jobs (lease_until, id) WHERE state = 'running';
CREATE INDEX IF NOT EXISTS workflow_jobs_created_idx ON workflow_jobs (created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS workflow_idempotency (
    key TEXT PRIMARY KEY,
    job_id TEXT NOT NULL UNIQUE REFERENCES workflow_jobs(id) ON DELETE CASCADE,
    fingerprint BYTEA NOT NULL CHECK (octet_length(fingerprint)=32)
);

-- Attempt records make crash and retry history inspectable, independent of
-- whether a job is currently queued, running, succeeded, or dead.
CREATE TABLE IF NOT EXISTS workflow_attempts (
    job_id TEXT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL CHECK (attempt BETWEEN 1 AND 8),
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome TEXT CHECK (outcome IN ('succeeded','failed','expired')),
    PRIMARY KEY (job_id, attempt)
);
CREATE INDEX IF NOT EXISTS workflow_attempts_started_idx ON workflow_attempts (started_at DESC);
