CREATE TYPE job_state AS ENUM (
    'pending',
    'processing',
    'succeeded',
    'failed'
);

CREATE TABLE jobs (
    id UUID PRIMARY KEY,
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    state job_state NOT NULL,
    attempts INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX jobs_available_idx
    ON jobs (state, available_at)
    WHERE state = 'pending';

CREATE INDEX jobs_lease_idx
    ON jobs (state, lease_expires_at)
    WHERE state = 'processing';