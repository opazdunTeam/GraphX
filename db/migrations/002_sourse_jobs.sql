-- +goose Up

CREATE TABLE source_jobs (
    id UUID PRIMARY KEY,

    investigation_id UUID NOT NULL
        REFERENCES investigations(id),

    source_id UUID NOT NULL
        REFERENCES sources(id),

    operation TEXT NOT NULL,
    purpose TEXT NOT NULL,
    status TEXT NOT NULL,

    attempt_no INTEGER NOT NULL DEFAULT 0
        CHECK (attempt_no >= 0),

    max_attempts INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts > 0),

    next_attempt_at TIMESTAMPTZ,

    request_hash TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,

    last_error_code TEXT,
    last_error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT source_jobs_attempt_check
        CHECK (attempt_no <= max_attempts)
);

CREATE INDEX idx_source_jobs_investigation
    ON source_jobs (investigation_id);

CREATE INDEX idx_source_jobs_source
    ON source_jobs (source_id);

CREATE INDEX idx_source_jobs_pending
    ON source_jobs (next_attempt_at, created_at)
    WHERE status IN ('queued', 'retry');

CREATE TABLE source_job_results (
    id UUID PRIMARY KEY,

    source_job_id UUID NOT NULL
        REFERENCES source_jobs(id),

    message_id UUID NOT NULL UNIQUE,

    attempt_no INTEGER NOT NULL
        CHECK (attempt_no > 0),

    result_digest TEXT NOT NULL,
    applied BOOLEAN NOT NULL DEFAULT FALSE,
    ignored_reason TEXT,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT source_job_results_attempt_unique
        UNIQUE (source_job_id, attempt_no)
);

CREATE INDEX idx_source_job_results_job
    ON source_job_results (source_job_id);

-- +goose Down

DROP TABLE IF EXISTS source_job_results;
DROP TABLE IF EXISTS source_jobs;