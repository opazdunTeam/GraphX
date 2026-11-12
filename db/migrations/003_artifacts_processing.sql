-- +goose Up

CREATE TABLE artifacts (
    id UUID PRIMARY KEY,

    source_id UUID NOT NULL
        REFERENCES sources(id),

    kind TEXT NOT NULL,
    media_type TEXT NOT NULL,

    object_key TEXT NOT NULL,
    source_uri TEXT,

    sha256 TEXT NOT NULL,
    byte_size BIGINT NOT NULL
        CHECK (byte_size >= 0),

    published_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ,

    http_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    adapter_version TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_artifacts_source
    ON artifacts (source_id);

CREATE INDEX idx_artifacts_sha256
    ON artifacts (sha256);

CREATE TABLE source_job_result_artifacts (
    source_job_result_id UUID NOT NULL
        REFERENCES source_job_results(id),

    artifact_id UUID NOT NULL
        REFERENCES artifacts(id),

    artifact_local_ref TEXT NOT NULL,

    role TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_job_result_id, artifact_id),

    CONSTRAINT source_job_result_artifacts_local_ref_unique
        UNIQUE (source_job_result_id, artifact_local_ref)
);

CREATE INDEX idx_source_job_result_artifacts_artifact
    ON source_job_result_artifacts (artifact_id);

CREATE TABLE processing_runs (
    id UUID PRIMARY KEY,

    artifact_id UUID NOT NULL
        REFERENCES artifacts(id),

    source_job_result_id UUID NOT NULL
        REFERENCES source_job_results(id),

    supersedes_run_id UUID
        REFERENCES processing_runs(id),

    processing_scope TEXT NOT NULL,
    status TEXT NOT NULL,

    schema_version TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,

    active BOOLEAN NOT NULL DEFAULT FALSE,

    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_processing_runs_artifact
    ON processing_runs (artifact_id);

CREATE INDEX idx_processing_runs_result
    ON processing_runs (source_job_result_id);

CREATE INDEX idx_processing_runs_active
    ON processing_runs (artifact_id)
    WHERE active = TRUE;

CREATE TABLE source_records (
    id UUID PRIMARY KEY,

    processing_run_id UUID NOT NULL
        REFERENCES processing_runs(id),

    local_ref TEXT NOT NULL,
    external_id TEXT,

    record_type TEXT NOT NULL,
    schema_version TEXT NOT NULL,

    raw_data JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (processing_run_id, local_ref)
);

CREATE INDEX idx_source_records_external_id
    ON source_records (external_id);

-- +goose Down

DROP TABLE IF EXISTS source_records;
DROP TABLE IF EXISTS processing_runs;
DROP TABLE IF EXISTS source_job_result_artifacts;
DROP TABLE IF EXISTS artifacts;