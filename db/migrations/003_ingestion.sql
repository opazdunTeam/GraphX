-- +goose Up

CREATE TABLE artifacts (
    id UUID PRIMARY KEY,
    source_id UUID NOT NULL
        REFERENCES sources(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL
        CHECK (kind IN ('raw_response', 'document', 'manifest', 'screenshot', 'other')),
    media_type TEXT NOT NULL
        CHECK (length(btrim(media_type)) > 0),
    object_key TEXT NOT NULL UNIQUE
        CHECK (length(btrim(object_key)) > 0),
    source_uri TEXT
        CHECK (source_uri IS NULL OR length(btrim(source_uri)) > 0),
    sha256 TEXT NOT NULL
        CHECK (sha256 ~ '^[a-f0-9]{64}$'),
    byte_size BIGINT NOT NULL
        CHECK (byte_size >= 0),
    published_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ NOT NULL,
    http_metadata JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(http_metadata) = 'object'),
    adapter_version TEXT NOT NULL
        CHECK (length(btrim(adapter_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_artifacts_source
    ON artifacts (source_id);

CREATE INDEX idx_artifacts_sha256
    ON artifacts (sha256);

CREATE TABLE source_job_result_artifacts (
    source_job_result_id UUID NOT NULL
        REFERENCES source_job_results(id) ON DELETE RESTRICT,
    artifact_id UUID NOT NULL
        REFERENCES artifacts(id) ON DELETE RESTRICT,
    artifact_local_ref TEXT NOT NULL
        CHECK (length(btrim(artifact_local_ref)) > 0),
    role TEXT NOT NULL
        CHECK (role IN ('raw_response', 'document', 'manifest', 'screenshot', 'other')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (source_job_result_id, artifact_id),
    UNIQUE (source_job_result_id, artifact_local_ref)
);

CREATE INDEX idx_source_job_result_artifacts_artifact
    ON source_job_result_artifacts (artifact_id);

CREATE TABLE processing_runs (
    id UUID PRIMARY KEY,
    artifact_id UUID NOT NULL
        REFERENCES artifacts(id) ON DELETE RESTRICT,
    source_job_result_id UUID,
    supersedes_run_id UUID
        REFERENCES processing_runs(id) ON DELETE RESTRICT,
    processing_scope TEXT NOT NULL
        CHECK (length(btrim(processing_scope)) > 0),
    status TEXT NOT NULL
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    schema_version TEXT NOT NULL
        CHECK (length(btrim(schema_version)) > 0),
    parser_version TEXT NOT NULL
        CHECK (length(btrim(parser_version)) > 0),
    mapping_version TEXT NOT NULL
        CHECK (length(btrim(mapping_version)) > 0),
    normalization_version TEXT NOT NULL
        CHECK (length(btrim(normalization_version)) > 0),
    active BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    FOREIGN KEY (source_job_result_id, artifact_id)
        REFERENCES source_job_result_artifacts(source_job_result_id, artifact_id)
        ON DELETE RESTRICT,
    UNIQUE (supersedes_run_id),
    CHECK (supersedes_run_id IS NULL OR supersedes_run_id <> id),
    CHECK (processing_scope <> 'source_collection' OR source_job_result_id IS NOT NULL),
    CHECK (NOT active OR status = 'succeeded'),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (finished_at IS NULL OR finished_at >= COALESCE(started_at, created_at))
);

-- +goose StatementBegin
CREATE FUNCTION require_applied_processing_result()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.source_job_result_id IS NOT NULL
        AND EXISTS (
            SELECT 1
            FROM source_job_results
            WHERE id = NEW.source_job_result_id
              AND NOT applied
        ) THEN
        RAISE EXCEPTION 'processing runs require an applied source job result';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION validate_processing_run_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    superseded_artifact_id UUID;
    superseded_scope TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'processing_runs history cannot be deleted';
    END IF;

    IF TG_OP = 'UPDATE' THEN
        IF NEW.id IS DISTINCT FROM OLD.id
            OR NEW.artifact_id IS DISTINCT FROM OLD.artifact_id
            OR NEW.source_job_result_id IS DISTINCT FROM OLD.source_job_result_id
            OR NEW.supersedes_run_id IS DISTINCT FROM OLD.supersedes_run_id
            OR NEW.processing_scope IS DISTINCT FROM OLD.processing_scope
            OR NEW.schema_version IS DISTINCT FROM OLD.schema_version
            OR NEW.parser_version IS DISTINCT FROM OLD.parser_version
            OR NEW.mapping_version IS DISTINCT FROM OLD.mapping_version
            OR NEW.normalization_version IS DISTINCT FROM OLD.normalization_version
            OR NEW.started_at IS DISTINCT FROM OLD.started_at
            OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'processing run provenance and version fields are immutable';
        END IF;

        IF OLD.status IN ('succeeded', 'failed') AND NEW.status <> OLD.status THEN
            RAISE EXCEPTION 'terminal processing run status cannot change';
        END IF;
        IF OLD.finished_at IS NOT NULL AND NEW.finished_at IS DISTINCT FROM OLD.finished_at THEN
            RAISE EXCEPTION 'processing run completion time is immutable once set';
        END IF;
        IF OLD.status = 'pending' AND NEW.status NOT IN ('pending', 'running', 'succeeded', 'failed')
            OR OLD.status = 'running' AND NEW.status NOT IN ('running', 'succeeded', 'failed') THEN
            RAISE EXCEPTION 'invalid processing run status transition: % -> %', OLD.status, NEW.status;
        END IF;
        IF NOT OLD.active AND NEW.active THEN
            RAISE EXCEPTION 'inactive processing run cannot be reactivated';
        END IF;
    END IF;

    IF NEW.supersedes_run_id IS NOT NULL THEN
        SELECT artifact_id, processing_scope
        INTO superseded_artifact_id, superseded_scope
        FROM processing_runs
        WHERE id = NEW.supersedes_run_id;

        IF superseded_artifact_id IS DISTINCT FROM NEW.artifact_id
            OR superseded_scope IS DISTINCT FROM NEW.processing_scope THEN
            RAISE EXCEPTION 'superseding run must use the same artifact and processing scope';
        END IF;
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER processing_runs_validate_history
BEFORE INSERT OR UPDATE OR DELETE ON processing_runs
FOR EACH ROW
EXECUTE FUNCTION validate_processing_run_history();

CREATE TRIGGER processing_runs_require_applied_result
BEFORE INSERT OR UPDATE OF source_job_result_id ON processing_runs
FOR EACH ROW
EXECUTE FUNCTION require_applied_processing_result();

CREATE INDEX idx_processing_runs_artifact
    ON processing_runs (artifact_id);

CREATE INDEX idx_processing_runs_result
    ON processing_runs (source_job_result_id)
    WHERE source_job_result_id IS NOT NULL;

CREATE UNIQUE INDEX idx_processing_runs_active
    ON processing_runs (artifact_id, processing_scope)
    WHERE active;

CREATE TABLE source_records (
    id UUID PRIMARY KEY,
    processing_run_id UUID NOT NULL
        REFERENCES processing_runs(id) ON DELETE RESTRICT,
    local_ref TEXT NOT NULL
        CHECK (length(btrim(local_ref)) > 0),
    external_id TEXT
        CHECK (external_id IS NULL OR length(btrim(external_id)) > 0),
    record_type TEXT NOT NULL
        CHECK (length(btrim(record_type)) > 0),
    schema_version TEXT NOT NULL
        CHECK (length(btrim(schema_version)) > 0),
    raw_data JSONB NOT NULL
        CHECK (jsonb_typeof(raw_data) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (processing_run_id, local_ref)
);

CREATE INDEX idx_source_records_external_id
    ON source_records (record_type, external_id)
    WHERE external_id IS NOT NULL;

CREATE TABLE claims (
    id UUID PRIMARY KEY,
    subject_source_record_id UUID NOT NULL
        REFERENCES source_records(id) ON DELETE RESTRICT,
    object_source_record_id UUID
        REFERENCES source_records(id) ON DELETE RESTRICT,
    kind TEXT NOT NULL
        CHECK (kind IN ('attribute', 'relationship', 'event')),
    predicate TEXT NOT NULL
        CHECK (length(btrim(predicate)) > 0),
    raw_value JSONB NOT NULL,
    normalized_value JSONB,
    qualifiers JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(qualifiers) = 'object'),
    valid_from DATE,
    valid_to DATE,
    valid_from_precision TEXT NOT NULL
        CHECK (valid_from_precision IN ('day', 'month', 'year', 'approximate', 'unknown')),
    valid_to_precision TEXT NOT NULL
        CHECK (valid_to_precision IN ('day', 'month', 'year', 'approximate', 'unknown')),
    temporal_basis TEXT NOT NULL
        CHECK (temporal_basis IN ('explicit', 'event_derived', 'snapshot', 'computed', 'unknown')),
    snapshot_at TIMESTAMPTZ,
    evidence_locator JSONB NOT NULL
        CHECK (jsonb_typeof(evidence_locator) = 'object' AND evidence_locator <> '{}'::jsonb),
    evidence_excerpt TEXT,
    parser_version TEXT NOT NULL
        CHECK (length(btrim(parser_version)) > 0),
    mapping_version TEXT NOT NULL
        CHECK (length(btrim(mapping_version)) > 0),
    normalization_version TEXT NOT NULL
        CHECK (length(btrim(normalization_version)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (
        (kind = 'relationship' AND object_source_record_id IS NOT NULL)
        OR (kind = 'attribute' AND object_source_record_id IS NULL)
        OR (
            kind = 'event'
            AND (
                object_source_record_id IS NOT NULL
                OR raw_value NOT IN ('null'::jsonb, '""'::jsonb, '{}'::jsonb, '[]'::jsonb)
                OR (
                    normalized_value IS NOT NULL
                    AND normalized_value NOT IN ('null'::jsonb, '""'::jsonb, '{}'::jsonb, '[]'::jsonb)
                )
            )
        )
    ),
    CHECK (
        kind <> 'attribute'
        OR raw_value NOT IN ('null'::jsonb, '""'::jsonb, '{}'::jsonb, '[]'::jsonb)
        OR (
            normalized_value IS NOT NULL
            AND normalized_value NOT IN ('null'::jsonb, '""'::jsonb, '{}'::jsonb, '[]'::jsonb)
        )
    ),
    CHECK (
        (valid_from IS NULL AND valid_from_precision = 'unknown')
        OR (valid_from IS NOT NULL AND valid_from_precision <> 'unknown')
    ),
    CHECK (
        (valid_to IS NULL AND valid_to_precision = 'unknown')
        OR (valid_to IS NOT NULL AND valid_to_precision <> 'unknown')
    ),
    CHECK (valid_from_precision <> 'month' OR EXTRACT(DAY FROM valid_from) = 1),
    CHECK (
        valid_from_precision <> 'year'
        OR (EXTRACT(MONTH FROM valid_from) = 1 AND EXTRACT(DAY FROM valid_from) = 1)
    ),
    CHECK (valid_to_precision <> 'month' OR EXTRACT(DAY FROM valid_to) = 1),
    CHECK (
        valid_to_precision <> 'year'
        OR (EXTRACT(MONTH FROM valid_to) = 1 AND EXTRACT(DAY FROM valid_to) = 1)
    ),
    CHECK (
        valid_from_precision <> 'day'
        OR valid_to_precision <> 'day'
        OR valid_to > valid_from
    ),
    CHECK (
        (
            temporal_basis = 'snapshot'
            AND snapshot_at IS NOT NULL
            AND valid_from IS NULL
            AND valid_to IS NULL
            AND valid_from_precision = 'unknown'
            AND valid_to_precision = 'unknown'
        )
        OR (temporal_basis <> 'snapshot' AND snapshot_at IS NULL)
    ),
    CHECK (
        temporal_basis <> 'unknown'
        OR (
            valid_from IS NULL
            AND valid_to IS NULL
            AND snapshot_at IS NULL
        )
    )
);

-- +goose StatementBegin
CREATE FUNCTION prevent_ingestion_history_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER artifacts_append_only
BEFORE UPDATE OR DELETE ON artifacts
FOR EACH ROW EXECUTE FUNCTION prevent_ingestion_history_mutation();

CREATE TRIGGER source_job_result_artifacts_append_only
BEFORE UPDATE OR DELETE ON source_job_result_artifacts
FOR EACH ROW EXECUTE FUNCTION prevent_ingestion_history_mutation();

CREATE TRIGGER source_records_append_only
BEFORE UPDATE OR DELETE ON source_records
FOR EACH ROW EXECUTE FUNCTION prevent_ingestion_history_mutation();

CREATE TRIGGER claims_append_only
BEFORE UPDATE OR DELETE ON claims
FOR EACH ROW EXECUTE FUNCTION prevent_ingestion_history_mutation();

-- +goose StatementBegin
CREATE FUNCTION validate_result_artifact_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    artifact_source_id UUID;
    job_source_id UUID;
BEGIN
    SELECT source_id INTO artifact_source_id
    FROM artifacts
    WHERE id = NEW.artifact_id;

    SELECT job.source_id INTO job_source_id
    FROM source_job_results AS result
    JOIN source_jobs AS job ON job.id = result.source_job_id
    WHERE result.id = NEW.source_job_result_id;

    IF artifact_source_id IS DISTINCT FROM job_source_id THEN
        RAISE EXCEPTION 'artifact source must match source job source';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER source_job_result_artifacts_validate_source
BEFORE INSERT ON source_job_result_artifacts
FOR EACH ROW EXECUTE FUNCTION validate_result_artifact_source();

-- +goose StatementBegin
CREATE FUNCTION validate_claim_record_boundary()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    subject_result_id UUID;
    object_result_id UUID;
BEGIN
    IF NEW.object_source_record_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT run.source_job_result_id INTO subject_result_id
    FROM source_records AS record
    JOIN processing_runs AS run ON run.id = record.processing_run_id
    WHERE record.id = NEW.subject_source_record_id;

    SELECT run.source_job_result_id INTO object_result_id
    FROM source_records AS record
    JOIN processing_runs AS run ON run.id = record.processing_run_id
    WHERE record.id = NEW.object_source_record_id;

    IF subject_result_id IS DISTINCT FROM object_result_id THEN
        RAISE EXCEPTION 'claim subject and object must belong to the same source job result';
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER claims_validate_record_boundary
BEFORE INSERT ON claims
FOR EACH ROW EXECUTE FUNCTION validate_claim_record_boundary();

CREATE INDEX idx_claims_subject_record
    ON claims (subject_source_record_id);

CREATE INDEX idx_claims_object_record
    ON claims (object_source_record_id)
    WHERE object_source_record_id IS NOT NULL;

CREATE INDEX idx_claims_predicate
    ON claims (predicate);

-- +goose Down

DROP TABLE claims;
DROP FUNCTION validate_claim_record_boundary();
DROP TABLE source_records;
DROP TABLE processing_runs;
DROP FUNCTION validate_processing_run_history();
DROP FUNCTION require_applied_processing_result();
DROP TABLE source_job_result_artifacts;
DROP FUNCTION validate_result_artifact_source();
DROP TABLE artifacts;
DROP FUNCTION prevent_ingestion_history_mutation();
