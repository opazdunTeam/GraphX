-- +goose Up

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,
    message_type TEXT NOT NULL
        CHECK (length(btrim(message_type)) > 0),
    aggregate_type TEXT NOT NULL
        CHECK (length(btrim(aggregate_type)) > 0),
    aggregate_id UUID NOT NULL,
    payload JSONB NOT NULL
        CHECK (jsonb_typeof(payload) = 'object'),
    publish_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (publish_attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    last_error TEXT
        CHECK (last_error IS NULL OR length(btrim(last_error)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (published_at IS NULL OR published_at >= created_at)
);

CREATE INDEX idx_outbox_messages_pending
    ON outbox_messages (available_at, created_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_outbox_messages_aggregate
    ON outbox_messages (aggregate_type, aggregate_id);

CREATE TABLE consumed_messages (
    consumer_name TEXT NOT NULL
        CHECK (length(btrim(consumer_name)) > 0),
    message_id UUID NOT NULL,
    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_name, message_id)
);

-- +goose StatementBegin
CREATE FUNCTION prevent_consumed_message_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'consumed_messages is append-only';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER consumed_messages_append_only
BEFORE UPDATE OR DELETE ON consumed_messages
FOR EACH ROW EXECUTE FUNCTION prevent_consumed_message_mutation();

-- +goose StatementBegin
CREATE FUNCTION validate_outbox_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'outbox_messages cannot be deleted without a retention policy';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.message_type IS DISTINCT FROM OLD.message_type
        OR NEW.aggregate_type IS DISTINCT FROM OLD.aggregate_type
        OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id
        OR NEW.payload IS DISTINCT FROM OLD.payload
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'outbox business payload is immutable';
    END IF;
    IF NEW.publish_attempts < OLD.publish_attempts THEN
        RAISE EXCEPTION 'outbox publish_attempts cannot decrease';
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'published outbox message is immutable';
    END IF;
    IF OLD.published_at IS NULL AND NEW.published_at IS NOT NULL
        AND NEW.published_at < OLD.created_at THEN
        RAISE EXCEPTION 'outbox published_at cannot precede creation';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER outbox_messages_validate_mutation
BEFORE UPDATE OR DELETE ON outbox_messages
FOR EACH ROW EXECUTE FUNCTION validate_outbox_mutation();

CREATE TABLE source_jobs (
    id UUID PRIMARY KEY,
    investigation_id UUID NOT NULL
        REFERENCES investigations(id) ON DELETE RESTRICT,
    source_id UUID NOT NULL
        REFERENCES sources(id) ON DELETE RESTRICT,
    operation TEXT NOT NULL
        CHECK (length(btrim(operation)) > 0),
    purpose TEXT NOT NULL
        CHECK (purpose IN ('subject_discovery', 'enrichment', 'branch_expansion')),
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (
            'pending',
            'queued',
            'running',
            'retry_wait',
            'succeeded',
            'not_found',
            'action_required',
            'unavailable',
            'failed',
            'cancelled'
        )),
    attempt_no INTEGER NOT NULL DEFAULT 1
        CHECK (attempt_no >= 1),
    max_attempts INTEGER NOT NULL DEFAULT 3
        CHECK (max_attempts >= 1),
    next_attempt_at TIMESTAMPTZ,
    request_hash TEXT NOT NULL
        CHECK (length(btrim(request_hash)) > 0),
    idempotency_key TEXT NOT NULL UNIQUE
        CHECK (length(btrim(idempotency_key)) > 0),
    last_error_code TEXT
        CHECK (last_error_code IS NULL OR length(btrim(last_error_code)) > 0),
    last_error_message TEXT
        CHECK (last_error_message IS NULL OR length(btrim(last_error_message)) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    queued_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (attempt_no <= max_attempts),
    CHECK (
        (status = 'retry_wait' AND next_attempt_at IS NOT NULL)
        OR (status <> 'retry_wait' AND next_attempt_at IS NULL)
    ),
    CHECK (updated_at >= created_at),
    CHECK (queued_at IS NULL OR queued_at >= created_at),
    CHECK (started_at IS NULL OR started_at >= created_at),
    CHECK (finished_at IS NULL OR finished_at >= created_at)
);

CREATE INDEX idx_source_jobs_investigation
    ON source_jobs (investigation_id);

CREATE INDEX idx_source_jobs_source
    ON source_jobs (source_id);

CREATE INDEX idx_source_jobs_investigation_status
    ON source_jobs (investigation_id, status);

CREATE INDEX idx_source_jobs_investigation_source_status
    ON source_jobs (investigation_id, source_id, status);

CREATE INDEX idx_source_jobs_retry_wait
    ON source_jobs (next_attempt_at, created_at)
    WHERE status = 'retry_wait';

-- +goose StatementBegin
CREATE FUNCTION validate_source_job_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    transition_allowed BOOLEAN := FALSE;
    starts_new_attempt BOOLEAN := FALSE;
    investigation_status TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'pending' OR NEW.attempt_no <> 1 THEN
            RAISE EXCEPTION 'a source job must start pending at attempt 1';
        END IF;
        SELECT status INTO investigation_status
        FROM investigations
        WHERE id = NEW.investigation_id
        FOR UPDATE;
        IF investigation_status IS DISTINCT FROM 'running' THEN
            RAISE EXCEPTION 'source jobs may be created only for a running investigation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.investigation_id IS DISTINCT FROM OLD.investigation_id
        OR NEW.source_id IS DISTINCT FROM OLD.source_id
        OR NEW.operation IS DISTINCT FROM OLD.operation
        OR NEW.purpose IS DISTINCT FROM OLD.purpose
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.max_attempts IS DISTINCT FROM OLD.max_attempts
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'source job identity and retry policy are immutable';
    END IF;

    IF NEW.status = 'retry_wait' AND NEW.attempt_no >= NEW.max_attempts THEN
        RAISE EXCEPTION 'retry_wait requires another available attempt';
    END IF;

    IF NEW.status = OLD.status THEN
        transition_allowed := TRUE;
    ELSIF OLD.status = 'pending' THEN
        transition_allowed := NEW.status IN (
            'queued', 'running', 'retry_wait', 'succeeded', 'not_found',
            'action_required', 'unavailable', 'failed', 'cancelled'
        );
    ELSIF OLD.status = 'queued' THEN
        transition_allowed := NEW.status IN (
            'running', 'retry_wait', 'succeeded', 'not_found',
            'action_required', 'unavailable', 'failed', 'cancelled'
        );
    ELSIF OLD.status = 'running' THEN
        transition_allowed := NEW.status IN (
            'retry_wait', 'succeeded', 'not_found',
            'action_required', 'unavailable', 'failed', 'cancelled'
        );
    ELSIF OLD.status = 'retry_wait' THEN
        transition_allowed := NEW.status IN ('pending', 'cancelled');
        starts_new_attempt := NEW.status = 'pending';
    ELSIF OLD.status = 'action_required' THEN
        transition_allowed := NEW.status IN ('pending', 'failed', 'cancelled');
        starts_new_attempt := NEW.status = 'pending';
    END IF;

    IF NOT transition_allowed THEN
        RAISE EXCEPTION 'invalid source job status transition: % -> %', OLD.status, NEW.status;
    END IF;

    IF starts_new_attempt AND NEW.attempt_no <> OLD.attempt_no + 1 THEN
        RAISE EXCEPTION 'a new source job attempt must increment attempt_no by one';
    END IF;
    IF NOT starts_new_attempt AND NEW.attempt_no <> OLD.attempt_no THEN
        RAISE EXCEPTION 'attempt_no may change only when a new attempt starts';
    END IF;

    IF NEW.status IN ('pending', 'queued', 'running', 'retry_wait')
        AND NEW.status IS DISTINCT FROM OLD.status THEN
        SELECT status INTO investigation_status
        FROM investigations
        WHERE id = NEW.investigation_id
        FOR UPDATE;
        IF investigation_status IS DISTINCT FROM 'running' THEN
            RAISE EXCEPTION 'source job work cannot start while investigation status is %',
                investigation_status;
        END IF;
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER source_jobs_status_transition
BEFORE INSERT OR UPDATE ON source_jobs
FOR EACH ROW
EXECUTE FUNCTION validate_source_job_transition();

CREATE TABLE source_job_results (
    id UUID PRIMARY KEY,
    source_job_id UUID NOT NULL
        REFERENCES source_jobs(id) ON DELETE RESTRICT,
    message_id UUID NOT NULL UNIQUE,
    attempt_no INTEGER NOT NULL
        CHECK (attempt_no >= 1),
    result_digest TEXT NOT NULL
        CHECK (result_digest ~ '^[a-f0-9]{64}$'),
    applied BOOLEAN NOT NULL DEFAULT FALSE,
    ignored_reason TEXT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (source_job_id, attempt_no),
    CHECK (
        (applied AND ignored_reason IS NULL)
        OR (
            NOT applied
            AND ignored_reason IS NOT NULL
            AND length(btrim(ignored_reason)) > 0
        )
    )
);

-- +goose StatementBegin
CREATE FUNCTION validate_source_job_result_applicability()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    current_attempt INTEGER;
    job_status TEXT;
    investigation_status TEXT;
    job_investigation_id UUID;
BEGIN
    SELECT attempt_no, status, investigation_id
    INTO current_attempt, job_status, job_investigation_id
    FROM source_jobs
    WHERE id = NEW.source_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'source job % does not exist', NEW.source_job_id;
    END IF;

    SELECT status INTO investigation_status
    FROM investigations
    WHERE id = job_investigation_id
    FOR UPDATE;

    IF NEW.attempt_no > current_attempt THEN
        RAISE EXCEPTION 'result attempt % is ahead of current attempt %',
            NEW.attempt_no, current_attempt;
    END IF;

    IF NEW.applied AND NEW.attempt_no <> current_attempt THEN
        RAISE EXCEPTION 'only the current source job attempt may be applied';
    END IF;

    IF NEW.applied AND job_status NOT IN ('pending', 'queued', 'running') THEN
        RAISE EXCEPTION 'source job result cannot be applied while job status is %', job_status;
    END IF;

    IF NEW.applied AND investigation_status <> 'running' THEN
        RAISE EXCEPTION 'source job result cannot be applied while investigation status is %',
            investigation_status;
    END IF;

    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER source_job_results_validate_applicability
BEFORE INSERT ON source_job_results
FOR EACH ROW
EXECUTE FUNCTION validate_source_job_result_applicability();

-- +goose StatementBegin
CREATE FUNCTION prevent_source_job_result_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'source_job_results is append-only';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER source_job_results_append_only
BEFORE UPDATE OR DELETE ON source_job_results
FOR EACH ROW
EXECUTE FUNCTION prevent_source_job_result_mutation();

CREATE TABLE api_idempotency_keys (
    scope TEXT NOT NULL
        CHECK (length(btrim(scope)) > 0),
    idempotency_key TEXT NOT NULL
        CHECK (length(btrim(idempotency_key)) > 0),
    request_hash TEXT NOT NULL
        CHECK (length(btrim(request_hash)) > 0),
    response_status INTEGER
        CHECK (response_status IS NULL OR response_status BETWEEN 100 AND 599),
    response_body JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (scope, idempotency_key),
    CHECK (response_status IS NOT NULL OR response_body IS NULL),
    CHECK (expires_at > created_at)
);

-- +goose StatementBegin
CREATE FUNCTION validate_api_idempotency_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.expires_at > NOW() THEN
            RAISE EXCEPTION 'unexpired API idempotency key cannot be deleted';
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.scope IS DISTINCT FROM OLD.scope
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR NEW.expires_at IS DISTINCT FROM OLD.expires_at THEN
        RAISE EXCEPTION 'API idempotency identity is immutable';
    END IF;
    IF OLD.response_status IS NOT NULL
        AND (
            NEW.response_status IS DISTINCT FROM OLD.response_status
            OR NEW.response_body IS DISTINCT FROM OLD.response_body
        ) THEN
        RAISE EXCEPTION 'completed API idempotency response is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER api_idempotency_keys_validate_mutation
BEFORE UPDATE OR DELETE ON api_idempotency_keys
FOR EACH ROW EXECUTE FUNCTION validate_api_idempotency_mutation();

CREATE INDEX idx_api_idempotency_keys_expires_at
    ON api_idempotency_keys (expires_at);

-- +goose Down

DROP TABLE api_idempotency_keys;
DROP FUNCTION validate_api_idempotency_mutation();
DROP TABLE source_job_results;
DROP FUNCTION prevent_source_job_result_mutation();
DROP FUNCTION validate_source_job_result_applicability();
DROP TABLE source_jobs;
DROP FUNCTION validate_source_job_transition();
DROP TABLE consumed_messages;
DROP FUNCTION prevent_consumed_message_mutation();
DROP TABLE outbox_messages;
DROP FUNCTION validate_outbox_mutation();
