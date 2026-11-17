-- +goose Up

CREATE TABLE schema_extension_ownership (
    extension_name TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- +goose StatementBegin
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        CREATE EXTENSION pg_trgm;
        INSERT INTO schema_extension_ownership (extension_name) VALUES ('pg_trgm');
    END IF;
END;
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION prevent_schema_extension_ownership_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'schema extension ownership is managed only by migrations';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER schema_extension_ownership_immutable
BEFORE INSERT OR UPDATE OR DELETE ON schema_extension_ownership
FOR EACH ROW
EXECUTE FUNCTION prevent_schema_extension_ownership_mutation();

CREATE TABLE investigations (
    id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL
        CHECK (subject_type IN ('person', 'organization')),
    subject_query JSONB NOT NULL
        CHECK (jsonb_typeof(subject_query) = 'object' AND subject_query <> '{}'::jsonb),
    status TEXT NOT NULL
        CHECK (status IN (
            'created',
            'running',
            'completed',
            'completed_partial',
            'failed',
            'cancelled'
        )),
    stage TEXT NOT NULL
        CHECK (stage IN (
            'subject_discovery',
            'subject_resolution',
            'enrichment',
            'analysis',
            'completed'
        )),
    limits JSONB NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(limits) = 'object'),
    pending_review_count INTEGER NOT NULL DEFAULT 0
        CHECK (pending_review_count >= 0),
    state_version BIGINT NOT NULL DEFAULT 0
        CHECK (state_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CHECK (updated_at >= created_at),
    CHECK (completed_at IS NULL OR completed_at >= created_at)
);

-- +goose StatementBegin
CREATE FUNCTION validate_investigation_initial_state()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status <> 'created' THEN
        RAISE EXCEPTION 'investigation must be created with status created';
    END IF;
    IF NEW.stage <> 'subject_discovery' THEN
        RAISE EXCEPTION 'investigation must start at subject_discovery stage';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER investigations_initial_state
BEFORE INSERT ON investigations
FOR EACH ROW
EXECUTE FUNCTION validate_investigation_initial_state();

CREATE TABLE sources (
    id UUID PRIMARY KEY,
    code TEXT NOT NULL UNIQUE
        CHECK (length(btrim(code)) > 0 AND code = lower(code)),
    name TEXT NOT NULL
        CHECK (length(btrim(name)) > 0),
    mode TEXT NOT NULL
        CHECK (mode IN ('api', 'bulk', 'file', 'http', 'browser', 'manual')),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    launch_order INTEGER NOT NULL DEFAULT 0
        CHECK (launch_order >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (updated_at >= created_at)
);

-- +goose StatementBegin
CREATE FUNCTION validate_investigation_status_transition()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = OLD.status THEN
        RETURN NEW;
    END IF;

    IF (OLD.status = 'created' AND NEW.status IN ('running', 'cancelled', 'failed'))
        OR (
            OLD.status = 'running'
            AND NEW.status IN ('completed', 'completed_partial', 'failed', 'cancelled')
        ) THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'invalid investigation status transition: % -> %', OLD.status, NEW.status;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER investigations_status_transition
BEFORE UPDATE OF status ON investigations
FOR EACH ROW
EXECUTE FUNCTION validate_investigation_status_transition();

-- +goose StatementBegin
CREATE FUNCTION prevent_investigation_input_rewrite()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.subject_type IS DISTINCT FROM OLD.subject_type
        OR NEW.subject_query IS DISTINCT FROM OLD.subject_query
        OR NEW.limits IS DISTINCT FROM OLD.limits
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'investigation input is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER investigations_input_immutable
BEFORE UPDATE ON investigations
FOR EACH ROW
EXECUTE FUNCTION prevent_investigation_input_rewrite();

-- +goose Down

DROP TABLE sources;
DROP TABLE investigations;
DROP FUNCTION prevent_investigation_input_rewrite();
DROP FUNCTION validate_investigation_status_transition();
DROP FUNCTION validate_investigation_initial_state();

-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM schema_extension_ownership WHERE extension_name = 'pg_trgm'
    ) THEN
        DROP EXTENSION pg_trgm;
    END IF;
END;
$$;
-- +goose StatementEnd

DROP TABLE schema_extension_ownership;
DROP FUNCTION prevent_schema_extension_ownership_mutation();
