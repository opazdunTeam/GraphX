-- +goose Up

ALTER TABLE investigations
    ADD COLUMN raw_subject JSONB,
    ADD COLUMN normalized_subject JSONB;

UPDATE investigations
SET raw_subject = subject_query,
    normalized_subject = subject_query
WHERE raw_subject IS NULL
   OR normalized_subject IS NULL;

ALTER TABLE investigations
    ALTER COLUMN raw_subject SET NOT NULL,
    ALTER COLUMN normalized_subject SET NOT NULL,
    ADD CONSTRAINT investigations_raw_subject_object
        CHECK (jsonb_typeof(raw_subject) = 'object' AND raw_subject <> '{}'::jsonb),
    ADD CONSTRAINT investigations_normalized_subject_object
        CHECK (jsonb_typeof(normalized_subject) = 'object' AND normalized_subject <> '{}'::jsonb),
    ALTER COLUMN state_version SET DEFAULT 1;

UPDATE investigations
SET state_version = 1
WHERE state_version = 0;

-- +goose StatementBegin
CREATE FUNCTION populate_investigation_subject_forms()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.raw_subject IS NULL THEN
        NEW.raw_subject = NEW.subject_query;
    END IF;
    IF NEW.normalized_subject IS NULL THEN
        NEW.normalized_subject = NEW.subject_query;
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER investigations_subject_forms_from_legacy_input
BEFORE INSERT ON investigations
FOR EACH ROW EXECUTE FUNCTION populate_investigation_subject_forms();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION prevent_investigation_input_rewrite()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.subject_type IS DISTINCT FROM OLD.subject_type
        OR NEW.subject_query IS DISTINCT FROM OLD.subject_query
        OR NEW.raw_subject IS DISTINCT FROM OLD.raw_subject
        OR NEW.normalized_subject IS DISTINCT FROM OLD.normalized_subject
        OR NEW.limits IS DISTINCT FROM OLD.limits
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'investigation input is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

-- +goose Down

DROP TRIGGER investigations_subject_forms_from_legacy_input ON investigations;
DROP FUNCTION populate_investigation_subject_forms();

ALTER TABLE investigations
    ALTER COLUMN state_version SET DEFAULT 0,
    DROP CONSTRAINT investigations_normalized_subject_object,
    DROP CONSTRAINT investigations_raw_subject_object,
    DROP COLUMN normalized_subject,
    DROP COLUMN raw_subject;

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION prevent_investigation_input_rewrite()
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
