-- +goose Up

ALTER TABLE artifacts
    ADD COLUMN original_filename TEXT
        CHECK (
            original_filename IS NULL OR (
                length(original_filename) BETWEEN 1 AND 255
                AND original_filename = btrim(original_filename, ' .')
                AND original_filename !~ '[/\\]'
                AND original_filename !~ '[[:cntrl:]]'
            )
        ),
    ADD COLUMN parent_artifact_id UUID
        REFERENCES artifacts(id) ON DELETE RESTRICT,
    ADD CONSTRAINT artifacts_parent_not_self
        CHECK (parent_artifact_id IS NULL OR parent_artifact_id <> id);

CREATE INDEX idx_artifacts_parent
    ON artifacts (parent_artifact_id)
    WHERE parent_artifact_id IS NOT NULL;

-- +goose StatementBegin
CREATE FUNCTION validate_artifact_parent_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    parent_source_id UUID;
BEGIN
    IF NEW.parent_artifact_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT source_id INTO parent_source_id
    FROM artifacts
    WHERE id = NEW.parent_artifact_id;

    IF parent_source_id IS NULL THEN
        RAISE EXCEPTION 'parent artifact does not exist';
    END IF;
    IF parent_source_id IS DISTINCT FROM NEW.source_id THEN
        RAISE EXCEPTION 'parent artifact source must match child artifact source';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER artifacts_validate_parent_source
BEFORE INSERT ON artifacts
FOR EACH ROW EXECUTE FUNCTION validate_artifact_parent_source();

-- +goose Down

DROP TRIGGER artifacts_validate_parent_source ON artifacts;
DROP FUNCTION validate_artifact_parent_source();
DROP INDEX idx_artifacts_parent;
ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_parent_not_self,
    DROP COLUMN parent_artifact_id,
    DROP COLUMN original_filename;
