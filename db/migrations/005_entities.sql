-- +goose Up

CREATE TABLE entities (
    id UUID PRIMARY KEY,
    entity_type TEXT NOT NULL
        CHECK (entity_type IN ('person', 'organization')),
    display_name TEXT NOT NULL
        CHECK (length(btrim(display_name)) > 0),
    status TEXT NOT NULL DEFAULT 'active'
        CHECK (status = 'active'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (updated_at >= created_at)
);

CREATE INDEX idx_entities_type
    ON entities (entity_type);

CREATE TABLE entity_names (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL
        REFERENCES entities(id) ON DELETE RESTRICT,
    claim_id UUID NOT NULL
        REFERENCES claims(id) ON DELETE RESTRICT,
    raw_name TEXT NOT NULL
        CHECK (length(btrim(raw_name)) > 0),
    normalized_name TEXT NOT NULL
        CHECK (length(btrim(normalized_name)) > 0),
    language TEXT
        CHECK (language IS NULL OR length(btrim(language)) > 0),
    script TEXT
        CHECK (script IS NULL OR length(btrim(script)) > 0),
    variant_type TEXT
        CHECK (variant_type IS NULL OR length(btrim(variant_type)) > 0),
    normalization_version TEXT NOT NULL
        CHECK (length(btrim(normalization_version)) > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entity_names_entity
    ON entity_names (entity_id);

CREATE INDEX idx_entity_names_claim
    ON entity_names (claim_id);

CREATE UNIQUE INDEX idx_entity_names_active_claim
    ON entity_names (claim_id)
    WHERE active;

CREATE INDEX idx_entity_names_active_normalized
    ON entity_names (normalized_name)
    WHERE active;

CREATE TABLE entity_identifiers (
    id UUID PRIMARY KEY,
    entity_id UUID NOT NULL
        REFERENCES entities(id) ON DELETE RESTRICT,
    claim_id UUID NOT NULL
        REFERENCES claims(id) ON DELETE RESTRICT,
    identifier_type TEXT NOT NULL
        CHECK (length(btrim(identifier_type)) > 0),
    raw_value TEXT NOT NULL
        CHECK (length(btrim(raw_value)) > 0),
    normalized_value TEXT NOT NULL
        CHECK (length(btrim(normalized_value)) > 0),
    jurisdiction TEXT
        CHECK (jurisdiction IS NULL OR length(btrim(jurisdiction)) > 0),
    issuer TEXT
        CHECK (issuer IS NULL OR length(btrim(issuer)) > 0),
    normalization_version TEXT NOT NULL
        CHECK (length(btrim(normalization_version)) > 0),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entity_identifiers_entity
    ON entity_identifiers (entity_id);

CREATE INDEX idx_entity_identifiers_claim
    ON entity_identifiers (claim_id);

CREATE UNIQUE INDEX idx_entity_identifiers_active_claim
    ON entity_identifiers (claim_id)
    WHERE active;

CREATE INDEX idx_entity_identifiers_active_value
    ON entity_identifiers (identifier_type, normalized_value)
    WHERE active;

-- +goose StatementBegin
CREATE FUNCTION validate_entity_projection_history()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    old_row JSONB := to_jsonb(OLD);
    new_row JSONB := to_jsonb(NEW);
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NOT NEW.active THEN
            RAISE EXCEPTION '% must be inserted active', TG_TABLE_NAME;
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION '% history cannot be deleted', TG_TABLE_NAME;
    END IF;
    IF NOT OLD.active
        OR NEW.active
        OR NEW.id IS DISTINCT FROM OLD.id
        OR NEW.entity_id IS DISTINCT FROM OLD.entity_id
        OR NEW.claim_id IS DISTINCT FROM OLD.claim_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION '% may only transition once from active to inactive', TG_TABLE_NAME;
    END IF;

    IF TG_TABLE_NAME = 'entity_names'
        AND (
            new_row -> 'raw_name' IS DISTINCT FROM old_row -> 'raw_name'
            OR new_row -> 'normalized_name' IS DISTINCT FROM old_row -> 'normalized_name'
            OR new_row -> 'language' IS DISTINCT FROM old_row -> 'language'
            OR new_row -> 'script' IS DISTINCT FROM old_row -> 'script'
            OR new_row -> 'variant_type' IS DISTINCT FROM old_row -> 'variant_type'
            OR new_row -> 'normalization_version' IS DISTINCT FROM old_row -> 'normalization_version'
        ) THEN
        RAISE EXCEPTION 'entity name value is immutable';
    END IF;
    IF TG_TABLE_NAME = 'entity_identifiers'
        AND (
            new_row -> 'identifier_type' IS DISTINCT FROM old_row -> 'identifier_type'
            OR new_row -> 'raw_value' IS DISTINCT FROM old_row -> 'raw_value'
            OR new_row -> 'normalized_value' IS DISTINCT FROM old_row -> 'normalized_value'
            OR new_row -> 'jurisdiction' IS DISTINCT FROM old_row -> 'jurisdiction'
            OR new_row -> 'issuer' IS DISTINCT FROM old_row -> 'issuer'
            OR new_row -> 'normalization_version' IS DISTINCT FROM old_row -> 'normalization_version'
        ) THEN
        RAISE EXCEPTION 'entity identifier value is immutable';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER entity_names_validate_history
BEFORE INSERT OR UPDATE OR DELETE ON entity_names
FOR EACH ROW EXECUTE FUNCTION validate_entity_projection_history();

CREATE TRIGGER entity_identifiers_validate_history
BEFORE INSERT OR UPDATE OR DELETE ON entity_identifiers
FOR EACH ROW EXECUTE FUNCTION validate_entity_projection_history();

-- +goose Down

DROP TABLE entity_identifiers;
DROP TABLE entity_names;
DROP FUNCTION validate_entity_projection_history();
DROP TABLE entities;
