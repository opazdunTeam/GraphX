-- +goose Up

CREATE TABLE claims (
    id UUID PRIMARY KEY,

    subject_source_record_id UUID NOT NULL
        REFERENCES source_records(id),

    object_source_record_id UUID
        REFERENCES source_records(id),

    kind TEXT NOT NULL,
    predicate TEXT NOT NULL,

    raw_value JSONB NOT NULL,
    normalized_value JSONB,

    qualifiers JSONB NOT NULL DEFAULT '{}'::jsonb,

    valid_from DATE,
    valid_to DATE,

    valid_from_precision TEXT,
    valid_to_precision TEXT,
    temporal_basis TEXT,

    snapshot_at TIMESTAMPTZ,

    evidence_locator JSONB,
    evidence_excerpt TEXT,

    parser_version TEXT NOT NULL,
    mapping_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT claims_valid_period_check
        CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to)
);

CREATE INDEX idx_claims_subject_record
    ON claims (subject_source_record_id);

CREATE INDEX idx_claims_object_record
    ON claims (object_source_record_id);

CREATE INDEX idx_claims_predicate
    ON claims (predicate);

CREATE TABLE entity_names (
    id UUID PRIMARY KEY,

    entity_id UUID NOT NULL
        REFERENCES entities(id),

    claim_id UUID NOT NULL
        REFERENCES claims(id),

    raw_name TEXT NOT NULL,
    normalized_name TEXT NOT NULL,

    language TEXT,
    script TEXT,
    variant_type TEXT,

    normalization_version TEXT NOT NULL,

    active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entity_names_entity
    ON entity_names (entity_id);

CREATE INDEX idx_entity_names_normalized
    ON entity_names (normalized_name);

CREATE TABLE entity_identifiers (
    id UUID PRIMARY KEY,

    entity_id UUID NOT NULL
        REFERENCES entities(id),

    claim_id UUID NOT NULL
        REFERENCES claims(id),

    identifier_type TEXT NOT NULL,

    raw_value TEXT NOT NULL,
    normalized_value TEXT NOT NULL,

    jurisdiction TEXT,
    issuer TEXT,

    normalization_version TEXT NOT NULL,

    active BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entity_identifiers_entity
    ON entity_identifiers (entity_id);

CREATE INDEX idx_entity_identifiers_value
    ON entity_identifiers (identifier_type, normalized_value);

-- +goose Down

DROP TABLE IF EXISTS entity_identifiers;
DROP TABLE IF EXISTS entity_names;
DROP TABLE IF EXISTS claims;