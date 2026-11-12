-- +goose Up

CREATE TABLE match_reviews (
    id UUID PRIMARY KEY,

    source_record_id UUID NOT NULL
        REFERENCES source_records(id),

    status TEXT NOT NULL,

    candidate_set_version INTEGER NOT NULL DEFAULT 1,
    lock_version BIGINT NOT NULL DEFAULT 0,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,

    CHECK (candidate_set_version > 0),
    CHECK (lock_version >= 0)
);

CREATE INDEX idx_match_reviews_source_record
    ON match_reviews (source_record_id);

CREATE TABLE match_candidates (
    id UUID PRIMARY KEY,

    match_review_id UUID NOT NULL
        REFERENCES match_reviews(id),

    entity_id UUID NOT NULL
        REFERENCES entities(id),

    score NUMERIC NOT NULL
        CHECK (score >= 0 AND score <= 1),

    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    conflicts JSONB NOT NULL DEFAULT '[]'::jsonb,

    recommendation TEXT NOT NULL,
    algorithm_version TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (match_review_id, entity_id)
);

CREATE INDEX idx_match_candidates_review
    ON match_candidates (match_review_id);

CREATE INDEX idx_match_candidates_entity
    ON match_candidates (entity_id);

CREATE TABLE match_decisions (
    id UUID PRIMARY KEY,

    match_review_id UUID NOT NULL
        REFERENCES match_reviews(id),

    selected_candidate_id UUID
        REFERENCES match_candidates(id),

    supersedes_decision_id UUID
        REFERENCES match_decisions(id),

    decision TEXT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id TEXT,

    reason TEXT,
    algorithm_version TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_match_decisions_review
    ON match_decisions (match_review_id);

CREATE TABLE entity_records (
    id UUID PRIMARY KEY,

    entity_id UUID NOT NULL
        REFERENCES entities(id),

    source_record_id UUID NOT NULL
        REFERENCES source_records(id),

    match_decision_id UUID
        REFERENCES match_decisions(id),

    link_method TEXT NOT NULL,

    active BOOLEAN NOT NULL DEFAULT TRUE,

    linked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    unlinked_at TIMESTAMPTZ,

    UNIQUE (entity_id, source_record_id),

    CHECK (unlinked_at IS NULL OR unlinked_at >= linked_at)
);

CREATE INDEX idx_entity_records_entity
    ON entity_records (entity_id);

CREATE INDEX idx_entity_records_source_record
    ON entity_records (source_record_id);

CREATE INDEX idx_entity_records_match_decision
    ON entity_records (match_decision_id);

CREATE TABLE relationships (
    id UUID PRIMARY KEY,

    from_entity_id UUID NOT NULL
        REFERENCES entities(id),

    to_entity_id UUID NOT NULL
        REFERENCES entities(id),

    relationship_type TEXT NOT NULL,

    from_role TEXT,
    to_role TEXT,

    ownership_share NUMERIC
        CHECK (ownership_share >= 0 AND ownership_share <= 1),

    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,

    valid_from DATE,
    valid_to DATE,

    valid_from_precision TEXT,
    valid_to_precision TEXT,

    temporal_basis TEXT,
    snapshot_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CHECK (from_entity_id <> to_entity_id),
    CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to)
);

CREATE INDEX idx_relationships_from_entity
    ON relationships (from_entity_id);

CREATE INDEX idx_relationships_to_entity
    ON relationships (to_entity_id);

CREATE INDEX idx_relationships_type
    ON relationships (relationship_type);

CREATE TABLE relationship_claims (
    relationship_id UUID NOT NULL
        REFERENCES relationships(id),

    claim_id UUID NOT NULL
        REFERENCES claims(id),

    support_role TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (relationship_id, claim_id)
);

CREATE INDEX idx_relationship_claims_claim
    ON relationship_claims (claim_id);

CREATE TABLE investigation_entities (
    investigation_id UUID NOT NULL
        REFERENCES investigations(id),

    entity_id UUID NOT NULL
        REFERENCES entities(id),

    depth INTEGER NOT NULL DEFAULT 0
        CHECK (depth >= 0),

    inclusion_reason TEXT NOT NULL,

    added_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (investigation_id, entity_id)
);

CREATE INDEX idx_investigation_entities_entity
    ON investigation_entities (entity_id);

-- +goose Down

DROP TABLE IF EXISTS investigation_entities;
DROP TABLE IF EXISTS relationship_claims;
DROP TABLE IF EXISTS relationships;
DROP TABLE IF EXISTS entity_records;
DROP TABLE IF EXISTS match_decisions;
DROP TABLE IF EXISTS match_candidates;
DROP TABLE IF EXISTS match_reviews;