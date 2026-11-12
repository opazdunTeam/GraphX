-- +goose Up

CREATE TABLE investigations (
    id UUID PRIMARY KEY,
    subject_type TEXT NOT NULL,
    subject_query JSONB NOT NULL,
    status TEXT NOT NULL,
    stage TEXT NOT NULL,
    limits JSONB NOT NULL DEFAULT '{}'::jsonb,
    pending_review_count INTEGER NOT NULL DEFAULT 0
        CHECK (pending_review_count >= 0),
    state_version BIGINT NOT NULL DEFAULT 0
        CHECK (state_version >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE sources (
    id UUID PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    mode TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    launch_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE entities (
    id UUID PRIMARY KEY,
    entity_type TEXT NOT NULL,
    display_name TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_entities_type
    ON entities (entity_type);

CREATE INDEX idx_entities_status
    ON entities (status);

-- +goose Down

DROP TABLE IF EXISTS entities;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS investigations;