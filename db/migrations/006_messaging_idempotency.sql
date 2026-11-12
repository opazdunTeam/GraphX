-- +goose Up

CREATE TABLE outbox_messages (
    id UUID PRIMARY KEY,

    message_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,

    payload JSONB NOT NULL,

    publish_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (publish_attempts >= 0),

    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,

    last_error TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_messages_pending
    ON outbox_messages (available_at, id)
    WHERE published_at IS NULL;

CREATE INDEX idx_outbox_messages_aggregate
    ON outbox_messages (aggregate_type, aggregate_id);

CREATE TABLE consumed_messages (
    consumer_name TEXT NOT NULL,
    message_id UUID NOT NULL,

    consumed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE api_idempotency_keys (
    scope TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,

    request_hash TEXT NOT NULL,

    response_status INTEGER,
    response_body JSONB,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (scope, idempotency_key),

    CHECK (
        response_status IS NULL
        OR response_status BETWEEN 100 AND 599
    ),

    CHECK (
        (response_status IS NULL AND response_body IS NULL)
        OR
        (response_status IS NOT NULL AND response_body IS NOT NULL)
    ),

    CHECK (expires_at > created_at)
);

CREATE INDEX idx_api_idempotency_keys_expires_at
    ON api_idempotency_keys (expires_at);

-- +goose Down

DROP TABLE IF EXISTS api_idempotency_keys;
DROP TABLE IF EXISTS consumed_messages;
DROP TABLE IF EXISTS outbox_messages;