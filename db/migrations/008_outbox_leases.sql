-- +goose Up

ALTER TABLE outbox_messages
    ADD COLUMN lease_token UUID,
    ADD COLUMN lease_until TIMESTAMPTZ,
    ADD CONSTRAINT outbox_lease_fields_match
        CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    ADD CONSTRAINT outbox_published_has_no_lease
        CHECK (published_at IS NULL OR lease_token IS NULL);

CREATE INDEX idx_outbox_messages_claimable
    ON outbox_messages (available_at, lease_until, created_at)
    WHERE published_at IS NULL;

-- +goose Down

DROP INDEX idx_outbox_messages_claimable;
ALTER TABLE outbox_messages
    DROP CONSTRAINT outbox_published_has_no_lease,
    DROP CONSTRAINT outbox_lease_fields_match,
    DROP COLUMN lease_until,
    DROP COLUMN lease_token;