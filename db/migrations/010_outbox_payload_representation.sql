-- +goose Up

-- +goose StatementBegin
CREATE FUNCTION prevent_outbox_payload_representation_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'outbox payload representation is immutable';
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER outbox_payload_representation_immutable
BEFORE UPDATE OF payload ON outbox_messages
FOR EACH ROW
WHEN ((OLD.payload::TEXT COLLATE "C") IS DISTINCT FROM (NEW.payload::TEXT COLLATE "C"))
EXECUTE FUNCTION prevent_outbox_payload_representation_change();

-- +goose Down

DROP TRIGGER outbox_payload_representation_immutable ON outbox_messages;
DROP FUNCTION prevent_outbox_payload_representation_change();
