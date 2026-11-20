package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

type SourceCollectRequestedIdentity struct {
	MessageID string
	JobID     string
	AttemptNo int
}

type PreparedOutboxMessage struct {
	id          string
	messageType string
	aggregateID string
	attemptNo   int
	payloadJSON string
}

var _ orchestration.OutboxMessage = PreparedOutboxMessage{}

func (m PreparedOutboxMessage) OutboxID() string    { return m.id }
func (m PreparedOutboxMessage) MessageType() string { return m.messageType }
func (m PreparedOutboxMessage) AggregateID() string { return m.aggregateID }
func (m PreparedOutboxMessage) AttemptNo() int      { return m.attemptNo }
func (m PreparedOutboxMessage) JSONPayload() string { return m.payloadJSON }

func PrepareSourceCollectRequestedMessage(
	registry *SchemaRegistry,
	envelope Envelope[SourceCollectRequestedPayload],
	expected SourceCollectRequestedIdentity,
) (PreparedOutboxMessage, error) {
	if registry == nil {
		return PreparedOutboxMessage{}, errors.New("schema registry is required")
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return PreparedOutboxMessage{}, fmt.Errorf("serialize source collect request: %w", err)
	}
	validated, err := registry.ValidateMessage(raw)
	if err != nil {
		return PreparedOutboxMessage{}, fmt.Errorf("validate source collect request: %w", err)
	}
	decoded, err := Decode[SourceCollectRequestedPayload](validated, SourceCollectRequestedType)
	if err != nil {
		return PreparedOutboxMessage{}, fmt.Errorf("decode validated source collect request: %w", err)
	}
	if decoded.MessageID != expected.MessageID {
		return PreparedOutboxMessage{}, fmt.Errorf(
			"message_id %q does not match expected %q",
			decoded.MessageID, expected.MessageID,
		)
	}
	if decoded.Payload.JobID != expected.JobID {
		return PreparedOutboxMessage{}, fmt.Errorf(
			"payload job_id %q does not match expected %q",
			decoded.Payload.JobID, expected.JobID,
		)
	}
	if decoded.Payload.AttemptNo != expected.AttemptNo {
		return PreparedOutboxMessage{}, fmt.Errorf(
			"payload attempt_no %d does not match expected %d",
			decoded.Payload.AttemptNo, expected.AttemptNo,
		)
	}
	return PreparedOutboxMessage{
		id:          decoded.MessageID,
		messageType: validated.Type,
		aggregateID: decoded.Payload.JobID,
		attemptNo:   decoded.Payload.AttemptNo,
		payloadJSON: string(validated.Raw),
	}, nil
}
