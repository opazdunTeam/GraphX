package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
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
	operation   string
	attemptNo   int
	payloadJSON string
}

var _ orchestration.OutboxMessage = PreparedOutboxMessage{}

func (m PreparedOutboxMessage) OutboxID() string    { return m.id }
func (m PreparedOutboxMessage) MessageType() string { return m.messageType }
func (m PreparedOutboxMessage) AggregateID() string { return m.aggregateID }
func (m PreparedOutboxMessage) Operation() string   { return m.operation }
func (m PreparedOutboxMessage) AttemptNo() int      { return m.attemptNo }
func (m PreparedOutboxMessage) JSONPayload() string { return m.payloadJSON }

type SourceCollectOutboxFactory struct {
	registry *SchemaRegistry
}

var _ investigations.OutboxFactory = (*SourceCollectOutboxFactory)(nil)
var _ investigations.OutboxMessage = PreparedOutboxMessage{}

func NewSourceCollectOutboxFactory(registry *SchemaRegistry) (*SourceCollectOutboxFactory, error) {
	if registry == nil {
		return nil, errors.New("RabbitMQ schema registry is required")
	}
	return &SourceCollectOutboxFactory{registry: registry}, nil
}

func (f *SourceCollectOutboxFactory) PrepareSourceCollect(
	investigationID, jobID, outboxID, sourceCode string,
	command investigations.CreateCommand,
	now time.Time,
) (investigations.OutboxMessage, error) {
	query := SourceQuery{
		Type:        command.Subject.Type,
		Identifiers: command.Subject.Identifiers,
	}
	if command.Subject.Name != nil {
		query.Name = command.Subject.Name
	}
	envelope := Envelope[SourceCollectRequestedPayload]{
		MessageID:     outboxID,
		Type:          SourceCollectRequestedType,
		SchemaVersion: 1,
		OccurredAt:    now,
		CorrelationID: investigationID,
		CausationID:   nil,
		Producer:      "graphx-api",
		RootCommandID: outboxID,
		Payload: SourceCollectRequestedPayload{
			JobID:           jobID,
			AttemptNo:       1,
			InvestigationID: investigationID,
			Source:          sourceCode,
			Operation:       SourceSearchSubjectOperation,
			Purpose:         "subject_discovery",
			Query:           query,
			Limits: CollectLimits{
				MaxRecords:     command.Limits.EntityLimit,
				MaxBytes:       1 << 20,
				TimeoutSeconds: 30,
			},
			Deadline: now.Add(30 * time.Second),
		},
	}
	return PrepareSourceCollectRequestedMessage(
		f.registry,
		envelope,
		SourceCollectRequestedIdentity{
			MessageID: outboxID,
			JobID:     jobID,
			AttemptNo: 1,
		},
	)
}

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
		operation:   decoded.Payload.Operation,
		attemptNo:   decoded.Payload.AttemptNo,
		payloadJSON: string(validated.Raw),
	}, nil
}
