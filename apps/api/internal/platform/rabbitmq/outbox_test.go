package rabbitmq

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
)

func TestSourceCollectOutboxFactoryProducesValidatedCommand(t *testing.T) {
	registry, _ := sourceCollectRequestedFixture(t)
	factory, err := NewSourceCollectOutboxFactory(registry)
	if err != nil {
		t.Fatal(err)
	}
	name := "Ada Lovelace"
	command := investigations.CreateCommand{
		Subject: investigations.Subject{Type: "person", Name: &name},
		Limits:  investigations.Limits{EntityLimit: 50},
	}
	now := time.Date(2026, 11, 26, 6, 0, 0, 0, time.UTC)
	message, err := factory.PrepareSourceCollect(
		"10000000-0000-4000-8000-000000000091",
		"20000000-0000-4000-8000-000000000091",
		"30000000-0000-4000-8000-000000000091",
		"fixture",
		command,
		now,
	)
	if err != nil {
		t.Fatalf("PrepareSourceCollect() error = %v", err)
	}
	investigationID := "10000000-0000-4000-8000-000000000091"
	if message.AttemptNo() != 1 || message.InvestigationID() != investigationID ||
		message.AggregateID() != "20000000-0000-4000-8000-000000000091" ||
		message.Operation() != SourceSearchSubjectOperation ||
		message.OutboxID() != "30000000-0000-4000-8000-000000000091" {
		t.Fatalf("prepared message identity = %#v", message)
	}
	validated, err := registry.ValidateMessage([]byte(message.JSONPayload()))
	if err != nil {
		t.Fatalf("factory payload is not contract-valid: %v", err)
	}
	envelope, err := Decode[SourceCollectRequestedPayload](validated, SourceCollectRequestedType)
	if err != nil {
		t.Fatal(err)
	}
	payload := envelope.Payload
	if envelope.Type != SourceCollectRequestedType || envelope.SchemaVersion != 1 ||
		envelope.MessageID != message.OutboxID() || envelope.CorrelationID != investigationID ||
		envelope.RootCommandID != message.OutboxID() || !envelope.OccurredAt.Equal(now) ||
		payload.JobID != message.AggregateID() || payload.AttemptNo != 1 ||
		payload.InvestigationID != investigationID || payload.Source != "fixture" ||
		payload.Operation != SourceSearchSubjectOperation || payload.Purpose != "subject_discovery" ||
		payload.Query.Type != "person" || payload.Query.Name == nil || *payload.Query.Name != name ||
		payload.Limits != (CollectLimits{MaxRecords: 50, MaxBytes: 1 << 20, TimeoutSeconds: 30}) ||
		!payload.Deadline.Equal(now.Add(30*time.Second)) {
		t.Fatalf("unexpected command: %#v", envelope)
	}
}

func TestPrepareSourceCollectRequestedMessage(t *testing.T) {
	registry, envelope := sourceCollectRequestedFixture(t)
	expected := SourceCollectRequestedIdentity{
		MessageID:       envelope.MessageID,
		JobID:           envelope.Payload.JobID,
		InvestigationID: envelope.Payload.InvestigationID,
		AttemptNo:       envelope.Payload.AttemptNo,
	}

	message, err := PrepareSourceCollectRequestedMessage(registry, envelope, expected)
	if err != nil {
		t.Fatalf("PrepareSourceCollectRequestedMessage() error = %v", err)
	}
	if message.OutboxID() != expected.MessageID ||
		message.MessageType() != SourceCollectRequestedType ||
		message.AggregateID() != expected.JobID ||
		message.InvestigationID() != expected.InvestigationID ||
		message.AttemptNo() != expected.AttemptNo ||
		message.JSONPayload() == "" {
		t.Fatalf(
			"prepared message = id %q type %q aggregate %q attempt %d payload bytes %d",
			message.OutboxID(), message.MessageType(), message.AggregateID(),
			message.AttemptNo(), len(message.JSONPayload()),
		)
	}
	if _, err := registry.ValidateMessage([]byte(message.JSONPayload())); err != nil {
		t.Fatalf("prepared payload is not contract-valid: %v", err)
	}
}

func TestPrepareSourceCollectRequestedMessageRejectsInvalidInput(t *testing.T) {
	registry, validEnvelope := sourceCollectRequestedFixture(t)
	validExpected := SourceCollectRequestedIdentity{
		MessageID:       validEnvelope.MessageID,
		JobID:           validEnvelope.Payload.JobID,
		InvestigationID: validEnvelope.Payload.InvestigationID,
		AttemptNo:       validEnvelope.Payload.AttemptNo,
	}
	otherID := "99999999-9999-4999-8999-999999999999"
	otherMessage, otherJob, otherAttempt := validExpected, validExpected, validExpected
	otherMessage.MessageID = otherID
	otherJob.JobID = otherID
	otherAttempt.AttemptNo++

	tests := []struct {
		name     string
		registry *SchemaRegistry
		envelope Envelope[SourceCollectRequestedPayload]
		expected SourceCollectRequestedIdentity
	}{
		{name: "missing registry", envelope: validEnvelope, expected: validExpected},
		{name: "different message id", registry: registry, envelope: validEnvelope, expected: otherMessage},
		{name: "contract-valid different job", registry: registry, envelope: validEnvelope, expected: otherJob},
		{name: "contract-valid different attempt", registry: registry, envelope: validEnvelope, expected: otherAttempt},
		{name: "different correlation", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) {
			value.CorrelationID = otherID
		}), expected: validExpected},
		{name: "different investigation", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) {
			value.CorrelationID = otherID
			value.Payload.InvestigationID = otherID
		}), expected: validExpected},
		{name: "wrong type", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) { value.Type = SourceCollectFailedType }), expected: validExpected},
		{name: "wrong schema version", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) { value.SchemaVersion = 2 }), expected: validExpected},
		{name: "schema-invalid source", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) { value.Payload.Source = "" }), expected: validExpected},
		{name: "schema-invalid job id", registry: registry, envelope: withRequestedEnvelopeMutation(validEnvelope, func(value *Envelope[SourceCollectRequestedPayload]) { value.Payload.JobID = "not-a-uuid" }), expected: validExpected},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PrepareSourceCollectRequestedMessage(
				test.registry, test.envelope, test.expected,
			); err == nil {
				t.Fatal("PrepareSourceCollectRequestedMessage() accepted invalid input")
			}
		})
	}
}

func sourceCollectRequestedFixture(
	t *testing.T,
) (*SchemaRegistry, Envelope[SourceCollectRequestedPayload]) {
	t.Helper()
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "messages", "source-collect-requested.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	validated, err := registry.ValidateMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := Decode[SourceCollectRequestedPayload](validated, SourceCollectRequestedType)
	if err != nil {
		t.Fatal(err)
	}
	return registry, envelope
}

func withRequestedEnvelopeMutation(
	envelope Envelope[SourceCollectRequestedPayload],
	mutate func(*Envelope[SourceCollectRequestedPayload]),
) Envelope[SourceCollectRequestedPayload] {
	mutate(&envelope)
	return envelope
}
