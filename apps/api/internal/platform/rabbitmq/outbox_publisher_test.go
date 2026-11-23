package rabbitmq

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

type recordingOutboxBroker struct {
	called bool
}

func (b *recordingOutboxBroker) Publish(context.Context, outbox.Message) error {
	b.called = true
	return nil
}

func TestSchemaValidatingPublisherChecksPayloadBeforePublishing(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(root, "contracts", "examples", "messages", "source-collect-requested.v1.json")
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	next := &recordingOutboxBroker{}
	publisher, err := NewSchemaValidatingPublisher(registry, next)
	if err != nil {
		t.Fatal(err)
	}

	message := outbox.Message{
		ID:      "fixture-message",
		Type:    SourceCollectRequestedType,
		Payload: payload,
	}
	if err := publisher.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish(valid message) error = %v", err)
	}
	if !next.called {
		t.Fatal("valid message was not passed to broker")
	}

	next.called = false
	message.Type = SourceCollectCompletedType
	if err := publisher.Publish(context.Background(), message); err == nil {
		t.Fatal("Publish(type mismatch) error = nil, want validation error")
	}
	if next.called {
		t.Fatal("message with mismatched type was passed to broker")
	}

	next.called = false
	message.Type = SourceCollectRequestedType
	message.Payload = []byte(`{"type":"source.collect.requested.v1"}`)
	if err := publisher.Publish(context.Background(), message); err == nil {
		t.Fatal("Publish(invalid payload) error = nil, want validation error")
	}
	if next.called {
		t.Fatal("invalid message was passed to broker")
	}
}
