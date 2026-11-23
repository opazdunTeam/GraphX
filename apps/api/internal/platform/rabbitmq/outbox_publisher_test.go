package rabbitmq

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

type recordingOutboxBroker struct {
	called bool
}

func TestConfirmPublisherHonorsContextDuringHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()

	publisher, err := NewConfirmPublisher(
		context.Background(),
		fmt.Sprintf("amqp://guest:guest@%s/", listener.Addr()),
		CommandExchange,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- publisher.Publish(ctx, outbox.Message{ID: "message", Type: SourceCollectRequestedType})
	}()

	select {
	case connection := <-accepted:
		t.Cleanup(func() { _ = connection.Close() })
	case <-time.After(time.Second):
		t.Fatal("publisher did not connect to the silent peer")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publisher did not stop after the handshake context expired")
	}
}

func TestConfirmPublisherClosesTransportAfterHandshakeFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	transportClosed := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			transportClosed <- acceptErr
			return
		}
		defer connection.Close()
		if err := connection.SetDeadline(time.Now().Add(time.Second)); err != nil {
			transportClosed <- err
			return
		}
		header := make([]byte, 8)
		if _, err := io.ReadFull(connection, header); err != nil {
			transportClosed <- err
			return
		}
		if _, err := connection.Write([]byte("NOTAMQP!")); err != nil {
			transportClosed <- err
			return
		}
		oneByte := make([]byte, 1)
		_, err := connection.Read(oneByte)
		transportClosed <- err
	}()

	publisher, err := NewConfirmPublisher(
		context.Background(),
		fmt.Sprintf("amqp://guest:guest@%s/", listener.Addr()),
		CommandExchange,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.Publish(context.Background(), outbox.Message{ID: "message", Type: SourceCollectRequestedType}); err == nil {
		t.Fatal("Publish() error = nil, want handshake failure")
	}
	select {
	case err := <-transportClosed:
		if err == nil {
			t.Fatal("peer transport remained open after failed handshake")
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatal("publisher did not close transport after failed handshake")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed handshake transport to close")
	}
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
