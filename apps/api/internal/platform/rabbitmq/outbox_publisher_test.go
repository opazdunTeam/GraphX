package rabbitmq

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

type recordingOutboxBroker struct {
	called bool
}

func TestConfirmPublisherClosesTransportWhenHeartbeatsOutliveClose(t *testing.T) {
	transport, peer := net.Pipe()
	t.Cleanup(func() { _ = transport.Close() })
	t.Cleanup(func() { _ = peer.Close() })
	peerDone := make(chan error, 1)
	go func() { peerDone <- runHeartbeatPeer(peer) }()

	connection, err := amqp.Open(transport, amqp.Config{
		Heartbeat: 10 * time.Second,
		SASL:      []amqp.Authentication{&amqp.PlainAuth{Username: "guest", Password: "guest"}},
		Locale:    "en_US",
	})
	if err != nil {
		t.Fatal(err)
	}

	publisher := &ConfirmPublisher{conn: connection, transport: transport}
	closed := make(chan error, 1)
	go func() { closed <- publisher.Close() }()
	select {
	case <-closed:
	case <-time.After(2 * publisherCloseTimeout):
		t.Fatal("publisher did not close the transport while the peer kept sending heartbeats")
	}
	select {
	case err := <-peerDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer did not observe the closed transport")
	}
}

func TestConfirmPublisherResetClosesTransport(t *testing.T) {
	transport, peer := net.Pipe()
	t.Cleanup(func() { _ = transport.Close() })
	t.Cleanup(func() { _ = peer.Close() })
	publisher := &ConfirmPublisher{transport: transport}

	if err := peer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	publisher.reset()

	if _, err := peer.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("read from discarded transport: %v, want EOF", err)
	}
	if publisher.transport != nil {
		t.Fatal("publisher retained the discarded transport")
	}
}

func runHeartbeatPeer(peer net.Conn) error {
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	if _, err := io.ReadFull(peer, make([]byte, 8)); err != nil {
		return err
	}
	start := []byte{0, 10, 0, 10, 0, 9, 0, 0, 0, 0, 0, 0, 0, 5, 'P', 'L', 'A', 'I', 'N', 0, 0, 0, 5, 'e', 'n', '_', 'U', 'S'}
	tune := []byte{0, 10, 0, 30, 0, 0, 0, 2, 0, 0, 0, 10}
	openOK := []byte{0, 10, 0, 41, 0}
	if err := writeTestAMQPFrame(peer, 1, start); err != nil {
		return err
	}
	steps := []struct {
		method   uint32
		response []byte
	}{
		{method: 10<<16 | 11, response: tune},
		{method: 10<<16 | 31},
		{method: 10<<16 | 40, response: openOK},
		{method: 10<<16 | 50},
	}
	for _, step := range steps {
		header := make([]byte, 7)
		if _, err := io.ReadFull(peer, header); err != nil {
			return err
		}
		size := binary.BigEndian.Uint32(header[3:])
		if header[0] != 1 || size < 4 || size > 4096 {
			return fmt.Errorf("unexpected AMQP frame header %x", header)
		}
		body := make([]byte, size+1)
		if _, err := io.ReadFull(peer, body); err != nil {
			return err
		}
		if method := binary.BigEndian.Uint32(body); method != step.method {
			return fmt.Errorf("AMQP method = %d, want %d", method, step.method)
		}
		if step.response != nil {
			if err := writeTestAMQPFrame(peer, 1, step.response); err != nil {
				return err
			}
		}
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		if err := writeTestAMQPFrame(peer, 8, nil); err != nil {
			if errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			return err
		}
	}
	return nil
}

func writeTestAMQPFrame(writer io.Writer, frameType byte, body []byte) error {
	frame := make([]byte, 7, 8+len(body))
	frame[0] = frameType
	binary.BigEndian.PutUint32(frame[3:], uint32(len(body)))
	frame = append(frame, body...)
	frame = append(frame, 0xce)
	_, err := writer.Write(frame)
	return err
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

func TestConfirmPublisherWithRabbitMQ(t *testing.T) {
	url := strings.TrimSpace(os.Getenv("RABBITMQ_URL"))
	if url == "" {
		t.Skip("RABBITMQ_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 10 * time.Second})
	if err != nil {
		t.Fatalf("connect RabbitMQ test topology: %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	channel, err := connection.Channel()
	if err != nil {
		t.Fatalf("open RabbitMQ test topology channel: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	exchange := CommandExchange + ".test." + uuid.NewString()
	if err := channel.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("declare command exchange: %v", err)
	}
	t.Cleanup(func() { _ = channel.ExchangeDelete(exchange, false, false) })
	queueName := "graphx-publisher-audit-" + uuid.NewString()
	if _, err := channel.QueueDeclare(queueName, true, false, true, false, amqp.Table{
		"x-max-length": int32(2),
		"x-overflow":   "reject-publish",
	}); err != nil {
		t.Fatalf("declare audit queue: %v", err)
	}
	t.Cleanup(func() { _, _ = channel.QueueDelete(queueName, false, false, false) })
	if err := channel.QueueBind(queueName, SourceCollectRequestedType, exchange, false, nil); err != nil {
		t.Fatalf("bind audit queue: %v", err)
	}

	publisher, err := NewConfirmPublisher(ctx, url, exchange)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	now := time.Now().UTC().Truncate(time.Second)
	investigationID := uuid.NewString()
	jobID := uuid.NewString()
	messageID := uuid.NewString()
	subjectName := "Ada Lovelace"
	envelope := Envelope[SourceCollectRequestedPayload]{
		MessageID:     messageID,
		Type:          SourceCollectRequestedType,
		SchemaVersion: 1,
		OccurredAt:    now,
		CorrelationID: investigationID,
		Producer:      "graphx-api",
		RootCommandID: messageID,
		Payload: SourceCollectRequestedPayload{
			JobID:           jobID,
			AttemptNo:       2,
			InvestigationID: investigationID,
			Source:          "fixture",
			Operation:       SourceSearchSubjectOperation,
			Purpose:         "subject_discovery",
			Query:           SourceQuery{Type: "person", Name: &subjectName},
			Limits:          CollectLimits{MaxRecords: 20, MaxBytes: 1_048_576, TimeoutSeconds: 30},
			Deadline:        now.Add(30 * time.Second),
		},
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	message := outbox.Message{
		ID:      messageID,
		Type:    SourceCollectRequestedType,
		Payload: payload,
	}
	for range 2 {
		if err := publisher.Publish(ctx, message); err != nil {
			t.Fatalf("publish routed message: %v", err)
		}
	}
	if err := publisher.Publish(ctx, message); !errors.Is(err, errNegativeConfirmation) {
		t.Fatalf("Publish(full queue) error = %v, want negative confirmation", err)
	}
	for range 2 {
		delivery, ok, err := channel.Get(queueName, false)
		if err != nil {
			t.Fatalf("read routed message: %v", err)
		}
		if !ok {
			t.Fatal("publisher confirm returned before the routed message was available")
		}
		if delivery.MessageId != message.ID || delivery.Type != message.Type ||
			delivery.DeliveryMode != amqp.Persistent || string(delivery.Body) != string(message.Payload) {
			t.Fatalf("published message properties differ: id=%q type=%q mode=%d",
				delivery.MessageId, delivery.Type, delivery.DeliveryMode)
		}
		var received Envelope[SourceCollectRequestedPayload]
		if err := json.Unmarshal(delivery.Body, &received); err != nil {
			t.Fatalf("decode published command: %v", err)
		}
		if received.MessageID != messageID || received.Payload.JobID != jobID ||
			received.Payload.AttemptNo != 2 || received.Payload.Operation != SourceSearchSubjectOperation {
			t.Fatalf("published command identity changed: message=%s job=%s attempt=%d operation=%s",
				received.MessageID, received.Payload.JobID, received.Payload.AttemptNo, received.Payload.Operation)
		}
	}

	unroutable := message
	unroutable.ID = uuid.NewString()
	unroutable.Type = "source.collect.audit.unroutable"
	if err := publisher.Publish(ctx, unroutable); !errors.Is(err, errUnroutableMessage) {
		t.Fatalf("Publish(unroutable command) error = %v, want mandatory return", err)
	}
	if err := publisher.Publish(ctx, message); err != nil {
		t.Fatalf("publish after rejected messages: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("close publisher connected to RabbitMQ: %v", err)
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
