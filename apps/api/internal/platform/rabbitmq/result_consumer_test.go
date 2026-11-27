package rabbitmq

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	amqp "github.com/rabbitmq/amqp091-go"
)

type resultHandlerFunc func(context.Context, ingestion.ResultDeliveryMetadata, []byte) (ingestion.ResultConsumerOutcome, error)

func (f resultHandlerFunc) Handle(ctx context.Context, metadata ingestion.ResultDeliveryMetadata, body []byte) (ingestion.ResultConsumerOutcome, error) {
	return f(ctx, metadata, body)
}

type resultAcknowledgment struct {
	ack     bool
	requeue bool
}

type resultAcknowledger chan resultAcknowledgment

func (a resultAcknowledger) Ack(uint64, bool) error {
	a <- resultAcknowledgment{ack: true}
	return nil
}

func (a resultAcknowledger) Nack(_ uint64, _ bool, requeue bool) error {
	a <- resultAcknowledgment{requeue: requeue}
	return nil
}

func (a resultAcknowledger) Reject(tag uint64, requeue bool) error {
	return a.Nack(tag, false, requeue)
}

func TestSourceResultDeliveryAcknowledgment(t *testing.T) {
	const privateValue = "unexpected metadata containing private data"
	for _, test := range []struct {
		name string
		err  error
		want resultAcknowledgment
	}{
		{name: "commit", want: resultAcknowledgment{ack: true}},
		{name: "permanent", err: ingestion.NewPermanentResultError(errors.New(privateValue))},
		{name: "transient", err: errors.New(privateValue), want: resultAcknowledgment{requeue: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			acknowledger := make(resultAcknowledger, 2)
			var logs bytes.Buffer
			consumer := SourceResultConsumer{
				logger: slog.New(slog.NewJSONHandler(&logs, nil)),
				handler: resultHandlerFunc(func(context.Context, ingestion.ResultDeliveryMetadata, []byte) (ingestion.ResultConsumerOutcome, error) {
					if len(acknowledger) != 0 {
						t.Fatal("delivery settled before handler completed")
					}
					return ingestion.ResultConsumerOutcome{}, test.err
				}),
			}
			delivery := amqp.Delivery{
				Acknowledger: acknowledger, MessageId: privateValue, Type: privateValue, RoutingKey: privateValue,
			}
			if err := consumer.handleDelivery(t.Context(), delivery); err != nil {
				t.Fatal(err)
			}
			if len(acknowledger) != 1 {
				t.Fatalf("settlements = %d, want one", len(acknowledger))
			}
			if got := <-acknowledger; got != test.want {
				t.Fatalf("settlement = %+v, want %+v", got, test.want)
			}
			if bytes.Contains(logs.Bytes(), []byte(privateValue)) {
				t.Fatal("log includes unvalidated delivery metadata or failure details")
			}
		})
	}
}

func TestSourceResultConsumerBoundsHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer := make(chan net.Conn, 1)
	go func() {
		if conn, err := listener.Accept(); err == nil {
			peer <- conn
		}
	}()
	consumer := &SourceResultConsumer{url: "amqp://" + listener.Addr().String(), connectTimeout: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	select {
	case conn := <-peer:
		defer conn.Close()
	case <-ctx.Done():
		t.Fatal("consumer did not connect to the silent peer")
	}
	select {
	case err := <-done:
		if err == nil || ctx.Err() != nil {
			t.Fatalf("expected the handshake deadline to expire first, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("AMQP handshake ignored the configured deadline")
	}
}

type resultStoreFunc func(context.Context, string, ingestion.SourceResultCommand) (ingestion.ResultApplyOutcome, error)

func (f resultStoreFunc) CheckExistingSourceResult(context.Context, string, ingestion.SourceResultCommand) (ingestion.ResultApplyOutcome, error) {
	return ingestion.ResultApplyOutcome{}, nil
}

func (f resultStoreFunc) ApplySourceResult(ctx context.Context, name string, command ingestion.SourceResultCommand) (ingestion.ResultApplyOutcome, error) {
	return f(ctx, name, command)
}

type unusedManifestReader struct{}

func (unusedManifestReader) Read(context.Context, string, int64) ([]byte, error) {
	return nil, errors.New("unexpected manifest read")
}

func TestSourceResultConsumerWithRabbitMQ(t *testing.T) {
	url := os.Getenv("GRAPHX_RESULT_RABBITMQ_TEST_URL")
	if url == "" {
		t.Skip("GRAPHX_RESULT_RABBITMQ_TEST_URL is not set to a dedicated test vhost")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, "contracts", "examples", "messages", "source-collect-completed.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := amqp.Dial(url)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()

	calls := make(chan ingestion.SourceResultCommand, 3)
	first := true
	store := resultStoreFunc(func(_ context.Context, _ string, command ingestion.SourceResultCommand) (ingestion.ResultApplyOutcome, error) {
		calls <- command
		if first {
			first = false
			return ingestion.ResultApplyOutcome{}, errors.New("temporary database error")
		}
		return ingestion.ResultApplyOutcome{Applied: true, Status: command.Status}, nil
	})
	handler, err := ingestion.NewResultConsumer(registry, unusedManifestReader{}, store, ingestion.ResultConsumerConfig{
		MaxMessageBytes: 1 << 20, MaxManifestBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := NewSourceResultConsumer(url, 5*time.Second, 1, handler, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()

	invalid := bytes.Replace(body, []byte("fixture-adapter/1.0.0"), []byte("fixture-adapter/2.0.0"), 1)
	const messageID = "55555555-5555-4555-8555-555555555555"
	poll := time.NewTicker(25 * time.Millisecond)
	defer poll.Stop()
	ready := false
	for !ready {
		select {
		case <-ctx.Done():
			t.Fatal("consumer queue did not become ready")
		case err := <-done:
			t.Fatalf("consumer stopped before binding: %v", err)
		case <-poll.C:
			probe, err := connection.Channel()
			if err != nil {
				t.Fatal(err)
			}
			queue, err := probe.QueueInspect(CoreEventsQueue)
			_ = probe.Close()
			ready = err == nil && queue.Consumers == 1
		}
	}
	publish := func(data []byte) {
		t.Helper()
		if err := channel.PublishWithContext(ctx, EventsExchange, SourceCollectCompletedType, true, false, amqp.Publishing{
			Body: data, Type: SourceCollectCompletedType, MessageId: messageID,
		}); err != nil {
			t.Fatal(err)
		}
	}
	publish(body)
	for range 2 {
		select {
		case command := <-calls:
			if command.MessageID != messageID || len(command.Claims) != 1 {
				t.Fatalf("unexpected command: %+v", command)
			}
		case <-ctx.Done():
			t.Fatal("valid result was not delivered again after transient failure")
		}
	}
	publish(invalid)
	for {
		delivery, ok, err := channel.Get(CoreEventsDeadLetter, true)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if !bytes.Equal(delivery.Body, invalid) {
				t.Fatal("unexpected dead-lettered result")
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("invalid result did not reach DLQ")
		case <-poll.C:
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok, err := channel.Get(CoreEventsQueue, true); err != nil || ok {
		t.Fatalf("source queue after ACK: delivery=%t, error=%v", ok, err)
	}
}
