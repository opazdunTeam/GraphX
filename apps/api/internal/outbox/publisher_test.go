package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type testStore struct {
	message      *Message
	claimedLease time.Duration
	published    bool
	attemptNo    int
	rescheduled  bool
	retryDelay   time.Duration
}

func TestPublishOneWaitsForBrokerConfirmationBeforeCompletion(t *testing.T) {
	store := &testStore{message: &Message{
		ID: "outbox-id", Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: "job-id",
		Payload: []byte(`{"message_id":"outbox-id","type":"source.collect.requested.v1","payload":{"job_id":"job-id","attempt_no":1}}`),
	}}
	broker := &blockingTestBroker{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, publishErr := publisher.PublishOne(context.Background())
		done <- publishErr
	}()
	<-broker.started
	if store.published {
		t.Fatal("outbox marked published before broker confirmation")
	}
	close(broker.release)
	if err := <-done; err != nil {
		t.Fatalf("PublishOne() error = %v", err)
	}
	if !store.published {
		t.Fatal("outbox was not marked published after broker confirmation")
	}
}

func (s *testStore) Claim(_ context.Context, lease time.Duration) (*Message, error) {
	s.claimedLease = lease
	if s.message == nil {
		return nil, nil
	}
	message := *s.message
	return &message, nil
}

type blockingTestBroker struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingTestBroker) Publish(ctx context.Context, _ Message) error {
	close(b.started)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *testStore) MarkPublished(_ context.Context, _ Message, attemptNo int) error {
	s.published = true
	s.attemptNo = attemptNo
	return nil
}

func (s *testStore) Reschedule(_ context.Context, _ Message, delay time.Duration) error {
	s.rescheduled = true
	s.retryDelay = delay
	return nil
}

type testBroker struct {
	publishErr error
	published  bool
}

func (b *testBroker) Publish(context.Context, Message) error {
	b.published = true
	return b.publishErr
}

func TestPublishOneMarksPublishedOnlyAfterBrokerConfirm(t *testing.T) {
	store := &testStore{message: &Message{
		ID: "outbox-id", Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: "job-id",
		Payload: []byte(`{"message_id":"outbox-id","type":"source.collect.requested.v1","payload":{"job_id":"job-id","attempt_no":3}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	processed, err := publisher.PublishOne(context.Background())
	if err != nil {
		t.Fatalf("PublishOne() error = %v", err)
	}
	if !processed || !broker.published || !store.published {
		t.Fatalf("processed/broker/published = %t/%t/%t, want all true", processed, broker.published, store.published)
	}
	if store.attemptNo != 3 {
		t.Fatalf("marked attempt = %d, want 3", store.attemptNo)
	}
	if store.claimedLease != DefaultConfig().LeaseDuration {
		t.Fatalf("claimed lease = %s, want %s", store.claimedLease, DefaultConfig().LeaseDuration)
	}
}

func TestPublishOneDefersFailedDeliveryWithoutMarkingPublished(t *testing.T) {
	store := &testStore{message: &Message{
		ID: "outbox-id", Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: "job-id",
		PublishAttempts: 2,
		Payload:         []byte(`{"message_id":"outbox-id","type":"source.collect.requested.v1","payload":{"job_id":"job-id","attempt_no":1}}`),
	}}
	broker := &testBroker{publishErr: errors.New("confirm was not received")}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	processed, err := publisher.PublishOne(context.Background())
	if !processed || !IsDeferred(err) {
		t.Fatalf("PublishOne() = %t, %v; want processed and deferred error", processed, err)
	}
	if store.published || !store.rescheduled {
		t.Fatalf("published/rescheduled = %t/%t, want false/true", store.published, store.rescheduled)
	}
	if store.retryDelay != 4*time.Second {
		t.Fatalf("retry delay = %s, want 4s", store.retryDelay)
	}
}

func TestPublishOneRejectsMismatchedSourceJobPayload(t *testing.T) {
	store := &testStore{message: &Message{
		ID: "outbox-id", Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: "job-id",
		Payload: []byte(`{"message_id":"different-id","type":"source.collect.requested.v1","payload":{"job_id":"job-id","attempt_no":1}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	processed, err := publisher.PublishOne(context.Background())
	if !processed || !IsDeferred(err) {
		t.Fatalf("PublishOne() = %t, %v; want message deferred", processed, err)
	}
	if broker.published || store.published || !store.rescheduled {
		t.Fatalf("broker/published/rescheduled = %t/%t/%t", broker.published, store.published, store.rescheduled)
	}
}

func TestBackoffDelayIsExponentialAndBounded(t *testing.T) {
	tests := []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: time.Second},
		{failures: 1, want: 2 * time.Second},
		{failures: 2, want: 4 * time.Second},
		{failures: 6, want: time.Minute},
		{failures: 100, want: time.Minute},
	}
	for _, test := range tests {
		if got := BackoffDelay(test.failures, time.Second, time.Minute); got != test.want {
			t.Errorf("BackoffDelay(%d) = %s, want %s", test.failures, got, test.want)
		}
	}
	if got := BackoffDelay(1, time.Second, 0); got != 0 {
		t.Errorf("invalid backoff config returned %s, want 0", got)
	}
}

func TestNewPublisherRejectsInvalidConfiguration(t *testing.T) {
	store := &testStore{}
	broker := &testBroker{}
	for _, config := range []Config{
		{BaseBackoff: time.Second, MaxBackoff: time.Minute},
		{LeaseDuration: time.Second, BaseBackoff: time.Minute, MaxBackoff: time.Second},
	} {
		if _, err := NewPublisher(store, broker, config); err == nil {
			t.Errorf("NewPublisher(%+v) error = nil, want configuration error", config)
		}
	}
}

func TestDeferredErrorPreservesCause(t *testing.T) {
	cause := errors.New("publish failed")
	err := &DeferredError{Cause: cause, RetryAfter: time.Second}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "1s") {
		t.Fatalf("deferred error = %v, want cause and retry delay", err)
	}
}
