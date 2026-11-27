package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const (
	testMessageID       = "30000000-0000-4000-8000-000000000001"
	testJobID           = "20000000-0000-4000-8000-000000000001"
	testInvestigationID = "10000000-0000-4000-8000-000000000001"
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
		ID: testMessageID, Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","correlation_id":"` + testInvestigationID + `","payload":{"job_id":"` + testJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":1}}`),
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

func TestPublishOneBoundsCompletionAfterConfirmation(t *testing.T) {
	store := &blockedCompletionStore{testStore: testStore{message: &Message{
		ID: testMessageID, Type: sourceCollectRequestedType,
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","correlation_id":"` + testInvestigationID + `","payload":{"job_id":"` + testJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":1}}`),
	}}}
	broker := &testBroker{}
	cfg := DefaultConfig()
	cfg.LeaseDuration = 100 * time.Millisecond
	publisher, err := NewPublisher(store, broker, cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	result, err := publisher.PublishOne(ctx)
	if !IsDeferred(err) || !errors.Is(err, ErrProcessingTimeout) || ctx.Err() != nil {
		t.Fatalf("PublishOne() error = %v, parent context error = %v, want lease deadline", err, ctx.Err())
	}
	if !broker.published || !result.Processed || result.Published || store.rescheduled {
		t.Fatalf("confirmation timeout state = broker %t, result %#v, rescheduled %t",
			broker.published, result, store.rescheduled)
	}
}

type blockedCompletionStore struct {
	testStore
}

func (s *blockedCompletionStore) MarkPublished(ctx context.Context, _ Message, _ int) error {
	<-ctx.Done()
	return ctx.Err()
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
	message    Message
}

func (b *testBroker) Publish(_ context.Context, message Message) error {
	b.published = true
	b.message = message
	return b.publishErr
}

func TestPublishOneMarksPublishedOnlyAfterBrokerConfirm(t *testing.T) {
	store := &testStore{message: &Message{
		ID: testMessageID, Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","correlation_id":"` + testInvestigationID + `","payload":{"job_id":"` + testJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":3}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if err != nil {
		t.Fatalf("PublishOne() error = %v", err)
	}
	if !result.Processed || !result.Published || !broker.published || !store.published {
		t.Fatalf("result/broker/store = %#v/%t/%t, want published", result, broker.published, store.published)
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
		ID: testMessageID, Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: testJobID,
		PublishAttempts: 2,
		Payload:         []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","correlation_id":"` + testInvestigationID + `","payload":{"job_id":"` + testJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":1}}`),
	}}
	broker := &testBroker{publishErr: errors.New("confirm was not received")}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if !result.Processed || !IsDeferred(err) {
		t.Fatalf("PublishOne() = %#v, %v; want processed and deferred error", result, err)
	}
	if !broker.published || store.published || !store.rescheduled || !errors.Is(err, broker.publishErr) {
		t.Fatalf("broker called = %t, published = %t, rescheduled = %t, error = %v",
			broker.published, store.published, store.rescheduled, err)
	}
	if store.retryDelay != 4*time.Second {
		t.Fatalf("retry delay = %s, want 4s", store.retryDelay)
	}
}

func TestPublishOneRejectsMismatchedSourceJobPayload(t *testing.T) {
	store := &testStore{message: &Message{
		ID: testMessageID, Type: "source.collect.requested.v1",
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"30000000-0000-4000-8000-000000000002","type":"source.collect.requested.v1","payload":{"job_id":"` + testJobID + `","attempt_no":1}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if !result.Processed || !IsDeferred(err) {
		t.Fatalf("PublishOne() = %#v, %v; want message deferred", result, err)
	}
	if broker.published || store.published || !store.rescheduled {
		t.Fatalf("broker/published/rescheduled = %t/%t/%t", broker.published, store.published, store.rescheduled)
	}
}

func TestPublishOneRejectsMismatchedCorrelationAndInvestigationIDs(t *testing.T) {
	store := &testStore{message: &Message{
		ID: testMessageID, Type: sourceCollectRequestedType,
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","correlation_id":"40000000-0000-4000-8000-000000000001","payload":{"job_id":"` + testJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":1}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if !result.Processed || !IsDeferred(err) || broker.published || store.published || !store.rescheduled {
		t.Fatalf("PublishOne() = %#v, %v broker=%t store=%t/%t", result, err, broker.published, store.published, store.rescheduled)
	}
}

func TestPublishOneAcceptsEquivalentUUIDAndIntegralNumberRepresentations(t *testing.T) {
	upperMessageID := "30000000-0000-4000-8000-00000000ABCD"
	upperJobID := "20000000-0000-4000-8000-00000000ABCD"
	store := &testStore{message: &Message{
		ID: strings.ToLower(upperMessageID), Type: sourceCollectRequestedType,
		AggregateType: "source_job", AggregateID: strings.ToLower(upperJobID),
		Payload: []byte(`{"message_id":"` + upperMessageID + `","type":"source.collect.requested.v1","correlation_id":"` + testInvestigationID + `","payload":{"job_id":"` + upperJobID + `","investigation_id":"` + testInvestigationID + `","attempt_no":1e0}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if err != nil || !result.Published || !broker.published || !store.published {
		t.Fatalf("PublishOne() = %#v, %v; want published", result, err)
	}
	if broker.message.ID != upperMessageID || broker.message.AggregateID != upperJobID || store.attemptNo != 1 {
		t.Fatalf("wire identity = %q/%q attempt %d", broker.message.ID, broker.message.AggregateID, store.attemptNo)
	}
	if result.InvestigationID != testInvestigationID || result.CorrelationID != testInvestigationID {
		t.Fatalf("result identity = investigation %q correlation %q", result.InvestigationID, result.CorrelationID)
	}
}

func TestPublishOneRejectsAttemptOutsideInteroperableJSONIntegerRange(t *testing.T) {
	store := &testStore{message: &Message{
		ID: testMessageID, Type: sourceCollectRequestedType,
		AggregateType: "source_job", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","payload":{"job_id":"` + testJobID + `","attempt_no":9007199254740993.0}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if !result.Processed || !IsDeferred(err) || broker.published || store.published || !store.rescheduled {
		t.Fatalf("PublishOne() = %#v, %v broker=%t store=%t/%t", result, err, broker.published, store.published, store.rescheduled)
	}
}

func TestExactPositiveJSONIntegerInteroperabilityBoundary(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{name: "maximum decimal", value: "9007199254740991.0", want: int(maximumSafeJSONInteger)},
		{name: "maximum exponent", value: "9.007199254740991e15", want: int(maximumSafeJSONInteger)},
		{name: "adjacent overflow decimal", value: "9007199254740992.0", wantErr: true},
		{name: "adjacent overflow exponent", value: "9.007199254740992e15", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := exactPositiveJSONInteger(json.Number(test.value))
			if test.wantErr {
				if err == nil {
					t.Fatalf("exactPositiveJSONInteger(%s) = %d, want error", test.value, got)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("exactPositiveJSONInteger(%s) = %d, %v; want %d", test.value, got, err, test.want)
			}
		})
	}
}

func TestPublishOneRejectsSourceRequestWithWrongAggregate(t *testing.T) {
	store := &testStore{message: &Message{
		ID: testMessageID, Type: sourceCollectRequestedType,
		AggregateType: "investigation", AggregateID: testJobID,
		Payload: []byte(`{"message_id":"` + testMessageID + `","type":"source.collect.requested.v1","payload":{"job_id":"` + testJobID + `","attempt_no":1}}`),
	}}
	broker := &testBroker{}
	publisher, err := NewPublisher(store, broker, DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.PublishOne(context.Background())
	if !result.Processed || !IsDeferred(err) || broker.published || store.published || !store.rescheduled {
		t.Fatalf("PublishOne() = %#v, %v broker=%t store=%t/%t", result, err, broker.published, store.published, store.rescheduled)
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
