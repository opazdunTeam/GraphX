package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
)

const sourceCollectRequestedType = "source.collect.requested.v1"

type Message struct {
	ID              string
	Type            string
	AggregateType   string
	AggregateID     string
	Payload         []byte
	PublishAttempts int
	LeaseToken      string
}

type Store interface {
	Claim(ctx context.Context, leaseDuration time.Duration) (*Message, error)
	MarkPublished(ctx context.Context, message Message, attemptNo int) error
	Reschedule(ctx context.Context, message Message, delay time.Duration) error
}

type Broker interface {
	Publish(ctx context.Context, message Message) error
}

type Config struct {
	LeaseDuration time.Duration
	BaseBackoff   time.Duration
	MaxBackoff    time.Duration
}

func DefaultConfig() Config {
	return Config{
		LeaseDuration: 30 * time.Second,
		BaseBackoff:   time.Second,
		MaxBackoff:    time.Minute,
	}
}

type Publisher struct {
	store  Store
	broker Broker
	config Config
}

func NewPublisher(store Store, broker Broker, config Config) (*Publisher, error) {
	if store == nil {
		return nil, errors.New("outbox store is required")
	}
	if broker == nil {
		return nil, errors.New("outbox broker is required")
	}
	if config.LeaseDuration < 2*time.Millisecond {
		return nil, errors.New("outbox lease duration must be at least two milliseconds")
	}
	if config.BaseBackoff < time.Millisecond || config.MaxBackoff < config.BaseBackoff {
		return nil, errors.New("outbox backoff must be at least one millisecond and no greater than its maximum")
	}

	return &Publisher{store: store, broker: broker, config: config}, nil
}

func (p *Publisher) PublishOne(ctx context.Context) (bool, error) {
	message, err := p.store.Claim(ctx, p.config.LeaseDuration)
	if err != nil {
		return false, fmt.Errorf("claim outbox message: %w", err)
	}
	if message == nil {
		return false, nil
	}

	attemptNo, err := sourceJobAttempt(*message)
	if err == nil {
		publishTimeout := p.config.LeaseDuration / 2
		publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
		err = p.broker.Publish(publishCtx, *message)
		cancel()
	}
	if err != nil {
		delay := BackoffDelay(message.PublishAttempts, p.config.BaseBackoff, p.config.MaxBackoff)
		if rescheduleErr := p.store.Reschedule(ctx, *message, delay); rescheduleErr != nil {
			return true, errors.Join(
				fmt.Errorf("publish outbox message: %w", err),
				fmt.Errorf("reschedule outbox message: %w", rescheduleErr),
			)
		}
		return true, &DeferredError{Cause: err, RetryAfter: delay}
	}

	if err := p.store.MarkPublished(ctx, *message, attemptNo); err != nil {
		return true, fmt.Errorf("mark outbox message %s published: %w", message.ID, err)
	}
	return true, nil
}

func BackoffDelay(previousFailures int, base, maximum time.Duration) time.Duration {
	if previousFailures < 0 || base <= 0 || maximum < base {
		return 0
	}
	delay := base
	for range previousFailures {
		if delay >= maximum || delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

type DeferredError struct {
	Cause      error
	RetryAfter time.Duration
}

func (e *DeferredError) Error() string {
	return fmt.Sprintf("outbox delivery deferred for %s: %v", e.RetryAfter, e.Cause)
}

func (e *DeferredError) Unwrap() error {
	return e.Cause
}

func IsDeferred(err error) bool {
	var deferred *DeferredError
	return errors.As(err, &deferred)
}

func sourceJobAttempt(message Message) (int, error) {
	if message.AggregateType != "source_job" {
		return 0, nil
	}
	if message.Type != sourceCollectRequestedType {
		return 0, fmt.Errorf("unsupported source job outbox message type %q", message.Type)
	}
	if strings.TrimSpace(message.AggregateID) == "" {
		return 0, errors.New("source job outbox aggregate id is required")
	}

	if _, err := contractjson.Decode(message.Payload); err != nil {
		return 0, fmt.Errorf("validate source job outbox payload: %w", err)
	}
	var envelope struct {
		MessageID string `json:"message_id"`
		Type      string `json:"type"`
		Payload   struct {
			JobID     string `json:"job_id"`
			AttemptNo int    `json:"attempt_no"`
		} `json:"payload"`
	}
	if err := json.Unmarshal(message.Payload, &envelope); err != nil {
		return 0, fmt.Errorf("decode source job outbox payload: %w", err)
	}
	if envelope.MessageID != message.ID || envelope.Type != message.Type ||
		envelope.Payload.JobID != message.AggregateID || envelope.Payload.AttemptNo < 1 {
		return 0, errors.New("source job outbox payload identity does not match its row")
	}
	return envelope.Payload.AttemptNo, nil
}
