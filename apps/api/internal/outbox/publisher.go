package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
)

const sourceCollectRequestedType = "source.collect.requested.v1"

const maximumSafeJSONInteger int64 = 1<<53 - 1

var ErrLeaseLost = errors.New("outbox lease lost")

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

type PublishResult struct {
	Processed       bool
	Published       bool
	MessageID       string
	JobID           string
	InvestigationID string
	CorrelationID   string
	RetryAfter      time.Duration
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

func (p *Publisher) PublishOne(ctx context.Context) (PublishResult, error) {
	var result PublishResult
	message, err := p.store.Claim(ctx, p.config.LeaseDuration)
	if err != nil {
		return result, fmt.Errorf("claim outbox message: %w", err)
	}
	if message == nil {
		return result, nil
	}
	result.Processed = true
	result.MessageID = message.ID
	result.JobID = message.AggregateID

	identity, err := sourceJobIdentity(*message)
	if err == nil {
		result.MessageID = identity.MessageID
		result.JobID = identity.JobID
		result.InvestigationID = identity.InvestigationID
		result.CorrelationID = identity.CorrelationID
		wireMessage := *message
		wireMessage.ID = identity.MessageID
		wireMessage.AggregateID = identity.JobID
		publishTimeout := p.config.LeaseDuration / 2
		publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
		err = p.broker.Publish(publishCtx, wireMessage)
		cancel()
	}
	if err != nil {
		delay := BackoffDelay(message.PublishAttempts, p.config.BaseBackoff, p.config.MaxBackoff)
		result.RetryAfter = delay
		if rescheduleErr := p.store.Reschedule(ctx, *message, delay); rescheduleErr != nil {
			if errors.Is(rescheduleErr, ErrLeaseLost) {
				return result, &DeferredError{Cause: ErrLeaseLost, RetryAfter: delay}
			}
			return result, errors.Join(
				fmt.Errorf("publish outbox message: %w", err),
				fmt.Errorf("reschedule outbox message: %w", rescheduleErr),
			)
		}
		return result, &DeferredError{Cause: err, RetryAfter: delay}
	}

	if err := p.store.MarkPublished(ctx, *message, identity.AttemptNo); err != nil {
		if errors.Is(err, ErrLeaseLost) {
			return result, &DeferredError{Cause: ErrLeaseLost}
		}
		return result, fmt.Errorf("mark outbox message %s published: %w", message.ID, err)
	}
	result.Published = true
	return result, nil
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

type sourceJobMessageIdentity struct {
	MessageID       string
	JobID           string
	InvestigationID string
	CorrelationID   string
	AttemptNo       int
}

func sourceJobIdentity(message Message) (sourceJobMessageIdentity, error) {
	if message.Type != sourceCollectRequestedType {
		return sourceJobMessageIdentity{}, fmt.Errorf("unsupported outbox message type %q", message.Type)
	}
	if message.AggregateType != "source_job" {
		return sourceJobMessageIdentity{}, fmt.Errorf(
			"source collect request requires source_job aggregate, got %q",
			message.AggregateType,
		)
	}
	if strings.TrimSpace(message.AggregateID) == "" {
		return sourceJobMessageIdentity{}, errors.New("source job outbox aggregate id is required")
	}

	value, err := contractjson.Decode(message.Payload)
	if err != nil {
		return sourceJobMessageIdentity{}, fmt.Errorf("validate source job outbox payload: %w", err)
	}
	envelope, ok := value.(map[string]any)
	if !ok {
		return sourceJobMessageIdentity{}, errors.New("source job outbox payload must be an object")
	}
	messageID, _ := envelope["message_id"].(string)
	messageType, _ := envelope["type"].(string)
	correlationID, _ := envelope["correlation_id"].(string)
	payload, ok := envelope["payload"].(map[string]any)
	if !ok {
		return sourceJobMessageIdentity{}, errors.New("source job outbox payload body must be an object")
	}
	jobID, _ := payload["job_id"].(string)
	investigationID, _ := payload["investigation_id"].(string)
	attemptNo, err := exactPositiveJSONInteger(payload["attempt_no"])
	if err != nil {
		return sourceJobMessageIdentity{}, fmt.Errorf("decode source job attempt number: %w", err)
	}
	if messageType != message.Type || !sameUUID(messageID, message.ID) ||
		!sameUUID(jobID, message.AggregateID) {
		return sourceJobMessageIdentity{}, errors.New("source job outbox payload identity does not match its row")
	}
	if _, err := uuid.Parse(correlationID); err != nil {
		correlationID = ""
	}
	if _, err := uuid.Parse(investigationID); err != nil {
		investigationID = ""
	}
	return sourceJobMessageIdentity{
		MessageID:       messageID,
		JobID:           jobID,
		InvestigationID: investigationID,
		CorrelationID:   correlationID,
		AttemptNo:       attemptNo,
	}, nil
}

func sameUUID(left, right string) bool {
	leftUUID, leftErr := uuid.Parse(left)
	rightUUID, rightErr := uuid.Parse(right)
	return leftErr == nil && rightErr == nil && leftUUID == rightUUID
}

func exactPositiveJSONInteger(value any) (int, error) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, errors.New("value must be a JSON number")
	}
	rational, ok := new(big.Rat).SetString(number.String())
	if !ok || !rational.IsInt() || rational.Sign() <= 0 || !rational.Num().IsInt64() {
		return 0, errors.New("value must be a positive exact integer")
	}
	integer64 := rational.Num().Int64()
	if integer64 > maximumSafeJSONInteger {
		return 0, errors.New("value is outside the interoperable JSON integer range")
	}
	integer := int(integer64)
	if int64(integer) != integer64 {
		return 0, errors.New("value is outside the supported integer range")
	}
	return integer, nil
}
