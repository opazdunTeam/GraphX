package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
)

const (
	SourceCollectCompletedType = "source.collect.completed.v1"
	SourceCollectFailedType    = "source.collect.failed.v1"
	DefaultResultConsumerName  = "source-result-ingestion"
)

type ValidatedResultMessage struct {
	Type    string
	Payload json.RawMessage
}

type ResultMessageValidator interface {
	ValidateResultMessage([]byte) (ValidatedResultMessage, error)
	ValidateResultManifest([]byte) error
}

type ResultManifestReader interface {
	Read(context.Context, string, int64) ([]byte, error)
}

type ResultStore interface {
	ApplySourceResult(context.Context, string, SourceResultCommand) (ResultApplyOutcome, error)
}

type ResultDeliveryMetadata struct {
	MessageID  string
	Type       string
	RoutingKey string
}

type ResultStatus string

const (
	ResultStatusRetryWait      ResultStatus = "retry_wait"
	ResultStatusSucceeded      ResultStatus = "succeeded"
	ResultStatusNotFound       ResultStatus = "not_found"
	ResultStatusActionRequired ResultStatus = "action_required"
	ResultStatusUnavailable    ResultStatus = "unavailable"
	ResultStatusFailed         ResultStatus = "failed"
)

type SourceResultCommand struct {
	ID             string
	MessageID      string
	CorrelationID  string
	JobID          string
	AttemptNo      int
	Digest         string
	AdapterVersion string
	Status         ResultStatus
	NextAttemptAt  time.Time
	ErrorCode      string
	ErrorMessage   string
	Artifacts      []ArtifactReference
	ProcessingRuns []ProcessingRunInput
	Claims         []Claim
}

type ResultApplyOutcome struct {
	Applied       bool
	Duplicate     bool
	Status        ResultStatus
	IgnoredReason string
}

type ResultConsumerOutcome struct {
	MessageID     string
	CorrelationID string
	JobID         string
	AttemptNo     int
	Status        ResultStatus
	Applied       bool
	Duplicate     bool
	IgnoredReason string
}

type PermanentResultError struct {
	cause error
	code  string
}

func (e *PermanentResultError) Error() string {
	return e.code
}

func (e *PermanentResultError) Unwrap() error {
	return e.cause
}

func NewPermanentResultError(cause error) error {
	return NewPermanentResultErrorWithCode("invalid_source_result", cause)
}

func NewPermanentResultErrorWithCode(code string, cause error) error {
	if cause == nil {
		cause = errors.New("invalid source result")
	}
	if strings.TrimSpace(code) == "" {
		code = "invalid_source_result"
	}
	return &PermanentResultError{cause: cause, code: code}
}

func IsPermanentResultError(err error) bool {
	var permanent *PermanentResultError
	return errors.As(err, &permanent)
}

func PermanentResultErrorCode(err error) string {
	var permanent *PermanentResultError
	if !errors.As(err, &permanent) {
		return ""
	}
	return permanent.code
}

type ResultConsumerConfig struct {
	Name             string
	MaxMessageBytes  int64
	MaxManifestBytes int64
	Now              func() time.Time
	Jitter           func(time.Duration) time.Duration
}

type ResultConsumer struct {
	validator        ResultMessageValidator
	manifestReader   ResultManifestReader
	store            ResultStore
	name             string
	maxManifestBytes int64
	now              func() time.Time
	jitter           func(time.Duration) time.Duration
	maxMessageBytes  int64
}

func NewResultConsumer(
	validator ResultMessageValidator,
	manifestReader ResultManifestReader,
	store ResultStore,
	config ResultConsumerConfig,
) (*ResultConsumer, error) {
	if validator == nil {
		return nil, errors.New("result message validator is required")
	}
	if manifestReader == nil {
		return nil, errors.New("result manifest reader is required")
	}
	if store == nil {
		return nil, errors.New("result store is required")
	}
	if config.Name == "" {
		config.Name = DefaultResultConsumerName
	}
	if strings.TrimSpace(config.Name) == "" {
		return nil, errors.New("result consumer name must not be blank")
	}
	if config.MaxManifestBytes < 1 {
		return nil, errors.New("maximum result manifest size must be positive")
	}
	if config.MaxMessageBytes < 1 {
		return nil, errors.New("maximum result message size must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Jitter == nil {
		config.Jitter = randomRetryJitter
	}
	return &ResultConsumer{
		validator:        validator,
		manifestReader:   manifestReader,
		store:            store,
		name:             config.Name,
		maxManifestBytes: config.MaxManifestBytes,
		maxMessageBytes:  config.MaxMessageBytes,
		now:              config.Now,
		jitter:           config.Jitter,
	}, nil
}

func (c *ResultConsumer) Handle(
	ctx context.Context,
	metadata ResultDeliveryMetadata,
	raw []byte,
) (ResultConsumerOutcome, error) {
	if int64(len(raw)) > c.maxMessageBytes {
		return ResultConsumerOutcome{}, NewPermanentResultError(errors.New("result message exceeds configured size limit"))
	}
	message, err := c.validator.ValidateResultMessage(raw)
	if err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(fmt.Errorf("validate result message: %w", err))
	}

	switch message.Type {
	case SourceCollectCompletedType:
		return c.handleCompleted(ctx, metadata, raw, message.Payload)
	case SourceCollectFailedType:
		return c.handleFailed(ctx, metadata, raw, message.Payload)
	default:
		return ResultConsumerOutcome{}, NewPermanentResultError(
			fmt.Errorf("unsupported result message type %q", message.Type),
		)
	}
}

func (c *ResultConsumer) handleCompleted(
	ctx context.Context,
	metadata ResultDeliveryMetadata,
	raw, rawPayload []byte,
) (ResultConsumerOutcome, error) {
	var envelope resultEnvelope[SourceResult]
	if err := decodeResultEnvelope(raw, &envelope); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(fmt.Errorf("decode completed result: %w", err))
	}
	if err := validateDeliveryMetadata(metadata, envelope.MessageID, envelope.Type, SourceCollectCompletedType); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(err)
	}
	if err := ValidateSourceResult(envelope.Payload, rawPayload); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(fmt.Errorf("validate completed result: %w", err))
	}

	manifest, err := c.loadManifest(ctx, envelope.Payload)
	if err != nil {
		return ResultConsumerOutcome{}, err
	}
	var manifestModel *SourceResultManifest
	if manifest != nil {
		if err := c.validator.ValidateResultManifest(manifest); err != nil {
			return ResultConsumerOutcome{}, NewPermanentResultError(
				fmt.Errorf("validate result manifest schema: %w", err),
			)
		}
		var decoded SourceResultManifest
		if err := decodeJSON(manifest, &decoded); err != nil {
			return ResultConsumerOutcome{}, NewPermanentResultError(
				fmt.Errorf("decode result manifest: %w", err),
			)
		}
		if err := ValidateManifest(envelope.Payload, decoded, manifest); err != nil {
			return ResultConsumerOutcome{}, NewPermanentResultError(
				fmt.Errorf("validate result manifest: %w", err),
			)
		}
		manifestModel = &decoded
	}

	runs, err := BuildProcessingRunInputs(envelope.Payload, manifestModel)
	if err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(
			fmt.Errorf("build processing inputs: %w", err),
		)
	}
	status, err := completedResultStatus(envelope.Payload.Outcome)
	if err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(err)
	}
	artifacts := append([]ArtifactReference(nil), envelope.Payload.Artifacts...)
	if envelope.Payload.Manifest != nil {
		artifacts = append(artifacts, *envelope.Payload.Manifest)
	}
	return c.apply(ctx, envelope.MessageID, envelope.CorrelationID, SourceResultCommand{
		ID:             uuid.NewString(),
		MessageID:      envelope.MessageID,
		CorrelationID:  envelope.CorrelationID,
		JobID:          envelope.Payload.JobID,
		AttemptNo:      envelope.Payload.AttemptNo,
		Digest:         envelope.Payload.Digest,
		AdapterVersion: envelope.Payload.AdapterVersion,
		Status:         status,
		Artifacts:      artifacts,
		ProcessingRuns: runs,
		Claims:         resultClaims(envelope.Payload, manifestModel),
	})
}

func (c *ResultConsumer) handleFailed(
	ctx context.Context,
	metadata ResultDeliveryMetadata,
	raw, rawPayload []byte,
) (ResultConsumerOutcome, error) {
	var envelope resultEnvelope[SourceCollectFailedPayload]
	if err := decodeResultEnvelope(raw, &envelope); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(fmt.Errorf("decode failed result: %w", err))
	}
	if err := validateDeliveryMetadata(metadata, envelope.MessageID, envelope.Type, SourceCollectFailedType); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(err)
	}
	if err := validateFailedResult(envelope.Payload); err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(fmt.Errorf("validate failed result: %w", err))
	}
	digest, err := canonicalPayloadDigest(rawPayload)
	if err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(
			fmt.Errorf("calculate failed result digest: %w", err),
		)
	}
	status, err := failedResultStatus(envelope.Payload.Error.Classification)
	if err != nil {
		return ResultConsumerOutcome{}, NewPermanentResultError(err)
	}
	messageText := envelope.Payload.Error.Message
	if envelope.Payload.Error.Classification == "bug" {
		messageText = "source adapter failed unexpectedly"
	}
	command := SourceResultCommand{
		ID:             uuid.NewString(),
		MessageID:      envelope.MessageID,
		CorrelationID:  envelope.CorrelationID,
		JobID:          envelope.Payload.JobID,
		AttemptNo:      envelope.Payload.AttemptNo,
		Digest:         digest,
		AdapterVersion: envelope.Payload.AdapterVersion,
		Status:         status,
		ErrorCode:      envelope.Payload.Error.Code,
		ErrorMessage:   messageText,
		Artifacts:      envelope.Payload.Artifacts,
	}
	if status == ResultStatusRetryWait {
		command.NextAttemptAt = c.now().Add(c.retryDelay(envelope.Payload.AttemptNo, envelope.Payload.Error.RetryAfterSeconds))
	}
	return c.apply(ctx, envelope.MessageID, envelope.CorrelationID, command)
}

func (c *ResultConsumer) loadManifest(
	ctx context.Context,
	result SourceResult,
) ([]byte, error) {
	if result.Manifest == nil {
		return nil, nil
	}
	if result.Manifest.ByteSize > c.maxManifestBytes {
		return nil, NewPermanentResultError(errors.New("result manifest exceeds configured size limit"))
	}
	manifest, err := c.manifestReader.Read(ctx, result.Manifest.ObjectKey, c.maxManifestBytes)
	if err != nil {
		if IsPermanentResultError(err) {
			return nil, err
		}
		return nil, fmt.Errorf("read result manifest: %w", err)
	}
	if int64(len(manifest)) > c.maxManifestBytes {
		return nil, NewPermanentResultError(errors.New("result manifest exceeds configured size limit"))
	}
	return manifest, nil
}

func (c *ResultConsumer) apply(
	ctx context.Context,
	messageID, correlationID string,
	command SourceResultCommand,
) (ResultConsumerOutcome, error) {
	applied, err := c.store.ApplySourceResult(ctx, c.name, command)
	if err != nil {
		return ResultConsumerOutcome{}, err
	}
	return ResultConsumerOutcome{
		MessageID:     messageID,
		CorrelationID: correlationID,
		JobID:         command.JobID,
		AttemptNo:     command.AttemptNo,
		Status:        applied.Status,
		Applied:       applied.Applied,
		Duplicate:     applied.Duplicate,
		IgnoredReason: applied.IgnoredReason,
	}, nil
}

type resultEnvelope[T any] struct {
	MessageID     string `json:"message_id"`
	Type          string `json:"type"`
	SchemaVersion int    `json:"schema_version"`
	CorrelationID string `json:"correlation_id"`
	Payload       T      `json:"payload"`
}

func decodeResultEnvelope[T any](raw []byte, target *resultEnvelope[T]) error {
	return decodeJSON(raw, target)
}

func decodeJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected data after JSON document")
		}
		return err
	}
	return nil
}

func validateDeliveryMetadata(
	metadata ResultDeliveryMetadata,
	messageID, messageType, expectedType string,
) error {
	if messageType != expectedType {
		return fmt.Errorf("unexpected result message type %q", messageType)
	}
	if metadata.MessageID == "" || metadata.MessageID != messageID {
		return errors.New("RabbitMQ message_id does not match the result envelope")
	}
	if metadata.Type != messageType || metadata.RoutingKey != messageType {
		return errors.New("RabbitMQ type or routing key does not match the result envelope")
	}
	if _, err := uuid.Parse(messageID); err != nil {
		return errors.New("result message_id is not a UUID")
	}
	return nil
}

func validateFailedResult(result SourceCollectFailedPayload) error {
	if strings.TrimSpace(result.AdapterVersion) == "" ||
		strings.ContainsRune(result.AdapterVersion, '\x00') {
		return errors.New("failed result adapter_version is invalid")
	}
	if strings.TrimSpace(result.Error.Message) == "" ||
		strings.ContainsRune(result.Error.Message, '\x00') {
		return errors.New("failed result message is invalid")
	}
	if err := ValidateArtifactRefs(result.Artifacts, result.JobID, result.AttemptNo); err != nil {
		return err
	}
	classification, ok := errorClassifications[result.Error.Code]
	if !ok || classification != result.Error.Classification {
		return errors.New("failed result code and classification do not match")
	}
	if result.Error.RetryAfterSeconds != nil && *result.Error.RetryAfterSeconds < 1 {
		return errors.New("retry_after_seconds is invalid for this failure")
	}
	return nil
}

var errorClassifications = map[string]string{
	"SOURCE_TIMEOUT":               "transient",
	"SOURCE_NETWORK_ERROR":         "transient",
	"SOURCE_RATE_LIMITED":          "rate_limited",
	"SOURCE_UNAVAILABLE":           "transient",
	"SOURCE_ACCESS_DENIED":         "permanent",
	"SOURCE_ACTION_REQUIRED":       "action_required",
	"SOURCE_FORMAT_CHANGED":        "permanent",
	"SOURCE_INVALID_RESPONSE":      "permanent",
	"SOURCE_UNSUPPORTED_OPERATION": "permanent",
	"CONTRACT_VALIDATION_FAILED":   "bug",
	"INTERNAL_ADAPTER_ERROR":       "bug",
}

func completedResultStatus(outcome string) (ResultStatus, error) {
	switch outcome {
	case "succeeded":
		return ResultStatusSucceeded, nil
	case "not_found":
		return ResultStatusNotFound, nil
	default:
		return "", fmt.Errorf("unsupported completed result outcome %q", outcome)
	}
}

func failedResultStatus(classification string) (ResultStatus, error) {
	switch classification {
	case "transient", "rate_limited":
		return ResultStatusRetryWait, nil
	case "action_required":
		return ResultStatusActionRequired, nil
	case "permanent", "bug":
		return ResultStatusFailed, nil
	default:
		return "", fmt.Errorf("unsupported failure classification %q", classification)
	}
}

func resultClaims(result SourceResult, manifest *SourceResultManifest) []Claim {
	if manifest != nil {
		return manifest.Claims
	}
	return result.Claims
}

func canonicalPayloadDigest(raw []byte) (string, error) {
	canonical, err := contractjson.Canonicalize(raw)
	if err != nil {
		return "", fmt.Errorf("canonicalize result payload: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}

func (c *ResultConsumer) retryDelay(attemptNo int, retryAfterSeconds *int) time.Duration {
	backoffs := [...]time.Duration{30 * time.Second, 2 * time.Minute, 10 * time.Minute}
	index := attemptNo - 1
	if index >= len(backoffs) {
		index = len(backoffs) - 1
	}
	delay := c.jitter(backoffs[index])
	if delay < 0 {
		delay = 0
	}
	if retryAfterSeconds != nil {
		retryAfter := maxRetryDuration
		if int64(*retryAfterSeconds) <= int64(maxRetryDuration/time.Second) {
			retryAfter = time.Duration(*retryAfterSeconds) * time.Second
		}
		if retryAfter > delay {
			delay = retryAfter
		}
	}
	if delay > maxRetryDuration {
		return maxRetryDuration
	}
	return delay
}

const maxRetryDuration = 24 * time.Hour

func randomRetryJitter(base time.Duration) time.Duration {
	return base*3/4 + time.Duration(rand.Int63n(int64(base/2)+1))
}
