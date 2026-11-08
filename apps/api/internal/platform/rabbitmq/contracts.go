package rabbitmq

import (
	"encoding/json"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
)

const (
	SourceCollectRequestedType = "source.collect.requested.v1"
	SourceCollectCompletedType = "source.collect.completed.v1"
	SourceCollectFailedType    = "source.collect.failed.v1"
)

type Envelope[T any] struct {
	MessageID     string    `json:"message_id"`
	Type          string    `json:"type"`
	SchemaVersion int       `json:"schema_version"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   *string   `json:"causation_id"`
	Producer      string    `json:"producer"`
	RootCommandID string    `json:"root_command_id"`
	Payload       T         `json:"payload"`
}

type SourceQuery struct {
	Type        string            `json:"type"`
	Name        *string           `json:"name,omitempty"`
	Identifiers map[string]string `json:"identifiers,omitempty"`
}

type CollectLimits struct {
	MaxRecords     int `json:"max_records"`
	MaxBytes       int `json:"max_bytes"`
	TimeoutSeconds int `json:"timeout_seconds"`
}

type SourceCollectRequestedPayload struct {
	JobID           string        `json:"job_id"`
	AttemptNo       int           `json:"attempt_no"`
	InvestigationID string        `json:"investigation_id"`
	Source          string        `json:"source"`
	Operation       string        `json:"operation"`
	Purpose         string        `json:"purpose"`
	Query           SourceQuery   `json:"query"`
	Limits          CollectLimits `json:"limits"`
	Deadline        time.Time     `json:"deadline"`
}

type SourceCollectError struct {
	Code              string `json:"code"`
	Classification    string `json:"classification"`
	Message           string `json:"message"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
}

type SourceCollectFailedPayload struct {
	JobID          string                        `json:"job_id"`
	AttemptNo      int                           `json:"attempt_no"`
	AdapterVersion string                        `json:"adapter_version"`
	Artifacts      []ingestion.ArtifactReference `json:"artifacts"`
	Error          SourceCollectError            `json:"error"`
}

type ValidatedMessage struct {
	Type          string
	SchemaVersion int
	Raw           []byte
	Payload       json.RawMessage
}

func Decode[T any](message ValidatedMessage, expectedType string) (Envelope[T], error) {
	var envelope Envelope[T]
	if message.Type != expectedType {
		return envelope, &UnexpectedMessageTypeError{Expected: expectedType, Actual: message.Type}
	}
	err := json.Unmarshal(message.Raw, &envelope)
	return envelope, err
}

type UnexpectedMessageTypeError struct {
	Expected string
	Actual   string
}

func (e *UnexpectedMessageTypeError) Error() string {
	return "unexpected message type: expected " + e.Expected + ", got " + e.Actual
}
