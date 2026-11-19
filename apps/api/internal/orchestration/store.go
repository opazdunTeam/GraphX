package orchestration

import (
	"context"
	"time"
)

type CreateJob struct {
	ID              string
	InvestigationID string
	SourceID        string
	Operation       string
	Purpose         string
	RequestHash     string
	IdempotencyKey  string
	MaxAttempts     int
}

type ResultReceipt struct {
	ID            string
	MessageID     string
	JobID         string
	AttemptNo     int
	Digest        string
	Status        Status
	NextAttemptAt time.Time
	ErrorCode     string
	ErrorMessage  string
}

type OutboxMessage interface {
	OutboxID() string
	MessageType() string
	AggregateID() string
	AttemptNo() int
	JSONPayload() string
}

type Store interface {
	Create(ctx context.Context, job CreateJob, message OutboxMessage) error
	MarkQueued(ctx context.Context, jobID string, attemptNo int) (bool, error)
	ScheduleRetry(ctx context.Context, jobID string, nextAttemptAt time.Time, errorCode, errorMessage string) error
	StartNewAttempt(ctx context.Context, jobID string, expectedAttemptNo int, message OutboxMessage) error
	AdvanceTerminal(ctx context.Context, jobID string, status Status, errorCode, errorMessage string) error
	Cancel(ctx context.Context, jobID string) error
	ApplyResult(ctx context.Context, consumerName string, receipt ResultReceipt) (applied bool, duplicate bool, err error)
}
