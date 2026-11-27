package investigations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
)

type Subject struct {
	Type        string
	Name        *string
	Identifiers map[string]string
}

type Limits struct {
	Depth       int
	EntityLimit int
	Sources     []string
}

type CreateCommand struct {
	IdempotencyKey  string
	RequestHash     string
	RawSubject      []byte
	Subject         Subject
	Limits          Limits
	InvestigationID string
	InitialJobs     []InitialJob
}

type InitialJob struct {
	JobID  string
	Source Source
	Outbox OutboxMessage
}

type Source struct {
	ID   string
	Code string
}

type OutboxMessage interface {
	OutboxID() string
	MessageType() string
	AggregateID() string
	InvestigationID() string
	Operation() string
	AttemptNo() int
	JSONPayload() string
}

type OutboxFactory interface {
	PrepareSourceCollect(
		investigationID, jobID, outboxID, sourceCode string,
		command CreateCommand,
		now time.Time,
	) (OutboxMessage, error)
}

type Accepted struct {
	ID        string
	StatusURL string
}

type Store interface {
	CheckIdempotency(ctx context.Context, key, requestHash string) (Accepted, bool, error)
	ResolveSources(ctx context.Context, codes []string) ([]Source, error)
	Create(ctx context.Context, command CreateCommand, sources []Source) (Accepted, error)
}

type Creator struct {
	store         Store
	outboxFactory OutboxFactory
}

type CreateUseCase interface {
	Create(ctx context.Context, command CreateCommand) (Accepted, error)
}

func NewCreator(store Store, outboxFactories ...OutboxFactory) (*Creator, error) {
	if store == nil {
		return nil, errors.New("investigation store is required")
	}
	if len(outboxFactories) == 0 || outboxFactories[0] == nil {
		return nil, errors.New("outbox factory is required")
	}
	return &Creator{store: store, outboxFactory: outboxFactories[0]}, nil
}

func (c *Creator) Create(ctx context.Context, command CreateCommand) (Accepted, error) {
	if strings.TrimSpace(command.IdempotencyKey) == "" {
		return Accepted{}, errors.New("idempotency key is required")
	}
	if len(command.RequestHash) != 64 {
		return Accepted{}, errors.New("request hash is required")
	}
	if len(command.RawSubject) == 0 {
		return Accepted{}, errors.New("raw subject is required")
	}
	normalizedSubject, err := NormalizeSubject(command.Subject)
	if err != nil {
		return Accepted{}, err
	}
	command.RawSubject = normalizedRawSubject(command.RawSubject)

	accepted, replayed, err := c.store.CheckIdempotency(ctx, command.IdempotencyKey, command.RequestHash)
	if err != nil {
		return Accepted{}, err
	}
	if replayed {
		return accepted, nil
	}

	sources, err := c.store.ResolveSources(ctx, append([]string(nil), command.Limits.Sources...))
	if err != nil {
		return Accepted{}, err
	}

	command.Subject = normalizedSubject
	investigationID, err := uuid.NewRandom()
	if err != nil {
		return Accepted{}, fmt.Errorf("generate investigation id: %w", err)
	}
	command.InvestigationID = investigationID.String()
	now := time.Now().UTC()
	command.InitialJobs = make([]InitialJob, 0, len(sources))
	for _, source := range sources {
		jobID, err := uuid.NewRandom()
		if err != nil {
			return Accepted{}, fmt.Errorf("generate source job id: %w", err)
		}
		outboxID, err := uuid.NewRandom()
		if err != nil {
			return Accepted{}, fmt.Errorf("generate outbox id: %w", err)
		}
		message, err := c.outboxFactory.PrepareSourceCollect(
			command.InvestigationID,
			jobID.String(),
			outboxID.String(),
			source.Code,
			command,
			now,
		)
		if err != nil {
			return Accepted{}, err
		}
		command.InitialJobs = append(command.InitialJobs, InitialJob{
			JobID:  jobID.String(),
			Source: source,
			Outbox: message,
		})
	}
	return c.store.Create(ctx, command, sources)
}

func NormalizeSubject(subject Subject) (Subject, error) {
	subjectType := strings.TrimSpace(subject.Type)
	if subjectType != "person" && subjectType != "organization" {
		return Subject{}, errors.New("subject type must be person or organization")
	}

	normalized := Subject{Type: subjectType, Identifiers: make(map[string]string)}
	if subject.Name != nil {
		name := normalizeText(*subject.Name)
		if name == "" {
			return Subject{}, errors.New("subject name must not be empty")
		}
		normalized.Name = &name
	}
	for name, value := range subject.Identifiers {
		key := normalizeIdentifierName(name)
		val := strings.TrimSpace(value)
		if key == "" || val == "" {
			return Subject{}, errors.New("subject identifiers must not be empty")
		}
		if existing, ok := normalized.Identifiers[key]; ok && existing != val {
			return Subject{}, fmt.Errorf("conflicting normalized identifier %q", key)
		}
		normalized.Identifiers[key] = val
	}
	if normalized.Name == nil && len(normalized.Identifiers) == 0 {
		return Subject{}, errors.New("subject must contain name or identifiers")
	}
	return normalized, nil
}

func NormalizeIdentifierName(name string) string {
	return normalizeIdentifierName(name)
}

func normalizeIdentifierName(name string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(name)), "_"))
}

func normalizeText(value string) string {
	value = strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return unicode.ToLower(r)
	}, value)
}

func normalizedRawSubject(raw []byte) []byte {
	canonical, err := contractjson.Canonicalize(raw)
	if err == nil {
		return canonical
	}
	return raw
}

type SourceSelectionError struct {
	Unknown  []string
	Disabled []string
}

func (e *SourceSelectionError) Error() string {
	return "selected sources are unknown or disabled"
}

type IdempotencyConflictError struct {
	Key string
}

func (e *IdempotencyConflictError) Error() string {
	return "idempotency key was already used with a different request"
}

type IdempotencyInProgressError struct {
	Key string
}

func (e *IdempotencyInProgressError) Error() string {
	return "idempotency key has a request in progress"
}

func MarshalNormalizedSubject(subject Subject) ([]byte, error) {
	value := map[string]any{"type": subject.Type}
	if subject.Name != nil {
		value["name"] = *subject.Name
	}
	if len(subject.Identifiers) > 0 {
		value["identifiers"] = subject.Identifiers
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return contractjson.Canonicalize(raw)
}
