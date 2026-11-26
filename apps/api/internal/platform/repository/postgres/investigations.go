package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opazdunTeam/GraphX/apps/api/internal/contractjson"
	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
)

const investigationIdempotencyScope = "investigation"
const investigationIdempotencyTTL = 24 * time.Hour

type InvestigationStore struct {
	pool *pgxpool.Pool
}

func NewInvestigationStore(pool *pgxpool.Pool) *InvestigationStore {
	return &InvestigationStore{pool: pool}
}

func (s *InvestigationStore) CheckIdempotency(
	ctx context.Context,
	key, requestHash string,
) (investigations.Accepted, bool, error) {
	if s.pool == nil {
		return investigations.Accepted{}, false, errors.New("investigation store requires a PostgreSQL pool")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return investigations.Accepted{}, false, fmt.Errorf("begin idempotency lookup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var storedHash string
	var responseStatus *int
	var responseBody []byte
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		select request_hash, response_status, response_body, expires_at
		from api_idempotency_keys
		where scope = $1 and idempotency_key = $2
		for update
	`, investigationIdempotencyScope, key).Scan(
		&storedHash, &responseStatus, &responseBody, &expiresAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return investigations.Accepted{}, false, fmt.Errorf("commit idempotency lookup: %w", err)
		}
		return investigations.Accepted{}, false, nil
	}
	if err != nil {
		return investigations.Accepted{}, false, fmt.Errorf("lock investigation idempotency: %w", err)
	}
	if expiresAt.Before(time.Now()) {
		if err := tx.Commit(ctx); err != nil {
			return investigations.Accepted{}, false, fmt.Errorf("commit expired idempotency lookup: %w", err)
		}
		return investigations.Accepted{}, false, nil
	}
	if storedHash != requestHash {
		return investigations.Accepted{}, false, &investigations.IdempotencyConflictError{Key: key}
	}
	if responseStatus == nil {
		return investigations.Accepted{}, false, &investigations.IdempotencyInProgressError{Key: key}
	}

	var replay struct {
		ID        string `json:"id"`
		StatusURL string `json:"status_url"`
	}
	if err := json.Unmarshal(responseBody, &replay); err != nil {
		return investigations.Accepted{}, false, fmt.Errorf("decode replayed investigation response: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return investigations.Accepted{}, false, fmt.Errorf("commit replayed investigation lookup: %w", err)
	}
	return investigations.Accepted{ID: replay.ID, StatusURL: replay.StatusURL}, true, nil
}

func (s *InvestigationStore) ResolveSources(
	ctx context.Context,
	codes []string,
) ([]investigations.Source, error) {
	if s.pool == nil {
		return nil, errors.New("investigation store requires a PostgreSQL pool")
	}

	if len(codes) == 0 {
		rows, err := s.pool.Query(ctx, `
			select id::text, code
			from sources
			where enabled
			order by launch_order, code, id
		`)
		if err != nil {
			return nil, fmt.Errorf("resolve enabled sources: %w", err)
		}
		defer rows.Close()
		var sources []investigations.Source
		for rows.Next() {
			var source investigations.Source
			if err := rows.Scan(&source.ID, &source.Code); err != nil {
				return nil, fmt.Errorf("read enabled source: %w", err)
			}
			sources = append(sources, source)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterate enabled sources: %w", err)
		}
		if len(sources) == 0 {
			return nil, &investigations.SourceSelectionError{Disabled: []string{"<none enabled>"}}
		}
		return sources, nil
	}

	seen := make(map[string]struct{}, len(codes))
	uniqueCodes := make([]string, 0, len(codes))
	for _, code := range codes {
		if code == "" {
			continue
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		uniqueCodes = append(uniqueCodes, code)
	}

	rows, err := s.pool.Query(ctx, `
		select id::text, code, enabled
		from sources
		where code = any($1)
		order by launch_order, code, id
	`, uniqueCodes)
	if err != nil {
		return nil, fmt.Errorf("resolve selected sources: %w", err)
	}
	defer rows.Close()

	found := make(map[string]struct{}, len(uniqueCodes))
	var sources []investigations.Source
	var disabled []string
	for rows.Next() {
		var source investigations.Source
		var enabled bool
		if err := rows.Scan(&source.ID, &source.Code, &enabled); err != nil {
			return nil, fmt.Errorf("read selected source: %w", err)
		}
		found[source.Code] = struct{}{}
		if enabled {
			sources = append(sources, source)
		} else {
			disabled = append(disabled, source.Code)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate selected sources: %w", err)
	}

	var unknown []string
	for _, code := range uniqueCodes {
		if _, ok := found[code]; !ok {
			unknown = append(unknown, code)
		}
	}
	if len(unknown) > 0 || len(disabled) > 0 {
		return nil, &investigations.SourceSelectionError{Unknown: unknown, Disabled: disabled}
	}
	return sources, nil
}

func (s *InvestigationStore) Create(
	ctx context.Context,
	command investigations.CreateCommand,
	sources []investigations.Source,
) (investigations.Accepted, error) {
	if s.pool == nil {
		return investigations.Accepted{}, errors.New("investigation store requires a PostgreSQL pool")
	}
	if len(sources) == 0 {
		return investigations.Accepted{}, &investigations.SourceSelectionError{Disabled: []string{"<none enabled>"}}
	}
	if len(command.InitialJobs) != len(sources) {
		return investigations.Accepted{}, errors.New("initial source jobs do not match selected sources")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("begin investigation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		insert into api_idempotency_keys (
			scope, idempotency_key, request_hash, expires_at
		) values ($1, $2, $3, now() + ($4::bigint * interval '1 second'))
		on conflict (scope, idempotency_key) do nothing
	`, investigationIdempotencyScope, command.IdempotencyKey, command.RequestHash,
		int64(investigationIdempotencyTTL/time.Second))
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("claim investigation idempotency: %w", err)
	}
	newKey := tag.RowsAffected() == 1

	var storedHash string
	var responseStatus *int
	var responseBody []byte
	var expiresAt time.Time
	err = tx.QueryRow(ctx, `
		select request_hash, response_status, response_body, expires_at
		from api_idempotency_keys
		where scope = $1 and idempotency_key = $2
		for update
	`, investigationIdempotencyScope, command.IdempotencyKey).Scan(
		&storedHash, &responseStatus, &responseBody, &expiresAt,
	)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("lock investigation idempotency: %w", err)
	}
	if !newKey && expiresAt.Before(time.Now()) {
		if _, err := tx.Exec(ctx, `
			delete from api_idempotency_keys
			where scope = $1 and idempotency_key = $2
			  and expires_at <= now()
		`, investigationIdempotencyScope, command.IdempotencyKey); err != nil {
			return investigations.Accepted{}, fmt.Errorf("remove expired investigation idempotency: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into api_idempotency_keys (
				scope, idempotency_key, request_hash, expires_at
			) values ($1, $2, $3, now() + ($4::bigint * interval '1 second'))
		`, investigationIdempotencyScope, command.IdempotencyKey, command.RequestHash,
			int64(investigationIdempotencyTTL/time.Second)); err != nil {
			return investigations.Accepted{}, fmt.Errorf("renew investigation idempotency: %w", err)
		}
		responseStatus = nil
		responseBody = nil
	} else if !newKey && storedHash != command.RequestHash {
		return investigations.Accepted{}, &investigations.IdempotencyConflictError{Key: command.IdempotencyKey}
	} else if !newKey && responseStatus != nil {
		var replay struct {
			ID        string `json:"id"`
			StatusURL string `json:"status_url"`
		}
		if err := json.Unmarshal(responseBody, &replay); err != nil {
			return investigations.Accepted{}, fmt.Errorf("decode replayed investigation response: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return investigations.Accepted{}, fmt.Errorf("commit replayed investigation response: %w", err)
		}
		return investigations.Accepted{ID: replay.ID, StatusURL: replay.StatusURL}, nil
	} else if !newKey {
		return investigations.Accepted{}, &investigations.IdempotencyInProgressError{Key: command.IdempotencyKey}
	}

	if err := s.revalidateSources(ctx, tx, sources); err != nil {
		return investigations.Accepted{}, err
	}

	if command.InvestigationID == "" {
		return investigations.Accepted{}, errors.New("investigation id is required")
	}
	investigationID, err := uuid.Parse(command.InvestigationID)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("parse investigation id: %w", err)
	}
	normalizedSubject, err := investigations.MarshalNormalizedSubject(command.Subject)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("serialize normalized subject: %w", err)
	}
	rawSubject, err := contractjson.Canonicalize(command.RawSubject)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("serialize raw subject: %w", err)
	}
	limitsValue := map[string]any{
		"depth":        command.Limits.Depth,
		"entity_limit": command.Limits.EntityLimit,
	}
	if len(command.Limits.Sources) > 0 {
		limitsValue["sources"] = command.Limits.Sources
	}
	limitsRaw, err := json.Marshal(limitsValue)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("serialize investigation limits: %w", err)
	}
	limitsRaw, err = contractjson.Canonicalize(limitsRaw)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("canonicalize investigation limits: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		insert into investigations (
			id, subject_type, subject_query, raw_subject, normalized_subject,
			status, stage, limits, state_version
		) values (
			$1, $2, $3::jsonb, $3::jsonb, $4::jsonb,
			'created', 'subject_discovery', $5::jsonb, 1
		)
	`, investigationID.String(), command.Subject.Type, string(rawSubject),
		string(normalizedSubject), string(limitsRaw)); err != nil {
		return investigations.Accepted{}, fmt.Errorf("insert investigation: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		update investigations
		set status = 'running', updated_at = now()
		where id = $1
	`, investigationID); err != nil {
		return investigations.Accepted{}, fmt.Errorf("start investigation: %w", err)
	}

	for _, initialJob := range command.InitialJobs {
		jobID, err := uuid.Parse(initialJob.JobID)
		if err != nil {
			return investigations.Accepted{}, fmt.Errorf("parse source job id: %w", err)
		}
		message := initialJob.Outbox
		if message.AggregateID() != jobID.String() ||
			strings.TrimSpace(message.Operation()) == "" ||
			message.AttemptNo() != 1 {
			return investigations.Accepted{}, fmt.Errorf("prepared source collect identity or operation mismatch")
		}
		source := initialJob.Source

		jobIdempotencyKey := fmt.Sprintf("%s:%s:attempt1", investigationID, source.Code)
		if _, err := tx.Exec(ctx, `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose,
				status, attempt_no, max_attempts, request_hash, idempotency_key
			) values (
				$1, $2, $3, $4, 'subject_discovery',
				'pending', 1, 3, $5, $6
			)
		`, jobID, investigationID, source.ID, message.Operation(),
			command.RequestHash, jobIdempotencyKey); err != nil {
			return investigations.Accepted{}, fmt.Errorf("insert initial source job: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into outbox_messages (
				id, message_type, aggregate_type, aggregate_id, payload
			) values ($1, $2, 'source_job', $3, $4::jsonb)
		`, message.OutboxID(), message.MessageType(), jobID, message.JSONPayload()); err != nil {
			return investigations.Accepted{}, fmt.Errorf("insert initial source outbox: %w", err)
		}
	}

	statusURL := "/api/v1/investigations/" + investigationID.String() + "/status"
	responseBody, err = json.Marshal(map[string]any{
		"id":         investigationID.String(),
		"status":     "running",
		"stage":      "subject_discovery",
		"status_url": statusURL,
	})
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("serialize investigation response: %w", err)
	}
	responseBody, err = contractjson.Canonicalize(responseBody)
	if err != nil {
		return investigations.Accepted{}, fmt.Errorf("canonicalize investigation response: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		update api_idempotency_keys
		set response_status = 202, response_body = $3::jsonb
		where scope = $1 and idempotency_key = $2
	`, investigationIdempotencyScope, command.IdempotencyKey, string(responseBody)); err != nil {
		return investigations.Accepted{}, fmt.Errorf("store investigation idempotency response: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return investigations.Accepted{}, fmt.Errorf("commit investigation transaction: %w", err)
	}
	return investigations.Accepted{ID: investigationID.String(), StatusURL: statusURL}, nil
}

func (s *InvestigationStore) revalidateSources(
	ctx context.Context,
	tx pgx.Tx,
	sources []investigations.Source,
) error {
	sourceIDs := make([]string, 0, len(sources))
	for _, source := range sources {
		sourceIDs = append(sourceIDs, source.ID)
	}
	rows, err := tx.Query(ctx, `
		select id::text, code, enabled
		from sources
		where id = any($1)
		for update
	`, sourceIDs)
	if err != nil {
		return fmt.Errorf("lock selected sources: %w", err)
	}
	defer rows.Close()

	enabled := make(map[string]string, len(sources))
	for rows.Next() {
		var id, code string
		var isEnabled bool
		if err := rows.Scan(&id, &code, &isEnabled); err != nil {
			return fmt.Errorf("read locked source: %w", err)
		}
		if isEnabled {
			enabled[id] = code
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate locked sources: %w", err)
	}
	if len(enabled) != len(sources) {
		var disabled []string
		for _, source := range sources {
			if _, ok := enabled[source.ID]; !ok {
				disabled = append(disabled, source.Code)
			}
		}
		return &investigations.SourceSelectionError{Disabled: disabled}
	}
	return nil
}
