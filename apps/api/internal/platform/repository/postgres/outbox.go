package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

type OutboxStore struct {
	pool *pgxpool.Pool
}

func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore {
	return &OutboxStore{pool: pool}
}

func (s *OutboxStore) Claim(ctx context.Context, leaseDuration time.Duration) (*outbox.Message, error) {
	if s.pool == nil {
		return nil, errors.New("outbox store requires a PostgreSQL pool")
	}
	leaseMillis := leaseDuration.Milliseconds()
	if leaseMillis < 1 {
		return nil, errors.New("outbox lease duration must be at least one millisecond")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin outbox claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	token, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("generate outbox lease token: %w", err)
	}
	leaseToken := token.String()
	var message outbox.Message
	err = tx.QueryRow(ctx, `
		with candidate as (
			select id
			from outbox_messages
			where published_at is null
			  and available_at <= now()
			  and (lease_until is null or lease_until <= now())
			order by available_at, created_at, id
			for update skip locked
			limit 1
		)
		update outbox_messages as message
		set lease_token = $1,
			lease_until = now() + ($2::bigint * interval '1 millisecond')
		from candidate
		where message.id = candidate.id
		returning
			message.id::text,
			message.message_type,
			message.aggregate_type,
			message.aggregate_id::text,
			convert_to(message.payload::text, 'UTF8'),
			message.publish_attempts,
			message.lease_token::text
	`, leaseToken, leaseMillis).Scan(
		&message.ID,
		&message.Type,
		&message.AggregateType,
		&message.AggregateID,
		&message.Payload,
		&message.PublishAttempts,
		&message.LeaseToken,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit empty outbox claim: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim pending outbox message: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit outbox claim: %w", err)
	}

	return &message, nil
}

func (s *OutboxStore) MarkPublished(
	ctx context.Context,
	message outbox.Message,
	attemptNo int,
) error {
	if s.pool == nil {
		return errors.New("outbox store requires a PostgreSQL pool")
	}
	if message.AggregateType == "source_job" && attemptNo < 1 {
		return errors.New("source job publication requires a positive attempt number")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin outbox completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		update outbox_messages
		set published_at = now(),
			last_error = null,
			lease_token = null,
			lease_until = null
		where id = $1
		  and lease_token = $2
		  and lease_until > now()
		  and published_at is null
	`, message.ID, message.LeaseToken)
	if err != nil {
		return fmt.Errorf("mark outbox message published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w before completing message %s", outbox.ErrLeaseLost, message.ID)
	}

	if message.AggregateType == "source_job" {
		var jobStatus, investigationID string
		var currentAttempt int
		err := tx.QueryRow(ctx, `
			select status, attempt_no, investigation_id::text
			from source_jobs
			where id = $1
			for update
		`, message.AggregateID).Scan(&jobStatus, &currentAttempt, &investigationID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock source job after publish confirm: %w", err)
		}
		if err == nil && jobStatus == "pending" && currentAttempt == attemptNo {
			var investigationStatus string
			if err := tx.QueryRow(ctx, `
				select status
				from investigations
				where id = $1
				for update
			`, investigationID).Scan(&investigationStatus); err != nil {
				return fmt.Errorf("lock source job investigation after publish confirm: %w", err)
			}
			if investigationStatus == "running" {
				if _, err := tx.Exec(ctx, `
			update source_jobs as job
			set status = 'queued',
				queued_at = coalesce(job.queued_at, now()),
				updated_at = now()
			where job.id = $1
			  and job.status = 'pending'
			  and job.attempt_no = $2
			`, message.AggregateID, attemptNo); err != nil {
					return fmt.Errorf("mark source job queued after publish confirm: %w", err)
				}
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit published outbox message: %w", err)
	}
	return nil
}

func (s *OutboxStore) Reschedule(ctx context.Context, message outbox.Message, delay time.Duration) error {
	if s.pool == nil {
		return errors.New("outbox store requires a PostgreSQL pool")
	}
	delayMillis := delay.Milliseconds()
	if delayMillis < 1 {
		return errors.New("outbox retry delay must be at least one millisecond")
	}

	tag, err := s.pool.Exec(ctx, `
		update outbox_messages
		set publish_attempts = least(publish_attempts::bigint + 1, 2147483647)::integer,
			available_at = now() + ($3::bigint * interval '1 millisecond'),
			last_error = 'publisher_failed',
			lease_token = null,
			lease_until = null
		where id = $1
		  and lease_token = $2
		  and lease_until > now()
		  and published_at is null
	`, message.ID, message.LeaseToken, delayMillis)
	if err != nil {
		return fmt.Errorf("reschedule outbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w before rescheduling message %s", outbox.ErrLeaseLost, message.ID)
	}
	return nil
}
