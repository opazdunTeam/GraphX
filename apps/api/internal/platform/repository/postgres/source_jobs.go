package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

func (s *Store) Create(ctx context.Context, job orchestration.CreateJob, outboxID, messagePayloadJSON string) error {
	if strings.TrimSpace(job.ID) == "" || strings.TrimSpace(job.InvestigationID) == "" ||
		strings.TrimSpace(job.SourceID) == "" || strings.TrimSpace(job.RequestHash) == "" ||
		strings.TrimSpace(job.IdempotencyKey) == "" {
		return errors.New("source job identity fields are required")
	}
	if job.MaxAttempts < 1 {
		job.MaxAttempts = 3
	}
	if s.pool == nil {
		return errors.New("source job creation requires a pool-backed store")
	}

	return s.WithinTransaction(ctx, func(txStore *Store) error {
		if _, err := txStore.db.Exec(ctx, `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose,
				max_attempts, request_hash, idempotency_key
			) values ($1, $2, $3, $4, $5, $6, $7, $8)
		`, job.ID, job.InvestigationID, job.SourceID, job.Operation, job.Purpose,
			job.MaxAttempts, job.RequestHash, job.IdempotencyKey); err != nil {
			return fmt.Errorf("insert source job: %w", err)
		}
		if _, err := txStore.db.Exec(ctx, `
			insert into outbox_messages (
				id, message_type, aggregate_type, aggregate_id, payload
			) values ($1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb)
		`, outboxID, job.ID, messagePayloadJSON); err != nil {
			return fmt.Errorf("insert source job outbox message: %w", err)
		}
		return nil
	})
}

func (s *Store) MarkQueued(ctx context.Context, jobID string, attemptNo int) (bool, error) {
	tag, err := s.db.Exec(ctx, `
		update source_jobs
		set status = 'queued', queued_at = coalesce(queued_at, now()), updated_at = now()
		where id = $1 and attempt_no = $2 and status = 'pending'
	`, jobID, attemptNo)
	if err != nil {
		return false, fmt.Errorf("mark source job queued: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) ScheduleRetry(
	ctx context.Context,
	jobID string,
	nextAttemptAt time.Time,
	errorCode, errorMessage string,
) error {
	tag, err := s.db.Exec(ctx, `
		update source_jobs
		set status = 'retry_wait',
			next_attempt_at = $2,
			last_error_code = $3,
			last_error_message = $4,
			updated_at = now()
		where id = $1
		  and status in ('pending', 'queued', 'running')
		  and attempt_no < max_attempts
	`, jobID, nextAttemptAt, errorCode, errorMessage)
	if err != nil {
		return fmt.Errorf("schedule source job retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("source job %s cannot be scheduled for retry", jobID)
	}
	return nil
}

func (s *Store) StartNewAttempt(
	ctx context.Context,
	jobID string,
	outboxID, messagePayloadJSON string,
) (int, error) {
	if s.pool == nil {
		return 0, errors.New("new source job attempt requires a pool-backed store")
	}

	attemptNo := 0
	err := s.WithinTransaction(ctx, func(txStore *Store) error {
		var status orchestration.Status
		var oldAttempt, maxAttempts int
		if err := txStore.db.QueryRow(ctx, `
			select status, attempt_no, max_attempts
			from source_jobs
			where id = $1
			for update
		`, jobID).Scan(&status, &oldAttempt, &maxAttempts); err != nil {
			return fmt.Errorf("lock source job: %w", err)
		}
		if err := orchestration.ValidateAttemptTransition(status, orchestration.StatusPending, oldAttempt, oldAttempt+1); err != nil {
			return err
		}
		if oldAttempt >= maxAttempts {
			return fmt.Errorf("source job %s has exhausted attempts", jobID)
		}

		attemptNo = oldAttempt + 1
		tag, err := txStore.db.Exec(ctx, `
			update source_jobs
			set status = 'pending',
				attempt_no = $2,
				next_attempt_at = null,
				updated_at = now()
			where id = $1
		`, jobID, attemptNo)
		if err != nil {
			return fmt.Errorf("start source job attempt: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("source job %s was not updated", jobID)
		}
		if _, err := txStore.db.Exec(ctx, `
			insert into outbox_messages (
				id, message_type, aggregate_type, aggregate_id, payload
			) values ($1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb)
		`, outboxID, jobID, messagePayloadJSON); err != nil {
			return fmt.Errorf("insert retry outbox message: %w", err)
		}
		return nil
	})
	return attemptNo, err
}

func (s *Store) AdvanceTerminal(
	ctx context.Context,
	jobID string,
	status orchestration.Status,
	errorCode, errorMessage string,
) error {
	if !orchestration.IsTerminal(status) && status != orchestration.StatusActionRequired {
		return fmt.Errorf("status %s is not terminal or action_required", status)
	}
	tag, err := s.db.Exec(ctx, `
		update source_jobs
		set status = $2,
			last_error_code = $3,
			last_error_message = $4,
			finished_at = coalesce(finished_at, now()),
			updated_at = now()
		where id = $1
		  and status in ('pending', 'queued', 'running')
	`, jobID, string(status), nullableText(errorCode), nullableText(errorMessage))
	if err != nil {
		return fmt.Errorf("advance source job terminal status: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("source job %s cannot transition to terminal status %s", jobID, status)
	}
	return nil
}

func (s *Store) Cancel(ctx context.Context, jobID string) error {
	tag, err := s.db.Exec(ctx, `
		update source_jobs
		set status = 'cancelled',
			finished_at = coalesce(finished_at, now()),
			next_attempt_at = null,
			updated_at = now()
		where id = $1
		  and status in ('pending', 'queued', 'running', 'retry_wait', 'action_required')
	`, jobID)
	if err != nil {
		return fmt.Errorf("cancel source job: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("source job %s is already terminal or cannot be cancelled", jobID)
	}
	return nil
}

func (s *Store) ApplyResult(
	ctx context.Context,
	consumerName string,
	receipt orchestration.ResultReceipt,
) (bool, bool, error) {
	if s.pool == nil {
		return false, false, errors.New("source job result requires a pool-backed store")
	}

	applied := false
	duplicate := false
	err := s.WithinTransaction(ctx, func(txStore *Store) error {
		var inserted bool
		err := txStore.db.QueryRow(ctx, `
			insert into consumed_messages (consumer_name, message_id)
			values ($1, $2)
			on conflict (consumer_name, message_id) do nothing
			returning true
		`, consumerName, receipt.MessageID).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			duplicate = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("register consumed result message: %w", err)
		}

		var status orchestration.Status
		var attemptNo int
		var investigationStatus string
		if err := txStore.db.QueryRow(ctx, `
			select job.status, job.attempt_no, investigation.status
			from source_jobs as job
			join investigations as investigation on investigation.id = job.investigation_id
			where job.id = $1
			for update of job
		`, receipt.JobID).Scan(&status, &attemptNo, &investigationStatus); err != nil {
			return fmt.Errorf("lock source job result: %w", err)
		}
		if receipt.AttemptNo != attemptNo {
			return txStore.insertIgnoredReceipt(ctx, receipt, "stale_attempt")
		}
		if orchestration.IsTerminal(status) || investigationStatus != "running" {
			reason := "terminal_job"
			if investigationStatus != "running" {
				reason = "late_cancelled_investigation"
			}
			return txStore.insertIgnoredReceipt(ctx, receipt, reason)
		}
		if status != orchestration.StatusPending && status != orchestration.StatusQueued &&
			status != orchestration.StatusRunning {
			return txStore.insertIgnoredReceipt(ctx, receipt, "job_not_accepting_result")
		}
		if !receipt.Applied {
			reason := receipt.IgnoredReason
			if strings.TrimSpace(reason) == "" {
				reason = "not_applied"
			}
			return txStore.insertIgnoredReceipt(ctx, receipt, reason)
		}

		var existingDigest string
		err = txStore.db.QueryRow(ctx, `
			select result_digest
			from source_job_results
			where source_job_id = $1 and attempt_no = $2
		`, receipt.JobID, receipt.AttemptNo).Scan(&existingDigest)
		switch {
		case err == nil:
			if existingDigest != receipt.Digest {
				return fmt.Errorf("conflicting result digest for source job %s attempt %d", receipt.JobID, receipt.AttemptNo)
			}
			duplicate = true
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("read existing source job result: %w", err)
		}

		if _, err := txStore.db.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied
			) values ($1, $2, $3, $4, $5, true)
		`, receipt.ID, receipt.JobID, receipt.MessageID, receipt.AttemptNo, receipt.Digest); err != nil {
			return fmt.Errorf("insert source job result: %w", err)
		}
		if receipt.IgnoredReason != "" {
			return fmt.Errorf("applied result cannot contain ignored reason")
		}
		switch {
		case receipt.Status == orchestration.StatusRetryWait:
			if receipt.NextAttemptAt.IsZero() {
				return errors.New("retry_wait result requires next_attempt_at")
			}
			if err := txStore.ScheduleRetry(
				ctx, receipt.JobID, receipt.NextAttemptAt,
				receipt.ErrorCode, receipt.ErrorMessage,
			); err != nil {
				return err
			}
		case receipt.Status == orchestration.StatusActionRequired ||
			orchestration.IsTerminal(receipt.Status):
			if err := txStore.AdvanceTerminal(
				ctx, receipt.JobID, receipt.Status,
				receipt.ErrorCode, receipt.ErrorMessage,
			); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported applied source job result status %q", receipt.Status)
		}
		applied = true
		return nil
	})
	return applied, duplicate, err
}

func (s *Store) insertIgnoredReceipt(ctx context.Context, receipt orchestration.ResultReceipt, reason string) error {
	if _, err := s.db.Exec(ctx, `
		insert into source_job_results (
			id, source_job_id, message_id, attempt_no,
			result_digest, applied, ignored_reason
		) values ($1, $2, $3, $4, $5, false, $6)
		on conflict (source_job_id, attempt_no) do nothing
	`, receipt.ID, receipt.JobID, receipt.MessageID, receipt.AttemptNo,
		receipt.Digest, reason); err != nil {
		return fmt.Errorf("insert ignored source job result: %w", err)
	}
	return nil
}

func nullableText(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}
