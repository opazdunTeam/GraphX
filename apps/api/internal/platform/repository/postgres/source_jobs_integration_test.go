//go:build integration

package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

func TestStoreMarkQueuedDoesNotLowerTerminalState(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000060"
	jobID := "20000000-0000-4000-8000-000000000060"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store queued")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-queued-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}

	store := NewStore(pool)
	changed, err := store.MarkQueued(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("MarkQueued() error = %v", err)
	}
	if !changed {
		t.Fatal("MarkQueued() changed = false, want pending -> queued")
	}
	changed, err = store.MarkQueued(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("second MarkQueued() error = %v", err)
	}
	if changed {
		t.Fatal("second MarkQueued() changed = true, want only pending to queue")
	}
	if _, err := pool.Exec(ctx, `
		update source_jobs set status = 'succeeded', finished_at = now()
		where id = $1
	`, jobID); err != nil {
		t.Fatalf("make source job terminal: %v", err)
	}
	changed, err = store.MarkQueued(ctx, jobID, 1)
	if err != nil {
		t.Fatalf("terminal MarkQueued() error = %v", err)
	}
	if changed {
		t.Fatal("terminal source job was lowered to queued")
	}
}

func TestStoreRetryAndNewAttemptIncrement(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000061"
	jobID := "20000000-0000-4000-8000-000000000061"
	outboxID := "30000000-0000-4000-8000-000000000061"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store retry")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-retry-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute),
		"SOURCE_TIMEOUT", "timeout",
	); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}

	attemptNo, err := store.StartNewAttempt(
		ctx,
		jobID,
		outboxID,
		`{"job":"retry"}`,
	)
	if err != nil {
		t.Fatalf("StartNewAttempt() error = %v", err)
	}
	if attemptNo != 2 {
		t.Fatalf("StartNewAttempt() attempt = %d, want 2", attemptNo)
	}

	var status string
	var storedAttempt int
	if err := pool.QueryRow(ctx, `
		select status, attempt_no
		from source_jobs
		where id = $1
	`, jobID).Scan(&status, &storedAttempt); err != nil {
		t.Fatalf("read retry state: %v", err)
	}
	if status != "pending" || storedAttempt != 2 {
		t.Fatalf("retry state = %s attempt %d, want pending attempt 2", status, storedAttempt)
	}
}

func TestStoreApplyResultStoresLateReceipt(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000062"
	jobID := "20000000-0000-4000-8000-000000000062"
	resultID := "30000000-0000-4000-8000-000000000062"
	messageID := "30000000-0000-4000-8000-000000000063"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store late")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-late-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.Cancel(ctx, jobID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	applied, duplicate, err := store.ApplyResult(ctx, "source-jobs-store", orchestration.ResultReceipt{
		ID:        resultID,
		MessageID: messageID,
		JobID:     jobID,
		AttemptNo: 1,
		Digest:    strings.Repeat("a", 64),
		Status:    orchestration.StatusSucceeded,
		Applied:   true,
	})
	if err != nil {
		t.Fatalf("ApplyResult() late receipt error = %v", err)
	}
	if applied || duplicate {
		t.Fatalf("late result = applied %t duplicate %t, want neither", applied, duplicate)
	}

	var storedApplied bool
	var ignoredReason *string
	if err := pool.QueryRow(ctx, `
		select applied, ignored_reason
		from source_job_results
		where source_job_id = $1
	`, jobID).Scan(&storedApplied, &ignoredReason); err != nil {
		t.Fatalf("read late result receipt: %v", err)
	}
	if storedApplied || ignoredReason == nil {
		t.Fatalf("late receipt = applied %t reason %v, want unapplied receipt", storedApplied, ignoredReason)
	}
}
