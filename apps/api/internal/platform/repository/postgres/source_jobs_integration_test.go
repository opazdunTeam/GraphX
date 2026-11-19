//go:build integration

package postgres

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/rabbitmq"
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

	err = store.StartNewAttempt(
		ctx,
		jobID,
		2,
		sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 2),
	)
	if err != nil {
		t.Fatalf("StartNewAttempt() error = %v", err)
	}

	var status, payloadJobID string
	var storedAttempt int
	if err := pool.QueryRow(ctx, `
		select job.status, job.attempt_no, message.payload->'payload'->>'job_id'
		from source_jobs as job
		join outbox_messages as message on message.aggregate_id = job.id
		where job.id = $1 and message.id = $2
	`, jobID, outboxID).Scan(&status, &storedAttempt, &payloadJobID); err != nil {
		t.Fatalf("read retry state: %v", err)
	}
	if status != "pending" || storedAttempt != 2 || payloadJobID != jobID {
		t.Fatalf(
			"retry state = %s attempt %d payload job %s, want pending attempt 2 job %s",
			status, storedAttempt, payloadJobID, jobID,
		)
	}
	var payloadAttempt int
	if err := pool.QueryRow(ctx, `
		select (payload->'payload'->>'attempt_no')::integer
		from outbox_messages
		where id = $1
	`, outboxID).Scan(&payloadAttempt); err != nil {
		t.Fatalf("read retry outbox attempt: %v", err)
	}
	if payloadAttempt != 2 {
		t.Fatalf("retry outbox attempt = %d, want 2", payloadAttempt)
	}
}

func TestStoreRetryRejectsMismatchedExpectedAttemptAtomically(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000063"
	jobID := "20000000-0000-4000-8000-000000000063"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store mismatched retry")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-mismatched-retry-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
	); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}

	valid := sourceCollectRequestedOutboxMessage(
		"30000000-0000-4000-8000-000000000064", jobID, investigationID, 2,
	).(testOutboxMessage)
	tests := []struct {
		name            string
		expectedAttempt int
		message         testOutboxMessage
	}{
		{name: "stale expected attempt", expectedAttempt: 1, message: valid},
		{name: "different aggregate", expectedAttempt: 2, message: func() testOutboxMessage {
			message := valid
			message.aggregateID = "20000000-0000-4000-8000-000000000099"
			return message
		}()},
		{name: "different message attempt", expectedAttempt: 2, message: func() testOutboxMessage {
			message := valid
			message.attemptNo = 1
			return message
		}()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := store.StartNewAttempt(
				ctx, jobID, test.expectedAttempt, test.message,
			); err == nil {
				t.Fatal("StartNewAttempt() accepted mismatched identity")
			}
		})
	}

	var status string
	var attemptNo, outboxCount int
	if err := pool.QueryRow(ctx, `
		select status, attempt_no,
			(select count(*) from outbox_messages where aggregate_id = $1)
		from source_jobs
		where id = $1
	`, jobID).Scan(&status, &attemptNo, &outboxCount); err != nil {
		t.Fatalf("read rejected retry state: %v", err)
	}
	if status != "retry_wait" || attemptNo != 1 || outboxCount != 0 {
		t.Fatalf(
			"rejected retry state = %s attempt %d outbox %d, want retry_wait attempt 1 no outbox",
			status, attemptNo, outboxCount,
		)
	}
}

func TestStoreActionRequiredCanFail(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000064"
	jobID := "20000000-0000-4000-8000-000000000064"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store action required")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-action-required-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.AdvanceTerminal(
		ctx, jobID, orchestration.StatusActionRequired,
		"SOURCE_ACTION_REQUIRED", "manual action required",
	); err != nil {
		t.Fatalf("AdvanceTerminal(action_required) error = %v", err)
	}
	var actionRequiredFinished bool
	if err := pool.QueryRow(ctx, `
		select finished_at is not null from source_jobs where id = $1
	`, jobID).Scan(&actionRequiredFinished); err != nil {
		t.Fatalf("read action-required timestamps: %v", err)
	}
	if actionRequiredFinished {
		t.Fatal("action_required source job has terminal finished_at")
	}
	if err := store.AdvanceTerminal(
		ctx, jobID, orchestration.StatusSucceeded, "", "",
	); err == nil {
		t.Fatal("AdvanceTerminal(succeeded) accepted action_required -> succeeded")
	}
	if err := store.AdvanceTerminal(
		ctx, jobID, orchestration.StatusFailed,
		"SOURCE_ACCESS_DENIED", "manual action failed",
	); err != nil {
		t.Fatalf("AdvanceTerminal(failed) error = %v", err)
	}

	var status string
	var finished bool
	if err := pool.QueryRow(ctx, `
		select status, finished_at is not null
		from source_jobs
		where id = $1
	`, jobID).Scan(&status, &finished); err != nil {
		t.Fatalf("read failed action-required job: %v", err)
	}
	if status != "failed" || !finished {
		t.Fatalf("action-required result = %s finished %t, want failed and finished", status, finished)
	}
}

func TestStoreActionRequiredStartsNewAttemptAndKeepsJobHistory(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000065"
	jobID := "20000000-0000-4000-8000-000000000065"
	outboxID := "30000000-0000-4000-8000-000000000065"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store action-required retry")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-action-required-retry-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if changed, err := store.MarkQueued(ctx, jobID, 1); err != nil || !changed {
		t.Fatalf("MarkQueued() = changed %t error %v", changed, err)
	}
	if err := store.AdvanceTerminal(
		ctx, jobID, orchestration.StatusActionRequired,
		"SOURCE_ACTION_REQUIRED", "manual action required",
	); err != nil {
		t.Fatalf("AdvanceTerminal(action_required) error = %v", err)
	}
	if err := store.StartNewAttempt(
		ctx, jobID, 2,
		sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 2),
	); err != nil {
		t.Fatalf("StartNewAttempt() error = %v", err)
	}

	var status, lastErrorCode string
	var attemptNo int
	var queuedAtPresent, finishedAtPresent, nextAttemptPresent bool
	if err := pool.QueryRow(ctx, `
		select status, attempt_no, queued_at is not null, finished_at is not null,
			next_attempt_at is not null, last_error_code
		from source_jobs
		where id = $1
	`, jobID).Scan(
		&status, &attemptNo, &queuedAtPresent, &finishedAtPresent,
		&nextAttemptPresent, &lastErrorCode,
	); err != nil {
		t.Fatalf("read restarted action-required job: %v", err)
	}
	if status != "pending" || attemptNo != 2 || !queuedAtPresent || finishedAtPresent ||
		nextAttemptPresent || lastErrorCode != "SOURCE_ACTION_REQUIRED" {
		t.Fatalf(
			"restarted job = status %s attempt %d queued %t finished %t next %t error %q",
			status, attemptNo, queuedAtPresent, finishedAtPresent, nextAttemptPresent, lastErrorCode,
		)
	}
}

func TestStoreRetryExhaustionDoesNotChangeJob(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000066"
	jobID := "20000000-0000-4000-8000-000000000066"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store retry exhaustion")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			max_attempts, request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', 1, $4, 'store-exhausted-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
	); err == nil {
		t.Fatal("ScheduleRetry() accepted exhausted job")
	}

	var status string
	var attemptNo int
	if err := pool.QueryRow(ctx, `
		select status, attempt_no from source_jobs where id = $1
	`, jobID).Scan(&status, &attemptNo); err != nil {
		t.Fatalf("read exhausted job: %v", err)
	}
	if status != "pending" || attemptNo != 1 {
		t.Fatalf("exhausted job = %s attempt %d, want pending attempt 1", status, attemptNo)
	}
}

func TestStoreNewAttemptRollsBackWhenOutboxInsertFails(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000067"
	jobID := "20000000-0000-4000-8000-000000000067"
	outboxID := "30000000-0000-4000-8000-000000000067"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store outbox rollback")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-outbox-rollback-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (id, message_type, aggregate_type, aggregate_id, payload)
		values ($1, 'existing.message.v1', 'source_job', $2, '{}'::jsonb)
	`, outboxID, jobID); err != nil {
		t.Fatalf("seed conflicting outbox message: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
	); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}
	if err := store.StartNewAttempt(
		ctx, jobID, 2,
		sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 2),
	); err == nil {
		t.Fatal("StartNewAttempt() succeeded despite duplicate outbox id")
	}

	var status string
	var attemptNo int
	if err := pool.QueryRow(ctx, `
		select status, attempt_no from source_jobs where id = $1
	`, jobID).Scan(&status, &attemptNo); err != nil {
		t.Fatalf("read rolled-back job: %v", err)
	}
	if status != "retry_wait" || attemptNo != 1 {
		t.Fatalf("rolled-back job = %s attempt %d, want retry_wait attempt 1", status, attemptNo)
	}
}

func TestStoreConcurrentNewAttemptCreatesOneCommand(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000068"
	jobID := "20000000-0000-4000-8000-000000000068"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store concurrent retry")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-concurrent-retry-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
	); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	for index := 0; index < 2; index++ {
		outboxID := fmt.Sprintf("30000000-0000-4000-8000-%012d", 680+index)
		go func() {
			<-start
			results <- store.StartNewAttempt(
				ctx, jobID, 2,
				sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 2),
			)
		}()
	}
	close(start)
	successes := 0
	for range 2 {
		if err := <-results; err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent StartNewAttempt() successes = %d, want 1", successes)
	}

	var status string
	var attemptNo, outboxCount int
	if err := pool.QueryRow(ctx, `
		select status, attempt_no,
			(select count(*) from outbox_messages where aggregate_id = $1)
		from source_jobs
		where id = $1
	`, jobID).Scan(&status, &attemptNo, &outboxCount); err != nil {
		t.Fatalf("read concurrent retry result: %v", err)
	}
	if status != "pending" || attemptNo != 2 || outboxCount != 1 {
		t.Fatalf(
			"concurrent retry = %s attempt %d outbox %d, want pending attempt 2 one outbox",
			status, attemptNo, outboxCount,
		)
	}
}

func TestPreparedSourceCollectRequestStartsNewAttempt(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000071"
	jobID := "20000000-0000-4000-8000-000000000071"
	messageID := "30000000-0000-4000-8000-000000000075"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Prepared retry command")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'prepared-retry-command-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	if err := store.ScheduleRetry(
		ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
	); err != nil {
		t.Fatalf("ScheduleRetry() error = %v", err)
	}

	repositoryRoot, err := filepath.Abs(filepath.Join("..", "..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}
	registry, err := rabbitmq.NewSchemaRegistry(filepath.Join(repositoryRoot, "contracts", "schemas"))
	if err != nil {
		t.Fatalf("NewSchemaRegistry() error = %v", err)
	}
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	envelope := rabbitmq.Envelope[rabbitmq.SourceCollectRequestedPayload]{
		MessageID:     messageID,
		Type:          rabbitmq.SourceCollectRequestedType,
		SchemaVersion: 1,
		OccurredAt:    deadline.Add(-time.Minute),
		CorrelationID: investigationID,
		Producer:      "graphx-api",
		RootCommandID: messageID,
		Payload: rabbitmq.SourceCollectRequestedPayload{
			JobID:           jobID,
			AttemptNo:       2,
			InvestigationID: investigationID,
			Source:          "fixture",
			Operation:       "collect",
			Purpose:         "subject_discovery",
			Query: rabbitmq.SourceQuery{
				Type: "person",
				Name: stringPointer("Ada Lovelace"),
			},
			Limits: rabbitmq.CollectLimits{
				MaxRecords: 20, MaxBytes: 1_048_576, TimeoutSeconds: 30,
			},
			Deadline: deadline,
		},
	}
	message, err := rabbitmq.PrepareSourceCollectRequestedMessage(
		registry,
		envelope,
		rabbitmq.SourceCollectRequestedIdentity{
			MessageID: messageID, JobID: jobID, AttemptNo: 2,
		},
	)
	if err != nil {
		t.Fatalf("PrepareSourceCollectRequestedMessage() error = %v", err)
	}
	if err := store.StartNewAttempt(ctx, jobID, 2, message); err != nil {
		t.Fatalf("StartNewAttempt() error = %v", err)
	}

	var status, storedMessageID, storedJobID, storedType string
	var attemptNo, storedAttempt int
	if err := pool.QueryRow(ctx, `
		select job.status, job.attempt_no,
			message.payload->>'message_id', message.payload->>'type',
			message.payload->'payload'->>'job_id',
			(message.payload->'payload'->>'attempt_no')::integer
		from source_jobs as job
		join outbox_messages as message on message.aggregate_id = job.id
		where job.id = $1 and message.id = $2
	`, jobID, messageID).Scan(
		&status, &attemptNo, &storedMessageID, &storedType, &storedJobID, &storedAttempt,
	); err != nil {
		t.Fatalf("read prepared retry state: %v", err)
	}
	if status != "pending" || attemptNo != 2 || storedMessageID != messageID ||
		storedType != rabbitmq.SourceCollectRequestedType || storedJobID != jobID || storedAttempt != 2 {
		t.Fatalf(
			"prepared retry = status %s attempt %d message %s type %s job %s payload attempt %d",
			status, attemptNo, storedMessageID, storedType, storedJobID, storedAttempt,
		)
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

func stringPointer(value string) *string {
	return &value
}

func TestStoreApplyResultDeduplicatesAndRejectsDigestConflict(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000069"
	jobID := "20000000-0000-4000-8000-000000000069"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store result dedup")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-result-dedup-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	digest := strings.Repeat("b", 64)
	applied, duplicate, err := store.ApplyResult(ctx, "source-jobs-store", orchestration.ResultReceipt{
		ID:        "30000000-0000-4000-8000-000000000069",
		MessageID: "30000000-0000-4000-8000-000000000070",
		JobID:     jobID,
		AttemptNo: 1,
		Digest:    digest,
		Status:    orchestration.StatusSucceeded,
	})
	if err != nil || !applied || duplicate {
		t.Fatalf("first result = applied %t duplicate %t error %v", applied, duplicate, err)
	}

	applied, duplicate, err = store.ApplyResult(ctx, "source-jobs-store", orchestration.ResultReceipt{
		ID:        "30000000-0000-4000-8000-000000000071",
		MessageID: "30000000-0000-4000-8000-000000000071",
		JobID:     jobID,
		AttemptNo: 1,
		Digest:    digest,
		Status:    orchestration.StatusSucceeded,
	})
	if err != nil || applied || !duplicate {
		t.Fatalf("same digest = applied %t duplicate %t error %v", applied, duplicate, err)
	}

	conflictMessageID := "30000000-0000-4000-8000-000000000072"
	_, _, err = store.ApplyResult(ctx, "source-jobs-store", orchestration.ResultReceipt{
		ID:        "30000000-0000-4000-8000-000000000072",
		MessageID: conflictMessageID,
		JobID:     jobID,
		AttemptNo: 1,
		Digest:    strings.Repeat("c", 64),
		Status:    orchestration.StatusSucceeded,
	})
	if err == nil {
		t.Fatal("ApplyResult() accepted conflicting digest")
	}

	var resultCount, conflictInboxCount int
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from source_job_results where source_job_id = $1),
			(select count(*) from consumed_messages where consumer_name = 'source-jobs-store' and message_id = $2)
	`, jobID, conflictMessageID).Scan(&resultCount, &conflictInboxCount); err != nil {
		t.Fatalf("read result conflict state: %v", err)
	}
	if resultCount != 1 || conflictInboxCount != 0 {
		t.Fatalf("conflict state = results %d inbox %d, want one result and no inbox", resultCount, conflictInboxCount)
	}
}

func TestStoreApplyResultRejectsFutureAttemptAtomically(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := "10000000-0000-4000-8000-000000000070"
	jobID := "20000000-0000-4000-8000-000000000070"
	messageID := "30000000-0000-4000-8000-000000000073"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Store future result")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'store-future-result-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
	store := NewStore(pool)
	_, _, err = store.ApplyResult(ctx, "source-jobs-store", orchestration.ResultReceipt{
		ID:        "30000000-0000-4000-8000-000000000074",
		MessageID: messageID,
		JobID:     jobID,
		AttemptNo: 2,
		Digest:    strings.Repeat("b", 64),
		Status:    orchestration.StatusSucceeded,
	})
	if err == nil {
		t.Fatal("ApplyResult() accepted future attempt")
	}

	var inboxCount, resultCount int
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from consumed_messages where consumer_name = 'source-jobs-store' and message_id = $1),
			(select count(*) from source_job_results where source_job_id = $2)
	`, messageID, jobID).Scan(&inboxCount, &resultCount); err != nil {
		t.Fatalf("read future result state: %v", err)
	}
	if inboxCount != 0 || resultCount != 0 {
		t.Fatalf("future result state = inbox %d results %d, want no writes", inboxCount, resultCount)
	}
}
