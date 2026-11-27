//go:build integration

package postgres

import (
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

func TestSourceJobAndOutboxAreCreatedAtomically(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewStore(pool)

	investigationID := "10000000-0000-4000-8000-000000000077"
	jobID := "20000000-0000-4000-8000-000000000077"
	outboxID := "30000000-0000-4000-8000-000000000077"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Atomic job creation")
	job := orchestration.CreateJob{
		ID:              jobID,
		InvestigationID: investigationID,
		SourceID:        testSourceID,
		Operation:       "search_subject",
		Purpose:         "subject_discovery",
		RequestHash:     strings.Repeat("a", 64),
		IdempotencyKey:  "atomic-source-job-create",
	}
	if err := store.Create(ctx, job, sourceCollectRequestedOutboxMessage(outboxID, jobID, investigationID, 1)); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	var jobs, messages int
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from source_jobs where id = $1),
			(select count(*) from outbox_messages where id = $2)
	`, jobID, outboxID).Scan(&jobs, &messages); err != nil {
		t.Fatalf("read created job and outbox counts: %v", err)
	}
	if jobs != 1 || messages != 1 {
		t.Fatalf("created rows = job %d outbox %d, want one each", jobs, messages)
	}
	mismatchedOperationJob := job
	mismatchedOperationJob.ID = "20000000-0000-4000-8000-000000000079"
	mismatchedOperationJob.Operation = "collect"
	mismatchedOperationJob.IdempotencyKey = "atomic-source-job-operation-mismatch"
	if err := store.Create(ctx, mismatchedOperationJob, sourceCollectRequestedOutboxMessage(
		"30000000-0000-4000-8000-000000000080",
		mismatchedOperationJob.ID,
		investigationID,
		1,
	)); err == nil {
		t.Fatal("Create() accepted a source job operation that differs from its outbox command")
	}
	mismatchedInvestigationJob := mismatchedOperationJob
	mismatchedInvestigationJob.Operation = job.Operation
	if err := store.Create(ctx, mismatchedInvestigationJob, sourceCollectRequestedOutboxMessage(
		"30000000-0000-4000-8000-000000000080",
		mismatchedInvestigationJob.ID,
		"10000000-0000-4000-8000-000000000099",
		1,
	)); err == nil {
		t.Fatal("Create() accepted a source job investigation that differs from its outbox command")
	}

	rollbackInvestigationID := "10000000-0000-4000-8000-000000000078"
	rollbackJobID := "20000000-0000-4000-8000-000000000078"
	conflictingOutboxID := "30000000-0000-4000-8000-000000000079"
	seedRunningInvestigation(t, ctx, pool, rollbackInvestigationID, "Atomic job rollback")
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload
		) values ($1, 'test', 'test', $2, '{"test":true}'::jsonb)
	`, conflictingOutboxID, rollbackJobID); err != nil {
		t.Fatalf("insert conflicting outbox row: %v", err)
	}
	rollbackJob := job
	rollbackJob.ID = rollbackJobID
	rollbackJob.InvestigationID = rollbackInvestigationID
	rollbackJob.IdempotencyKey = "atomic-source-job-rollback"
	if err := store.Create(ctx, rollbackJob, sourceCollectRequestedOutboxMessage(
		conflictingOutboxID, rollbackJobID, rollbackInvestigationID, 1,
	)); err == nil {
		t.Fatal("Create() error = nil, want duplicate outbox id error")
	}
	var rolledBackJobs int
	if err := pool.QueryRow(ctx, "select count(*) from source_jobs where id = $1", rollbackJobID).Scan(&rolledBackJobs); err != nil {
		t.Fatalf("read rolled-back source job: %v", err)
	}
	if rolledBackJobs != 0 {
		t.Fatalf("source job count after outbox insert failure = %d, want 0", rolledBackJobs)
	}
}

func TestOutboxClaimUsesLeasesAndReschedulesWithBoundedInput(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	firstID := "30000000-0000-4000-8000-000000000070"
	secondID := "30000000-0000-4000-8000-000000000071"
	for _, id := range []string{firstID, secondID} {
		if _, err := pool.Exec(ctx, `
			insert into outbox_messages (
				id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
			) values (
				$1, 'source.collect.requested.v1', 'test', $2, '{"test":true}'::jsonb,
				now() - interval '1 day', now() - interval '1 day'
			)
		`, id, testJobID); err != nil {
			t.Fatalf("insert outbox message %s: %v", id, err)
		}
	}

	type claimResult struct {
		message *outbox.Message
		err     error
	}
	start := make(chan struct{})
	claims := make(chan claimResult, 2)
	for range 2 {
		go func() {
			<-start
			message, claimErr := store.Claim(ctx, time.Minute)
			claims <- claimResult{message: message, err: claimErr}
		}()
	}
	close(start)
	firstResult := <-claims
	secondResult := <-claims
	if firstResult.err != nil {
		t.Fatalf("claim first outbox message: %v", firstResult.err)
	}
	if secondResult.err != nil {
		t.Fatalf("claim second outbox message: %v", secondResult.err)
	}
	first := firstResult.message
	second := secondResult.message
	if first == nil || second == nil || first.ID == second.ID {
		t.Fatalf("claimed ids = %#v and %#v, want two different messages", first, second)
	}

	if err := store.Reschedule(ctx, *first, time.Hour); err != nil {
		t.Fatalf("reschedule outbox message: %v", err)
	}
	if err := store.MarkPublished(ctx, *second, 0); err != nil {
		t.Fatalf("complete second outbox message: %v", err)
	}
	var attempts int
	var availableAt time.Time
	var leaseToken *string
	if err := pool.QueryRow(ctx, `
		select publish_attempts, available_at, lease_token
		from outbox_messages
		where id = $1
	`, first.ID).Scan(&attempts, &availableAt, &leaseToken); err != nil {
		t.Fatalf("read deferred outbox message: %v", err)
	}
	if attempts != 1 || !availableAt.After(time.Now()) || leaseToken != nil {
		t.Fatalf(
			"deferred outbox = attempts %d available_at %s lease %v",
			attempts, availableAt, leaseToken,
		)
	}
}

func TestOutboxExpiredLeaseRepeatsSameMessageAndRejectsOldOwner(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	messageID := "30000000-0000-4000-8000-000000000072"
	payload := `{"message_id":"30000000-0000-4000-8000-000000000072","type":"source.collect.requested.v1","payload":{"job_id":"20000000-0000-4000-8000-000000000072","attempt_no":1,"query":{"name":"Иван"}}}`
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
		) values (
			$1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb,
			now() - interval '1 day', now() - interval '1 day'
		)
	`, messageID, "20000000-0000-4000-8000-000000000072", payload); err != nil {
		t.Fatalf("insert outbox message: %v", err)
	}

	first, err := store.Claim(ctx, time.Minute)
	if err != nil || first == nil {
		t.Fatalf("first claim = %#v, error %v", first, err)
	}
	if _, err := pool.Exec(ctx, `
		update outbox_messages set lease_until = now() - interval '1 second'
		where id = $1
	`, messageID); err != nil {
		t.Fatalf("expire outbox lease: %v", err)
	}

	retryConfig, err := pgxpool.ParseConfig(testPostgresConfig().DSN)
	if err != nil {
		t.Fatal(err)
	}
	retryConfig.ConnConfig.RuntimeParams["client_encoding"] = "LATIN1"
	retryPool, err := pgxpool.NewWithConfig(ctx, retryConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(retryPool.Close)
	second, err := NewOutboxStore(retryPool).Claim(ctx, time.Minute)
	if err != nil || second == nil {
		t.Fatalf("second claim = %#v, error %v", second, err)
	}
	if first.ID != second.ID || string(first.Payload) != string(second.Payload) ||
		!strings.Contains(string(second.Payload), "Иван") ||
		first.LeaseToken == second.LeaseToken {
		t.Fatalf("reclaimed message changed: first %#v second %#v", first, second)
	}

	if err := store.MarkPublished(ctx, *first, 1); err == nil {
		t.Fatal("old lease owner marked message published after lease was replaced")
	}
	if err := store.MarkPublished(ctx, *second, 1); err != nil {
		t.Fatalf("current lease owner could not mark message published: %v", err)
	}

	var publishedAt *time.Time
	var finalLease *string
	if err := pool.QueryRow(ctx, `
		select published_at, lease_token
		from outbox_messages
		where id = $1
	`, messageID).Scan(&publishedAt, &finalLease); err != nil {
		t.Fatalf("read published outbox message: %v", err)
	}
	if publishedAt == nil || finalLease != nil {
		t.Fatalf("published_at/lease = %v/%v, want published with no lease", publishedAt, finalLease)
	}
}

func TestOutboxConfirmQueuesOnlyMatchingPendingAttempt(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	investigationID := "10000000-0000-4000-8000-000000000073"
	jobID := "20000000-0000-4000-8000-000000000073"
	messageID := "30000000-0000-4000-8000-000000000073"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Outbox confirm")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'search_subject', 'subject_discovery', $4, 'outbox-confirm-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("insert source job: %v", err)
	}
	payload := `{"message_id":"30000000-0000-4000-8000-000000000073","type":"source.collect.requested.v1","payload":{"job_id":"20000000-0000-4000-8000-000000000073","attempt_no":1}}`
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
		) values (
			$1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb,
			now() - interval '1 day', now() - interval '1 day'
		)
	`, messageID, jobID, payload); err != nil {
		t.Fatalf("insert outbox message: %v", err)
	}

	message, err := store.Claim(ctx, time.Minute)
	if err != nil || message == nil {
		t.Fatalf("claim source job message = %#v, error %v", message, err)
	}
	if err := store.MarkPublished(ctx, *message, 1); err != nil {
		t.Fatalf("mark published after confirm: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, "select status from source_jobs where id = $1", jobID).Scan(&status); err != nil {
		t.Fatalf("read source job state: %v", err)
	}
	if status != "queued" {
		t.Fatalf("source job status = %s, want queued", status)
	}
}

func TestOutboxRetryDoesNotChangeSourceJobBusinessState(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	investigationID := "10000000-0000-4000-8000-000000000074"
	jobID := "20000000-0000-4000-8000-000000000074"
	messageID := "30000000-0000-4000-8000-000000000074"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Outbox retry")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'search_subject', 'subject_discovery', $4, 'outbox-retry-job')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("insert source job: %v", err)
	}
	payload := `{"message_id":"30000000-0000-4000-8000-000000000074","type":"source.collect.requested.v1","payload":{"job_id":"20000000-0000-4000-8000-000000000074","attempt_no":1}}`
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
		) values (
			$1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb,
			now() - interval '1 day', now() - interval '1 day'
		)
	`, messageID, jobID, payload); err != nil {
		t.Fatalf("insert outbox message: %v", err)
	}

	message, err := store.Claim(ctx, time.Minute)
	if err != nil || message == nil {
		t.Fatalf("claim source job message = %#v, error %v", message, err)
	}
	if err := store.Reschedule(ctx, *message, time.Second); err != nil {
		t.Fatalf("reschedule after temporary publish failure: %v", err)
	}

	var status string
	if err := pool.QueryRow(ctx, "select status from source_jobs where id = $1", jobID).Scan(&status); err != nil {
		t.Fatalf("read source job state: %v", err)
	}
	if status != "pending" {
		t.Fatalf("source job status after publish failure = %s, want pending", status)
	}
}

func TestOutboxConfirmDoesNotChangeTerminalOrNewerAttempt(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	tests := []struct {
		name                string
		investigation       string
		jobID               string
		messageID           string
		idempotencyKey      string
		finalize            string
		cancelInvestigation bool
		wantStatus          string
		wantAttempt         int
	}{
		{
			name:           "terminal job",
			investigation:  "10000000-0000-4000-8000-000000000075",
			jobID:          "20000000-0000-4000-8000-000000000075",
			messageID:      "30000000-0000-4000-8000-000000000083",
			idempotencyKey: "outbox-terminal-job",
			finalize:       `update source_jobs set status = 'succeeded', finished_at = now() where id = $1`,
			wantStatus:     "succeeded",
			wantAttempt:    1,
		},
		{
			name:           "newer attempt",
			investigation:  "10000000-0000-4000-8000-000000000076",
			jobID:          "20000000-0000-4000-8000-000000000076",
			messageID:      "30000000-0000-4000-8000-000000000076",
			idempotencyKey: "outbox-newer-attempt",
			finalize: `update source_jobs
				set status = 'retry_wait', next_attempt_at = now() + interval '1 minute'
				where id = $1`,
			wantStatus:  "retry_wait",
			wantAttempt: 1,
		},
		{
			name:                "cancelled investigation",
			investigation:       "10000000-0000-4000-8000-000000000082",
			jobID:               "20000000-0000-4000-8000-000000000082",
			messageID:           "30000000-0000-4000-8000-000000000082",
			idempotencyKey:      "outbox-cancelled-investigation",
			cancelInvestigation: true,
			wantStatus:          "pending",
			wantAttempt:         1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seedRunningInvestigation(t, ctx, pool, test.investigation, test.name)
			if _, err := pool.Exec(ctx, `
				insert into source_jobs (
					id, investigation_id, source_id, operation, purpose,
					request_hash, idempotency_key
				) values ($1, $2, $3, 'search_subject', 'subject_discovery', $4, $5)
			`, test.jobID, test.investigation, testSourceID, strings.Repeat("a", 64), test.idempotencyKey); err != nil {
				t.Fatalf("insert source job: %v", err)
			}
			payload := `{"message_id":"` + test.messageID + `","type":"source.collect.requested.v1","payload":{"job_id":"` + test.jobID + `","attempt_no":1}}`
			if _, err := pool.Exec(ctx, `
				insert into outbox_messages (
					id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
				) values (
					$1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb,
					now() - interval '1 day', now() - interval '1 day'
				)
			`, test.messageID, test.jobID, payload); err != nil {
				t.Fatalf("insert outbox message: %v", err)
			}
			message, err := store.Claim(ctx, time.Minute)
			if err != nil || message == nil || message.ID != test.messageID {
				t.Fatalf("claim message = %#v, error %v", message, err)
			}
			if test.finalize != "" {
				if _, err := pool.Exec(ctx, test.finalize, test.jobID); err != nil {
					t.Fatalf("change job before publisher confirm: %v", err)
				}
			}
			if test.name == "newer attempt" {
				if _, err := pool.Exec(ctx, `
					update source_jobs
					set status = 'pending', attempt_no = 2, next_attempt_at = null
					where id = $1
				`, test.jobID); err != nil {
					t.Fatalf("start newer attempt: %v", err)
				}
				test.wantStatus = "pending"
				test.wantAttempt = 2
			}
			if test.cancelInvestigation {
				if _, err := pool.Exec(ctx, `
					update investigations
					set status = 'cancelled', completed_at = now()
					where id = $1
				`, test.investigation); err != nil {
					t.Fatalf("cancel investigation before publisher confirm: %v", err)
				}
			}

			if err := store.MarkPublished(ctx, *message, 1); err != nil {
				t.Fatalf("mark published: %v", err)
			}
			var status string
			var attempt int
			if err := pool.QueryRow(ctx, `
				select status, attempt_no from source_jobs where id = $1
			`, test.jobID).Scan(&status, &attempt); err != nil {
				t.Fatalf("read final job state: %v", err)
			}
			if status != test.wantStatus || attempt != test.wantAttempt {
				t.Fatalf("job state = %s attempt %d, want %s attempt %d",
					status, attempt, test.wantStatus, test.wantAttempt)
			}
			var published bool
			if err := pool.QueryRow(ctx, `
				select published_at is not null from outbox_messages where id = $1
			`, test.messageID).Scan(&published); err != nil {
				t.Fatalf("read outbox publication state: %v", err)
			}
			if !published {
				t.Fatal("outbox message was not marked published after confirm")
			}
		})
	}
}

func TestOutboxConfirmCommitsWhileInvestigationCancellationIsConcurrent(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)

	investigationID := "10000000-0000-4000-8000-000000000084"
	jobID := "20000000-0000-4000-8000-000000000084"
	messageID := "30000000-0000-4000-8000-000000000084"
	seedRunningInvestigation(t, ctx, pool, investigationID, "Concurrent publisher confirm and cancellation")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'search_subject', 'subject_discovery', $4, 'outbox-cancel-race')
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("insert source job: %v", err)
	}
	payload := `{"message_id":"` + messageID + `","type":"source.collect.requested.v1","payload":{"job_id":"` + jobID + `","attempt_no":1}}`
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload, created_at, available_at
		) values (
			$1, 'source.collect.requested.v1', 'source_job', $2, $3::jsonb,
			now() - interval '100 years', now() - interval '100 years'
		)
	`, messageID, jobID, payload); err != nil {
		t.Fatalf("insert outbox message: %v", err)
	}
	message, err := store.Claim(ctx, time.Minute)
	if err != nil || message == nil || message.ID != messageID {
		t.Fatalf("Claim() = %#v, %v", message, err)
	}

	cancelTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin cancellation: %v", err)
	}
	if _, err := cancelTx.Exec(ctx, `
		update investigations
		set status = 'cancelled', completed_at = now()
		where id = $1
	`, investigationID); err != nil {
		_ = cancelTx.Rollback(ctx)
		t.Fatalf("stage cancellation: %v", err)
	}
	confirmed := make(chan error, 1)
	go func() {
		confirmed <- store.MarkPublished(ctx, *message, 1)
	}()
	select {
	case confirmErr := <-confirmed:
		_ = cancelTx.Rollback(ctx)
		t.Fatalf("MarkPublished() returned before cancellation committed: %v", confirmErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := cancelTx.Commit(ctx); err != nil {
		t.Fatalf("commit cancellation: %v", err)
	}
	if err := <-confirmed; err != nil {
		t.Fatalf("MarkPublished() after concurrent cancellation error = %v", err)
	}

	var published bool
	var jobStatus string
	if err := pool.QueryRow(ctx, `
		select message.published_at is not null, job.status
		from outbox_messages as message
		join source_jobs as job on job.id = message.aggregate_id
		where message.id = $1
	`, messageID).Scan(&published, &jobStatus); err != nil {
		t.Fatalf("read confirm/cancellation result: %v", err)
	}
	if !published || jobStatus != "pending" {
		t.Fatalf("confirm/cancellation result = published %t job %s", published, jobStatus)
	}
}

func TestOutboxRescheduleSaturatesMaximumAttemptCount(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)
	store := NewOutboxStore(pool)
	messageID := "30000000-0000-4000-8000-000000000085"
	if _, err := pool.Exec(ctx, `
		insert into outbox_messages (
			id, message_type, aggregate_type, aggregate_id, payload,
			publish_attempts, available_at, created_at
		) values (
			$1, 'test.message.v1', 'test', $1, '{"test":true}'::jsonb, 2147483647,
			now() - interval '200 years', now() - interval '200 years'
		)
	`, messageID); err != nil {
		t.Fatalf("insert saturated outbox message: %v", err)
	}
	message, err := store.Claim(ctx, time.Minute)
	if err != nil || message == nil || message.ID != messageID {
		t.Fatalf("Claim() = %#v, %v", message, err)
	}
	if err := store.Reschedule(ctx, *message, time.Second); err != nil {
		t.Fatalf("Reschedule() saturated counter error = %v", err)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `select publish_attempts from outbox_messages where id = $1`, messageID).Scan(&attempts); err != nil {
		t.Fatalf("read saturated counter: %v", err)
	}
	if attempts != 2147483647 {
		t.Fatalf("publish_attempts = %d, want 2147483647", attempts)
	}
}
