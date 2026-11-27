//go:build integration

package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/apperrors"
)

func TestReadInvestigationStatus(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		seedProjectionEntities(t, ctx, tx)
		const otherInvestigation = "11000000-0000-4000-8000-000000000022"
		const emptyInvestigation = "11000000-0000-4000-8000-000000000023"
		seedRunningInvestigation(t, ctx, tx, otherInvestigation, "Other subject")
		seedRunningInvestigation(t, ctx, tx, emptyInvestigation, "Empty")
		seedInitialResolution(t, ctx, tx,
			"80000000-0000-4000-8000-000000000001", "80000000-0000-4000-8000-000000000021",
			"81000000-0000-4000-8000-000000000021", "82000000-0000-4000-8000-000000000021",
			"83000000-0000-4000-8000-000000000021",
		)
		if _, err := tx.Exec(ctx, `
			insert into source_records (id, processing_run_id, local_ref, record_type, schema_version, raw_data)
			values ('60000000-0000-4000-8000-000000000022', $1, 'second', 'person', 'fixture/1', '{"name":"Bob"}')
		`, testRunID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id)
			values ('80000000-0000-4000-8000-000000000022', '60000000-0000-4000-8000-000000000022')
		`); err != nil {
			t.Fatal(err)
		}
		const retryJob = "20000000-0000-4000-8000-000000000024"
		if _, err := tx.Exec(ctx, `
			insert into source_jobs (id, investigation_id, source_id, operation, purpose, request_hash, idempotency_key)
			values ($1, $2, $3, 'collect', 'subject_discovery', 'status-retry', 'status-retry'),
			       ('20000000-0000-4000-8000-000000000025', $4, $3, 'collect', 'subject_discovery', 'status-other', 'status-other')
		`, retryJob, testInvestigationID, testSourceID, otherInvestigation); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `
			update source_jobs set status = 'retry_wait', next_attempt_at = now() + interval '1 minute',
				last_error_code = 'SOURCE_TIMEOUT', last_error_message = 'private diagnostic'
			where id = $1
		`, retryJob); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "update source_jobs set status = 'succeeded', finished_at = now() where id = $1", testJobID); err != nil {
			t.Fatal(err)
		}

		id := uuid.MustParse(testInvestigationID)
		snapshot, err := readInvestigationStatus(ctx, tx, id)
		if err != nil {
			t.Fatal(err)
		}
		summary := snapshot.Summarize()
		if snapshot.Status != "running" || snapshot.Stage != "subject_discovery" || snapshot.StateVersion != 1 ||
			snapshot.PendingReviewCount != 1 || summary.KnownJobs != 2 || summary.ActiveJobs != 1 ||
			summary.SucceededJobs != 1 || len(summary.Sources) != 1 || summary.Sources[0].Status != "retry_wait" ||
			summary.Sources[0].NextAttemptAt == nil || summary.Sources[0].ErrorCode == nil || *summary.Sources[0].ErrorCode != "SOURCE_TIMEOUT" {
			t.Fatalf("snapshot = %+v, summary = %+v", snapshot, summary)
		}
		other, err := readInvestigationStatus(ctx, tx, uuid.MustParse(otherInvestigation))
		if err != nil || len(other.Jobs) != 1 || other.PendingReviewCount != 0 {
			t.Fatalf("other investigation = %+v, %v", other, err)
		}
		empty, err := readInvestigationStatus(ctx, tx, uuid.MustParse(emptyInvestigation))
		if err != nil || empty.Status != "running" || len(empty.Jobs) != 0 || empty.PendingReviewCount != 0 {
			t.Fatalf("empty investigation = %+v, %v", empty, err)
		}
		if _, err := readInvestigationStatus(ctx, tx, uuid.Nil); !errors.Is(err, apperrors.ErrNotFound) {
			t.Fatalf("missing investigation error = %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, supersedes_decision_id,
				expected_lock_version, decision, actor_type, reason, created_at
			) values (
				'82000000-0000-4000-8000-000000000022', '80000000-0000-4000-8000-000000000021',
				1, '82000000-0000-4000-8000-000000000021', 2, 'revert', 'user', 'Correction',
				now() + interval '1 minute'
			)
		`); err != nil {
			t.Fatal(err)
		}
		reopened, err := readInvestigationStatus(ctx, tx, id)
		if err != nil || reopened.PendingReviewCount != 2 || !reopened.UpdatedAt.Equal(snapshot.UpdatedAt.Add(time.Minute)) {
			t.Fatalf("reopened investigation = %+v, %v", reopened, err)
		}
		assertStatusIndexPaths(t, ctx, tx, id)
	})
}

func TestStatusKeepsErrorTimeWhenRetryIsQueued(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		store := newTransactionalStore(tx)
		schedule := func() {
			t.Helper()
			if err := store.ScheduleRetry(ctx, testJobID, time.Now().Add(time.Minute), "SOURCE_TIMEOUT", "private diagnostic"); err != nil {
				t.Fatal(err)
			}
		}
		errorTime := func() time.Time {
			t.Helper()
			snapshot, err := readInvestigationStatus(ctx, tx, uuid.MustParse(testInvestigationID))
			if err != nil || len(snapshot.Jobs) != 1 || snapshot.Jobs[0].ErrorAt == nil {
				t.Fatalf("missing error time: %+v, %v", snapshot, err)
			}
			return *snapshot.Jobs[0].ErrorAt
		}

		schedule()
		firstError := errorTime()
		if _, err := tx.Exec(ctx, `
			update source_jobs set status = 'pending', attempt_no = 2, next_attempt_at = null where id = $1;
		`, testJobID); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "update source_jobs set status = 'queued' where id = $1", testJobID); err != nil {
			t.Fatal(err)
		}
		if got := errorTime(); !got.Equal(firstError) {
			t.Fatalf("queued retry changed the previous error time from %v to %v", firstError, got)
		}
		schedule()
		if got := errorTime(); !got.After(firstError) {
			t.Fatalf("new failure with the same error code did not update the time: %v after %v", got, firstError)
		}
	})
}

func assertStatusIndexPaths(t *testing.T, ctx context.Context, tx pgx.Tx, id uuid.UUID) {
	t.Helper()
	if _, err := tx.Exec(ctx, "set local enable_seqscan = off"); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "explain (costs off) "+investigationStatusSQL, id)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	plan := strings.Join(lines, "\n")
	for _, index := range []string{
		"investigations_pkey", "idx_source_jobs_investigation",
		"source_job_results_source_job_id_attempt_no_key", "idx_processing_runs_result",
		"source_records_processing_run_id_local_ref_key", "idx_match_reviews_source_record", "idx_match_decisions_review",
	} {
		if !strings.Contains(plan, index) {
			t.Errorf("missing index path %s:\n%s", index, plan)
		}
	}
}
