//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
)

func TestStoreApplySourceResultMaterializesAtomicallyAndDeduplicates(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	investigationID := uuid.NewString()
	jobID := uuid.NewString()
	seedRunningInvestigation(t, ctx, pool, investigationID, "Ingest completed result")
	seedSourceResultJob(t, ctx, pool, jobID, investigationID, "source-result-atomic-job")

	store := NewStore(pool)
	result := testSourceResultCommand(jobID, investigationID, 1, uuid.NewString())
	outcome, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result)
	if err != nil {
		t.Fatalf("ApplySourceResult() error = %v", err)
	}
	if !outcome.Applied || outcome.Duplicate || outcome.Status != ingestion.ResultStatusSucceeded {
		t.Fatalf("first result outcome = %#v", outcome)
	}

	outcome, err = store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result)
	if err != nil || outcome.Applied || !outcome.Duplicate {
		t.Fatalf("redelivered message = %#v, error %v", outcome, err)
	}

	duplicate := result
	duplicate.ID = uuid.NewString()
	duplicate.MessageID = duplicate.ID
	outcome, err = store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, duplicate)
	if err != nil || outcome.Applied || !outcome.Duplicate {
		t.Fatalf("same business result = %#v, error %v", outcome, err)
	}

	conflict := result
	conflict.ID = uuid.NewString()
	conflict.MessageID = conflict.ID
	conflict.Digest = strings.Repeat("c", 64)
	_, err = store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, conflict)
	if !ingestion.IsPermanentResultError(err) ||
		ingestion.PermanentResultErrorCode(err) != "source_result_attempt_digest_conflict" {
		t.Fatalf("different digest error = %v, want permanent producer conflict", err)
	}

	var inboxCount, resultCount, artifactCount, linkCount int
	var runCount, recordCount, claimCount int
	var status, adapterVersion string
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from consumed_messages where consumer_name = $1),
			(select count(*) from source_job_results where source_job_id = $2),
			(select count(*) from artifacts where object_key = $3),
			(select count(*) from source_job_result_artifacts where source_job_result_id = $4),
			(select count(*) from processing_runs where source_job_result_id = $4),
			(select count(*) from source_records where processing_run_id in
			 (select id from processing_runs where source_job_result_id = $4)),
			(select count(*) from claims where subject_source_record_id in
			 (select id from source_records where processing_run_id in
			  (select id from processing_runs where source_job_result_id = $4))),
			(select status from source_jobs where id = $2),
			(select adapter_version from artifacts where object_key = $3)
	`, ingestion.DefaultResultConsumerName, jobID,
		result.Artifacts[0].ObjectKey, result.ID).Scan(
		&inboxCount, &resultCount, &artifactCount, &linkCount, &runCount,
		&recordCount, &claimCount, &status, &adapterVersion,
	); err != nil {
		t.Fatalf("read materialized result state: %v", err)
	}
	if inboxCount != 2 || resultCount != 1 || artifactCount != 1 ||
		linkCount != 1 || runCount != 1 || recordCount != 1 || claimCount != 1 ||
		status != "succeeded" || adapterVersion != "fixture-adapter/1" {
		t.Fatalf(
			"materialized result state = inbox %d receipts %d artifacts %d links %d runs %d records %d claims %d status %s adapter %s",
			inboxCount, resultCount, artifactCount, linkCount, runCount, recordCount, claimCount,
			status, adapterVersion,
		)
	}

	var conflictInboxCount int
	if err := pool.QueryRow(ctx, `
		select count(*) from consumed_messages
		where consumer_name = $1 and message_id = $2
	`, ingestion.DefaultResultConsumerName, conflict.MessageID).Scan(&conflictInboxCount); err != nil {
		t.Fatalf("read conflict inbox state: %v", err)
	}
	if conflictInboxCount != 0 {
		t.Fatalf("conflicting digest inbox entries = %d, want 0", conflictInboxCount)
	}

	future := testSourceResultCommand(jobID, investigationID, 2, uuid.NewString())
	_, err = store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, future)
	if !ingestion.IsPermanentResultError(err) {
		t.Fatalf("future attempt error = %v, want permanent result error", err)
	}
	var futureInboxCount int
	if err := pool.QueryRow(ctx, `
		select count(*) from consumed_messages
		where consumer_name = $1 and message_id = $2
	`, ingestion.DefaultResultConsumerName, future.MessageID).Scan(&futureInboxCount); err != nil {
		t.Fatalf("read future attempt inbox state: %v", err)
	}
	if futureInboxCount != 0 {
		t.Fatalf("future attempt inbox entries = %d, want 0", futureInboxCount)
	}
}

func TestStoreApplySourceResultKeepsStaleAndCancelledArtifactsWithoutBusinessEffects(t *testing.T) {
	tests := []struct {
		name          string
		prepare       func(*testing.T, context.Context, *Store, string, string)
		ignoredReason string
	}{
		{
			name: "stale attempt",
			prepare: func(t *testing.T, ctx context.Context, store *Store, jobID, investigationID string) {
				t.Helper()
				if err := store.ScheduleRetry(
					ctx, jobID, time.Now().UTC().Add(time.Minute), "SOURCE_TIMEOUT", "timeout",
				); err != nil {
					t.Fatalf("ScheduleRetry() error = %v", err)
				}
				if err := store.StartNewAttempt(
					ctx, jobID, 2,
					sourceCollectRequestedOutboxMessage(
						uuid.NewString(), jobID, investigationID, 2,
					),
				); err != nil {
					t.Fatalf("StartNewAttempt() error = %v", err)
				}
			},
			ignoredReason: "stale_attempt",
		},
		{
			name: "cancelled investigation",
			prepare: func(t *testing.T, ctx context.Context, store *Store, _, investigationID string) {
				t.Helper()
				if _, err := store.pool.Exec(ctx, `
					update investigations set status = 'cancelled' where id = $1
				`, investigationID); err != nil {
					t.Fatalf("cancel investigation: %v", err)
				}
			},
			ignoredReason: "late_cancelled_investigation",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			investigationID := uuid.NewString()
			jobID := uuid.NewString()
			messageID := uuid.NewString()
			pool, err := Open(ctx, testPostgresConfig())
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			t.Cleanup(pool.Close)
			seedRunningInvestigation(t, ctx, pool, investigationID, test.name)
			seedSourceResultJob(t, ctx, pool, jobID, investigationID, test.name)

			store := NewStore(pool)
			test.prepare(t, ctx, store, jobID, investigationID)
			result := testSourceResultCommand(jobID, investigationID, 1, messageID)
			outcome, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result)
			if err != nil {
				t.Fatalf("ApplySourceResult() error = %v", err)
			}
			if outcome.Applied || outcome.Duplicate || outcome.IgnoredReason != test.ignoredReason {
				t.Fatalf("ignored result outcome = %#v", outcome)
			}

			var artifactCount, linkCount, runCount, recordCount, claimCount int
			var applied bool
			var ignoredReason string
			if err := pool.QueryRow(ctx, `
				select
					(select count(*) from artifacts where object_key = $1),
					(select count(*) from source_job_result_artifacts where source_job_result_id = $2),
					(select count(*) from processing_runs where source_job_result_id = $2),
					(select count(*) from source_records where processing_run_id in
					 (select id from processing_runs where source_job_result_id = $2)),
					(select count(*) from claims where subject_source_record_id in
					 (select id from source_records where processing_run_id in
					  (select id from processing_runs where source_job_result_id = $2))),
					(select applied from source_job_results where id = $2),
					(select ignored_reason from source_job_results where id = $2)
			`, result.Artifacts[0].ObjectKey, result.ID).Scan(
				&artifactCount, &linkCount, &runCount, &recordCount, &claimCount,
				&applied, &ignoredReason,
			); err != nil {
				t.Fatalf("read ignored result state: %v", err)
			}
			if artifactCount != 1 || linkCount != 1 || runCount != 0 ||
				recordCount != 0 || claimCount != 0 || applied ||
				ignoredReason != test.ignoredReason {
				t.Fatalf(
					"ignored result state = artifacts %d links %d runs %d records %d claims %d applied %t reason %s",
					artifactCount, linkCount, runCount, recordCount, claimCount, applied, ignoredReason,
				)
			}
		})
	}
}

func seedSourceResultJob(
	t *testing.T,
	ctx context.Context,
	pool DBTX,
	jobID, investigationID, idempotencyKey string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'search_subject', 'subject_discovery', $4, $5)
	`, jobID, investigationID, testSourceID, strings.Repeat("a", 64), idempotencyKey); err != nil {
		t.Fatalf("seed source job: %v", err)
	}
}

func testSourceResultCommand(
	jobID, investigationID string,
	attemptNo int,
	messageID string,
) ingestion.SourceResultCommand {
	localRef := "artifact:raw:1"
	return ingestion.SourceResultCommand{
		ID:             messageID,
		MessageID:      messageID,
		CorrelationID:  investigationID,
		JobID:          jobID,
		AttemptNo:      attemptNo,
		Digest:         strings.Repeat("b", 64),
		AdapterVersion: "fixture-adapter/1",
		Status:         ingestion.ResultStatusSucceeded,
		Artifacts: []ingestion.ArtifactReference{{
			LocalRef:     localRef,
			Kind:         "raw_response",
			ObjectKey:    fmt.Sprintf("artifacts/v1/%s/%d/YXJ0aWZhY3Q6cmF3OjE", jobID, attemptNo),
			SHA256:       strings.Repeat("a", 64),
			MediaType:    "application/json",
			ByteSize:     2,
			SourceURI:    "https://example.test/source/1",
			RetrievedAt:  time.Now().UTC(),
			HTTPMetadata: map[string]string{},
		}},
		ProcessingRuns: []ingestion.ProcessingRunInput{{
			ArtifactLocalRef:     localRef,
			ProcessingScope:      "source_collection",
			SchemaVersion:        "person/1",
			ParserVersion:        "parser/1",
			MappingVersion:       "mapping/1",
			NormalizationVersion: "normalization/1",
			Records: []ingestion.SourceRecord{{
				LocalRef:         localRef + ":record:1",
				ArtifactLocalRef: localRef,
				RecordType:       "person",
				SchemaVersion:    "person/1",
				RawData:          map[string]any{"name": "Example"},
			}},
		}},
		Claims: []ingestion.Claim{{
			ClaimKind:          "attribute",
			SubjectLocalRef:    localRef + ":record:1",
			Predicate:          "name",
			RawValue:           "Example",
			NormalizedValue:    "example",
			Qualifiers:         map[string]any{},
			ValidFromPrecision: "unknown",
			ValidToPrecision:   "unknown",
			TemporalBasis:      "unknown",
			EvidenceLocator:    map[string]any{"json_pointer": "/name"},
		}},
	}
}
