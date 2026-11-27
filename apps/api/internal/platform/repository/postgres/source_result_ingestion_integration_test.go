//go:build integration

package postgres

import (
	"context"
	"encoding/json"
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
	for _, reusedID := range []string{result.MessageID, duplicate.MessageID} {
		conflict.MessageID = reusedID
		_, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, conflict)
		if ingestion.PermanentResultErrorCode(err) != "source_result_attempt_digest_conflict" {
			t.Fatalf("reused message_id with different digest error = %v", err)
		}
	}
	conflict.MessageID = conflict.ID

	otherJobID := uuid.NewString()
	seedSourceResultJob(t, ctx, pool, otherJobID, investigationID, "source-result-other-job")
	reused := testSourceResultCommand(otherJobID, investigationID, 1, duplicate.MessageID)
	reused.ID = uuid.NewString()
	if _, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, reused); ingestion.PermanentResultErrorCode(err) != "source_result_message_id_conflict" {
		t.Fatalf("reused alias for unseen job error = %v", err)
	}
	var otherReceipts int
	if err := pool.QueryRow(ctx, `
		select count(*) from source_job_results where source_job_id = $1
	`, otherJobID).Scan(&otherReceipts); err != nil || otherReceipts != 0 {
		t.Fatalf("other job receipts = %d, error %v", otherReceipts, err)
	}
	const legacyConsumer = "source-result-ingestion-legacy"
	if _, err := pool.Exec(ctx, `
		insert into consumed_messages (consumer_name, message_id)
		values ($1, $2), ($1, $3)
	`, legacyConsumer, result.MessageID, duplicate.MessageID); err != nil {
		t.Fatal(err)
	}
	for _, prior := range []ingestion.SourceResultCommand{result, duplicate} {
		outcome, err := store.CheckExistingSourceResult(ctx, legacyConsumer, prior)
		if err != nil || !outcome.Duplicate {
			t.Fatalf("historical inbox delivery = %#v, error %v", outcome, err)
		}
	}
	if _, err := store.ApplySourceResult(ctx, legacyConsumer, reused); ingestion.PermanentResultErrorCode(err) != "source_result_message_id_conflict" {
		t.Fatalf("reused historical alias for unseen job error = %v", err)
	}

	var inboxCount, resultCount, artifactCount, linkCount int
	var runCount, recordCount, claimCount int
	var status, adapterVersion string
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from consumed_messages where consumer_name = $1 and source_job_result_id = $4),
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

func TestStoreApplySourceResultRollsBackAndResolvesCrossArtifactClaim(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	investigationID, jobID := uuid.NewString(), uuid.NewString()
	seedRunningInvestigation(t, ctx, pool, investigationID, "Ingest cross-artifact claim")
	seedSourceResultJob(t, ctx, pool, jobID, investigationID, jobID)
	result := testSourceResultCommand(jobID, strings.ToUpper(investigationID), 1, uuid.NewString())
	parent := result.Artifacts[0]
	child := parent
	child.LocalRef = "child"
	child.ObjectKey = fmt.Sprintf("artifacts/v1/%s/1/Y2hpbGQ", jobID)
	child.ParentLocalRef = &parent.LocalRef
	result.Artifacts = []ingestion.ArtifactReference{child, parent}
	objectRecord := ingestion.SourceRecord{
		LocalRef: "object", ArtifactLocalRef: child.LocalRef, RecordType: "person",
		SchemaVersion: "person/1", RawData: map[string]any{"ratio": json.Number("1e-20000")},
	}
	objectRun := result.ProcessingRuns[0]
	objectRun.ArtifactLocalRef = child.LocalRef
	objectRun.Records = []ingestion.SourceRecord{objectRecord}
	result.ProcessingRuns = append(result.ProcessingRuns, objectRun)
	result.Claims[0].ClaimKind = "relationship"
	result.Claims[0].ObjectLocalRef = &objectRecord.LocalRef
	store := NewStore(pool)
	if _, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result); ingestion.PermanentResultErrorCode(err) != "source_result_value_out_of_range" {
		t.Fatalf("unrepresentable raw_data error = %v", err)
	}
	var status string
	var writes int
	if err := pool.QueryRow(ctx, `
		select status,
		       (select count(*) from source_job_results where source_job_id = $1) +
		       (select count(*) from consumed_messages where message_id = $2) +
		       (select count(*) from artifacts where object_key in ($3, $4))
		from source_jobs where id = $1
	`, jobID, result.MessageID, parent.ObjectKey, child.ObjectKey).Scan(&status, &writes); err != nil || writes != 0 || status != "pending" {
		t.Fatalf("after rollback: job %q, writes %d, error %v", status, writes, err)
	}

	parentID := uuid.NewString()
	if _, err := pool.Exec(ctx, `
		insert into artifacts (
			id, source_id, kind, media_type, object_key, source_uri, sha256, byte_size,
			retrieved_at, adapter_version
		) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, parentID, testSourceID, parent.Kind, parent.MediaType, parent.ObjectKey,
		parent.SourceURI, parent.SHA256, parent.ByteSize, parent.RetrievedAt, result.AdapterVersion); err != nil {
		t.Fatal(err)
	}
	objectRecord.RawData["ratio"] = json.Number("1.0000000000000001")
	result.Artifacts[1].SHA256 = strings.Repeat("d", 64)
	if _, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result); ingestion.PermanentResultErrorCode(err) != "source_result_artifact_key_conflict" {
		t.Fatalf("conflicting immutable artifact error = %v", err)
	}
	result.Artifacts[1].SHA256 = parent.SHA256
	if outcome, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result); err != nil || !outcome.Applied {
		t.Fatalf("corrected delivery = %#v, error %v", outcome, err)
	}
	var persistedParentID, ratio string
	if err := pool.QueryRow(ctx, `
		select child.parent_artifact_id, object.raw_data->>'ratio'
		from claims
		join source_records subject on subject.id = claims.subject_source_record_id
		join source_records object on object.id = claims.object_source_record_id
		join processing_runs on processing_runs.id = object.processing_run_id
		join artifacts child on child.id = processing_runs.artifact_id
		where processing_runs.source_job_result_id = $1
		and subject.processing_run_id <> object.processing_run_id
	`, result.ID).Scan(&persistedParentID, &ratio); err != nil || persistedParentID != parentID || ratio != "1.0000000000000001" {
		t.Fatalf("cross-artifact claim: parent %q, ratio %q, error %v", persistedParentID, ratio, err)
	}
}

func TestStoreApplySourceResultConcurrentDeliveriesApplyOnce(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	investigationID, jobID := uuid.NewString(), uuid.NewString()
	seedRunningInvestigation(t, ctx, pool, investigationID, "Ingest concurrent results")
	seedSourceResultJob(t, ctx, pool, jobID, investigationID, jobID)

	type deliveryResult struct {
		ingestion.ResultApplyOutcome
		err error
	}
	store := NewStore(pool)
	start := make(chan struct{})
	results := make(chan deliveryResult, 2)
	for range cap(results) {
		go func() {
			result := testSourceResultCommand(jobID, investigationID, 1, uuid.NewString())
			<-start
			outcome, err := store.ApplySourceResult(ctx, ingestion.DefaultResultConsumerName, result)
			results <- deliveryResult{outcome, err}
		}()
	}
	close(start)

	applied, duplicate := 0, 0
	for range cap(results) {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.Applied {
			applied++
		}
		if result.Duplicate {
			duplicate++
		}
	}
	if applied != 1 || duplicate != 1 {
		t.Fatalf("concurrent results: applied = %d, duplicate = %d", applied, duplicate)
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
