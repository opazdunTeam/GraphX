//go:build integration

package postgres

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	testInvestigationID = "10000000-0000-4000-8000-000000000001"
	testSourceID        = "00000000-0000-4000-8000-000000000001"
	testJobID           = "20000000-0000-4000-8000-000000000001"
	testResultID        = "30000000-0000-4000-8000-000000000001"
	testArtifactID      = "40000000-0000-4000-8000-000000000001"
	testRunID           = "50000000-0000-4000-8000-000000000001"
	testRecordID        = "60000000-0000-4000-8000-000000000001"
)

func TestMigrationSeedIsPresent(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	var name, mode string
	var enabled bool
	if err := pool.QueryRow(
		ctx,
		"select name, mode, enabled from sources where code = 'fixture'",
	).Scan(&name, &mode, &enabled); err != nil {
		t.Fatalf("read fixture source: %v", err)
	}
	if name != "Fixture" || mode != "file" || !enabled {
		t.Fatalf("fixture source = %q/%q/%t, want Fixture/file/true", name, mode, enabled)
	}

	if _, err := pool.Exec(ctx, `
		insert into sources (id, code, name, mode, enabled, launch_order)
		values ('00000000-0000-4000-8000-000000000001', 'fixture', 'Fixture', 'file', true, 0)
		on conflict (code) do nothing
	`); err != nil {
		t.Fatalf("repeat fixture seed: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, "select count(*) from sources where code = 'fixture'").Scan(&count); err != nil {
		t.Fatalf("count fixture source: %v", err)
	}
	if count != 1 {
		t.Fatalf("fixture source count = %d, want 1", count)
	}
}

func TestMigrationOwnershipMarkerAndResolutionIndexes(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	_, err = pool.Exec(ctx, `
		insert into schema_extension_ownership (extension_name) values ('pg_trgm')
	`)
	requireDatabaseRejection(t, err, "forged extension ownership marker")

	var entityRecordIndexIsFull, entityRecordIndexIsUnique, resolvedEntityIndexIsPartial bool
	if err := pool.QueryRow(ctx, `
		select
			(select indpred is null from pg_index
			 where indexrelid = 'idx_entity_records_match_decision'::regclass),
			(select indisunique from pg_index
			 where indexrelid = 'idx_entity_records_match_decision'::regclass),
			(select indpred is not null from pg_index
			 where indexrelid = 'idx_match_decisions_resolved_entity'::regclass)
	`).Scan(&entityRecordIndexIsFull, &entityRecordIndexIsUnique, &resolvedEntityIndexIsPartial); err != nil {
		t.Fatalf("read resolution index definitions: %v", err)
	}
	if !entityRecordIndexIsFull || !entityRecordIndexIsUnique || !resolvedEntityIndexIsPartial {
		t.Fatalf(
			"resolution indexes = entity_records full %t unique %t, resolved_entity partial %t",
			entityRecordIndexIsFull, entityRecordIndexIsUnique, resolvedEntityIndexIsPartial,
		)
	}
}

func TestProjectionMigrationLockSerializesConcurrentInserts(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin migration lock transaction: %v", err)
	}
	defer func() { _ = lockTx.Rollback(context.Background()) }()
	if _, err := lockTx.Exec(ctx,
		"lock table entity_names, entity_identifiers in share row exclusive mode",
	); err != nil {
		t.Fatalf("lock projection tables: %v", err)
	}

	type insertResult struct {
		table string
		err   error
	}
	started := make(chan string, 2)
	results := make(chan insertResult, 2)
	go func() {
		started <- "entity_names"
		_, execErr := pool.Exec(ctx, `
			insert into entity_names (
				id, entity_id, claim_id, raw_name, normalized_name, normalization_version
			) values (
				'81000000-0000-4000-8000-000000000097',
				'80000000-0000-4000-8000-000000000097',
				'70000000-0000-4000-8000-000000000097',
				'Concurrent', 'concurrent', 'normalizer/1'
			)
		`)
		results <- insertResult{table: "entity_names", err: execErr}
	}()
	go func() {
		started <- "entity_identifiers"
		_, execErr := pool.Exec(ctx, `
			insert into entity_identifiers (
				id, entity_id, claim_id, identifier_type,
				raw_value, normalized_value, normalization_version
			) values (
				'82000000-0000-4000-8000-000000000097',
				'80000000-0000-4000-8000-000000000097',
				'70000000-0000-4000-8000-000000000097',
				'test', 'Concurrent', 'concurrent', 'normalizer/1'
			)
		`)
		results <- insertResult{table: "entity_identifiers", err: execErr}
	}()

	<-started
	<-started
	waitDeadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case result := <-results:
			t.Fatalf("concurrent insert into %s was not blocked: %v", result.table, result.err)
		default:
		}

		var waitingInserts int
		if err := pool.QueryRow(ctx, `
			select count(*)
			from pg_locks
			where relation in ('entity_names'::regclass, 'entity_identifiers'::regclass)
			  and mode = 'RowExclusiveLock'
			  and not granted
		`).Scan(&waitingInserts); err != nil {
			t.Fatalf("inspect concurrent projection locks: %v", err)
		}
		if waitingInserts == 2 {
			break
		}
		if time.Now().After(waitDeadline) {
			t.Fatalf("waiting projection inserts = %d, want 2", waitingInserts)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := lockTx.Commit(ctx); err != nil {
		t.Fatalf("commit migration lock transaction: %v", err)
	}
	for range 2 {
		select {
		case result := <-results:
			requireDatabaseRejection(t, result.err, "orphan insert into "+result.table+" after migration lock")
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent projection insert did not resume after migration lock")
		}
	}
}

func TestMigrationAcceptsValidProvenanceChain(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)

		const claimID = "70000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into claims (
				id, subject_source_record_id, kind, predicate,
				raw_value, normalized_value, valid_from, valid_to,
				valid_from_precision, valid_to_precision, temporal_basis,
				evidence_locator, parser_version, mapping_version, normalization_version
			) values (
				$1, $2, 'attribute', 'name',
				'"Ada"'::jsonb, '"ada"'::jsonb, date '2020-01-01', date '2021-01-01',
				'day', 'day', 'explicit',
				'{"pointer":"/name"}'::jsonb, 'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, claimID, testRecordID); err != nil {
			t.Fatalf("insert valid claim: %v", err)
		}

		const entityID = "80000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name)
			values ($1, 'person', 'Ada')
		`, entityID); err != nil {
			t.Fatalf("insert entity: %v", err)
		}
		const reviewID = "90000000-0000-4000-8000-000000000001"
		const candidateID = "91000000-0000-4000-8000-000000000001"
		const decisionID = "92000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id)
			values ($1, $2)
		`, reviewID, testRecordID); err != nil {
			t.Fatalf("insert review: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id,
				score, recommendation, algorithm_version
			) values ($1, $2, 1, $3, 0.95, 'review', 'matcher/1')
		`, candidateID, reviewID, entityID); err != nil {
			t.Fatalf("insert candidate: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
		`, decisionID, reviewID, candidateID, entityID); err != nil {
			t.Fatalf("insert decision: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id, link_method
			) values (
				'93000000-0000-4000-8000-000000000001', $1, $2, $3, 'manual'
			)
		`, entityID, testRecordID, decisionID); err != nil {
			t.Fatalf("insert entity record: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entity_names (
				id, entity_id, claim_id, raw_name, normalized_name, normalization_version
			) values (
				'81000000-0000-4000-8000-000000000001', $1, $2, 'Ada', 'ada', 'normalizer/1'
			)
		`, entityID, claimID); err != nil {
			t.Fatalf("insert entity name: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into investigation_entities (
				investigation_id, entity_id, depth, inclusion_reason
			) values ($1, $2, 0, 'subject')
		`, testInvestigationID, entityID); err != nil {
			t.Fatalf("insert investigation entity: %v", err)
		}
	})
}

func TestMigrationRejectsInvalidResultState(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		_, err := tx.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied
			) values (
				$1, $2, '30000000-0000-4000-8000-000000000099', 1, $3, false
			)
		`, testResultID, testJobID, strings.Repeat("b", 64))
		requireDatabaseRejection(t, err, "unapplied result without ignored_reason")
	})
}

func TestDuplicateAttemptDoesNotReplaceAcceptedResult(t *testing.T) {
	testCases := []struct {
		name   string
		digest string
	}{
		{name: "same digest", digest: strings.Repeat("b", 64)},
		{name: "conflicting digest", digest: strings.Repeat("c", 64)},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
				seedIngestionChain(t, ctx, tx)

				if _, err := tx.Exec(ctx, "savepoint duplicate_result"); err != nil {
					t.Fatalf("create savepoint: %v", err)
				}
				_, duplicateErr := tx.Exec(ctx, `
					insert into source_job_results (
						id, source_job_id, message_id, attempt_no, result_digest, applied
					) values (
						'30000000-0000-4000-8000-000000000002', $1,
						'30000000-0000-4000-8000-000000000002', 1, $2, true
					)
				`, testJobID, testCase.digest)
				requireDatabaseRejection(t, duplicateErr, "second result for the same job attempt")
				if _, err := tx.Exec(ctx, "rollback to savepoint duplicate_result"); err != nil {
					t.Fatalf("rollback duplicate result: %v", err)
				}

				var count int
				var digest string
				if err := tx.QueryRow(ctx, `
					select count(*), min(result_digest)
					from source_job_results
					where source_job_id = $1 and attempt_no = 1
				`, testJobID).Scan(&count, &digest); err != nil {
					t.Fatalf("read accepted result: %v", err)
				}
				if count != 1 || digest != strings.Repeat("b", 64) {
					t.Fatalf("accepted result = count %d digest %q", count, digest)
				}

				var links, runs, records int
				if err := tx.QueryRow(ctx, `
					select
						(select count(*) from source_job_result_artifacts where source_job_result_id = $1),
						(select count(*) from processing_runs where source_job_result_id = $1),
						(select count(*) from source_records where processing_run_id = $2)
				`, testResultID, testRunID).Scan(&links, &runs, &records); err != nil {
					t.Fatalf("read duplicate downstream state: %v", err)
				}
				if links != 1 || runs != 1 || records != 1 {
					t.Fatalf("duplicate downstream state = links %d runs %d records %d", links, runs, records)
				}
			})
		})
	}
}

func TestSourceJobResultCannotBeRewritten(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		if _, err := tx.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied
			) values ($1, $2, '30000000-0000-4000-8000-000000000001', 1, $3, true)
		`, testResultID, testJobID, strings.Repeat("b", 64)); err != nil {
			t.Fatalf("insert accepted result: %v", err)
		}
		_, err := tx.Exec(ctx, `
			update source_job_results
			set result_digest = $2
			where id = $1
		`, testResultID, strings.Repeat("c", 64))
		requireDatabaseRejection(t, err, "rewrite of accepted result")
	})
}

func TestMigrationRejectsCrossResultProcessingRun(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const otherJobID = "20000000-0000-4000-8000-000000000002"
		const otherResultID = "30000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose, request_hash, idempotency_key
			) values ($1, $2, $3, 'collect', 'enrichment', $4, 'migration-test-job-2')
		`, otherJobID, testInvestigationID, testSourceID, strings.Repeat("d", 64)); err != nil {
			t.Fatalf("insert second job: %v", err)
		}
		if _, err := tx.Exec(ctx, "update source_jobs set status = 'running' where id = $1", otherJobID); err != nil {
			t.Fatalf("start second job: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied
			) values ($1, $2, '30000000-0000-4000-8000-000000000002', 1, $3, true)
		`, otherResultID, otherJobID, strings.Repeat("c", 64)); err != nil {
			t.Fatalf("insert second result: %v", err)
		}
		_, err := tx.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, processing_scope, status,
				schema_version, parser_version, mapping_version, normalization_version
			) values (
				'50000000-0000-4000-8000-000000000099', $1, $2, 'source_collection', 'succeeded',
				'schema/1', 'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, testArtifactID, otherResultID)
		requireDatabaseRejection(t, err, "processing run with artifact from another result")
	})
}

func TestArtifactSourceMustMatchImmutableJobSource(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const otherSourceID = "00000000-0000-4000-8000-000000000002"
		const otherArtifactID = "40000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `insert into sources (id, code, name, mode) values ($1, 'other', 'Other', 'manual')`, otherSourceID); err != nil {
			t.Fatalf("insert other source: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key, sha256, byte_size, retrieved_at, adapter_version
			) values ($2, $1, 'document', 'application/pdf', 'migration-test/wrong-source.pdf', $3, 1, now(), 'other/1')
		`, otherSourceID, otherArtifactID, strings.Repeat("d", 64)); err != nil {
			t.Fatalf("insert other-source artifact: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "artifact from a different source", `
			insert into source_job_result_artifacts (source_job_result_id, artifact_id, artifact_local_ref, role)
			values ($1, $2, 'wrong-source', 'document')
		`, testResultID, otherArtifactID)
		requireRejectedStatement(t, ctx, tx, "rewrite source job source", `
			update source_jobs set source_id = $2 where id = $1
		`, testJobID, otherSourceID)
	})
}

func TestArtifactLineageIsImmutableAndRequiresTheSameSource(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const childArtifactID = "40000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key, sha256, byte_size,
				retrieved_at, adapter_version, original_filename, parent_artifact_id
			) values (
				$1, $2, 'document', 'application/pdf', 'migration-test/child.pdf', $3, 1,
				now(), 'fixture/1', 'safe.pdf', $4
			)
		`, childArtifactID, testSourceID, strings.Repeat("d", 64), testArtifactID); err != nil {
			t.Fatalf("insert child artifact: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "rewrite artifact parent", `
			update artifacts set parent_artifact_id = null where id = $1
		`, childArtifactID)
		for _, filename := range []string{" ", "...", " report.pdf", "report.pdf ", "bad/name", "bad\\name", "bad\x7fname", "bad\u0085name"} {
			requireRejectedStatement(t, ctx, tx, "unsafe original filename", `
				insert into artifacts (
					id, source_id, kind, media_type, object_key, sha256, byte_size,
					retrieved_at, adapter_version, original_filename
				) values (
					'40000000-0000-4000-8000-000000000099', $1, 'document',
					'application/pdf', 'migration-test/unsafe.pdf', $2, 1,
					now(), 'fixture/1', $3
				)
			`, testSourceID, strings.Repeat("f", 64), filename)
		}

		const otherSourceID = "00000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into sources (id, code, name, mode)
			values ($1, 'lineage-other', 'Lineage Other', 'manual')
		`, otherSourceID); err != nil {
			t.Fatalf("insert other source: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "cross-source artifact parent", `
			insert into artifacts (
				id, source_id, kind, media_type, object_key, sha256, byte_size,
				retrieved_at, adapter_version, parent_artifact_id
			) values (
				'40000000-0000-4000-8000-000000000003', $1, 'document',
				'application/pdf', 'migration-test/cross-source.pdf', $2, 1,
				now(), 'other/1', $3
			)
		`, otherSourceID, strings.Repeat("e", 64), testArtifactID)
	})
}

func TestLateCancelledResultKeepsReceiptAndArtifactsOnly(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		if _, err := tx.Exec(ctx, `
			update source_jobs
			set status = 'cancelled', finished_at = now()
			where id = $1
		`, testJobID); err != nil {
			t.Fatalf("cancel source job: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no,
				result_digest, applied, ignored_reason
			) values (
				$1, $2, '30000000-0000-4000-8000-000000000001', 1,
				$3, false, 'late_cancelled_result'
			)
		`, testResultID, testJobID, strings.Repeat("b", 64)); err != nil {
			t.Fatalf("insert late result receipt: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key,
				sha256, byte_size, retrieved_at, adapter_version
			) values (
				$1, $2, 'raw_response', 'application/json', 'migration-test/late.json',
				$3, 2, now(), 'fixture/1'
			)
		`, testArtifactID, testSourceID, strings.Repeat("c", 64)); err != nil {
			t.Fatalf("insert late artifact: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_result_artifacts (
				source_job_result_id, artifact_id, artifact_local_ref, role
			) values ($1, $2, 'late-artifact', 'raw_response')
		`, testResultID, testArtifactID); err != nil {
			t.Fatalf("link late artifact: %v", err)
		}

		if _, err := tx.Exec(ctx, "savepoint late_processing"); err != nil {
			t.Fatalf("create savepoint: %v", err)
		}
		_, processingErr := tx.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, processing_scope, status,
				schema_version, parser_version, mapping_version, normalization_version
			) values (
				$1, $2, $3, 'source_collection', 'succeeded',
				'schema/1', 'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, testRunID, testArtifactID, testResultID)
		requireDatabaseRejection(t, processingErr, "processing run for an unapplied result")
		if _, err := tx.Exec(ctx, "rollback to savepoint late_processing"); err != nil {
			t.Fatalf("rollback late processing: %v", err)
		}

		var receipts, artifactLinks, runs int
		if err := tx.QueryRow(ctx, `
			select
				(select count(*) from source_job_results where id = $1),
				(select count(*) from source_job_result_artifacts where source_job_result_id = $1),
				(select count(*) from processing_runs where source_job_result_id = $1)
		`, testResultID).Scan(&receipts, &artifactLinks, &runs); err != nil {
			t.Fatalf("read late result state: %v", err)
		}
		if receipts != 1 || artifactLinks != 1 || runs != 0 {
			t.Fatalf("late result state = receipts %d links %d runs %d", receipts, artifactLinks, runs)
		}
	})
}

func TestAppliedResultRequiresCurrentLiveAttempt(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		if _, err := tx.Exec(ctx, `
			update source_jobs set status = 'cancelled', finished_at = now() where id = $1
		`, testJobID); err != nil {
			t.Fatalf("cancel source job: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "applied result for cancelled job", `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied
			) values ($1, $2, '30000000-0000-4000-8000-000000000010', 1, $3, true)
		`, testResultID, testJobID, strings.Repeat("b", 64))

		if _, err := tx.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied, ignored_reason
			) values ($1, $2, '30000000-0000-4000-8000-000000000011', 1, $3, false, 'late_cancelled_result')
		`, testResultID, testJobID, strings.Repeat("b", 64)); err != nil {
			t.Fatalf("store ignored late result: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "future attempt receipt", `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest, applied, ignored_reason
			) values (
				'30000000-0000-4000-8000-000000000012', $1,
				'30000000-0000-4000-8000-000000000012', 2, $2, false, 'future_attempt'
			)
		`, testJobID, strings.Repeat("c", 64))
	})
}

func TestConcurrentResultDeliveriesSerializeOnSourceJob(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	const investigationID = "10000000-0000-4000-8000-000000000010"
	const sourceID = "00000000-0000-4000-8000-000000000010"
	const jobID = "20000000-0000-4000-8000-000000000010"
	if _, err := pool.Exec(ctx, `
		insert into sources (id, code, name, mode) values ($1, 'concurrent', 'Concurrent', 'manual')
	`, sourceID); err != nil {
		t.Fatalf("seed concurrent source: %v", err)
	}
	seedRunningInvestigation(t, ctx, pool, investigationID, "Concurrent")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose, request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'subject_discovery', $4, 'concurrent-result-job')
	`, jobID, investigationID, sourceID, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("seed concurrent job: %v", err)
	}
	if _, err := pool.Exec(ctx, "update source_jobs set status = 'running' where id = $1", jobID); err != nil {
		t.Fatalf("start concurrent job: %v", err)
	}

	firstTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin first delivery: %v", err)
	}
	if _, err := firstTx.Exec(ctx, `
		insert into source_job_results (id, source_job_id, message_id, attempt_no, result_digest, applied)
		values ('30000000-0000-4000-8000-000000000020', $1,
			'30000000-0000-4000-8000-000000000020', 1, $2, true)
	`, jobID, strings.Repeat("b", 64)); err != nil {
		t.Fatalf("insert first concurrent result: %v", err)
	}

	secondTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin second delivery: %v", err)
	}
	secondResult := make(chan error, 1)
	go func() {
		_, insertErr := secondTx.Exec(ctx, `
			insert into source_job_results (id, source_job_id, message_id, attempt_no, result_digest, applied)
			values ('30000000-0000-4000-8000-000000000021', $1,
				'30000000-0000-4000-8000-000000000021', 1, $2, true)
		`, jobID, strings.Repeat("c", 64))
		secondResult <- insertErr
	}()

	if err := firstTx.Commit(ctx); err != nil {
		t.Fatalf("commit first delivery: %v", err)
	}
	select {
	case secondErr := <-secondResult:
		requireDatabaseRejection(t, secondErr, "second concurrent result for one attempt")
	case <-time.After(5 * time.Second):
		t.Fatal("second concurrent result did not resume after the first commit")
	}
	_ = secondTx.Rollback(context.Background())
}

func TestConcurrentInvestigationCancellationAndJobRetrySerialize(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	const cancelFirstInvestigationID = "10000000-0000-4000-8000-000000000030"
	const cancelFirstJobID = "20000000-0000-4000-8000-000000000030"
	seedRunningInvestigation(t, ctx, pool, cancelFirstInvestigationID, "Cancel first")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'enrichment', 'cancel-first', 'cancel-first-job')
	`, cancelFirstJobID, cancelFirstInvestigationID, testSourceID); err != nil {
		t.Fatalf("insert cancel-first job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		update source_jobs
		set status = 'retry_wait', next_attempt_at = now() + interval '1 minute'
		where id = $1
	`, cancelFirstJobID); err != nil {
		t.Fatalf("prepare cancel-first retry: %v", err)
	}

	cancelTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin cancellation-first transaction: %v", err)
	}
	if _, err := cancelTx.Exec(ctx, `
		update investigations set status = 'cancelled' where id = $1
	`, cancelFirstInvestigationID); err != nil {
		t.Fatalf("stage cancellation-first update: %v", err)
	}
	retryResult := make(chan error, 1)
	go func() {
		_, updateErr := pool.Exec(ctx, `
			update source_jobs
			set status = 'pending', attempt_no = 2, next_attempt_at = null
			where id = $1
		`, cancelFirstJobID)
		retryResult <- updateErr
	}()
	select {
	case updateErr := <-retryResult:
		t.Fatalf("retry did not wait for investigation cancellation: %v", updateErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := cancelTx.Commit(ctx); err != nil {
		t.Fatalf("commit cancellation-first transaction: %v", err)
	}
	requireDatabaseRejection(t, <-retryResult, "retry committed after cancellation")

	const retryFirstInvestigationID = "10000000-0000-4000-8000-000000000031"
	const retryFirstJobID = "20000000-0000-4000-8000-000000000031"
	seedRunningInvestigation(t, ctx, pool, retryFirstInvestigationID, "Retry first")
	if _, err := pool.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			request_hash, idempotency_key
		) values ($1, $2, $3, 'collect', 'enrichment', 'retry-first', 'retry-first-job')
	`, retryFirstJobID, retryFirstInvestigationID, testSourceID); err != nil {
		t.Fatalf("insert retry-first job: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		update source_jobs set status = 'action_required' where id = $1
	`, retryFirstJobID); err != nil {
		t.Fatalf("prepare retry-first job: %v", err)
	}

	retryTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin retry-first transaction: %v", err)
	}
	if _, err := retryTx.Exec(ctx, `
		update source_jobs set status = 'pending', attempt_no = 2 where id = $1
	`, retryFirstJobID); err != nil {
		t.Fatalf("stage retry-first update: %v", err)
	}
	cancelResult := make(chan error, 1)
	go func() {
		_, updateErr := pool.Exec(ctx, `
			update investigations set status = 'cancelled' where id = $1
		`, retryFirstInvestigationID)
		cancelResult <- updateErr
	}()
	select {
	case updateErr := <-cancelResult:
		t.Fatalf("cancellation did not wait for job retry: %v", updateErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := retryTx.Commit(ctx); err != nil {
		t.Fatalf("commit retry-first transaction: %v", err)
	}
	if err := <-cancelResult; err != nil {
		t.Fatalf("cancel investigation after committed retry: %v", err)
	}

	var investigationStatus, jobStatus string
	var attemptNo int
	if err := pool.QueryRow(ctx, `
		select investigation.status, job.status, job.attempt_no
		from investigations as investigation
		join source_jobs as job on job.investigation_id = investigation.id
		where investigation.id = $1
	`, retryFirstInvestigationID).Scan(&investigationStatus, &jobStatus, &attemptNo); err != nil {
		t.Fatalf("read retry-first final state: %v", err)
	}
	if investigationStatus != "cancelled" || jobStatus != "pending" || attemptNo != 2 {
		t.Fatalf(
			"retry-first final state = investigation %s job %s attempt %d",
			investigationStatus, jobStatus, attemptNo,
		)
	}
}

func TestRetryWaitRequiresRemainingAttempt(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJobWithMaxAttempts(t, ctx, tx, 1)
		requireRejectedStatement(t, ctx, tx, "retry after attempt exhaustion", `
			update source_jobs
			set status = 'retry_wait', next_attempt_at = now() + interval '30 seconds'
			where id = $1
		`, testJobID)
		if _, err := tx.Exec(ctx, `
			update source_jobs set status = 'unavailable', finished_at = now() where id = $1
		`, testJobID); err != nil {
			t.Fatalf("finish exhausted job as unavailable: %v", err)
		}
	})
}

func TestSourceJobInitialStateAndInvestigationBoundary(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		requireRejectedStatement(t, ctx, tx, "source job starting at attempt 2", `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose,
				attempt_no, request_hash, idempotency_key
			) values (
				'20000000-0000-4000-8000-000000000090', $1, $2,
				'collect', 'enrichment', 2, 'attempt-2', 'attempt-2-job'
			)
		`, testInvestigationID, testSourceID)
		requireRejectedStatement(t, ctx, tx, "source job starting queued", `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose,
				status, request_hash, idempotency_key
			) values (
				'20000000-0000-4000-8000-000000000091', $1, $2,
				'collect', 'enrichment', 'queued', 'queued', 'queued-job'
			)
		`, testInvestigationID, testSourceID)

		if _, err := tx.Exec(ctx, `
			update source_jobs set status = 'action_required' where id = $1
		`, testJobID); err != nil {
			t.Fatalf("prepare action-required job: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			update investigations set status = 'cancelled' where id = $1
		`, testInvestigationID); err != nil {
			t.Fatalf("cancel investigation for source-job boundary: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "source job created after cancellation", `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose,
				request_hash, idempotency_key
			) values (
				'20000000-0000-4000-8000-000000000092', $1, $2,
				'collect', 'enrichment', 'cancelled', 'cancelled-job'
			)
		`, testInvestigationID, testSourceID)
		requireRejectedStatement(t, ctx, tx, "source job retry after cancellation", `
			update source_jobs set status = 'pending', attempt_no = 2 where id = $1
		`, testJobID)
	})
}

func TestIngestionProvenanceIsAppendOnly(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const claimID = "70000000-0000-4000-8000-000000000001"
		seedAttributeClaim(t, ctx, tx, claimID, "name", `"Ada"`)

		for _, testCase := range []struct {
			name  string
			query string
			args  []any
		}{
			{name: "artifact update", query: "update artifacts set sha256 = $2 where id = $1", args: []any{testArtifactID, strings.Repeat("d", 64)}},
			{name: "artifact delete", query: "delete from artifacts where id = $1", args: []any{testArtifactID}},
			{name: "artifact link update", query: "update source_job_result_artifacts set role = 'document' where artifact_id = $1", args: []any{testArtifactID}},
			{name: "artifact link delete", query: "delete from source_job_result_artifacts where artifact_id = $1", args: []any{testArtifactID}},
			{name: "run provenance update", query: "update processing_runs set source_job_result_id = null where id = $1", args: []any{testRunID}},
			{name: "run delete", query: "delete from processing_runs where id = $1", args: []any{testRunID}},
			{name: "record update", query: "update source_records set local_ref = 'rewritten' where id = $1", args: []any{testRecordID}},
			{name: "record delete", query: "delete from source_records where id = $1", args: []any{testRecordID}},
			{name: "claim update", query: "update claims set predicate = 'rewritten' where id = $1", args: []any{claimID}},
			{name: "claim delete", query: "delete from claims where id = $1", args: []any{claimID}},
		} {
			t.Run(testCase.name, func(t *testing.T) {
				requireRejectedStatement(t, ctx, tx, testCase.name, testCase.query, testCase.args...)
			})
		}
	})
}

func TestRelationshipClaimMayReferenceRecordsFromDifferentArtifacts(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const secondArtifactID = "40000000-0000-4000-8000-000000000002"
		const secondRunID = "50000000-0000-4000-8000-000000000002"
		const secondRecordID = "60000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key,
				sha256, byte_size, retrieved_at, adapter_version
			) values (
				$1, $2, 'document', 'application/pdf', 'migration-test/document.pdf',
				$3, 10, now(), 'fixture/1'
			)
		`, secondArtifactID, testSourceID, strings.Repeat("d", 64)); err != nil {
			t.Fatalf("insert second artifact: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_result_artifacts (
				source_job_result_id, artifact_id, artifact_local_ref, role
			) values ($1, $2, 'artifact-2', 'document')
		`, testResultID, secondArtifactID); err != nil {
			t.Fatalf("link second artifact: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, processing_scope, status,
				schema_version, parser_version, mapping_version, normalization_version, active
			) values (
				$1, $2, $3, 'source_collection', 'succeeded',
				'schema/1', 'parser/1', 'mapper/1', 'normalizer/1', true
			)
		`, secondRunID, secondArtifactID, testResultID); err != nil {
			t.Fatalf("insert second processing run: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_records (
				id, processing_run_id, local_ref, record_type, schema_version, raw_data
			) values ($1, $2, 'record-2', 'organization', 'fixture/1', '{"name":"Example"}'::jsonb)
		`, secondRecordID, secondRunID); err != nil {
			t.Fatalf("insert second source record: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into claims (
				id, subject_source_record_id, object_source_record_id, kind, predicate,
				raw_value, valid_from_precision, valid_to_precision, temporal_basis,
				evidence_locator, parser_version, mapping_version, normalization_version
			) values (
				'70000000-0000-4000-8000-000000000001', $1, $2, 'relationship', 'officer_of',
				'null'::jsonb, 'unknown', 'unknown', 'unknown',
				'{"pointer":"/roles/0"}'::jsonb, 'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, testRecordID, secondRecordID); err != nil {
			t.Fatalf("insert cross-artifact relationship claim: %v", err)
		}
	})
}

func TestRelationshipClaimRejectsRecordsFromDifferentResults(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const otherJobID = "20000000-0000-4000-8000-000000000002"
		const otherResultID = "30000000-0000-4000-8000-000000000002"
		const otherArtifactID = "40000000-0000-4000-8000-000000000002"
		const otherRunID = "50000000-0000-4000-8000-000000000002"
		const otherRecordID = "60000000-0000-4000-8000-000000000002"

		if _, err := tx.Exec(ctx, `
			insert into source_jobs (
				id, investigation_id, source_id, operation, purpose, request_hash, idempotency_key
			) values ($1, $2, $3, 'collect', 'enrichment', $4, 'cross-result-job')
		`, otherJobID, testInvestigationID, testSourceID, strings.Repeat("d", 64)); err != nil {
			t.Fatalf("insert other job: %v", err)
		}
		if _, err := tx.Exec(ctx, "update source_jobs set status = 'running' where id = $1", otherJobID); err != nil {
			t.Fatalf("start other job: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_results (id, source_job_id, message_id, attempt_no, result_digest, applied)
			values ($1, $2, '30000000-0000-4000-8000-000000000002', 1, $3, true)
		`, otherResultID, otherJobID, strings.Repeat("e", 64)); err != nil {
			t.Fatalf("insert other result: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key, sha256, byte_size, retrieved_at, adapter_version
			) values ($1, $2, 'document', 'application/pdf', 'migration-test/other.pdf', $3, 10, now(), 'fixture/1')
		`, otherArtifactID, testSourceID, strings.Repeat("f", 64)); err != nil {
			t.Fatalf("insert other artifact: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_job_result_artifacts (source_job_result_id, artifact_id, artifact_local_ref, role)
			values ($1, $2, 'other-artifact', 'document')
		`, otherResultID, otherArtifactID); err != nil {
			t.Fatalf("link other artifact: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, processing_scope, status,
				schema_version, parser_version, mapping_version, normalization_version, active
			) values ($1, $2, $3, 'source_collection', 'succeeded', 'schema/1', 'parser/1', 'mapper/1', 'normalizer/1', true)
		`, otherRunID, otherArtifactID, otherResultID); err != nil {
			t.Fatalf("insert other run: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into source_records (id, processing_run_id, local_ref, record_type, schema_version, raw_data)
			values ($1, $2, 'other-record', 'organization', 'fixture/1', '{"name":"Other"}'::jsonb)
		`, otherRecordID, otherRunID); err != nil {
			t.Fatalf("insert other record: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "cross-result relationship claim", `
			insert into claims (
				id, subject_source_record_id, object_source_record_id, kind, predicate, raw_value,
				valid_from_precision, valid_to_precision, temporal_basis, evidence_locator,
				parser_version, mapping_version, normalization_version
			) values (
				'70000000-0000-4000-8000-000000000002', $1, $2, 'relationship', 'officer_of', 'null'::jsonb,
				'unknown', 'unknown', 'unknown', '{"pointer":"/role"}'::jsonb,
				'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, testRecordID, otherRecordID)
	})
}

func TestProcessingRunSupersedesOnlySameArtifactAndScope(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		if _, err := tx.Exec(ctx, "update processing_runs set active = false where id = $1", testRunID); err != nil {
			t.Fatalf("deactivate old processing run: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, supersedes_run_id, processing_scope, status,
				schema_version, parser_version, mapping_version, normalization_version, active
			) values (
				'50000000-0000-4000-8000-000000000002', $1, $2, $3, 'source_collection', 'succeeded',
				'schema/2', 'parser/2', 'mapper/2', 'normalizer/2', true
			)
		`, testArtifactID, testResultID, testRunID); err != nil {
			t.Fatalf("insert valid superseding run: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "reactivate old processing run", `
			update processing_runs set active = true where id = $1
		`, testRunID)
	})
}

func TestMigrationRejectsInvalidTemporalClaim(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		_, err := tx.Exec(ctx, `
			insert into claims (
				id, subject_source_record_id, kind, predicate,
				raw_value, valid_from, valid_to,
				valid_from_precision, valid_to_precision, temporal_basis,
				evidence_locator, parser_version, mapping_version, normalization_version
			) values (
				'70000000-0000-4000-8000-000000000099', $1, 'attribute', 'name',
				'"Ada"'::jsonb, date '2020-01-01', date '2020-01-01',
				'day', 'day', 'explicit',
				'{"pointer":"/name"}'::jsonb, 'parser/1', 'mapper/1', 'normalizer/1'
			)
		`, testRecordID)
		requireDatabaseRejection(t, err, "empty exact half-open interval")
	})
}

func TestInboxAndBusinessResultRollbackTogether(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into consumed_messages (consumer_name, message_id)
		values ('migration-test-consumer', '30000000-0000-4000-8000-000000000001')
	`); err != nil {
		t.Fatalf("insert inbox receipt: %v", err)
	}
	seedJob(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
		insert into source_job_results (
			id, source_job_id, message_id, attempt_no, result_digest, applied
		) values ($1, $2, '30000000-0000-4000-8000-000000000001', 1, $3, true)
	`, testResultID, testJobID, strings.Repeat("b", 64)); err != nil {
		t.Fatalf("insert business result: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback consumer transaction: %v", err)
	}

	var inboxCount, resultCount int
	if err := pool.QueryRow(ctx, `
		select
			(select count(*) from consumed_messages
			 where consumer_name = 'migration-test-consumer'
			   and message_id = '30000000-0000-4000-8000-000000000001'),
			(select count(*) from source_job_results where id = $1)
	`, testResultID).Scan(&inboxCount, &resultCount); err != nil {
		t.Fatalf("read rolled back consumer state: %v", err)
	}
	if inboxCount != 0 || resultCount != 0 {
		t.Fatalf("rolled back state = inbox %d results %d", inboxCount, resultCount)
	}
}

func TestMessagingAndIdempotencyHistoryGuards(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		if _, err := tx.Exec(ctx, `
			insert into consumed_messages (consumer_name, message_id)
			values ('guard-test', '30000000-0000-4000-8000-000000000030')
		`); err != nil {
			t.Fatalf("insert consumed message: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "update consumed message", `
			update consumed_messages set consumed_at = now() + interval '1 minute'
			where consumer_name = 'guard-test'
		`)
		requireRejectedStatement(t, ctx, tx, "delete consumed message", `
			delete from consumed_messages where consumer_name = 'guard-test'
		`)

		const outboxID = "30000000-0000-4000-8000-000000000031"
		if _, err := tx.Exec(ctx, `
			insert into outbox_messages (id, message_type, aggregate_type, aggregate_id, payload)
			values ($1, 'source.collect.requested.v1', 'source_job', $2, '{"job":"test","attempt_no":1}'::jsonb)
		`, outboxID, testJobID); err != nil {
			t.Fatalf("insert outbox message: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "rewrite outbox payload", `
			update outbox_messages set message_type = 'other', payload = '{"rewritten":true}'::jsonb
			where id = $1
		`, outboxID)
		requireRejectedStatement(t, ctx, tx, "change outbox numeric representation before publication", `
			update outbox_messages set payload = jsonb_set(payload, '{attempt_no}', '1.0'::jsonb)
			where id = $1
		`, outboxID)
		if _, err := tx.Exec(ctx, `
			update outbox_messages
			set publish_attempts = publish_attempts + 1, last_error = 'temporary', available_at = now() + interval '1 minute'
			where id = $1
		`, outboxID); err != nil {
			t.Fatalf("update outbox delivery metadata: %v", err)
		}
		if _, err := tx.Exec(ctx, `update outbox_messages set published_at = now(), last_error = null where id = $1`, outboxID); err != nil {
			t.Fatalf("publish outbox message: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "rewrite published outbox message", `
			update outbox_messages set publish_attempts = publish_attempts + 1 where id = $1
		`, outboxID)
		requireRejectedStatement(t, ctx, tx, "change outbox numeric representation after publication", `
			update outbox_messages set payload = jsonb_set(payload, '{attempt_no}', '1.0'::jsonb)
			where id = $1
		`, outboxID)

		if _, err := tx.Exec(ctx, `
			insert into api_idempotency_keys (scope, idempotency_key, request_hash, expires_at)
			values ('investigation', 'guard-key', 'hash-a', now() + interval '1 hour')
		`); err != nil {
			t.Fatalf("insert API idempotency key: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "rewrite idempotency request hash", `
			update api_idempotency_keys set request_hash = 'hash-b'
			where scope = 'investigation' and idempotency_key = 'guard-key'
		`)
		if _, err := tx.Exec(ctx, `
			update api_idempotency_keys set response_status = 201, response_body = '{"id":"ok"}'::jsonb
			where scope = 'investigation' and idempotency_key = 'guard-key'
		`); err != nil {
			t.Fatalf("complete API idempotency response: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "rewrite completed idempotency response", `
			update api_idempotency_keys set response_status = 200
			where scope = 'investigation' and idempotency_key = 'guard-key'
		`)
		requireRejectedStatement(t, ctx, tx, "delete unexpired idempotency key", `
			delete from api_idempotency_keys
			where scope = 'investigation' and idempotency_key = 'guard-key'
		`)
	})
}

func TestDocumentedStatusTransitions(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		if _, err := tx.Exec(ctx, `
			update source_jobs
			set status = 'retry_wait', next_attempt_at = now() + interval '30 seconds'
			where id = $1
		`, testJobID); err != nil {
			t.Fatalf("running -> retry_wait: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			update source_jobs
			set status = 'pending', next_attempt_at = null, attempt_no = 2
			where id = $1
		`, testJobID); err != nil {
			t.Fatalf("retry_wait -> pending: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			update source_jobs
			set status = 'succeeded', finished_at = now()
			where id = $1
		`, testJobID); err != nil {
			t.Fatalf("pending -> succeeded: %v", err)
		}

		if _, err := tx.Exec(ctx, "savepoint terminal_transition"); err != nil {
			t.Fatalf("create savepoint: %v", err)
		}
		_, transitionErr := tx.Exec(ctx, `
			update source_jobs set status = 'running' where id = $1
		`, testJobID)
		requireDatabaseRejection(t, transitionErr, "terminal source job transition")
		if _, err := tx.Exec(ctx, "rollback to savepoint terminal_transition"); err != nil {
			t.Fatalf("rollback terminal transition: %v", err)
		}

		if _, err := tx.Exec(ctx, `
			update investigations set status = 'completed', completed_at = now() where id = $1
		`, testInvestigationID); err != nil {
			t.Fatalf("running -> completed investigation: %v", err)
		}
		_, investigationErr := tx.Exec(ctx, `
			update investigations set status = 'running' where id = $1
		`, testInvestigationID)
		requireDatabaseRejection(t, investigationErr, "terminal investigation transition")
	})
}

func TestInvestigationMustStartCreatedAtSubjectDiscovery(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		if _, err := tx.Exec(ctx, `
			insert into investigations (id, subject_type, subject_query, status, stage)
			values (
				'10000000-0000-4000-8000-000000000090',
				'person', '{"name":"Initial"}'::jsonb, 'created', 'subject_discovery'
			)
		`); err != nil {
			t.Fatalf("insert investigation in documented initial state: %v", err)
		}

		for index, status := range []string{
			"running", "completed", "completed_partial", "failed", "cancelled",
		} {
			id := fmt.Sprintf("10000000-0000-4000-8000-%012d", 91+index)
			requireRejectedStatement(t, ctx, tx, "investigation inserted as "+status, `
				insert into investigations (id, subject_type, subject_query, status, stage)
				values ($1, 'person', '{"name":"Invalid initial status"}'::jsonb, $2, 'subject_discovery')
			`, id, status)
		}
		requireRejectedStatement(t, ctx, tx, "investigation inserted after initial stage", `
			insert into investigations (id, subject_type, subject_query, status, stage)
			values (
				'10000000-0000-4000-8000-000000000096',
				'person', '{"name":"Invalid initial stage"}'::jsonb, 'created', 'enrichment'
			)
		`)
	})
}

func TestInvestigationInputIsImmutable(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)
		requireRejectedStatement(t, ctx, tx, "rewrite investigation subject", `
			update investigations
			set subject_type = 'organization', subject_query = '{"name":"Changed"}'::jsonb
			where id = $1
		`, testInvestigationID)
		requireRejectedStatement(t, ctx, tx, "rewrite investigation creation time", `
			update investigations set created_at = created_at - interval '1 day' where id = $1
		`, testInvestigationID)
	})
}

func TestDocumentedStatusTransitionMatrices(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedJob(t, ctx, tx)

		jobStatuses := []string{
			"pending", "queued", "running", "retry_wait", "succeeded",
			"not_found", "action_required", "unavailable", "failed", "cancelled",
		}
		jobAllowed := map[string]map[string]bool{
			"pending":         {"queued": true, "running": true, "retry_wait": true, "succeeded": true, "not_found": true, "action_required": true, "unavailable": true, "failed": true, "cancelled": true},
			"queued":          {"running": true, "retry_wait": true, "succeeded": true, "not_found": true, "action_required": true, "unavailable": true, "failed": true, "cancelled": true},
			"running":         {"retry_wait": true, "succeeded": true, "not_found": true, "action_required": true, "unavailable": true, "failed": true, "cancelled": true},
			"retry_wait":      {"pending": true, "cancelled": true},
			"action_required": {"pending": true, "failed": true, "cancelled": true},
		}
		caseNo := 0
		for _, oldStatus := range jobStatuses {
			for _, newStatus := range jobStatuses {
				if oldStatus == newStatus {
					continue
				}
				caseNo++
				if _, err := tx.Exec(ctx, "savepoint status_matrix"); err != nil {
					t.Fatalf("create job matrix savepoint: %v", err)
				}
				jobID := fmt.Sprintf("20000000-0000-4000-8000-%012d", 100+caseNo)
				if _, err := tx.Exec(ctx, `
					insert into source_jobs (
						id, investigation_id, source_id, operation, purpose,
						request_hash, idempotency_key
					) values ($1, $2, $3, 'collect', 'enrichment', $4, $5)
				`, jobID, testInvestigationID, testSourceID,
					strings.Repeat("a", 64), "matrix-"+jobID); err != nil {
					t.Fatalf("insert pending matrix job for %s: %v", oldStatus, err)
				}
				if oldStatus != "pending" {
					if _, err := tx.Exec(ctx, `
						update source_jobs
						set status = $2,
							next_attempt_at = case when $2 = 'retry_wait' then now() + interval '1 minute' else null end
						where id = $1
					`, jobID, oldStatus); err != nil {
						t.Fatalf("prepare matrix job in %s: %v", oldStatus, err)
					}
				}
				newAttempt := 1
				if (oldStatus == "retry_wait" || oldStatus == "action_required") && newStatus == "pending" {
					newAttempt = 2
				}
				_, transitionErr := tx.Exec(ctx, `
					update source_jobs
					set status = $2,
						attempt_no = $3,
						next_attempt_at = case when $2 = 'retry_wait' then now() + interval '1 minute' else null end
					where id = $1
				`, jobID, newStatus, newAttempt)
				expected := jobAllowed[oldStatus][newStatus]
				if expected && transitionErr != nil {
					t.Fatalf("documented job transition %s -> %s rejected: %v", oldStatus, newStatus, transitionErr)
				}
				if !expected && transitionErr == nil {
					t.Fatalf("undocumented job transition %s -> %s accepted", oldStatus, newStatus)
				}
				if _, err := tx.Exec(ctx, "rollback to savepoint status_matrix"); err != nil {
					t.Fatalf("rollback job matrix case: %v", err)
				}
			}
		}

		investigationStatuses := []string{"created", "running", "completed", "completed_partial", "failed", "cancelled"}
		investigationAllowed := map[string]map[string]bool{
			"created": {"running": true, "cancelled": true, "failed": true},
			"running": {"completed": true, "completed_partial": true, "failed": true, "cancelled": true},
		}
		caseNo = 0
		for _, oldStatus := range investigationStatuses {
			for _, newStatus := range investigationStatuses {
				if oldStatus == newStatus {
					continue
				}
				caseNo++
				if _, err := tx.Exec(ctx, "savepoint investigation_matrix"); err != nil {
					t.Fatalf("create investigation matrix savepoint: %v", err)
				}
				investigationID := fmt.Sprintf("10000000-0000-4000-8000-%012d", 100+caseNo)
				if _, err := tx.Exec(ctx, `
					insert into investigations (id, subject_type, subject_query, status, stage)
					values ($1, 'person', '{"name":"Matrix"}'::jsonb, 'created', 'subject_discovery')
				`, investigationID); err != nil {
					t.Fatalf("insert created matrix investigation for %s: %v", oldStatus, err)
				}
				if oldStatus == "running" || oldStatus == "completed" || oldStatus == "completed_partial" {
					if _, err := tx.Exec(ctx,
						"update investigations set status = 'running' where id = $1",
						investigationID,
					); err != nil {
						t.Fatalf("prepare running matrix investigation for %s: %v", oldStatus, err)
					}
				}
				if oldStatus != "created" && oldStatus != "running" {
					if _, err := tx.Exec(ctx,
						"update investigations set status = $2 where id = $1",
						investigationID, oldStatus,
					); err != nil {
						t.Fatalf("prepare matrix investigation in %s through documented lifecycle: %v", oldStatus, err)
					}
				}
				_, transitionErr := tx.Exec(ctx, "update investigations set status = $2 where id = $1", investigationID, newStatus)
				expected := investigationAllowed[oldStatus][newStatus]
				if expected && transitionErr != nil {
					t.Fatalf("documented investigation transition %s -> %s rejected: %v", oldStatus, newStatus, transitionErr)
				}
				if !expected && transitionErr == nil {
					t.Fatalf("undocumented investigation transition %s -> %s accepted", oldStatus, newStatus)
				}
				if _, err := tx.Exec(ctx, "rollback to savepoint investigation_matrix"); err != nil {
					t.Fatalf("rollback investigation matrix case: %v", err)
				}
			}
		}
	})
}

func TestMatchReviewMustStartOpenAtInitialVersions(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		requireRejectedStatement(t, ctx, tx, "review initially decided without decision", `
			insert into match_reviews (id, source_record_id, status, resolved_at)
			values ('90000000-0000-4000-8000-000000000090', $1, 'decided', now())
		`, testRecordID)
		requireRejectedStatement(t, ctx, tx, "review with noninitial candidate set version", `
			insert into match_reviews (id, source_record_id, candidate_set_version)
			values ('90000000-0000-4000-8000-000000000091', $1, 2)
		`, testRecordID)
		requireRejectedStatement(t, ctx, tx, "review with noninitial lock version", `
			insert into match_reviews (id, source_record_id, lock_version)
			values ('90000000-0000-4000-8000-000000000092', $1, 2)
		`, testRecordID)
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id)
			values ('90000000-0000-4000-8000-000000000093', $1)
		`, testRecordID); err != nil {
			t.Fatalf("insert valid initial review: %v", err)
		}
	})
}

func TestMigrationAllowsOnlyOneActiveEntityLink(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		for _, entity := range []struct {
			id   string
			name string
		}{
			{id: "80000000-0000-4000-8000-000000000001", name: "Ada One"},
			{id: "80000000-0000-4000-8000-000000000002", name: "Ada Two"},
		} {
			if _, err := tx.Exec(ctx, `
				insert into entities (id, entity_type, display_name)
				values ($1, 'person', $2)
			`, entity.id, entity.name); err != nil {
				t.Fatalf("insert entity: %v", err)
			}
		}
		requireRejectedStatement(t, ctx, tx, "active entity link without decision", `
			insert into entity_records (id, entity_id, source_record_id, link_method)
			values ('93000000-0000-4000-8000-000000000099', $1, $2, 'automatic')
		`, "80000000-0000-4000-8000-000000000001", testRecordID)
		seedInitialResolution(
			t, ctx, tx,
			"80000000-0000-4000-8000-000000000001",
			"90000000-0000-4000-8000-000000000001",
			"91000000-0000-4000-8000-000000000001",
			"92000000-0000-4000-8000-000000000001",
			"93000000-0000-4000-8000-000000000001",
		)
		_, err := tx.Exec(ctx, `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id, link_method
			) values ('93000000-0000-4000-8000-000000000002', $1, $2, $3, 'automatic')
		`, "80000000-0000-4000-8000-000000000001", testRecordID,
			"92000000-0000-4000-8000-000000000001")
		requireDatabaseRejection(t, err, "second active entity link")
	})
}

func TestCancelledInvestigationRejectsNewCanonicalEffects(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const secondRecordID = "60000000-0000-4000-8000-000000000002"
		const thirdRecordID = "60000000-0000-4000-8000-000000000003"
		if _, err := tx.Exec(ctx, `
			insert into source_records (id, processing_run_id, local_ref, record_type, schema_version, raw_data) values
				($1, $3, 'cancel-record-2', 'person', 'fixture/1', '{"name":"Two"}'::jsonb),
				($2, $3, 'cancel-record-3', 'person', 'fixture/1', '{"name":"Three"}'::jsonb)
		`, secondRecordID, thirdRecordID, testRunID); err != nil {
			t.Fatalf("insert cancellation test records: %v", err)
		}
		const entityID = "80000000-0000-4000-8000-000000000001"
		const otherEntityID = "80000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name) values
				($1, 'person', 'Ada'), ($2, 'person', 'Other')
		`, entityID, otherEntityID); err != nil {
			t.Fatalf("insert cancellation test entities: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into investigation_entities (investigation_id, entity_id, inclusion_reason)
			values ($1, $2, 'initial')
		`, testInvestigationID, entityID); err != nil {
			t.Fatalf("insert pre-cancellation membership: %v", err)
		}
		const otherInvestigationID = "10000000-0000-4000-8000-000000000002"
		seedRunningInvestigation(t, ctx, tx, otherInvestigationID, "Other")
		if _, err := tx.Exec(ctx, `
			insert into investigation_entities (investigation_id, entity_id, inclusion_reason)
			values ($1, $2, 'other investigation')
		`, otherInvestigationID, otherEntityID); err != nil {
			t.Fatalf("insert transferable membership: %v", err)
		}

		const openReviewID = "90000000-0000-4000-8000-000000000001"
		const decidedReviewID = "90000000-0000-4000-8000-000000000002"
		const openCandidateID = "91000000-0000-4000-8000-000000000001"
		const decidedCandidateID = "91000000-0000-4000-8000-000000000002"
		const decidedDecisionID = "92000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id) values ($1, $2), ($3, $4)
		`, openReviewID, testRecordID, decidedReviewID, secondRecordID); err != nil {
			t.Fatalf("insert pre-cancellation reviews: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id, score, recommendation, algorithm_version
			) values
				($1, $2, 1, $5, 0.9, 'review', 'matcher/1'),
				($3, $4, 1, $5, 0.9, 'review', 'matcher/1')
		`, openCandidateID, openReviewID, decidedCandidateID, decidedReviewID, entityID); err != nil {
			t.Fatalf("insert pre-cancellation candidates: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
		`, decidedDecisionID, decidedReviewID, decidedCandidateID, entityID); err != nil {
			t.Fatalf("insert pre-cancellation decision: %v", err)
		}
		if _, err := tx.Exec(ctx, "update investigations set status = 'cancelled' where id = $1", testInvestigationID); err != nil {
			t.Fatalf("cancel investigation: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "new review after cancellation", `
			insert into match_reviews (id, source_record_id)
			values ('90000000-0000-4000-8000-000000000003', $1)
		`, thirdRecordID)
		requireRejectedStatement(t, ctx, tx, "new decision after cancellation", `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ('92000000-0000-4000-8000-000000000002', $1, 1, $2, $3, 1, 'select_candidate', 'user')
		`, openReviewID, openCandidateID, entityID)
		requireRejectedStatement(t, ctx, tx, "new entity link after cancellation", `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000001', $1, $2, $3, 'manual')
		`, entityID, secondRecordID, decidedDecisionID)
		requireRejectedStatement(t, ctx, tx, "investigation membership after cancellation", `
			insert into investigation_entities (investigation_id, entity_id, inclusion_reason)
			values ($1, $2, 'late')
		`, testInvestigationID, otherEntityID)
		requireRejectedStatement(t, ctx, tx, "rewrite investigation membership entity", `
			update investigation_entities set entity_id = $1
			where investigation_id = $2 and entity_id = $3
		`, otherEntityID, testInvestigationID, entityID)
		requireRejectedStatement(t, ctx, tx, "move membership into cancelled investigation", `
			update investigation_entities set investigation_id = $1
			where investigation_id = $2 and entity_id = $3
		`, testInvestigationID, otherInvestigationID, otherEntityID)
		requireRejectedStatement(t, ctx, tx, "rewrite investigation membership metadata", `
			update investigation_entities set depth = depth + 1
			where investigation_id = $1 and entity_id = $2
		`, testInvestigationID, entityID)
		requireRejectedStatement(t, ctx, tx, "delete investigation membership", `
			delete from investigation_entities where investigation_id = $1 and entity_id = $2
		`, testInvestigationID, entityID)
	})
}

func TestEntityLinkMustMatchResolutionDecision(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const otherRecordID = "60000000-0000-4000-8000-000000000002"
		const selectedEntityID = "80000000-0000-4000-8000-000000000001"
		const otherEntityID = "80000000-0000-4000-8000-000000000002"
		const reviewID = "90000000-0000-4000-8000-000000000001"
		const candidateID = "91000000-0000-4000-8000-000000000001"
		const decisionID = "92000000-0000-4000-8000-000000000001"

		if _, err := tx.Exec(ctx, `
			insert into source_records (id, processing_run_id, local_ref, record_type, schema_version, raw_data)
			values ($1, $2, 'other-record', 'person', 'fixture/1', '{"name":"Other"}'::jsonb)
		`, otherRecordID, testRunID); err != nil {
			t.Fatalf("insert other record: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name) values
				($1, 'person', 'Selected'), ($2, 'person', 'Other')
		`, selectedEntityID, otherEntityID); err != nil {
			t.Fatalf("insert entities: %v", err)
		}
		if _, err := tx.Exec(ctx, "insert into match_reviews (id, source_record_id) values ($1, $2)", reviewID, testRecordID); err != nil {
			t.Fatalf("insert review: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id, score, recommendation, algorithm_version
			) values ($1, $2, 1, $3, 0.95, 'review', 'matcher/1')
		`, candidateID, reviewID, selectedEntityID); err != nil {
			t.Fatalf("insert candidate: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
		`, decisionID, reviewID, candidateID, selectedEntityID); err != nil {
			t.Fatalf("insert decision: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "entity link inserted inactive", `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id,
				link_method, active, unlinked_at
			) values (
				'93000000-0000-4000-8000-000000000090', $1, $2, $3,
				'manual', false, now()
			)
		`, selectedEntityID, testRecordID, decisionID)
		requireRejectedStatement(t, ctx, tx, "active entity link inserted with unlinked_at", `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id,
				link_method, active, unlinked_at
			) values (
				'93000000-0000-4000-8000-000000000091', $1, $2, $3,
				'manual', true, now()
			)
		`, selectedEntityID, testRecordID, decisionID)
		requireRejectedStatement(t, ctx, tx, "decision link for wrong record", `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000001', $1, $2, $3, 'manual')
		`, selectedEntityID, otherRecordID, decisionID)
		requireRejectedStatement(t, ctx, tx, "decision link for wrong entity", `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000002', $1, $2, $3, 'manual')
		`, otherEntityID, testRecordID, decisionID)
		if _, err := tx.Exec(ctx, `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000003', $1, $2, $3, 'manual')
		`, selectedEntityID, testRecordID, decisionID); err != nil {
			t.Fatalf("insert matching entity link: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "second entity link for one decision", `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000004', $1, $2, $3, 'manual')
		`, selectedEntityID, testRecordID, decisionID)
	})
}

func TestNoneOfAboveResolvesToEntityOutsideCandidateSet(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const candidateEntityID = "80000000-0000-4000-8000-000000000001"
		const newEntityID = "80000000-0000-4000-8000-000000000002"
		const reviewID = "90000000-0000-4000-8000-000000000001"
		const candidateID = "91000000-0000-4000-8000-000000000001"
		const decisionID = "92000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name) values
				($1, 'person', 'Candidate'), ($2, 'person', 'New entity')
		`, candidateEntityID, newEntityID); err != nil {
			t.Fatalf("insert none-of-above entities: %v", err)
		}
		if _, err := tx.Exec(ctx, "insert into match_reviews (id, source_record_id) values ($1, $2)", reviewID, testRecordID); err != nil {
			t.Fatalf("insert review: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id, score, recommendation, algorithm_version
			) values ($1, $2, 1, $3, 0.5, 'review', 'matcher/1')
		`, candidateID, reviewID, candidateEntityID); err != nil {
			t.Fatalf("insert candidate: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "none_of_above resolving to candidate entity", `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, resolved_entity_id,
				expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, 1, 'none_of_above', 'user')
		`, decisionID, reviewID, candidateEntityID)
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, resolved_entity_id,
				expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, 1, 'none_of_above', 'user')
		`, decisionID, reviewID, newEntityID); err != nil {
			t.Fatalf("insert none_of_above decision: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000001', $1, $2, $3, 'manual')
		`, newEntityID, testRecordID, decisionID); err != nil {
			t.Fatalf("link none_of_above entity: %v", err)
		}
	})
}

func TestEntityLinkCanBeUnlinkedAndReassigned(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const firstEntityID = "80000000-0000-4000-8000-000000000001"
		const secondEntityID = "80000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name) values
				($1, 'person', 'Ada One'),
				($2, 'person', 'Ada Two')
		`, firstEntityID, secondEntityID); err != nil {
			t.Fatalf("insert entities: %v", err)
		}
		const reviewID = "90000000-0000-4000-8000-000000000001"
		const firstCandidateID = "91000000-0000-4000-8000-000000000001"
		const firstDecisionID = "92000000-0000-4000-8000-000000000001"
		const revertDecisionID = "92000000-0000-4000-8000-000000000002"
		seedInitialResolution(
			t, ctx, tx, firstEntityID, reviewID, firstCandidateID, firstDecisionID,
			"93000000-0000-4000-8000-000000000001",
		)
		requireRejectedStatement(t, ctx, tx, "direct entity unlink without revert", `
			update entity_records set active = false, unlinked_at = now()
			where id = '93000000-0000-4000-8000-000000000001'
		`)
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, supersedes_decision_id,
				expected_lock_version, decision, actor_type, reason
			) values ($1, $2, 1, $3, 2, 'revert', 'user', 'incorrect link')
		`, revertDecisionID, reviewID, firstDecisionID); err != nil {
			t.Fatalf("revert first entity link: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "reactivate historical entity link", `
			update entity_records set active = true, unlinked_at = null
			where id = '93000000-0000-4000-8000-000000000001'
		`)
		requireRejectedStatement(t, ctx, tx, "deactivate historical entity link twice", `
			update entity_records set active = false, unlinked_at = now()
			where id = '93000000-0000-4000-8000-000000000001'
		`)
		const secondCandidateID = "91000000-0000-4000-8000-000000000002"
		const secondDecisionID = "92000000-0000-4000-8000-000000000003"
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id,
				score, recommendation, algorithm_version
			) values ($1, $2, 1, $3, 0.9, 'review', 'matcher/1')
		`, secondCandidateID, reviewID, secondEntityID); err != nil {
			t.Fatalf("insert replacement candidate: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, supersedes_decision_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, $5, 3, 'select_candidate', 'user')
		`, secondDecisionID, reviewID, secondCandidateID, secondEntityID, revertDecisionID); err != nil {
			t.Fatalf("insert replacement decision: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entity_records (id, entity_id, source_record_id, match_decision_id, link_method)
			values ('93000000-0000-4000-8000-000000000002', $1, $2, $3, 'manual')
		`, secondEntityID, testRecordID, secondDecisionID); err != nil {
			t.Fatalf("reassign source record: %v", err)
		}
	})
}

func TestActiveEntityProjectionsAreUnique(t *testing.T) {
	t.Run("names", func(t *testing.T) {
		withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
			seedIngestionChain(t, ctx, tx)
			const claimID = "70000000-0000-4000-8000-000000000001"
			seedAttributeClaim(t, ctx, tx, claimID, "name", `"Ada"`)
			seedProjectionEntities(t, ctx, tx)
			seedInitialResolution(
				t, ctx, tx,
				"80000000-0000-4000-8000-000000000001",
				"90000000-0000-4000-8000-000000000001",
				"91000000-0000-4000-8000-000000000001",
				"92000000-0000-4000-8000-000000000001",
				"93000000-0000-4000-8000-000000000001",
			)
			requireRejectedStatement(t, ctx, tx, "entity name inserted inactive", `
				insert into entity_names (
					id, entity_id, claim_id, raw_name, normalized_name,
					normalization_version, active
				) values (
					'81000000-0000-4000-8000-000000000098',
					'80000000-0000-4000-8000-000000000001', $1,
					'Ada', 'ada', 'normalizer/1', false
				)
			`, claimID)
			requireRejectedStatement(t, ctx, tx, "active projection for another entity", `
				insert into entity_names (
					id, entity_id, claim_id, raw_name, normalized_name, normalization_version
				) values (
					'81000000-0000-4000-8000-000000000099',
					'80000000-0000-4000-8000-000000000002', $1, 'Ada', 'ada', 'normalizer/1'
				)
			`, claimID)
			if _, err := tx.Exec(ctx, `
				insert into entity_names (
					id, entity_id, claim_id, raw_name, normalized_name, normalization_version
				) values (
					'81000000-0000-4000-8000-000000000001',
					'80000000-0000-4000-8000-000000000001', $1, 'Ada', 'ada', 'normalizer/1'
				)
			`, claimID); err != nil {
				t.Fatalf("insert active name: %v", err)
			}
			requireRejectedStatement(t, ctx, tx, "rewrite active entity name", `
				update entity_names set normalized_name = 'rewritten'
				where id = '81000000-0000-4000-8000-000000000001'
			`)
			if _, err := tx.Exec(ctx, `
				update entity_names set active = false
				where id = '81000000-0000-4000-8000-000000000001'
			`); err != nil {
				t.Fatalf("deactivate name: %v", err)
			}
			requireRejectedStatement(t, ctx, tx, "reactivate historical entity name", `
				update entity_names set active = true
				where id = '81000000-0000-4000-8000-000000000001'
			`)
			requireRejectedStatement(t, ctx, tx, "delete historical entity name", `
				delete from entity_names where id = '81000000-0000-4000-8000-000000000001'
			`)
			if _, err := tx.Exec(ctx, `
				insert into entity_names (
					id, entity_id, claim_id, raw_name, normalized_name, normalization_version
				) values (
					'81000000-0000-4000-8000-000000000002',
					'80000000-0000-4000-8000-000000000001', $1, 'Ada', 'ada', 'normalizer/2'
				)
			`, claimID); err != nil {
				t.Fatalf("replace active name: %v", err)
			}
			_, err := tx.Exec(ctx, `
				insert into entity_names (
					id, entity_id, claim_id, raw_name, normalized_name, normalization_version
				) values (
					'81000000-0000-4000-8000-000000000003',
					'80000000-0000-4000-8000-000000000001', $1, 'Ada', 'ada', 'normalizer/2'
				)
			`, claimID)
			requireDatabaseRejection(t, err, "second active name projection for one claim")
		})
	})

	t.Run("identifiers", func(t *testing.T) {
		withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
			seedIngestionChain(t, ctx, tx)
			const claimID = "70000000-0000-4000-8000-000000000002"
			seedAttributeClaim(t, ctx, tx, claimID, "registration_number", `"123"`)
			seedProjectionEntities(t, ctx, tx)
			seedInitialResolution(
				t, ctx, tx,
				"80000000-0000-4000-8000-000000000001",
				"90000000-0000-4000-8000-000000000001",
				"91000000-0000-4000-8000-000000000001",
				"92000000-0000-4000-8000-000000000001",
				"93000000-0000-4000-8000-000000000001",
			)
			requireRejectedStatement(t, ctx, tx, "entity identifier inserted inactive", `
				insert into entity_identifiers (
					id, entity_id, claim_id, identifier_type,
					raw_value, normalized_value, normalization_version, active
				) values (
					'82000000-0000-4000-8000-000000000098',
					'80000000-0000-4000-8000-000000000001', $1,
					'registration_number', '123', '123', 'normalizer/1', false
				)
			`, claimID)
			requireRejectedStatement(t, ctx, tx, "active identifier projection for another entity", `
				insert into entity_identifiers (
					id, entity_id, claim_id, identifier_type,
					raw_value, normalized_value, normalization_version
				) values (
					'82000000-0000-4000-8000-000000000099',
					'80000000-0000-4000-8000-000000000002', $1,
					'registration_number', '123', '123', 'normalizer/1'
				)
			`, claimID)
			if _, err := tx.Exec(ctx, `
				insert into entity_identifiers (
					id, entity_id, claim_id, identifier_type,
					raw_value, normalized_value, normalization_version
				) values (
					'82000000-0000-4000-8000-000000000001',
					'80000000-0000-4000-8000-000000000001', $1,
					'registration_number', '123', '123', 'normalizer/1'
				)
			`, claimID); err != nil {
				t.Fatalf("insert active identifier: %v", err)
			}
			requireRejectedStatement(t, ctx, tx, "rewrite active entity identifier", `
				update entity_identifiers set normalized_value = 'rewritten'
				where id = '82000000-0000-4000-8000-000000000001'
			`)
			if _, err := tx.Exec(ctx, `
				update entity_identifiers set active = false
				where id = '82000000-0000-4000-8000-000000000001'
			`); err != nil {
				t.Fatalf("deactivate identifier: %v", err)
			}
			requireRejectedStatement(t, ctx, tx, "reactivate historical entity identifier", `
				update entity_identifiers set active = true
				where id = '82000000-0000-4000-8000-000000000001'
			`)
			requireRejectedStatement(t, ctx, tx, "delete historical entity identifier", `
				delete from entity_identifiers
				where id = '82000000-0000-4000-8000-000000000001'
			`)
			if _, err := tx.Exec(ctx, `
				insert into entity_identifiers (
					id, entity_id, claim_id, identifier_type,
					raw_value, normalized_value, normalization_version
				) values (
					'82000000-0000-4000-8000-000000000003',
					'80000000-0000-4000-8000-000000000001', $1,
					'registration_number', '123', '123', 'normalizer/2'
				)
			`, claimID); err != nil {
				t.Fatalf("replace active identifier: %v", err)
			}
			_, err := tx.Exec(ctx, `
				insert into entity_identifiers (
					id, entity_id, claim_id, identifier_type,
					raw_value, normalized_value, normalization_version
				) values (
					'82000000-0000-4000-8000-000000000002',
					'80000000-0000-4000-8000-000000000001', $1,
					'registration_number', '123', '123', 'normalizer/1'
				)
			`, claimID)
			requireDatabaseRejection(t, err, "second active identifier projection for one claim")
		})
	})
}

func TestDecisionRevertChainReviewScopeAndOptimisticLock(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const secondRecordID = "60000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into source_records (
				id, processing_run_id, local_ref, record_type, schema_version, raw_data
			) values ($1, $2, 'record-2', 'person', 'fixture/1', '{"name":"Other"}'::jsonb)
		`, secondRecordID, testRunID); err != nil {
			t.Fatalf("insert second source record: %v", err)
		}

		const firstEntityID = "80000000-0000-4000-8000-000000000001"
		const secondEntityID = "80000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name) values
				($1, 'person', 'Ada'),
				($2, 'person', 'Other')
		`, firstEntityID, secondEntityID); err != nil {
			t.Fatalf("insert entities: %v", err)
		}

		const firstReviewID = "90000000-0000-4000-8000-000000000001"
		const secondReviewID = "90000000-0000-4000-8000-000000000002"
		const firstCandidateID = "91000000-0000-4000-8000-000000000001"
		const secondCandidateID = "91000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id) values
				($1, $2),
				($3, $4)
		`, firstReviewID, testRecordID, secondReviewID, secondRecordID); err != nil {
			t.Fatalf("insert reviews: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id,
				score, recommendation, algorithm_version
			) values
				($1, $2, 1, $3, 0.95, 'review', 'matcher/1'),
				($4, $5, 1, $6, 0.90, 'review', 'matcher/1')
		`, firstCandidateID, firstReviewID, firstEntityID, secondCandidateID, secondReviewID, secondEntityID); err != nil {
			t.Fatalf("insert candidates: %v", err)
		}

		const firstDecisionID = "92000000-0000-4000-8000-000000000001"
		const revertDecisionID = "92000000-0000-4000-8000-000000000002"
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
		`, firstDecisionID, firstReviewID, firstCandidateID, firstEntityID); err != nil {
			t.Fatalf("insert first decision: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id, link_method
			) values ('93000000-0000-4000-8000-000000000001', $1, $2, $3, 'manual')
		`, firstEntityID, testRecordID, firstDecisionID); err != nil {
			t.Fatalf("insert first entity link: %v", err)
		}
		const claimID = "70000000-0000-4000-8000-000000000001"
		seedAttributeClaim(t, ctx, tx, claimID, "name", `"Ada"`)
		if _, err := tx.Exec(ctx, `
			insert into entity_names (
				id, entity_id, claim_id, raw_name, normalized_name, normalization_version
			) values ('81000000-0000-4000-8000-000000000001', $1, $2, 'Ada', 'ada', 'normalizer/1')
		`, firstEntityID, claimID); err != nil {
			t.Fatalf("insert first entity projection: %v", err)
		}
		const identifierClaimID = "70000000-0000-4000-8000-000000000002"
		seedAttributeClaim(t, ctx, tx, identifierClaimID, "email", `"ada@example.test"`)
		if _, err := tx.Exec(ctx, `
			insert into entity_identifiers (
				id, entity_id, claim_id, identifier_type,
				raw_value, normalized_value, normalization_version
			) values (
				'82000000-0000-4000-8000-000000000001', $1, $2,
				'email', 'Ada@example.test', 'ada@example.test', 'normalizer/1'
			)
		`, firstEntityID, identifierClaimID); err != nil {
			t.Fatalf("insert identifier projection: %v", err)
		}

		requireRejectedStatement(t, ctx, tx, "second root decision", `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values (
				'92000000-0000-4000-8000-000000000010', $1, 1, $2,
				$3, 2, 'select_candidate', 'user'
			)
		`, firstReviewID, firstCandidateID, firstEntityID)
		requireRejectedStatement(t, ctx, tx, "rewrite selected candidate", `
			update match_candidates set score = 0.1 where id = $1
		`, firstCandidateID)
		requireRejectedStatement(t, ctx, tx, "reopen review without revert", `
			update match_reviews set status = 'open', resolved_at = null where id = $1
		`, firstReviewID)
		for label, reason := range map[string]any{
			"null":       nil,
			"empty":      "",
			"whitespace": "   ",
			"too long":   strings.Repeat("x", 2001),
		} {
			requireRejectedStatement(t, ctx, tx, "revert reason "+label, `
				insert into match_decisions (
					id, match_review_id, candidate_set_version, supersedes_decision_id,
					expected_lock_version, decision, actor_type, reason
				) values (
					'92000000-0000-4000-8000-000000000099', $1, 1, $2,
					2, 'revert', 'user', $3
				)
			`, firstReviewID, firstDecisionID, reason)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, supersedes_decision_id,
				expected_lock_version, decision, actor_type, reason
			) values ($1, $2, 1, $3, 2, 'revert', 'user', 'reverted')
		`, revertDecisionID, firstReviewID, firstDecisionID); err != nil {
			t.Fatalf("insert revert decision: %v", err)
		}
		var reviewStatus string
		var linkActive, nameActive, identifierActive bool
		if err := tx.QueryRow(ctx, `
			select review.status, link.active, name.active, identifier.active
			from match_reviews as review
			join entity_records as link on link.source_record_id = review.source_record_id
			join entity_names as name on name.entity_id = link.entity_id
			join entity_identifiers as identifier on identifier.entity_id = link.entity_id
			where review.id = $1 and link.id = '93000000-0000-4000-8000-000000000001'
		`, firstReviewID).Scan(&reviewStatus, &linkActive, &nameActive, &identifierActive); err != nil {
			t.Fatalf("read reverted resolution state: %v", err)
		}
		if reviewStatus != "open" || linkActive || nameActive || identifierActive {
			t.Fatalf(
				"reverted state = review %s link %t name %t identifier %t",
				reviewStatus, linkActive, nameActive, identifierActive,
			)
		}

		if _, err := tx.Exec(ctx, "savepoint cross_review_candidate"); err != nil {
			t.Fatalf("create cross-review savepoint: %v", err)
		}
		_, crossReviewErr := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, supersedes_decision_id, expected_lock_version, decision, actor_type
			) values (
				'92000000-0000-4000-8000-000000000003', $1, 1, $2,
				$3, $4, 3, 'select_candidate', 'user'
			)
		`, firstReviewID, secondCandidateID, secondEntityID, revertDecisionID)
		requireDatabaseRejection(t, crossReviewErr, "candidate from another review")
		if _, err := tx.Exec(ctx, "rollback to savepoint cross_review_candidate"); err != nil {
			t.Fatalf("rollback cross-review decision: %v", err)
		}

		commandTag, err := tx.Exec(ctx, `
			update match_reviews
			set candidate_set_version = candidate_set_version + 1,
				lock_version = lock_version + 1
			where id = $1 and lock_version = 3
		`, firstReviewID)
		if err != nil || commandTag.RowsAffected() != 1 {
			t.Fatalf("optimistic candidate refresh = rows %d error %v", commandTag.RowsAffected(), err)
		}
		commandTag, err = tx.Exec(ctx, `
			update match_reviews
			set candidate_set_version = candidate_set_version + 1,
				lock_version = lock_version + 1
			where id = $1 and lock_version = 3
		`, firstReviewID)
		if err != nil {
			t.Fatalf("stale optimistic review update: %v", err)
		}
		if commandTag.RowsAffected() != 0 {
			t.Fatalf("stale optimistic review update affected %d rows", commandTag.RowsAffected())
		}
		requireRejectedStatement(t, ctx, tx, "decision using stale candidate set", `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, supersedes_decision_id, expected_lock_version, decision, actor_type
			) values (
				'92000000-0000-4000-8000-000000000011', $1, 1, $2,
				$3, $4, 4, 'select_candidate', 'user'
			)
		`, firstReviewID, firstCandidateID, firstEntityID, revertDecisionID)
	})
}

func TestMatchDecisionsAreAppendOnly(t *testing.T) {
	withMigrationTx(t, func(ctx context.Context, tx pgx.Tx) {
		seedIngestionChain(t, ctx, tx)
		const entityID = "80000000-0000-4000-8000-000000000001"
		const reviewID = "90000000-0000-4000-8000-000000000001"
		const candidateID = "91000000-0000-4000-8000-000000000001"
		const decisionID = "92000000-0000-4000-8000-000000000001"
		if _, err := tx.Exec(ctx, `
			insert into entities (id, entity_type, display_name)
			values ($1, 'person', 'Ada')
		`, entityID); err != nil {
			t.Fatalf("insert entity: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_reviews (id, source_record_id)
			values ($1, $2)
		`, reviewID, testRecordID); err != nil {
			t.Fatalf("insert review: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_candidates (
				id, match_review_id, candidate_set_version, entity_id,
				score, recommendation, algorithm_version
			) values ($1, $2, 1, $3, 0.95, 'review', 'matcher/1')
		`, candidateID, reviewID, entityID); err != nil {
			t.Fatalf("insert candidate: %v", err)
		}
		if _, err := tx.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, selected_candidate_id,
				resolved_entity_id, expected_lock_version, decision, actor_type
			) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
		`, decisionID, reviewID, candidateID, entityID); err != nil {
			t.Fatalf("insert decision: %v", err)
		}
		requireRejectedStatement(t, ctx, tx, "update of append-only decision", `
			update match_decisions set reason = 'rewritten' where id = $1
		`, decisionID)
		requireRejectedStatement(t, ctx, tx, "delete of append-only decision", `
			delete from match_decisions where id = $1
		`, decisionID)
	})
}

func TestConcurrentRevertSerializesLinkAndProjection(t *testing.T) {
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	setupTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin concurrent setup: %v", err)
	}
	seedIngestionChain(t, ctx, setupTx)
	const firstEntityID = "80000000-0000-4000-8000-000000000001"
	const secondEntityID = "80000000-0000-4000-8000-000000000002"
	const thirdEntityID = "80000000-0000-4000-8000-000000000003"
	if _, err := setupTx.Exec(ctx, `
		insert into entities (id, entity_type, display_name) values
			($1, 'person', 'Projection race'),
			($2, 'person', 'Revert-first race'),
			($3, 'person', 'Link-first race')
	`, firstEntityID, secondEntityID, thirdEntityID); err != nil {
		t.Fatalf("insert concurrent entities: %v", err)
	}
	const firstReviewID = "90000000-0000-4000-8000-000000000001"
	const firstDecisionID = "92000000-0000-4000-8000-000000000001"
	seedInitialResolution(
		t, ctx, setupTx, firstEntityID, firstReviewID,
		"91000000-0000-4000-8000-000000000001", firstDecisionID,
		"93000000-0000-4000-8000-000000000001",
	)
	const projectionClaimID = "70000000-0000-4000-8000-000000000001"
	seedAttributeClaim(t, ctx, setupTx, projectionClaimID, "name", `"Concurrent"`)

	const secondRecordID = "60000000-0000-4000-8000-000000000002"
	const secondReviewID = "90000000-0000-4000-8000-000000000002"
	const secondCandidateID = "91000000-0000-4000-8000-000000000002"
	const secondDecisionID = "92000000-0000-4000-8000-000000000002"
	if _, err := setupTx.Exec(ctx, `
		insert into source_records (
			id, processing_run_id, local_ref, record_type, schema_version, raw_data
		) values ($1, $2, 'link-race', 'person', 'fixture/1', '{"name":"Link race"}'::jsonb)
	`, secondRecordID, testRunID); err != nil {
		t.Fatalf("insert concurrent link source record: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_reviews (id, source_record_id) values ($1, $2)
	`, secondReviewID, secondRecordID); err != nil {
		t.Fatalf("insert concurrent link review: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_candidates (
			id, match_review_id, candidate_set_version, entity_id,
			score, recommendation, algorithm_version
		) values ($1, $2, 1, $3, 0.9, 'review', 'matcher/1')
	`, secondCandidateID, secondReviewID, secondEntityID); err != nil {
		t.Fatalf("insert concurrent link candidate: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_decisions (
			id, match_review_id, candidate_set_version, selected_candidate_id,
			resolved_entity_id, expected_lock_version, decision, actor_type
		) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
	`, secondDecisionID, secondReviewID, secondCandidateID, secondEntityID); err != nil {
		t.Fatalf("insert concurrent link decision: %v", err)
	}
	const thirdRecordID = "60000000-0000-4000-8000-000000000003"
	const thirdReviewID = "90000000-0000-4000-8000-000000000003"
	const thirdCandidateID = "91000000-0000-4000-8000-000000000003"
	const thirdDecisionID = "92000000-0000-4000-8000-000000000005"
	if _, err := setupTx.Exec(ctx, `
		insert into source_records (
			id, processing_run_id, local_ref, record_type, schema_version, raw_data
		) values ($1, $2, 'reverse-link-race', 'person', 'fixture/1', '{"name":"Reverse link race"}'::jsonb)
	`, thirdRecordID, testRunID); err != nil {
		t.Fatalf("insert reverse-race source record: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_reviews (id, source_record_id) values ($1, $2)
	`, thirdReviewID, thirdRecordID); err != nil {
		t.Fatalf("insert reverse-race review: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_candidates (
			id, match_review_id, candidate_set_version, entity_id,
			score, recommendation, algorithm_version
		) values ($1, $2, 1, $3, 0.9, 'review', 'matcher/1')
	`, thirdCandidateID, thirdReviewID, thirdEntityID); err != nil {
		t.Fatalf("insert reverse-race candidate: %v", err)
	}
	if _, err := setupTx.Exec(ctx, `
		insert into match_decisions (
			id, match_review_id, candidate_set_version, selected_candidate_id,
			resolved_entity_id, expected_lock_version, decision, actor_type
		) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
	`, thirdDecisionID, thirdReviewID, thirdCandidateID, thirdEntityID); err != nil {
		t.Fatalf("insert reverse-race decision: %v", err)
	}
	if err := setupTx.Commit(ctx); err != nil {
		t.Fatalf("commit concurrent setup: %v", err)
	}

	projectionRevertTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin projection revert: %v", err)
	}
	if _, err := projectionRevertTx.Exec(ctx, `
		insert into match_decisions (
			id, match_review_id, candidate_set_version, supersedes_decision_id,
			expected_lock_version, decision, actor_type, reason
		) values (
			'92000000-0000-4000-8000-000000000003', $1, 1, $2,
			2, 'revert', 'user', 'concurrent projection test'
		)
	`, firstReviewID, firstDecisionID); err != nil {
		t.Fatalf("insert projection-race revert: %v", err)
	}
	projectionResult := make(chan error, 1)
	go func() {
		_, insertErr := pool.Exec(ctx, `
			insert into entity_names (
				id, entity_id, claim_id, raw_name, normalized_name, normalization_version
			) values (
				'81000000-0000-4000-8000-000000000001', $1, $2,
				'Concurrent', 'concurrent', 'normalizer/1'
			)
		`, firstEntityID, projectionClaimID)
		projectionResult <- insertErr
	}()
	select {
	case insertErr := <-projectionResult:
		t.Fatalf("projection insert did not wait for revert: %v", insertErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := projectionRevertTx.Commit(ctx); err != nil {
		t.Fatalf("commit projection-race revert: %v", err)
	}
	requireDatabaseRejection(t, <-projectionResult, "projection concurrent with revert")

	linkRevertTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin link revert: %v", err)
	}
	if _, err := linkRevertTx.Exec(ctx, `
		insert into match_decisions (
			id, match_review_id, candidate_set_version, supersedes_decision_id,
			expected_lock_version, decision, actor_type, reason
		) values (
			'92000000-0000-4000-8000-000000000004', $1, 1, $2,
			2, 'revert', 'user', 'concurrent link test'
		)
	`, secondReviewID, secondDecisionID); err != nil {
		t.Fatalf("insert link-race revert: %v", err)
	}
	linkResult := make(chan error, 1)
	go func() {
		_, insertErr := pool.Exec(ctx, `
			insert into entity_records (
				id, entity_id, source_record_id, match_decision_id, link_method
			) values (
				'93000000-0000-4000-8000-000000000002', $1, $2, $3, 'manual'
			)
		`, secondEntityID, secondRecordID, secondDecisionID)
		linkResult <- insertErr
	}()
	select {
	case insertErr := <-linkResult:
		t.Fatalf("entity link did not wait for revert: %v", insertErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := linkRevertTx.Commit(ctx); err != nil {
		t.Fatalf("commit link-race revert: %v", err)
	}
	requireDatabaseRejection(t, <-linkResult, "entity link concurrent with revert")

	linkFirstTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin link-first transaction: %v", err)
	}
	if _, err := linkFirstTx.Exec(ctx, `
		insert into entity_records (
			id, entity_id, source_record_id, match_decision_id, link_method
		) values (
			'93000000-0000-4000-8000-000000000003', $1, $2, $3, 'manual'
		)
	`, thirdEntityID, thirdRecordID, thirdDecisionID); err != nil {
		t.Fatalf("insert link-first entity link: %v", err)
	}
	reverseRevertResult := make(chan error, 1)
	go func() {
		_, insertErr := pool.Exec(ctx, `
			insert into match_decisions (
				id, match_review_id, candidate_set_version, supersedes_decision_id,
				expected_lock_version, decision, actor_type, reason
			) values (
				'92000000-0000-4000-8000-000000000006', $1, 1, $2,
				2, 'revert', 'user', 'link committed before revert'
			)
		`, thirdReviewID, thirdDecisionID)
		reverseRevertResult <- insertErr
	}()
	select {
	case revertErr := <-reverseRevertResult:
		t.Fatalf("revert did not wait for uncommitted link: %v", revertErr)
	case <-time.After(200 * time.Millisecond):
	}
	if err := linkFirstTx.Commit(ctx); err != nil {
		t.Fatalf("commit link-first transaction: %v", err)
	}
	if err := <-reverseRevertResult; err != nil {
		t.Fatalf("revert after committed link: %v", err)
	}

	var revertFirstOpen, linkFirstOpen bool
	var activeLinks, activeProjections int
	if err := pool.QueryRow(ctx, `
		select
			(select status = 'open' from match_reviews where id = $1),
			(select status = 'open' from match_reviews where id = $2),
			(select count(*) from entity_records where source_record_id in ($3, $4) and active),
			(select count(*) from entity_names where claim_id = $5 and active)
	`, secondReviewID, thirdReviewID, secondRecordID, thirdRecordID, projectionClaimID).Scan(
		&revertFirstOpen, &linkFirstOpen, &activeLinks, &activeProjections,
	); err != nil {
		t.Fatalf("read concurrent final state: %v", err)
	}
	if !revertFirstOpen || !linkFirstOpen || activeLinks != 0 || activeProjections != 0 {
		t.Fatalf(
			"concurrent final state = revert-first open %t, link-first open %t, active links %d, active projections %d",
			revertFirstOpen, linkFirstOpen, activeLinks, activeProjections,
		)
	}
}

func withMigrationTx(t *testing.T, run func(context.Context, pgx.Tx)) {
	t.Helper()
	ctx := t.Context()
	pool, err := Open(ctx, testPostgresConfig())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(pool.Close)

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	run(ctx, tx)
}

func seedJob(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	seedJobWithMaxAttempts(t, ctx, tx, 3)
}

func seedJobWithMaxAttempts(t *testing.T, ctx context.Context, tx pgx.Tx, maxAttempts int) {
	t.Helper()
	seedRunningInvestigation(t, ctx, tx, testInvestigationID, "Ada")
	if _, err := tx.Exec(ctx, `
		insert into source_jobs (
			id, investigation_id, source_id, operation, purpose,
			max_attempts, request_hash, idempotency_key
		) values (
			$1, $2, $3, 'collect', 'subject_discovery',
			$4, $5, 'migration-test-job'
		)
	`, testJobID, testInvestigationID, testSourceID, maxAttempts, strings.Repeat("a", 64)); err != nil {
		t.Fatalf("insert source job: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		update source_jobs set status = 'running' where id = $1
	`, testJobID); err != nil {
		t.Fatalf("start source job: %v", err)
	}
}

func seedRunningInvestigation(
	t *testing.T,
	ctx context.Context,
	db DBTX,
	investigationID string,
	name string,
) {
	t.Helper()
	if _, err := db.Exec(ctx, `
		insert into investigations (
			id, subject_type, subject_query, status, stage
		) values ($1, 'person', jsonb_build_object('name', $2::text), 'created', 'subject_discovery')
	`, investigationID, name); err != nil {
		t.Fatalf("insert created investigation: %v", err)
	}
	if _, err := db.Exec(ctx,
		"update investigations set status = 'running' where id = $1",
		investigationID,
	); err != nil {
		t.Fatalf("start investigation: %v", err)
	}
}

func seedIngestionChain(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	seedJob(t, ctx, tx)
	if _, err := tx.Exec(ctx, `
		insert into source_job_results (
			id, source_job_id, message_id, attempt_no, result_digest, applied
		) values (
			$1, $2, '30000000-0000-4000-8000-000000000001', 1, $3, true
		)
	`, testResultID, testJobID, strings.Repeat("b", 64)); err != nil {
		t.Fatalf("insert source job result: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into artifacts (
			id, source_id, kind, media_type, object_key, source_uri,
			sha256, byte_size, retrieved_at, adapter_version
		) values (
			$1, $2, 'raw_response', 'application/json', 'migration-test/response.json',
			'https://example.test/fixture', $3, 2, now(), 'fixture/1'
		)
	`, testArtifactID, testSourceID, strings.Repeat("c", 64)); err != nil {
		t.Fatalf("insert artifact: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into source_job_result_artifacts (
			source_job_result_id, artifact_id, artifact_local_ref, role
		) values ($1, $2, 'artifact-1', 'raw_response')
	`, testResultID, testArtifactID); err != nil {
		t.Fatalf("insert result artifact: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into processing_runs (
			id, artifact_id, source_job_result_id, processing_scope, status,
			schema_version, parser_version, mapping_version, normalization_version, active
		) values (
			$1, $2, $3, 'source_collection', 'succeeded',
			'schema/1', 'parser/1', 'mapper/1', 'normalizer/1', true
		)
	`, testRunID, testArtifactID, testResultID); err != nil {
		t.Fatalf("insert processing run: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into source_records (
			id, processing_run_id, local_ref, external_id, record_type, schema_version, raw_data
		) values ($1, $2, 'record-1', 'fixture-1', 'person', 'fixture/1', '{"name":"Ada"}'::jsonb)
	`, testRecordID, testRunID); err != nil {
		t.Fatalf("insert source record: %v", err)
	}
}

func seedAttributeClaim(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	claimID string,
	predicate string,
	rawJSON string,
) {
	t.Helper()
	if _, err := tx.Exec(ctx, `
		insert into claims (
			id, subject_source_record_id, kind, predicate,
			raw_value, valid_from_precision, valid_to_precision, temporal_basis,
			evidence_locator, parser_version, mapping_version, normalization_version
		) values (
			$1, $2, 'attribute', $3,
			$4::jsonb, 'unknown', 'unknown', 'unknown',
			'{"pointer":"/value"}'::jsonb, 'parser/1', 'mapper/1', 'normalizer/1'
		)
	`, claimID, testRecordID, predicate, rawJSON); err != nil {
		t.Fatalf("insert attribute claim: %v", err)
	}
}

func seedProjectionEntities(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	if _, err := tx.Exec(ctx, `
		insert into entities (id, entity_type, display_name) values
			('80000000-0000-4000-8000-000000000001', 'person', 'Ada One'),
			('80000000-0000-4000-8000-000000000002', 'person', 'Ada Two')
	`); err != nil {
		t.Fatalf("insert projection entities: %v", err)
	}
}

func seedInitialResolution(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	entityID string,
	reviewID string,
	candidateID string,
	decisionID string,
	linkID string,
) {
	t.Helper()
	if _, err := tx.Exec(ctx, `
		insert into match_reviews (id, source_record_id) values ($1, $2)
	`, reviewID, testRecordID); err != nil {
		t.Fatalf("insert initial match review: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into match_candidates (
			id, match_review_id, candidate_set_version, entity_id,
			score, recommendation, algorithm_version
		) values ($1, $2, 1, $3, 0.95, 'review', 'matcher/1')
	`, candidateID, reviewID, entityID); err != nil {
		t.Fatalf("insert initial match candidate: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into match_decisions (
			id, match_review_id, candidate_set_version, selected_candidate_id,
			resolved_entity_id, expected_lock_version, decision, actor_type
		) values ($1, $2, 1, $3, $4, 1, 'select_candidate', 'user')
	`, decisionID, reviewID, candidateID, entityID); err != nil {
		t.Fatalf("insert initial match decision: %v", err)
	}
	if _, err := tx.Exec(ctx, `
		insert into entity_records (
			id, entity_id, source_record_id, match_decision_id, link_method
		) values ($1, $2, $3, $4, 'manual')
	`, linkID, entityID, testRecordID, decisionID); err != nil {
		t.Fatalf("insert initial entity link: %v", err)
	}
}

func requireDatabaseRejection(t *testing.T, err error, operation string) {
	t.Helper()
	if err == nil {
		t.Fatalf("database accepted %s", operation)
	}
}

func requireRejectedStatement(
	t *testing.T,
	ctx context.Context,
	tx pgx.Tx,
	operation string,
	query string,
	args ...any,
) {
	t.Helper()
	if _, err := tx.Exec(ctx, "savepoint expected_rejection"); err != nil {
		t.Fatalf("create savepoint for %s: %v", operation, err)
	}
	_, statementErr := tx.Exec(ctx, query, args...)
	requireDatabaseRejection(t, statementErr, operation)
	if _, err := tx.Exec(ctx, "rollback to savepoint expected_rejection"); err != nil {
		t.Fatalf("rollback rejected %s: %v", operation, err)
	}
	if _, err := tx.Exec(ctx, "release savepoint expected_rejection"); err != nil {
		t.Fatalf("release savepoint for %s: %v", operation, err)
	}
}
