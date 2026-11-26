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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

func (s *Store) ApplySourceResult(
	ctx context.Context,
	consumerName string,
	result ingestion.SourceResultCommand,
) (ingestion.ResultApplyOutcome, error) {
	if s.pool == nil {
		return ingestion.ResultApplyOutcome{}, errors.New("source result ingestion requires a pool-backed store")
	}
	if err := validateSourceResultCommand(consumerName, result); err != nil {
		return ingestion.ResultApplyOutcome{}, err
	}

	var outcome ingestion.ResultApplyOutcome
	err := s.WithinTransaction(ctx, func(txStore *Store) error {
		var inserted bool
		err := txStore.db.QueryRow(ctx, `
			insert into consumed_messages (consumer_name, message_id)
			values ($1, $2)
			on conflict (consumer_name, message_id) do nothing
			returning true
		`, consumerName, result.MessageID).Scan(&inserted)
		if errors.Is(err, pgx.ErrNoRows) {
			outcome.Duplicate = true
			return nil
		}
		if err != nil {
			return fmt.Errorf("register source result inbox message: %w", err)
		}

		var jobStatus orchestration.Status
		var currentAttempt, maxAttempts int
		var investigationID, investigationStatus, sourceID string
		if err := txStore.db.QueryRow(ctx, `
			select status, attempt_no, max_attempts, investigation_id, source_id
			from source_jobs
			where id = $1
			for update
		`, result.JobID).Scan(
			&jobStatus, &currentAttempt, &maxAttempts, &investigationID, &sourceID,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ingestion.NewPermanentResultError(errors.New("result references an unknown source job"))
			}
			return fmt.Errorf("lock source result job: %w", err)
		}
		if err := txStore.db.QueryRow(ctx, `
			select status
			from investigations
			where id = $1
			for update
		`, investigationID).Scan(&investigationStatus); err != nil {
			return fmt.Errorf("lock source result investigation: %w", err)
		}
		if result.CorrelationID != investigationID {
			return ingestion.NewPermanentResultError(errors.New("result correlation_id does not match the source job"))
		}
		if result.AttemptNo > currentAttempt {
			return ingestion.NewPermanentResultError(
				fmt.Errorf("result attempt %d is ahead of current attempt %d", result.AttemptNo, currentAttempt),
			)
		}

		var existingDigest string
		err = txStore.db.QueryRow(ctx, `
			select result_digest
			from source_job_results
			where source_job_id = $1 and attempt_no = $2
		`, result.JobID, result.AttemptNo).Scan(&existingDigest)
		switch {
		case err == nil:
			if existingDigest != result.Digest {
				return ingestion.NewPermanentResultErrorWithCode(
					"source_result_attempt_digest_conflict",
					errors.New("conflicting result digest for an already accepted attempt"),
				)
			}
			outcome.Duplicate = true
			return nil
		case !errors.Is(err, pgx.ErrNoRows):
			return fmt.Errorf("read existing source job result: %w", err)
		}

		ignoredReason := ""
		switch {
		case result.AttemptNo < currentAttempt:
			ignoredReason = "stale_attempt"
		case investigationStatus != "running":
			ignoredReason = "late_cancelled_investigation"
		case orchestration.IsTerminal(jobStatus):
			ignoredReason = "terminal_job"
		case jobStatus != orchestration.StatusPending &&
			jobStatus != orchestration.StatusQueued &&
			jobStatus != orchestration.StatusRunning:
			ignoredReason = "job_not_accepting_result"
		}

		actualStatus := result.Status
		if ignoredReason == "" && result.Status == ingestion.ResultStatusRetryWait &&
			result.AttemptNo >= maxAttempts {
			actualStatus = ingestion.ResultStatusUnavailable
		}
		applied := ignoredReason == ""
		var ignoredReasonValue any
		if !applied {
			ignoredReasonValue = ignoredReason
		}
		if _, err := txStore.db.Exec(ctx, `
			insert into source_job_results (
				id, source_job_id, message_id, attempt_no, result_digest,
				applied, ignored_reason
			) values ($1, $2, $3, $4, $5, $6, $7)
		`, result.ID, result.JobID, result.MessageID, result.AttemptNo, result.Digest,
			applied, ignoredReasonValue); err != nil {
			var pgError *pgconn.PgError
			if errors.As(err, &pgError) && pgError.Code == "23505" {
				return ingestion.NewPermanentResultError(
					errors.New("result message conflicts with an existing producer identity"),
				)
			}
			return fmt.Errorf("insert source job result receipt: %w", err)
		}

		if err := txStore.insertSourceResultArtifacts(
			ctx, result.ID, sourceID, result.AdapterVersion, result.Artifacts,
		); err != nil {
			return err
		}
		outcome.IgnoredReason = ignoredReason
		if !applied {
			return nil
		}
		if err := txStore.insertSourceResultProcessing(
			ctx, result.ID, result.Artifacts, result.ProcessingRuns, result.Claims,
		); err != nil {
			return err
		}

		switch actualStatus {
		case ingestion.ResultStatusRetryWait:
			if result.NextAttemptAt.IsZero() {
				return errors.New("retry_wait result requires next_attempt_at")
			}
			if err := txStore.ScheduleRetry(
				ctx, result.JobID, result.NextAttemptAt, result.ErrorCode, result.ErrorMessage,
			); err != nil {
				return err
			}
		case ingestion.ResultStatusSucceeded,
			ingestion.ResultStatusNotFound,
			ingestion.ResultStatusActionRequired,
			ingestion.ResultStatusUnavailable,
			ingestion.ResultStatusFailed:
			if err := txStore.AdvanceTerminal(
				ctx, result.JobID, orchestration.Status(actualStatus), result.ErrorCode, result.ErrorMessage,
			); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported source result status %q", actualStatus)
		}
		outcome.Applied = true
		outcome.Status = actualStatus
		return nil
	})
	return outcome, err
}

func validateSourceResultCommand(consumerName string, result ingestion.SourceResultCommand) error {
	if strings.TrimSpace(consumerName) == "" {
		return errors.New("source result consumer name is required")
	}
	for _, field := range []struct {
		name  string
		value string
	}{
		{name: "result id", value: result.ID},
		{name: "message id", value: result.MessageID},
		{name: "correlation id", value: result.CorrelationID},
		{name: "job id", value: result.JobID},
	} {
		if _, err := uuid.Parse(field.value); err != nil {
			return fmt.Errorf("source result %s must be a UUID", field.name)
		}
	}
	if len(result.Digest) != 64 {
		return errors.New("source result digest must be a SHA-256 hex value")
	}
	for _, character := range result.Digest {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return errors.New("source result digest must be a SHA-256 hex value")
		}
	}
	if result.AttemptNo < 1 {
		return errors.New("source result attempt_no must be positive")
	}
	if result.Status == ingestion.ResultStatusRetryWait && result.NextAttemptAt.IsZero() {
		return errors.New("retry_wait result requires next_attempt_at")
	}
	return nil
}

func (s *Store) insertSourceResultArtifacts(
	ctx context.Context,
	resultID, sourceID, adapterVersion string,
	artifacts []ingestion.ArtifactReference,
) error {
	ordered, err := orderSourceResultArtifacts(artifacts)
	if err != nil {
		return err
	}
	artifactIDs := make(map[string]string, len(ordered))
	for _, artifact := range ordered {
		metadata, err := json.Marshal(artifact.HTTPMetadata)
		if err != nil {
			return fmt.Errorf("encode artifact HTTP metadata: %w", err)
		}
		id := uuid.NewString()
		var parentID any
		if artifact.ParentLocalRef != nil {
			resolvedParentID, exists := artifactIDs[*artifact.ParentLocalRef]
			if !exists {
				return errors.New("artifact parent was not inserted before its child")
			}
			parentID = resolvedParentID
		}
		if _, err := s.db.Exec(ctx, `
			insert into artifacts (
				id, source_id, kind, media_type, object_key, source_uri,
				sha256, byte_size, published_at, retrieved_at, http_metadata,
				adapter_version, original_filename, parent_artifact_id
			) values (
				$1, $2, $3, $4, $5, $6,
				$7, $8, $9, $10, $11::jsonb,
				$12, $13, $14
			)
		`, id, sourceID, artifact.Kind, artifact.MediaType, artifact.ObjectKey,
			artifact.SourceURI, artifact.SHA256, artifact.ByteSize, artifact.PublishedAt,
			artifact.RetrievedAt, string(metadata), adapterVersion, artifact.OriginalFilename, parentID); err != nil {
			return fmt.Errorf("insert source result artifact: %w", err)
		}
		artifactIDs[artifact.LocalRef] = id
	}
	for _, artifact := range artifacts {
		artifactID, exists := artifactIDs[artifact.LocalRef]
		if !exists {
			return errors.New("source result artifact was not inserted")
		}
		if _, err := s.db.Exec(ctx, `
			insert into source_job_result_artifacts (
				source_job_result_id, artifact_id, artifact_local_ref, role
			) values ($1, $2, $3, $4)
		`, resultID, artifactID, artifact.LocalRef, artifact.Kind); err != nil {
			return fmt.Errorf("link source result artifact: %w", err)
		}
	}
	return nil
}

func orderSourceResultArtifacts(
	artifacts []ingestion.ArtifactReference,
) ([]ingestion.ArtifactReference, error) {
	remaining := append([]ingestion.ArtifactReference(nil), artifacts...)
	ordered := make([]ingestion.ArtifactReference, 0, len(artifacts))
	inserted := make(map[string]struct{}, len(artifacts))
	for len(remaining) > 0 {
		next := make([]ingestion.ArtifactReference, 0, len(remaining))
		progress := false
		for _, artifact := range remaining {
			if artifact.ParentLocalRef != nil {
				if _, exists := inserted[*artifact.ParentLocalRef]; !exists {
					next = append(next, artifact)
					continue
				}
			}
			ordered = append(ordered, artifact)
			inserted[artifact.LocalRef] = struct{}{}
			progress = true
		}
		if !progress {
			return nil, errors.New("source result artifact parents cannot be resolved")
		}
		remaining = next
	}
	return ordered, nil
}

func (s *Store) insertSourceResultProcessing(
	ctx context.Context,
	resultID string,
	artifacts []ingestion.ArtifactReference,
	runs []ingestion.ProcessingRunInput,
	claims []ingestion.Claim,
) error {
	artifactIDs := make(map[string]string, len(artifacts))
	rows, err := s.db.Query(ctx, `
		select artifact_local_ref, artifact_id
		from source_job_result_artifacts
		where source_job_result_id = $1
	`, resultID)
	if err != nil {
		return fmt.Errorf("read source result artifact links: %w", err)
	}
	for rows.Next() {
		var localRef, artifactID string
		if err := rows.Scan(&localRef, &artifactID); err != nil {
			rows.Close()
			return fmt.Errorf("scan source result artifact link: %w", err)
		}
		artifactIDs[localRef] = artifactID
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate source result artifact links: %w", err)
	}
	rows.Close()

	recordIDs := make(map[string]string)
	recordRuns := make(map[string]ingestion.ProcessingRunInput)
	for _, run := range runs {
		artifactID, exists := artifactIDs[run.ArtifactLocalRef]
		if !exists {
			return fmt.Errorf("processing run references unknown artifact %q", run.ArtifactLocalRef)
		}
		runID := uuid.NewString()
		if _, err := s.db.Exec(ctx, `
			insert into processing_runs (
				id, artifact_id, source_job_result_id, processing_scope,
				status, schema_version, parser_version, mapping_version,
				normalization_version, active, started_at, finished_at
			) values ($1, $2, $3, $4, 'succeeded', $5, $6, $7, $8, true, now(), now())
		`, runID, artifactID, resultID, run.ProcessingScope, run.SchemaVersion,
			run.ParserVersion, run.MappingVersion, run.NormalizationVersion); err != nil {
			return fmt.Errorf("insert source result processing run: %w", err)
		}
		for _, record := range run.Records {
			rawData, err := json.Marshal(record.RawData)
			if err != nil {
				return fmt.Errorf("encode source record raw_data: %w", err)
			}
			recordID := uuid.NewString()
			if _, err := s.db.Exec(ctx, `
				insert into source_records (
					id, processing_run_id, local_ref, external_id,
					record_type, schema_version, raw_data
				) values ($1, $2, $3, $4, $5, $6, $7::jsonb)
			`, recordID, runID, record.LocalRef, record.ExternalID,
				record.RecordType, record.SchemaVersion, string(rawData)); err != nil {
				return fmt.Errorf("insert source result record: %w", err)
			}
			recordIDs[record.LocalRef] = recordID
			recordRuns[record.LocalRef] = run
		}
	}
	for _, claim := range claims {
		subjectID, exists := recordIDs[claim.SubjectLocalRef]
		if !exists {
			return fmt.Errorf("claim references unknown subject record %q", claim.SubjectLocalRef)
		}
		var objectID any
		if claim.ObjectLocalRef != nil {
			resolvedObjectID, exists := recordIDs[*claim.ObjectLocalRef]
			if !exists {
				return fmt.Errorf("claim references unknown object record %q", *claim.ObjectLocalRef)
			}
			objectID = resolvedObjectID
		}
		run, exists := recordRuns[claim.SubjectLocalRef]
		if !exists {
			return fmt.Errorf("claim references unknown processing run for record %q", claim.SubjectLocalRef)
		}
		rawValue, err := json.Marshal(claim.RawValue)
		if err != nil {
			return fmt.Errorf("encode claim raw_value: %w", err)
		}
		qualifiers, err := json.Marshal(claim.Qualifiers)
		if err != nil {
			return fmt.Errorf("encode claim qualifiers: %w", err)
		}
		evidenceLocator, err := json.Marshal(claim.EvidenceLocator)
		if err != nil {
			return fmt.Errorf("encode claim evidence_locator: %w", err)
		}
		var normalizedValue any
		if claim.NormalizedValue != nil {
			encoded, err := json.Marshal(claim.NormalizedValue)
			if err != nil {
				return fmt.Errorf("encode claim normalized_value: %w", err)
			}
			normalizedValue = string(encoded)
		}
		validFrom, err := dateValue(claim.ValidFrom)
		if err != nil {
			return fmt.Errorf("parse claim valid_from: %w", err)
		}
		validTo, err := dateValue(claim.ValidTo)
		if err != nil {
			return fmt.Errorf("parse claim valid_to: %w", err)
		}
		if _, err := s.db.Exec(ctx, `
			insert into claims (
				id, subject_source_record_id, object_source_record_id,
				kind, predicate, raw_value, normalized_value, qualifiers,
				valid_from, valid_to, valid_from_precision, valid_to_precision,
				temporal_basis, snapshot_at, evidence_locator, evidence_excerpt,
				parser_version, mapping_version, normalization_version
			) values (
				$1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::jsonb,
				$9, $10, $11, $12, $13, $14, $15::jsonb, $16, $17, $18, $19
			)
		`, uuid.NewString(), subjectID, objectID, claim.ClaimKind, claim.Predicate,
			string(rawValue), normalizedValue, string(qualifiers), validFrom,
			validTo, claim.ValidFromPrecision, claim.ValidToPrecision,
			claim.TemporalBasis, claim.SnapshotAt, string(evidenceLocator), claim.EvidenceExcerpt,
			run.ParserVersion, run.MappingVersion, run.NormalizationVersion); err != nil {
			return fmt.Errorf("insert source result claim: %w", err)
		}
	}
	return nil
}

func dateValue(value *string) (any, error) {
	if value == nil {
		return nil, nil
	}
	date, err := time.Parse(time.DateOnly, *value)
	if err != nil {
		return nil, err
	}
	return date, nil
}
