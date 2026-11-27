package postgres

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/apperrors"
)

const investigationStatusSQL = `
	with target as materialized (
		select i.id, i.status, i.stage, i.state_version,
		       reviews.open_count, greatest(i.updated_at, reviews.updated_at) as updated_at
		from investigations i
		cross join lateral (
			select count(*) filter (where review.status = 'open') as open_count,
			       max(greatest(review.created_at, review.resolved_at, decisions.updated_at)) as updated_at
			from source_jobs review_job
			join source_job_results result on result.source_job_id = review_job.id
			join processing_runs run on run.source_job_result_id = result.id
			join source_records record on record.processing_run_id = run.id
			join match_reviews review on review.source_record_id = record.id
			left join lateral (
				select max(decision.created_at) as updated_at
				from match_decisions decision
				where decision.match_review_id = review.id
			) decisions on true
			where review_job.investigation_id = i.id
		) reviews
		where i.id = $1
	)
	select i.status, i.stage, i.state_version, i.open_count, i.updated_at,
	       job.id, coalesce(source.code, ''), coalesce(job.status, ''),
	       coalesce(job.attempt_no, 0), coalesce(job.updated_at, i.updated_at),
	       job.next_attempt_at, coalesce(job.last_error_code, ''), job.last_error_at
	from target i
	left join source_jobs job on job.investigation_id = i.id
	left join sources source on source.id = job.source_id
`

func (s *InvestigationStore) ReadStatus(ctx context.Context, id uuid.UUID) (investigations.StatusSnapshot, error) {
	return readInvestigationStatus(ctx, s.pool, id)
}

func readInvestigationStatus(ctx context.Context, db DBTX, id uuid.UUID) (investigations.StatusSnapshot, error) {
	rows, err := db.Query(ctx, investigationStatusSQL, id)
	if err != nil {
		return investigations.StatusSnapshot{}, fmt.Errorf("query investigation status: %w", err)
	}
	defer rows.Close()

	var snapshot investigations.StatusSnapshot
	found := false
	for rows.Next() {
		found = true
		var job investigations.StatusJob
		var jobID *uuid.UUID
		if err := rows.Scan(
			&snapshot.Status, &snapshot.Stage, &snapshot.StateVersion, &snapshot.PendingReviewCount, &snapshot.UpdatedAt,
			&jobID, &job.SourceCode, &job.Status, &job.AttemptNo, &job.UpdatedAt,
			&job.NextAttemptAt, &job.ErrorCode, &job.ErrorAt,
		); err != nil {
			return investigations.StatusSnapshot{}, fmt.Errorf("scan investigation status: %w", err)
		}
		if jobID != nil {
			job.ID = *jobID
			snapshot.Jobs = append(snapshot.Jobs, job)
		}
	}
	if err := rows.Err(); err != nil {
		return investigations.StatusSnapshot{}, fmt.Errorf("read investigation status: %w", err)
	}
	if !found {
		return investigations.StatusSnapshot{}, apperrors.ErrNotFound
	}
	return snapshot, nil
}
