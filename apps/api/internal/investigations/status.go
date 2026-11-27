package investigations

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

type StatusReader interface {
	ReadStatus(context.Context, uuid.UUID) (StatusSnapshot, error)
}

type StatusSnapshot struct {
	Status             string
	Stage              string
	StateVersion       int
	PendingReviewCount int
	UpdatedAt          time.Time
	Jobs               []StatusJob
}

type StatusJob struct {
	ID            uuid.UUID
	SourceCode    string
	Status        orchestration.Status
	AttemptNo     int
	UpdatedAt     time.Time
	NextAttemptAt *time.Time
	ErrorCode     string
	ErrorAt       *time.Time
}

type Progress struct {
	ActiveJobs   int
	TerminalJobs int
	KnownJobs    int
}

type SourceProgress struct {
	Code          string
	Status        string
	Progress      Progress
	SucceededJobs int
	AttemptCount  int
	ErrorCode     *string
	ErrorDetail   *string
	NextAttemptAt *time.Time
	UpdatedAt     time.Time
}

type sourceAccumulator struct {
	SourceProgress
	counts    map[orchestration.Status]int
	lastError *StatusJob
}

type SourceAction struct {
	JobID      uuid.UUID
	SourceCode string
	Code       string
	Detail     string
}

type ProgressSummary struct {
	Progress
	SucceededJobs      int
	SourcesWithResults int
	Sources            []SourceProgress
	Actions            []SourceAction
	UpdatedAt          time.Time
}

func (snapshot StatusSnapshot) Summarize() ProgressSummary {
	summary := ProgressSummary{
		Sources:   make([]SourceProgress, 0),
		Actions:   make([]SourceAction, 0),
		UpdatedAt: snapshot.UpdatedAt,
	}
	sources := make(map[string]*sourceAccumulator)
	for i := range snapshot.Jobs {
		job := &snapshot.Jobs[i]
		source := sources[job.SourceCode]
		if source == nil {
			source = &sourceAccumulator{
				SourceProgress: SourceProgress{Code: job.SourceCode},
				counts:         make(map[orchestration.Status]int),
			}
			sources[job.SourceCode] = source
		}
		source.add(job)
		if job.Status == orchestration.StatusActionRequired {
			summary.Actions = append(summary.Actions, SourceAction{
				JobID: job.ID, SourceCode: job.SourceCode,
				Code: "SOURCE_ACTION_REQUIRED", Detail: sourceErrorDetails["SOURCE_ACTION_REQUIRED"],
			})
		}
	}
	for _, source := range sources {
		source.Status = aggregateSourceState(source.counts)
		source.SucceededJobs = source.counts[orchestration.StatusSucceeded]
		if source.lastError != nil {
			code, detail := safeSourceError(*source.lastError)
			source.ErrorCode, source.ErrorDetail = &code, &detail
		}
		summary.Sources = append(summary.Sources, source.SourceProgress)
		summary.KnownJobs += source.Progress.KnownJobs
		summary.ActiveJobs += source.Progress.ActiveJobs
		summary.TerminalJobs += source.Progress.TerminalJobs
		if source.SucceededJobs > 0 {
			summary.SucceededJobs += source.SucceededJobs
			summary.SourcesWithResults++
		}
		if source.UpdatedAt.After(summary.UpdatedAt) {
			summary.UpdatedAt = source.UpdatedAt
		}
	}
	sort.Slice(summary.Sources, func(i, j int) bool {
		return summary.Sources[i].Code < summary.Sources[j].Code
	})
	sort.Slice(summary.Actions, func(i, j int) bool {
		left, right := summary.Actions[i], summary.Actions[j]
		if left.SourceCode != right.SourceCode {
			return left.SourceCode < right.SourceCode
		}
		return slices.Compare(left.JobID[:], right.JobID[:]) < 0
	})
	return summary
}

func (source *sourceAccumulator) add(job *StatusJob) {
	source.counts[job.Status]++
	source.Progress.KnownJobs++
	if orchestration.IsTerminal(job.Status) {
		source.Progress.TerminalJobs++
	} else {
		source.Progress.ActiveJobs++
	}
	source.AttemptCount += job.AttemptNo
	if job.UpdatedAt.After(source.UpdatedAt) {
		source.UpdatedAt = job.UpdatedAt
	}
	if job.NextAttemptAt != nil && (source.NextAttemptAt == nil || job.NextAttemptAt.Before(*source.NextAttemptAt)) {
		source.NextAttemptAt = job.NextAttemptAt
	}
	if job.ErrorAt != nil && isLaterError(job, source.lastError) {
		source.lastError = job
	}
}

func isLaterError(job, previous *StatusJob) bool {
	return previous == nil || job.ErrorAt.After(*previous.ErrorAt) ||
		(job.ErrorAt.Equal(*previous.ErrorAt) && slices.Compare(job.ID[:], previous.ID[:]) < 0)
}

func aggregateSourceState(counts map[orchestration.Status]int) string {
	for _, state := range []orchestration.Status{
		orchestration.StatusActionRequired, orchestration.StatusRunning, orchestration.StatusRetryWait,
		orchestration.StatusPending, orchestration.StatusQueued,
	} {
		if counts[state] > 0 {
			if state == orchestration.StatusQueued {
				return string(orchestration.StatusPending)
			}
			return string(state)
		}
	}
	switch {
	case counts[orchestration.StatusSucceeded] > 0:
		if counts[orchestration.StatusFailed]+counts[orchestration.StatusUnavailable] > 0 {
			return "completed_partial"
		}
		return string(orchestration.StatusSucceeded)
	case counts[orchestration.StatusFailed] > 0:
		return string(orchestration.StatusFailed)
	case counts[orchestration.StatusUnavailable] > 0:
		return string(orchestration.StatusUnavailable)
	case counts[orchestration.StatusNotFound] > 0:
		return string(orchestration.StatusNotFound)
	default:
		return string(orchestration.StatusCancelled)
	}
}

// Public details are fixed text. Stored adapter diagnostics can contain sensitive data.
var sourceErrorDetails = map[string]string{
	"SOURCE_TIMEOUT":               "The source did not respond in time.",
	"SOURCE_NETWORK_ERROR":         "A network error prevented access to the source.",
	"SOURCE_RATE_LIMITED":          "The source temporarily limited requests.",
	"SOURCE_UNAVAILABLE":           "The source is unavailable.",
	"SOURCE_ACCESS_DENIED":         "The source denied access.",
	"SOURCE_ACTION_REQUIRED":       "This source requires user action before work can continue.",
	"SOURCE_FORMAT_CHANGED":        "The source returned data in an unexpected format.",
	"SOURCE_INVALID_RESPONSE":      "The source returned an invalid response.",
	"SOURCE_UNSUPPORTED_OPERATION": "The source does not support this operation.",
	"CONTRACT_VALIDATION_FAILED":   "The source result could not be processed.",
	"INTERNAL_ADAPTER_ERROR":       "The source result could not be processed.",
}

func safeSourceError(job StatusJob) (string, string) {
	if detail, known := sourceErrorDetails[job.ErrorCode]; known {
		return job.ErrorCode, detail
	}
	return "SOURCE_ERROR", "A source job encountered an error."
}
