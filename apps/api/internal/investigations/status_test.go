package investigations

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/orchestration"
)

func TestAggregateSourceState(t *testing.T) {
	for _, tc := range []struct {
		want string
		jobs []orchestration.Status
	}{
		{"pending", []orchestration.Status{"pending"}},
		{"pending", []orchestration.Status{"queued", "failed"}},
		{"retry_wait", []orchestration.Status{"retry_wait", "pending", "succeeded"}},
		{"running", []orchestration.Status{"running", "retry_wait", "queued"}},
		{"action_required", []orchestration.Status{"action_required", "running", "retry_wait"}},
		{"succeeded", []orchestration.Status{"succeeded", "not_found", "cancelled"}},
		{"completed_partial", []orchestration.Status{"succeeded", "failed"}},
		{"completed_partial", []orchestration.Status{"succeeded", "unavailable"}},
		{"not_found", []orchestration.Status{"not_found", "cancelled"}},
		{"unavailable", []orchestration.Status{"not_found", "unavailable"}},
		{"failed", []orchestration.Status{"failed", "unavailable", "not_found"}},
		{"cancelled", []orchestration.Status{"cancelled", "cancelled"}},
	} {
		counts := make(map[orchestration.Status]int)
		for _, status := range tc.jobs {
			counts[status]++
		}
		if got := aggregateSourceState(counts); got != tc.want {
			t.Errorf("aggregateSourceState(%v) = %q, want %q", tc.jobs, got, tc.want)
		}
	}
}

func TestStatusSummaryCountsResultsDuringWorkAndOrdersSourcesAndActions(t *testing.T) {
	now := time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)
	early, late := now.Add(time.Minute), now.Add(2*time.Minute)
	previousError := now.Add(-time.Minute)
	firstID := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	secondID := uuid.MustParse("00000000-0000-4000-8000-000000000002")
	jobs := []StatusJob{
		{ID: secondID, SourceCode: "z", Status: "action_required", AttemptNo: 1},
		{ID: firstID, SourceCode: "a", Status: "retry_wait", AttemptNo: 2, UpdatedAt: now, NextAttemptAt: &late, ErrorAt: &now, ErrorCode: "SOURCE_TIMEOUT"},
		{ID: secondID, SourceCode: "a", Status: "retry_wait", AttemptNo: 3, UpdatedAt: now, NextAttemptAt: &early, ErrorAt: &now, ErrorCode: "SOURCE_NETWORK_ERROR"},
		{SourceCode: "a", Status: "queued", AttemptNo: 1, UpdatedAt: now.Add(time.Second), ErrorAt: &previousError, ErrorCode: "SOURCE_ACCESS_DENIED"},
		{SourceCode: "a", Status: "succeeded", AttemptNo: 1, UpdatedAt: now.Add(time.Second)},
		{ID: firstID, SourceCode: "z", Status: "action_required", AttemptNo: 1},
	}
	for range 2 {
		summary := (StatusSnapshot{Jobs: jobs}).Summarize()
		if summary.Progress != (Progress{ActiveJobs: 5, TerminalJobs: 1, KnownJobs: 6}) ||
			summary.SucceededJobs != 1 || summary.SourcesWithResults != 1 ||
			!summary.UpdatedAt.Equal(now.Add(time.Second)) {
			t.Fatalf("summary = %+v", summary)
		}
		source := summary.Sources[0]
		if source.Code != "a" || source.Status != "retry_wait" || source.AttemptCount != 7 ||
			source.ErrorCode == nil || *source.ErrorCode != "SOURCE_TIMEOUT" ||
			source.NextAttemptAt == nil || !source.NextAttemptAt.Equal(early) {
			t.Fatalf("source summary = %+v", source)
		}
		if len(summary.Actions) != 2 || summary.Actions[0].JobID != firstID || summary.Actions[1].JobID != secondID {
			t.Fatalf("actions = %+v", summary.Actions)
		}
		slices.Reverse(jobs)
	}
}

func TestStatusSummaryDistinguishesResultsOfActiveSources(t *testing.T) {
	summary := (StatusSnapshot{Jobs: []StatusJob{
		{SourceCode: "a", Status: "running", AttemptNo: 1},
		{SourceCode: "a", Status: "not_found", AttemptNo: 1},
		{SourceCode: "b", Status: "running", AttemptNo: 1},
		{SourceCode: "b", Status: "succeeded", AttemptNo: 1},
	}}).Summarize()
	a, b := summary.Sources[0], summary.Sources[1]
	if a.Code != "a" || b.Code != "b" || a.Status != "running" || b.Status != "running" ||
		a.Progress != b.Progress || a.SucceededJobs != 0 || b.SucceededJobs != 1 {
		t.Fatalf("source summaries = %+v", summary.Sources)
	}
}
