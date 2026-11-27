package handlers

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/maperrors"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/apperrors"
)

func (h *Handler) GetInvestigationStatus(
	c *gin.Context,
	id generated.InvestigationID,
	_ generated.GetInvestigationStatusParams,
) {
	if h.status == nil {
		h.notImplemented(c)
		return
	}
	if !strings.EqualFold(c.Param("investigation_id"), id.String()) {
		maperrors.HandleError(c, apperrors.ErrInvalidInput)
		return
	}
	snapshot, err := h.status.ReadStatus(c.Request.Context(), id)
	if err != nil {
		maperrors.HandleError(c, err)
		return
	}
	summary := snapshot.Summarize()
	dto := generated.InvestigationStatus{
		Id:                 id,
		Status:             generated.InvestigationState(snapshot.Status),
		Stage:              generated.InvestigationStage(snapshot.Stage),
		StateVersion:       snapshot.StateVersion,
		PendingReviewCount: snapshot.PendingReviewCount,
		RequiresAction:     snapshot.PendingReviewCount > 0 || len(summary.Actions) > 0,
		Sources:            make([]generated.SourceStatus, 0, len(summary.Sources)),
		SourceActions:      make([]generated.SourceAction, 0, len(summary.Actions)),
		UpdatedAt:          summary.UpdatedAt.UTC(),
	}
	dto.Progress.ActiveJobs = summary.ActiveJobs
	dto.Progress.TerminalJobs = summary.TerminalJobs
	dto.Progress.KnownJobs = summary.KnownJobs
	dto.PartialResult.SucceededJobs = summary.SucceededJobs
	dto.PartialResult.SourcesWithResults = summary.SourcesWithResults
	if snapshot.PendingReviewCount > 0 {
		reviewsURL := "/api/v1/investigations/" + id.String() + "/reviews"
		dto.ReviewsUrl = &reviewsURL
	}
	for _, source := range summary.Sources {
		dto.Sources = append(dto.Sources, generated.SourceStatus{
			SourceCode: source.Code, Status: generated.SourceAggregateState(source.Status),
			ActiveJobCount: source.Progress.ActiveJobs, TerminalJobCount: source.Progress.TerminalJobs,
			KnownJobCount: source.Progress.KnownJobs, SucceededJobCount: source.SucceededJobs, AttemptCount: source.AttemptCount,
			ErrorCode: source.ErrorCode, ErrorDetail: source.ErrorDetail,
			NextAttemptAt: source.NextAttemptAt, UpdatedAt: source.UpdatedAt.UTC(),
		})
	}
	for _, action := range summary.Actions {
		dto.SourceActions = append(dto.SourceActions, generated.SourceAction{
			JobId: action.JobID, SourceCode: action.SourceCode, ActionCode: action.Code, Detail: action.Detail,
		})
	}
	c.JSON(http.StatusOK, dto)
}
