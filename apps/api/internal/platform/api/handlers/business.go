package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/utils"
)

var _ generated.ServerInterface = (*Handler)(nil)

func (*Handler) notImplemented(c *gin.Context) {
	utils.WriteProblem(
		c,
		http.StatusNotImplemented,
		"NOT_IMPLEMENTED",
		"Not implemented",
		"This operation is not implemented yet.",
	)
}

func (h *Handler) GetArtifactContent(
	c *gin.Context,
	_ generated.ArtifactID,
	_ generated.GetArtifactContentParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetClaimEvidence(
	c *gin.Context,
	_ generated.ClaimID,
	_ generated.GetClaimEvidenceParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetEntity(
	c *gin.Context,
	_ generated.EntityID,
	_ generated.GetEntityParams,
) {
	h.notImplemented(c)
}

func (h *Handler) ExpandEntity(
	c *gin.Context,
	_ generated.EntityID,
	_ generated.ExpandEntityParams,
) {
	h.notImplemented(c)
}

func (h *Handler) CreateInvestigation(
	c *gin.Context,
	_ generated.CreateInvestigationParams,
) {
	if _, err := h.bindCreateInvestigationRequest(c); err != nil {
		status := http.StatusBadRequest
		code := "INVALID_REQUEST"
		title := "Invalid request"
		detail := "The request body does not match the HTTP API contract."
		var maxBytesError *http.MaxBytesError
		switch {
		case errors.Is(err, errUnsupportedMediaType):
			status = http.StatusUnsupportedMediaType
			code = "UNSUPPORTED_MEDIA_TYPE"
			title = "Unsupported media type"
			detail = "The request body must use application/json."
		case errors.As(err, &maxBytesError):
			status = http.StatusRequestEntityTooLarge
			code = "REQUEST_TOO_LARGE"
			title = "Request too large"
			detail = "The request body exceeds the allowed size."
		}
		utils.WriteProblem(
			c,
			status,
			code,
			title,
			detail,
		)
		return
	}
	h.notImplemented(c)
}

func (h *Handler) GetInvestigation(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.GetInvestigationParams,
) {
	h.notImplemented(c)
}

func (h *Handler) CancelInvestigation(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.CancelInvestigationParams,
) {
	h.notImplemented(c)
}

func (h *Handler) ListInvestigationEntities(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.ListInvestigationEntitiesParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetInvestigationGraph(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.GetInvestigationGraphParams,
) {
	h.notImplemented(c)
}

func (h *Handler) ListInvestigationReviews(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.ListInvestigationReviewsParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetInvestigationStatus(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.GetInvestigationStatusParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetInvestigationTimeline(
	c *gin.Context,
	_ generated.InvestigationID,
	_ generated.GetInvestigationTimelineParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetRelationship(
	c *gin.Context,
	_ generated.RelationshipID,
	_ generated.GetRelationshipParams,
) {
	h.notImplemented(c)
}

func (h *Handler) GetReview(
	c *gin.Context,
	_ generated.ReviewID,
	_ generated.GetReviewParams,
) {
	h.notImplemented(c)
}

func (h *Handler) DecideReview(
	c *gin.Context,
	_ generated.ReviewID,
	_ generated.DecideReviewParams,
) {
	h.notImplemented(c)
}

func (h *Handler) RevertReviewDecision(
	c *gin.Context,
	_ generated.ReviewID,
	_ generated.RevertReviewDecisionParams,
) {
	h.notImplemented(c)
}

func (h *Handler) ListSources(
	c *gin.Context,
	_ generated.ListSourcesParams,
) {
	h.notImplemented(c)
}
