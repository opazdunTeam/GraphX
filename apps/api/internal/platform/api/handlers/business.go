package handlers

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
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
	params generated.CreateInvestigationParams,
) {
	input, err := h.bindCreateInvestigationRequest(c)
	if err != nil {
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
	if h.create == nil {
		h.notImplemented(c)
		return
	}

	subject, err := subjectFromGenerated(input.Subject)
	if err != nil {
		utils.WriteProblem(c, http.StatusBadRequest, "INVALID_REQUEST", "Invalid request", err.Error())
		return
	}
	command := investigations.CreateCommand{
		IdempotencyKey: params.IdempotencyKey.String(),
		RequestHash:    input.RequestHash,
		RawSubject:     input.RawSubject,
		Subject:        subject,
		Limits: investigations.Limits{
			Depth:       input.Limits.Depth,
			EntityLimit: input.Limits.EntityLimit,
			Sources:     append([]string(nil), input.Limits.Sources...),
		},
	}
	accepted, err := h.create.Create(c.Request.Context(), command)
	if err != nil {
		var conflict *investigations.IdempotencyConflictError
		var inProgress *investigations.IdempotencyInProgressError
		var sourceSelection *investigations.SourceSelectionError
		switch {
		case errors.As(err, &conflict):
			utils.WriteProblem(
				c,
				http.StatusConflict,
				"IDEMPOTENCY_CONFLICT",
				"Idempotency key conflict",
				"The same Idempotency-Key was used with a different request.",
			)
		case errors.As(err, &inProgress):
			utils.WriteProblem(
				c,
				http.StatusConflict,
				"IDEMPOTENCY_IN_PROGRESS",
				"Idempotency key in progress",
				"A request with this Idempotency-Key is already in progress.",
			)
		case errors.As(err, &sourceSelection):
			detail := "One or more selected sources are unknown or disabled."
			if len(sourceSelection.Unknown) > 0 {
				detail = "Unknown source codes: " + strings.Join(sourceSelection.Unknown, ", ") + "."
			} else if len(sourceSelection.Disabled) > 0 {
				detail = "Disabled source codes: " + strings.Join(sourceSelection.Disabled, ", ") + "."
			}
			utils.WriteProblem(
				c,
				http.StatusBadRequest,
				"SOURCE_NOT_AVAILABLE",
				"Source not available",
				detail,
			)
		default:
			utils.WriteProblem(
				c,
				http.StatusInternalServerError,
				"INTERNAL_ERROR",
				"Internal server error",
				"The investigation could not be created.",
			)
		}
		return
	}

	investigationID, err := uuid.Parse(accepted.ID)
	if err != nil {
		utils.WriteProblem(
			c,
			http.StatusInternalServerError,
			"INTERNAL_ERROR",
			"Internal server error",
			"The investigation could not be created.",
		)
		return
	}
	c.Header("Location", accepted.StatusURL)
	c.JSON(http.StatusAccepted, generated.InvestigationAccepted{
		Id:        investigationID,
		Status:    generated.InvestigationAcceptedStatusRunning,
		Stage:     generated.InvestigationAcceptedStageSubjectDiscovery,
		StatusUrl: accepted.StatusURL,
	})
}

func subjectFromGenerated(input generated.SubjectInput) (investigations.Subject, error) {
	if named, err := input.AsNamedSubjectInput(); err == nil && named.Name != "" {
		identifiers := copyIdentifierPointer(named.Identifiers)
		return investigations.Subject{
			Type:        string(named.Type),
			Name:        &named.Name,
			Identifiers: identifiers,
		}, nil
	}
	identified, err := input.AsIdentifiedSubjectInput()
	if err != nil {
		return investigations.Subject{}, err
	}
	identifiers := copyIdentifierMap(identified.Identifiers)
	return investigations.Subject{
		Type:        string(identified.Type),
		Identifiers: identifiers,
	}, nil
}

func copyIdentifierPointer(input *map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(*input))
	for key, value := range *input {
		result[key] = value
	}
	return result
}

func copyIdentifierMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
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
