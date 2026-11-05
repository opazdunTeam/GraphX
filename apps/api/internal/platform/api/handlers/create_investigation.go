package handlers

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
)

type createInvestigationInput struct {
	Subject generated.SubjectInput
	Limits  InvestigationLimits
}

func (h *Handler) bindCreateInvestigationRequest(c *gin.Context) (createInvestigationInput, error) {
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()

	var request generated.CreateInvestigationRequest
	if err := decoder.Decode(&request); err != nil {
		return createInvestigationInput{}, fmt.Errorf("decode request body: %w", err)
	}
	if err := ensureJSONDocumentEnded(decoder); err != nil {
		return createInvestigationInput{}, err
	}

	limits, err := h.ResolveInvestigationLimits(request.Limits)
	if err != nil {
		return createInvestigationInput{}, err
	}

	return createInvestigationInput{
		Subject: request.Subject,
		Limits:  limits,
	}, nil
}

func ensureJSONDocumentEnded(decoder *json.Decoder) error {
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain one JSON value")
		}
		return fmt.Errorf("decode trailing request data: %w", err)
	}
	return nil
}
