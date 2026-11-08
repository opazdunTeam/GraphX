package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
)

const (
	maxCreateInvestigationRequestBytes = 1 << 20
	maxSubjectNameLength               = 500
)

var errUnsupportedMediaType = errors.New("request content type must be application/json")

type createInvestigationInput struct {
	Subject generated.SubjectInput
	Limits  InvestigationLimits
}

type createInvestigationRequest struct {
	Subject json.RawMessage `json:"subject"`
	Limits  json.RawMessage `json:"limits,omitempty"`
}

func (h *Handler) bindCreateInvestigationRequest(c *gin.Context) (createInvestigationInput, error) {
	if err := requireJSONContentType(c.GetHeader("Content-Type")); err != nil {
		return createInvestigationInput{}, err
	}

	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxCreateInvestigationRequestBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()

	var request createInvestigationRequest
	if err := decoder.Decode(&request); err != nil {
		return createInvestigationInput{}, fmt.Errorf("decode request body: %w", err)
	}
	if err := ensureJSONDocumentEnded(decoder); err != nil {
		return createInvestigationInput{}, err
	}

	subject, err := validateSubjectInput(request.Subject)
	if err != nil {
		return createInvestigationInput{}, err
	}

	limitsInput, err := decodeInvestigationLimits(request.Limits)
	if err != nil {
		return createInvestigationInput{}, err
	}
	limits, err := h.ResolveInvestigationLimits(limitsInput)
	if err != nil {
		return createInvestigationInput{}, err
	}

	return createInvestigationInput{Subject: subject, Limits: limits}, nil
}

func requireJSONContentType(value string) error {
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || mediaType != "application/json" {
		return errUnsupportedMediaType
	}
	return nil
}

func validateSubjectInput(raw json.RawMessage) (generated.SubjectInput, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return generated.SubjectInput{}, fmt.Errorf("subject is required")
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return generated.SubjectInput{}, fmt.Errorf("subject must be an object")
	}
	for name := range fields {
		switch name {
		case "type", "name", "identifiers":
		default:
			return generated.SubjectInput{}, fmt.Errorf("subject contains unknown field %q", name)
		}
	}

	var subjectType string
	if err := json.Unmarshal(fields["type"], &subjectType); err != nil {
		return generated.SubjectInput{}, fmt.Errorf("subject type is required")
	}
	if subjectType != "person" && subjectType != "organization" {
		return generated.SubjectInput{}, fmt.Errorf("subject type must be person or organization")
	}

	nameRaw, hasName := fields["name"]
	identifiersRaw, hasIdentifiers := fields["identifiers"]
	if hasName {
		var name string
		if err := json.Unmarshal(nameRaw, &name); err != nil {
			return generated.SubjectInput{}, fmt.Errorf("subject name must be a string")
		}
		nameLength := utf8.RuneCountInString(name)
		if nameLength == 0 || nameLength > maxSubjectNameLength {
			return generated.SubjectInput{}, fmt.Errorf("subject name length must be between 1 and %d", maxSubjectNameLength)
		}
	} else if !hasIdentifiers {
		return generated.SubjectInput{}, fmt.Errorf("subject must contain name or identifiers")
	}

	if hasIdentifiers {
		if err := validateSubjectIdentifiers(identifiersRaw); err != nil {
			return generated.SubjectInput{}, err
		}
	}

	var subject generated.SubjectInput
	if err := json.Unmarshal(raw, &subject); err != nil {
		return generated.SubjectInput{}, fmt.Errorf("decode subject: %w", err)
	}
	return subject, nil
}

func validateSubjectIdentifiers(raw json.RawMessage) error {
	var identifiers map[string]string
	if err := json.Unmarshal(raw, &identifiers); err != nil || identifiers == nil {
		return fmt.Errorf("subject identifiers must be an object")
	}
	if len(identifiers) == 0 {
		return fmt.Errorf("subject identifiers must contain at least one property")
	}
	for name, value := range identifiers {
		if name == "" || value == "" {
			return fmt.Errorf("subject identifier names and values must not be empty")
		}
	}
	return nil
}

func decodeInvestigationLimits(raw json.RawMessage) (*generated.InvestigationLimits, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("limits must be an object")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var limits generated.InvestigationLimits
	if err := decoder.Decode(&limits); err != nil {
		return nil, fmt.Errorf("decode investigation limits: %w", err)
	}
	if err := ensureJSONDocumentEnded(decoder); err != nil {
		return nil, err
	}
	return &limits, nil
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
