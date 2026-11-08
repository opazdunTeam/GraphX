package handlers

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func createInvestigationContext(body string, contentType string) *gin.Context {
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/investigations",
		strings.NewReader(body),
	)
	if contentType != "" {
		context.Request.Header.Set("Content-Type", contentType)
	}
	return context
}

func TestBindCreateInvestigationRequestAppliesConfiguredDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context := createInvestigationContext(
		`{"subject":{"type":"person","name":"Ada"}}`,
		"application/json; charset=utf-8",
	)

	input, err := investigationHandler().bindCreateInvestigationRequest(context)
	if err != nil {
		t.Fatalf("bindCreateInvestigationRequest() error = %v", err)
	}
	if input.Limits.Depth != 1 || input.Limits.EntityLimit != 50 {
		t.Fatalf("limits = %#v, want configured defaults", input.Limits)
	}
	named, err := input.Subject.AsNamedSubjectInput()
	if err != nil || named.Name != "Ada" {
		t.Fatalf("subject = %#v, error = %v; want named Ada", named, err)
	}
}

func TestBindCreateInvestigationRequestAcceptsIdentifiedSubject(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context := createInvestigationContext(
		`{"subject":{"type":"organization","identifiers":{"registration_number":"42"}}}`,
		"application/json",
	)

	input, err := investigationHandler().bindCreateInvestigationRequest(context)
	if err != nil {
		t.Fatalf("bindCreateInvestigationRequest() error = %v", err)
	}
	identified, err := input.Subject.AsIdentifiedSubjectInput()
	if err != nil || identified.Identifiers["registration_number"] != "42" {
		t.Fatalf("subject = %#v, error = %v; want identified organization", identified, err)
	}
}

func TestBindCreateInvestigationRequestRejectsContractViolations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		body string
	}{
		{name: "missing subject", body: `{}`},
		{name: "null subject", body: `{"subject":null}`},
		{name: "scalar subject", body: `{"subject":"Ada"}`},
		{name: "missing type", body: `{"subject":{"name":"Ada"}}`},
		{name: "unknown type", body: `{"subject":{"type":"company","name":"Ada"}}`},
		{name: "missing name and identifiers", body: `{"subject":{"type":"person"}}`},
		{name: "empty name", body: `{"subject":{"type":"person","name":""}}`},
		{
			name: "name too long",
			body: fmt.Sprintf(`{"subject":{"type":"person","name":"%s"}}`, strings.Repeat("я", 501)),
		},
		{name: "null optional identifiers", body: `{"subject":{"type":"person","name":"Ada","identifiers":null}}`},
		{name: "empty identifiers", body: `{"subject":{"type":"person","identifiers":{}}}`},
		{name: "empty identifier name", body: `{"subject":{"type":"person","identifiers":{"":"42"}}}`},
		{name: "empty identifier value", body: `{"subject":{"type":"person","identifiers":{"id":""}}}`},
		{name: "unknown subject field", body: `{"subject":{"type":"person","name":"Ada","unknown":true}}`},
		{name: "unknown top-level field", body: `{"subject":{"type":"person","name":"Ada"},"unknown":true}`},
		{name: "null limits", body: `{"subject":{"type":"person","name":"Ada"},"limits":null}`},
		{name: "unknown limits field", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"unknown":true}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			context := createInvestigationContext(tt.body, "application/json")
			if _, err := investigationHandler().bindCreateInvestigationRequest(context); err == nil {
				t.Fatal("bindCreateInvestigationRequest() error = nil, want validation error")
			}
		})
	}
}

func TestBindCreateInvestigationRequestRequiresJSONContentType(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context := createInvestigationContext(`{"subject":{"type":"person","name":"Ada"}}`, "text/plain")

	_, err := investigationHandler().bindCreateInvestigationRequest(context)
	if !errors.Is(err, errUnsupportedMediaType) {
		t.Fatalf("bindCreateInvestigationRequest() error = %v, want unsupported media type", err)
	}
}
