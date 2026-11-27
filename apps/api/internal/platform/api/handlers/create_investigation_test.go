package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
)

type fakeInvestigationCreator struct {
	accepted investigations.Accepted
	err      error
	command  investigations.CreateCommand
}

func (f *fakeInvestigationCreator) Create(
	_ context.Context,
	command investigations.CreateCommand,
) (investigations.Accepted, error) {
	f.command = command
	return f.accepted, f.err
}

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
		{name: "blank name", body: `{"subject":{"type":"person","name":"  "}}`},
		{name: "null character", body: `{"subject":{"type":"person","name":"A\u0000"}}`},
		{name: "case alias subject", body: `{"Subject":{"type":"person","name":"Ada"}}`},
		{name: "case alias limit", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"Depth":1}}`},
		{name: "null depth", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"depth":null}}`},
		{name: "null entity limit", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"entity_limit":null}}`},
		{name: "null sources", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"sources":null}}`},
		{name: "null character source", body: `{"subject":{"type":"person","name":"Ada"},"limits":{"sources":["fixture\u0000"]}}`},
		{name: "blank identifier", body: `{"subject":{"type":"person","identifiers":{"id":"  "}}}`},
		{name: "normalized identifier conflict", body: `{"subject":{"type":"person","identifiers":{"ID":"1","id":"2"}}}`},
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

func TestSubjectFromGeneratedPreservesIdentifierOnlyInput(t *testing.T) {
	context := createInvestigationContext(`{"subject":{"type":"organization","identifiers":{"registration_number":"42"}}}`, "application/json")
	input, err := investigationHandler().bindCreateInvestigationRequest(context)
	if err != nil {
		t.Fatal(err)
	}
	subject, err := subjectFromGenerated(input.Subject)
	if err != nil {
		t.Fatal(err)
	}
	if subject.Name != nil || subject.Identifiers["registration_number"] != "42" {
		t.Fatalf("subject = %#v", subject)
	}
	if _, err := investigations.NormalizeSubject(subject); err != nil {
		t.Fatalf("NormalizeSubject() error = %v", err)
	}
}

func TestCreateInvestigationAcceptsIntegralNumberRepresentations(t *testing.T) {
	for _, number := range []string{"1", "1.0", "1e0"} {
		context := createInvestigationContext(`{"subject":{"type":"person","name":"Ada"},"limits":{"depth":`+number+`}}`, "application/json")
		input, err := investigationHandler().bindCreateInvestigationRequest(context)
		if err != nil || input.Limits.Depth != 1 {
			t.Fatalf("depth %s: input = %#v, error = %v", number, input, err)
		}
	}
}

func TestCreateInvestigationReturnsAcceptedAndStoresNormalizedCommand(t *testing.T) {
	gin.SetMode(gin.TestMode)
	creator := &fakeInvestigationCreator{
		accepted: investigations.Accepted{
			ID:        "00000000-0000-4000-8000-000000000099",
			StatusURL: "/api/v1/investigations/00000000-0000-4000-8000-000000000099/status",
		},
	}
	handler := investigationHandler()
	handler.create = creator
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":" Ada "},"limits":{"depth":1,"entity_limit":50,"sources":["fixture"]}}`),
	)
	context.Request.Header.Set("Content-Type", "application/json")
	key := uuid.MustParse("00000000-0000-4000-8000-000000000098")
	handler.CreateInvestigation(context, generated.CreateInvestigationParams{IdempotencyKey: key})

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	if recorder.Header().Get("Location") != creator.accepted.StatusURL {
		t.Fatalf("Location = %q, want %q", recorder.Header().Get("Location"), creator.accepted.StatusURL)
	}
	if creator.command.IdempotencyKey != key.String() || creator.command.RequestHash == "" {
		t.Fatalf("command = %#v, want key and request hash", creator.command)
	}
	if creator.command.Subject.Name == nil || *creator.command.Subject.Name != " Ada " {
		t.Fatalf("subject name = %v, want raw handler subject", creator.command.Subject.Name)
	}
}

func TestCreateInvestigationMapsIdempotencyConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := investigationHandler()
	handler.create = &fakeInvestigationCreator{
		err: &investigations.IdempotencyConflictError{
			Key: "00000000-0000-4000-8000-000000000098",
		},
	}
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(
		http.MethodPost,
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":"Ada"}}`),
	)
	context.Request.Header.Set("Content-Type", "application/json")
	key := uuid.MustParse("00000000-0000-4000-8000-000000000098")
	handler.CreateInvestigation(context, generated.CreateInvestigationParams{IdempotencyKey: key})

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"code":"IDEMPOTENCY_CONFLICT"`) {
		t.Fatalf("body = %s, want idempotency conflict", recorder.Body.String())
	}
}
