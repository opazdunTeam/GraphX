package handlers

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestBindCreateInvestigationRequestAppliesConfiguredDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(
		"POST",
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":"Ada"}}`),
	)

	input, err := investigationHandler().bindCreateInvestigationRequest(context)
	if err != nil {
		t.Fatalf("bindCreateInvestigationRequest() error = %v", err)
	}
	if input.Limits.Depth != 1 || input.Limits.EntityLimit != 50 {
		t.Fatalf("limits = %#v, want configured defaults", input.Limits)
	}
}

func TestBindCreateInvestigationRequestRejectsUnknownTopLevelField(t *testing.T) {
	gin.SetMode(gin.TestMode)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = httptest.NewRequest(
		"POST",
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":"Ada"},"unknown":true}`),
	)

	if _, err := investigationHandler().bindCreateInvestigationRequest(context); err == nil {
		t.Fatal("bindCreateInvestigationRequest() error = nil, want validation error")
	}
}
