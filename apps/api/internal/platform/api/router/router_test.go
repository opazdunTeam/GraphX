package router

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
)

type readinessStub struct {
	err    error
	called bool
}

func (s *readinessStub) Ping(context.Context) error {
	s.called = true
	return s.err
}

func testRouter(checker *readinessStub) http.Handler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	handler := handlers.New(checker, 50*time.Millisecond)
	return NewRouter(logger, handler)
}

func TestLiveness(t *testing.T) {
	checker := &readinessStub{err: errors.New("must not be called")}
	router := testRouter(checker)

	request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	request.Header.Set("X-Request-ID", "request-123")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if checker.called {
		t.Fatal("liveness called the readiness dependency")
	}
	if got := response.Header().Get("X-Request-ID"); got != "request-123" {
		t.Errorf("X-Request-ID = %q, want request-123", got)
	}
	if got := response.Body.String(); got != `{"status":"ok"}` {
		t.Errorf("body = %s, want liveness response", got)
	}
}

func TestReadiness(t *testing.T) {
	checker := &readinessStub{}
	router := testRouter(checker)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if !checker.called {
		t.Fatal("readiness did not check PostgreSQL")
	}
	if !strings.Contains(response.Body.String(), `"postgres":"ready"`) {
		t.Errorf("body = %s, want ready PostgreSQL check", response.Body.String())
	}
}

func TestReadinessUnavailable(t *testing.T) {
	checker := &readinessStub{err: errors.New("database unavailable")}
	router := testRouter(checker)
	response := httptest.NewRecorder()

	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/problem+json") {
		t.Errorf("Content-Type = %q, want application/problem+json", contentType)
	}

	var problem struct {
		Code   string `json:"code"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &problem); err != nil {
		t.Fatalf("decode problem response: %v", err)
	}
	if problem.Code != "SERVICE_UNAVAILABLE" {
		t.Errorf("problem code = %q, want SERVICE_UNAVAILABLE", problem.Code)
	}
	if strings.Contains(problem.Detail, "database unavailable") {
		t.Errorf("problem detail leaks dependency error: %q", problem.Detail)
	}
}

func TestRoutes(t *testing.T) {
	router := testRouter(&readinessStub{})
	tests := []struct {
		method string
		path   string
	}{
		{method: http.MethodPost, path: "/api/v1/investigations"},
		{method: http.MethodGet, path: "/api/v1/investigations/inv-1"},
		{method: http.MethodGet, path: "/api/v1/investigations/inv-1/status"},
		{method: http.MethodPost, path: "/api/v1/investigations/inv-1/cancel"},
		{method: http.MethodGet, path: "/api/v1/claims/claim-1/evidence"},
		{method: http.MethodGet, path: "/api/v1/artifacts/artifact-1/content"},
		{method: http.MethodGet, path: "/api/v1/sources"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, nil)
			router.ServeHTTP(response, request)

			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNotImplemented)
			}
		})
	}
}

func TestServiceRoutesDoNotUseBusinessPrefix(t *testing.T) {
	router := testRouter(&readinessStub{})
	response := httptest.NewRecorder()

	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/health/live", nil))

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNotFound)
	}
}
