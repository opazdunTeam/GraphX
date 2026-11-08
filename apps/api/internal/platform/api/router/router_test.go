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
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
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
	handler := handlers.New(checker, 50*time.Millisecond, config.Investigation{
		DefaultDepth: 1, MaxDepth: 2, DefaultEntityLimit: 50, MaxEntityLimit: 100,
	})
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
	const id = "00000000-0000-4000-8000-000000000001"
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{
			method: http.MethodPost,
			path:   "/api/v1/investigations",
			body:   `{"subject":{"type":"person","name":"Ada"}}`,
		},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id + "/status"},
		{method: http.MethodPost, path: "/api/v1/investigations/" + id + "/cancel"},
		{method: http.MethodGet, path: "/api/v1/sources"},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id + "/reviews"},
		{method: http.MethodGet, path: "/api/v1/reviews/" + id},
		{method: http.MethodPost, path: "/api/v1/reviews/" + id + "/decision"},
		{method: http.MethodPost, path: "/api/v1/reviews/" + id + "/decision/revert"},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id + "/entities"},
		{method: http.MethodGet, path: "/api/v1/entities/" + id},
		{method: http.MethodPost, path: "/api/v1/entities/" + id + "/expand"},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id + "/graph"},
		{method: http.MethodGet, path: "/api/v1/investigations/" + id + "/timeline"},
		{method: http.MethodGet, path: "/api/v1/relationships/" + id},
		{method: http.MethodGet, path: "/api/v1/claims/" + id + "/evidence"},
		{method: http.MethodGet, path: "/api/v1/artifacts/" + id + "/content"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			response := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Idempotency-Key", id)
			if tt.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			router.ServeHTTP(response, request)

			if response.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusNotImplemented)
			}
		})
	}
}

func TestCreateInvestigationRejectsInvalidLimits(t *testing.T) {
	router := testRouter(&readinessStub{})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":"Ada"},"limits":{"sources":[]}}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "00000000-0000-4000-8000-000000000001")
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if !strings.Contains(response.Body.String(), `"code":"INVALID_REQUEST"`) {
		t.Errorf("body = %s, want invalid request problem", response.Body.String())
	}
}

func TestCreateInvestigationRejectsUnsupportedMediaType(t *testing.T) {
	router := testRouter(&readinessStub{})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/investigations",
		strings.NewReader(`{"subject":{"type":"person","name":"Ada"}}`),
	)
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Idempotency-Key", "00000000-0000-4000-8000-000000000001")
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnsupportedMediaType)
	}
	if !strings.Contains(response.Body.String(), `"code":"UNSUPPORTED_MEDIA_TYPE"`) {
		t.Errorf("body = %s, want unsupported media type problem", response.Body.String())
	}
}

func TestCreateInvestigationRejectsOversizedBody(t *testing.T) {
	router := testRouter(&readinessStub{})
	response := httptest.NewRecorder()
	body := `{"subject":{"type":"person","name":"` +
		strings.Repeat("a", 1<<20) +
		`"}}`
	request := httptest.NewRequest(http.MethodPost, "/api/v1/investigations", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", "00000000-0000-4000-8000-000000000001")
	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	if !strings.Contains(response.Body.String(), `"code":"REQUEST_TOO_LARGE"`) {
		t.Errorf("body = %s, want request too large problem", response.Body.String())
	}
}

func TestGeneratedRouteRejectsInvalidUUID(t *testing.T) {
	router := testRouter(&readinessStub{})
	response := httptest.NewRecorder()

	router.ServeHTTP(
		response,
		httptest.NewRequest(http.MethodGet, "/api/v1/investigations/not-a-uuid", nil),
	)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "application/problem+json") {
		t.Errorf("Content-Type = %q, want application/problem+json", contentType)
	}
	if !strings.Contains(response.Body.String(), `"code":"INVALID_REQUEST"`) {
		t.Errorf("body = %s, want generated adapter problem code", response.Body.String())
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
