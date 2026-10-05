package httpserver

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthEndpoints(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(logger)

	for _, path := range []string{"/health/live", "/health/ready"} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, path, nil)
		router.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d", path, recorder.Code)
		}
		if recorder.Header().Get("X-Request-ID") == "" {
			t.Fatalf("GET %s did not return X-Request-ID", path)
		}
	}
}
