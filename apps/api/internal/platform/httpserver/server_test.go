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

func TestRequestID(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	router := NewRouter(logger)

	t.Run("keeps valid client value", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		request.Header.Set("X-Request-ID", "web-01:request_42")
		router.ServeHTTP(recorder, request)

		if got := recorder.Header().Get("X-Request-ID"); got != "web-01:request_42" {
			t.Fatalf("unexpected X-Request-ID: %q", got)
		}
	})

	t.Run("replaces unsafe client value", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		request.Header.Set("X-Request-ID", "unsafe value")
		router.ServeHTTP(recorder, request)

		got := recorder.Header().Get("X-Request-ID")
		if got == "" || got == "unsafe value" {
			t.Fatalf("unsafe X-Request-ID was not replaced: %q", got)
		}
	})
}
