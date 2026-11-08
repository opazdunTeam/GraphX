package middlewares

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRequestIDAndAccessLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	router := gin.New()
	router.Use(RequestIDMiddleware(), LoggerMiddleware(logger), RecoverMiddleware(logger))
	router.GET("/items/:item_id", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/items/sensitive-value", nil)
	request.Header.Set("X-Request-ID", "request-123")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if response.Header().Get("X-Request-ID") != "request-123" {
		t.Errorf("X-Request-ID = %q, want request-123", response.Header().Get("X-Request-ID"))
	}

	var entry map[string]any
	if err := json.Unmarshal(logs.Bytes(), &entry); err != nil {
		t.Fatalf("decode access log: %v", err)
	}
	if entry["request_id"] != "request-123" {
		t.Errorf("request_id = %v, want request-123", entry["request_id"])
	}
	if entry["path"] != "/items/:item_id" {
		t.Errorf("path = %v, want route template", entry["path"])
	}
	if strings.Contains(logs.String(), "sensitive-value") {
		t.Errorf("access log contains path parameter: %s", logs.String())
	}
}

func TestInvalidRequestIDIsReplaced(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestIDMiddleware())
	var handlerHeader string
	var handlerContextID string
	router.GET("/", func(c *gin.Context) {
		handlerHeader = c.GetHeader("X-Request-ID")
		handlerContextID = GetRequestID(c.Request.Context())
		c.Status(http.StatusNoContent)
	})

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("X-Request-ID", "invalid request id")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	requestID := response.Header().Get("X-Request-ID")
	if !regexp.MustCompile(`^[0-9a-f-]{36}$`).MatchString(requestID) {
		t.Errorf("X-Request-ID = %q, want generated UUID", requestID)
	}
	if handlerHeader != requestID || handlerContextID != requestID {
		t.Errorf(
			"request IDs = header %q, context %q, response %q; want one sanitized value",
			handlerHeader,
			handlerContextID,
			requestID,
		)
	}
}

func TestRecoveryDoesNotExposePanic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	router := gin.New()
	router.Use(RequestIDMiddleware(), LoggerMiddleware(logger), RecoverMiddleware(logger))
	router.GET("/panic", func(*gin.Context) { panic("sensitive panic detail") })

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/panic", nil))

	if response.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusInternalServerError)
	}
	if !strings.HasPrefix(response.Header().Get("Content-Type"), "application/problem+json") {
		t.Errorf("Content-Type = %q, want application/problem+json", response.Header().Get("Content-Type"))
	}
	if strings.Contains(response.Body.String(), "sensitive panic detail") {
		t.Errorf("response exposes panic: %s", response.Body.String())
	}
	if strings.Contains(logs.String(), "sensitive panic detail") {
		t.Errorf("log exposes panic value: %s", logs.String())
	}
	if !strings.Contains(logs.String(), `"panic_type":"string"`) {
		t.Errorf("log does not contain safe panic type: %s", logs.String())
	}
}
