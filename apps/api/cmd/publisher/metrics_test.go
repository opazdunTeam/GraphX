package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

func TestPublisherMetricsExposePublishOutcomesAndDurations(t *testing.T) {
	metrics, err := newPublisherMetrics()
	if err != nil {
		t.Fatal(err)
	}
	result := outbox.PublishResult{
		MessageID:       "30000000-0000-4000-8000-000000000001",
		JobID:           "20000000-0000-4000-8000-000000000001",
		InvestigationID: "10000000-0000-4000-8000-000000000001",
	}
	metrics.record("published", 250*time.Millisecond, result)
	metrics.record("deferred", time.Second, result)
	metrics.record("failed", 2*time.Second, result)

	server, serveErrors, err := startPublisherMetricsServer("127.0.0.1:0", metrics)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("shutdown metrics server: %v", err)
		}
	})

	client := &http.Client{Timeout: time.Second}
	request, err := http.NewRequest(http.MethodGet, "http://"+server.Addr+"/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Accept", "application/openmetrics-text; version=1.0.0")
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	body, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		t.Fatalf("read metrics response: %v", readErr)
	}
	if closeErr != nil {
		t.Fatalf("close metrics response: %v", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	text := string(body)
	for _, expected := range []string{
		`graphx_outbox_publish_total{outcome="published"} 1`,
		`graphx_outbox_publish_total{outcome="deferred"} 1`,
		`graphx_outbox_publish_total{outcome="failed"} 1`,
		`graphx_outbox_publish_duration_seconds_count{outcome="published"} 1`,
		`graphx_outbox_publish_duration_seconds_count{outcome="deferred"} 1`,
		`graphx_outbox_publish_duration_seconds_count{outcome="failed"} 1`,
		`inv_id="10000000-0000-4000-8000-000000000001"`,
		`job_id="20000000-0000-4000-8000-000000000001"`,
		`msg_id="30000000-0000-4000-8000-000000000001"`,
	} {
		if !strings.Contains(text, expected) {
			t.Errorf("metrics response does not contain %q", expected)
		}
	}

	rootResponse, err := client.Get("http://" + server.Addr + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	if err := rootResponse.Body.Close(); err != nil {
		t.Fatalf("close root response: %v", err)
	}
	if rootResponse.StatusCode != http.StatusNotFound {
		t.Fatalf("root status = %d, want %d", rootResponse.StatusCode, http.StatusNotFound)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("shutdown metrics server: %v", err)
	}
	if serveErr := <-serveErrors; !errors.Is(serveErr, http.ErrServerClosed) {
		t.Fatalf("metrics server error = %v, want http.ErrServerClosed", serveErr)
	}
}
