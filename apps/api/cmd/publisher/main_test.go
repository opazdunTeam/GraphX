package main

import (
	"errors"
	"testing"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
)

func TestPublisherLogFieldsContainOnlyStructuredOperationalContext(t *testing.T) {
	result := outbox.PublishResult{
		Processed:       true,
		MessageID:       "message-id",
		JobID:           "job-id",
		InvestigationID: "investigation-id",
		CorrelationID:   "correlation-id",
		RetryAfter:      time.Second,
	}
	fields := publisherLogFields(result, "delivery_failed", publisherMetrics{
		published: 2,
		deferred:  3,
		failed:    4,
	})
	values := make(map[string]any, len(fields)/2)
	for index := 0; index < len(fields); index += 2 {
		key, ok := fields[index].(string)
		if !ok {
			t.Fatalf("log key %#v is not a string", fields[index])
		}
		values[key] = fields[index+1]
	}
	if values["message_id"] != result.MessageID || values["job_id"] != result.JobID ||
		values["investigation_id"] != result.InvestigationID ||
		values["correlation_id"] != result.CorrelationID ||
		values["outcome"] != "delivery_failed" {
		t.Fatalf("unexpected publisher log context: %#v", values)
	}
	if _, exists := values["error"]; exists {
		t.Fatal("publisher operational log must not include an untrusted raw error")
	}
}

func TestPublisherCloseLogFieldsDoNotRenderBrokerError(t *testing.T) {
	fields := publisherCloseLogFields()
	if len(fields) != 2 || fields[0] != "outcome" || fields[1] != "close_failed" {
		t.Fatalf("unexpected close log fields: %#v", fields)
	}
	for _, field := range fields {
		if field == "error" || field == "private broker marker" {
			t.Fatalf("unsafe close log field: %#v", field)
		}
	}
}

func TestDeferredReasonClassifiesLeaseLossWithoutRenderingCause(t *testing.T) {
	if got := deferredReason(&outbox.DeferredError{Cause: outbox.ErrLeaseLost}); got != "lease_lost" {
		t.Fatalf("deferredReason(lease lost) = %q", got)
	}
	if got := deferredReason(errors.New("payload marker must stay private")); got != "delivery_failed" {
		t.Fatalf("deferredReason(other) = %q", got)
	}
}
