package ingestion

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

const (
	testResultJobID         = "22222222-2222-4222-8222-222222222222"
	testResultMessageID     = "33333333-3333-4333-8333-333333333333"
	testResultInvestigation = "44444444-4444-4444-8444-444444444444"
)

type resultValidatorStub struct {
	messageError  error
	manifestError error
}

func (s resultValidatorStub) ValidateResultMessage(raw []byte) (ValidatedResultMessage, error) {
	if s.messageError != nil {
		return ValidatedResultMessage{}, s.messageError
	}
	var envelope struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return ValidatedResultMessage{}, err
	}
	return ValidatedResultMessage{Type: envelope.Type, Payload: envelope.Payload}, nil
}

func (s resultValidatorStub) ValidateResultManifest([]byte) error {
	return s.manifestError
}

type manifestReaderStub struct {
	data      []byte
	err       error
	objectKey string
	maxBytes  int64
	calls     int
}

func (s *manifestReaderStub) Read(
	_ context.Context,
	objectKey string,
	maxBytes int64,
) ([]byte, error) {
	s.calls++
	s.objectKey = objectKey
	s.maxBytes = maxBytes
	return s.data, s.err
}

type resultStoreStub struct {
	command    ingestionResultCommand
	outcome    ResultApplyOutcome
	err        error
	calls      int
	existing   ResultApplyOutcome
	checkError error
}

type ingestionResultCommand = SourceResultCommand

func (s *resultStoreStub) CheckExistingSourceResult(context.Context, string, SourceResultCommand) (ResultApplyOutcome, error) {
	return s.existing, s.checkError
}

func (s *resultStoreStub) ApplySourceResult(
	_ context.Context,
	_ string,
	command SourceResultCommand,
) (ResultApplyOutcome, error) {
	s.calls++
	s.command = command
	return s.outcome, s.err
}

func TestResultConsumerAppliesCompletedPayload(t *testing.T) {
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	result := validNotFoundResult()
	payload := sourceResultPayload(t, &result)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectCompletedType, payload)
	store := &resultStoreStub{outcome: ResultApplyOutcome{
		Applied: true,
		Status:  ResultStatusNotFound,
	}}
	reader := &manifestReaderStub{}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, reader, store, now)

	outcome, err := consumer.Handle(context.Background(), deliveryMetadata(SourceCollectCompletedType), body)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	if outcome.JobID != testResultJobID || outcome.CorrelationID != testResultInvestigation ||
		outcome.AttemptNo != 1 || outcome.Status != ResultStatusNotFound || !outcome.Applied {
		t.Fatalf("unexpected result outcome: %#v", outcome)
	}
	if store.command.Digest != result.Digest || store.command.AdapterVersion != "fixture-adapter/1" ||
		store.command.Status != ResultStatusNotFound || len(store.command.Artifacts) != 1 {
		t.Fatalf("unexpected stored command: %#v", store.command)
	}
	if len(store.command.ProcessingRuns) != 0 || len(store.command.Claims) != 0 {
		t.Fatalf("not_found result unexpectedly contains processing data: %#v", store.command)
	}
	if reader.calls != 0 {
		t.Fatalf("manifest reader calls = %d, want 0", reader.calls)
	}
}

func TestResultConsumerUsesCanonicalInlineNumbers(t *testing.T) {
	result := validNotFoundResult()
	result.Outcome = "succeeded"
	result.RecordCount = 1
	result.Records = []SourceRecord{{
		LocalRef: "record:1", ArtifactLocalRef: result.Artifacts[0].LocalRef,
		RecordType: "person", SchemaVersion: "person/1",
		RawData: map[string]any{"ratio": json.Number("1.0000000000000001")},
	}}
	store := &resultStoreStub{}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, &manifestReaderStub{}, store, time.Now())
	payload := sourceResultPayload(t, &result)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectCompletedType, payload)
	if _, err := consumer.Handle(t.Context(), deliveryMetadata(SourceCollectCompletedType), body); err != nil {
		t.Fatal(err)
	}
	if ratio := store.command.ProcessingRuns[0].Records[0].RawData["ratio"]; ratio != json.Number("1") {
		t.Fatalf("inline record ratio = %#v, want the numeric value represented by the digest", ratio)
	}
}

func TestResultConsumerLoadsAndValidatesManifest(t *testing.T) {
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	manifestBody := []byte(fmt.Sprintf(
		`{"schema_version":1,"job_id":%q,"attempt_no":1,"record_count":1,"records":[{"local_ref":"record:1","artifact_local_ref":"artifact:raw:1","record_type":"person","external_id":null,"schema_version":"person/1","raw_data":{"name":"Example","ratio":1.0000000000000001}}],"claims":[]}`,
		testResultJobID,
	))
	manifestHash := sha256.Sum256(manifestBody)
	result := validNotFoundResult()
	result.Outcome = "succeeded"
	result.RecordCount = 1
	result.Manifest = &ArtifactReference{
		LocalRef:     "artifact:manifest:1",
		Kind:         "manifest",
		ObjectKey:    artifactKey(testResultJobID, 1, "artifact:manifest:1"),
		SHA256:       hex.EncodeToString(manifestHash[:]),
		MediaType:    "application/json",
		ByteSize:     int64(len(manifestBody)),
		SourceURI:    "https://example.test/manifest",
		RetrievedAt:  now,
		HTTPMetadata: map[string]string{},
	}
	payload := sourceResultPayload(t, &result)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectCompletedType, payload)
	store := &resultStoreStub{outcome: ResultApplyOutcome{
		Applied: true,
		Status:  ResultStatusSucceeded,
	}}
	reader := &manifestReaderStub{data: manifestBody}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, reader, store, now)

	if _, err := consumer.Handle(context.Background(), deliveryMetadata(SourceCollectCompletedType), body); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if reader.calls != 1 || reader.objectKey != result.Manifest.ObjectKey ||
		reader.maxBytes != 1024 {
		t.Fatalf("manifest read = %d calls, key %q, limit %d", reader.calls, reader.objectKey, reader.maxBytes)
	}
	if len(store.command.Artifacts) != 2 || len(store.command.ProcessingRuns) != 1 ||
		len(store.command.ProcessingRuns[0].Records) != 1 {
		t.Fatalf("manifest content was not prepared for storage: %#v", store.command)
	}
	ratio := store.command.ProcessingRuns[0].Records[0].RawData["ratio"]
	if ratio != json.Number("1.0000000000000001") {
		t.Fatalf("manifest record ratio = %#v", ratio)
	}
	store.existing = ResultApplyOutcome{Duplicate: true}
	reader.err = errors.New("manifest store unavailable")
	outcome, err := consumer.Handle(t.Context(), deliveryMetadata(SourceCollectCompletedType), body)
	if err != nil || !outcome.Duplicate || reader.calls != 1 || store.calls != 1 {
		t.Fatalf("duplicate manifest: outcome = %#v, reads = %d, writes = %d, error = %v", outcome, reader.calls, store.calls, err)
	}
	store.checkError = NewPermanentResultErrorWithCode("source_result_attempt_digest_conflict", nil)
	_, err = consumer.Handle(t.Context(), deliveryMetadata(SourceCollectCompletedType), body)
	if PermanentResultErrorCode(err) != "source_result_attempt_digest_conflict" || reader.calls != 1 || store.calls != 1 {
		t.Fatalf("conflicting manifest: reads = %d, writes = %d, error = %v", reader.calls, store.calls, err)
	}
}

func TestResultConsumerComputesFailedDigestAndRetryDeadline(t *testing.T) {
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	retryAfter := 60
	payload, err := json.Marshal(SourceCollectFailedPayload{
		JobID:          testResultJobID,
		AttemptNo:      1,
		AdapterVersion: "fixture-adapter/1",
		Artifacts:      []ArtifactReference{validArtifact()},
		Error: SourceCollectError{
			Code:              "SOURCE_RATE_LIMITED",
			Classification:    "rate_limited",
			Message:           "slow down",
			RetryAfterSeconds: &retryAfter,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte(`"attempt_no":1`), []byte(`"attempt_no":1e0`), 1)
	payload = bytes.Replace(payload, []byte(`"retry_after_seconds":60`), []byte(`"retry_after_seconds":6e1`), 1)
	payload = bytes.Replace(payload, []byte(`"message":"slow down"`), []byte(`"message":"\u0000"`), 1)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectFailedType, payload)
	store := &resultStoreStub{outcome: ResultApplyOutcome{Applied: true, Status: ResultStatusRetryWait}}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, &manifestReaderStub{}, store, now)

	if _, err := consumer.Handle(context.Background(), deliveryMetadata(SourceCollectFailedType), body); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	expectedDigest, err := canonicalPayloadDigest(payload)
	if err != nil {
		t.Fatal(err)
	}
	if store.command.Digest != expectedDigest {
		t.Fatalf("failed result digest = %q, want %q", store.command.Digest, expectedDigest)
	}
	if store.command.Status != ResultStatusRetryWait ||
		!store.command.NextAttemptAt.Equal(now.Add(60*time.Second)) {
		t.Fatalf("retry deadline = %s status %s", store.command.NextAttemptAt, store.command.Status)
	}
	if store.command.ErrorMessage != "source adapter failed" {
		t.Fatalf("persisted failure message = %q", store.command.ErrorMessage)
	}
}

func TestResultConsumerDoesNotStorePermanentInvalidMessages(t *testing.T) {
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	result := validNotFoundResult()
	payload := sourceResultPayload(t, &result)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectCompletedType, payload)
	store := &resultStoreStub{}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, &manifestReaderStub{}, store, now)

	metadata := deliveryMetadata(SourceCollectCompletedType)
	metadata.MessageID = "55555555-5555-4555-8555-555555555555"
	_, err := consumer.Handle(context.Background(), metadata, body)
	if !IsPermanentResultError(err) {
		t.Fatalf("Handle() error = %v, want permanent result error", err)
	}
	if store.calls != 0 {
		t.Fatalf("store calls = %d, want 0", store.calls)
	}
}

func TestResultConsumerClassifiesFailedResults(t *testing.T) {
	tests := []struct {
		classification string
		wantStatus     ResultStatus
	}{
		{"transient", ResultStatusRetryWait},
		{"rate_limited", ResultStatusRetryWait},
		{"action_required", ResultStatusActionRequired},
		{"permanent", ResultStatusFailed},
		{"bug", ResultStatusFailed},
	}
	for _, test := range tests {
		t.Run(test.classification, func(t *testing.T) {
			got, err := failedResultStatus(test.classification)
			if err != nil || got != test.wantStatus {
				t.Fatalf("failedResultStatus() = %q, %v; want %q", got, err, test.wantStatus)
			}
		})
	}
}

func newResultConsumerForTest(
	t *testing.T,
	validator ResultMessageValidator,
	reader ResultManifestReader,
	store ResultStore,
	now time.Time,
) *ResultConsumer {
	t.Helper()
	consumer, err := NewResultConsumer(validator, reader, store, ResultConsumerConfig{
		MaxMessageBytes:  1 << 20,
		MaxManifestBytes: 1024,
		Now:              func() time.Time { return now },
		Jitter:           func(value time.Duration) time.Duration { return value },
	})
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func validNotFoundResult() SourceResult {
	return SourceResult{
		JobID:                testResultJobID,
		AttemptNo:            1,
		Outcome:              "not_found",
		RecordCount:          0,
		AdapterVersion:       "fixture-adapter/1",
		ParserVersion:        "fixture-parser/1",
		MappingVersion:       "fixture-mapper/1",
		NormalizationVersion: "normalization/1",
		Artifacts:            []ArtifactReference{validArtifact()},
		Records:              []SourceRecord{},
		Claims:               []Claim{},
		Warnings:             []string{},
	}
}

func validArtifact() ArtifactReference {
	return ArtifactReference{
		LocalRef:     "artifact:raw:1",
		Kind:         "raw_response",
		ObjectKey:    artifactKey(testResultJobID, 1, "artifact:raw:1"),
		SHA256:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		MediaType:    "application/json",
		ByteSize:     2,
		SourceURI:    "https://example.test/source/1",
		RetrievedAt:  time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC),
		HTTPMetadata: map[string]string{},
	}
}

func artifactKey(jobID string, attemptNo int, localRef string) string {
	encoded := "YXJ0aWZhY3Q6cmF3OjE"
	if localRef == "artifact:manifest:1" {
		encoded = "YXJ0aWZhY3Q6bWFuaWZlc3Q6MQ"
	}
	return fmt.Sprintf("artifacts/v1/%s/%d/%s", jobID, attemptNo, encoded)
}

func resultEnvelopeBytes(t *testing.T, messageID, messageType string, payload []byte) []byte {
	t.Helper()
	return []byte(fmt.Sprintf(
		`{"message_id":%q,"type":%q,"schema_version":1,"occurred_at":"2026-11-20T00:00:00Z","correlation_id":%q,"causation_id":null,"producer":"graphx-worker","root_command_id":"66666666-6666-4666-8666-666666666666","payload":%s}`,
		messageID, messageType, testResultInvestigation, payload,
	))
}

func deliveryMetadata(messageType string) ResultDeliveryMetadata {
	return ResultDeliveryMetadata{
		MessageID:  testResultMessageID,
		Type:       messageType,
		RoutingKey: messageType,
	}
}

func TestResultConsumerPropagatesTransientManifestReadFailure(t *testing.T) {
	now := time.Date(2026, time.November, 20, 0, 0, 0, 0, time.UTC)
	result := validNotFoundResult()
	result.Outcome = "succeeded"
	result.RecordCount = 1
	result.Records = []SourceRecord{{
		LocalRef: "record:1", ArtifactLocalRef: "artifact:raw:1",
		RecordType: "person", SchemaVersion: "person/1",
		RawData: map[string]any{"name": "Example"},
	}}
	result.Manifest = &ArtifactReference{
		LocalRef:     "artifact:manifest:1",
		Kind:         "manifest",
		ObjectKey:    artifactKey(testResultJobID, 1, "artifact:manifest:1"),
		SHA256:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		MediaType:    "application/json",
		ByteSize:     2,
		SourceURI:    "https://example.test/manifest",
		RetrievedAt:  now,
		HTTPMetadata: map[string]string{},
	}
	result.Claims = []Claim{}
	result.Records = []SourceRecord{}
	payload := sourceResultPayload(t, &result)
	body := resultEnvelopeBytes(t, testResultMessageID, SourceCollectCompletedType, payload)
	store := &resultStoreStub{}
	reader := &manifestReaderStub{err: errors.New("temporary object storage failure")}
	consumer := newResultConsumerForTest(t, resultValidatorStub{}, reader, store, now)

	_, err := consumer.Handle(context.Background(), deliveryMetadata(SourceCollectCompletedType), body)
	if err == nil || IsPermanentResultError(err) {
		t.Fatalf("Handle() error = %v, want retryable object storage error", err)
	}
	if store.calls != 0 {
		t.Fatalf("store calls = %d, want 0", store.calls)
	}
}
