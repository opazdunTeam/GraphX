package ingestion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

type ArtifactReference struct {
	LocalRef         string            `json:"local_ref"`
	Kind             string            `json:"kind"`
	ObjectKey        string            `json:"object_key"`
	SHA256           string            `json:"sha256"`
	MediaType        string            `json:"media_type"`
	ByteSize         int64             `json:"byte_size"`
	SourceURI        string            `json:"source_uri"`
	RetrievedAt      time.Time         `json:"retrieved_at"`
	PublishedAt      *time.Time        `json:"published_at"`
	HTTPMetadata     map[string]string `json:"http_metadata"`
	OriginalFilename *string           `json:"original_filename,omitempty"`
	ParentLocalRef   *string           `json:"parent_local_ref,omitempty"`
}

func (artifact *ArtifactReference) UnmarshalJSON(data []byte) error {
	type alias ArtifactReference
	decoded := struct {
		ByteSize    json.RawMessage `json:"byte_size"`
		RetrievedAt wireTimestamp   `json:"retrieved_at"`
		PublishedAt *wireTimestamp  `json:"published_at"`
		*alias
	}{alias: (*alias)(artifact)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.ByteSize != nil {
		size, err := decodeExactJSONInteger(decoded.ByteSize)
		if err != nil {
			return fmt.Errorf("decode byte_size: %w", err)
		}
		artifact.ByteSize = size
	}
	artifact.RetrievedAt = time.Time(decoded.RetrievedAt)
	artifact.PublishedAt = (*time.Time)(decoded.PublishedAt)
	return nil
}

type Claim struct {
	ClaimKind          string         `json:"claim_kind"`
	SubjectLocalRef    string         `json:"subject_local_ref"`
	Predicate          string         `json:"predicate"`
	ObjectLocalRef     *string        `json:"object_local_ref"`
	RawValue           any            `json:"raw_value"`
	NormalizedValue    any            `json:"normalized_value"`
	Qualifiers         map[string]any `json:"qualifiers"`
	ValidFrom          *string        `json:"valid_from"`
	ValidTo            *string        `json:"valid_to"`
	ValidFromPrecision string         `json:"valid_from_precision"`
	ValidToPrecision   string         `json:"valid_to_precision"`
	TemporalBasis      string         `json:"temporal_basis"`
	SnapshotAt         *time.Time     `json:"snapshot_at"`
	EvidenceLocator    map[string]any `json:"evidence_locator"`
	EvidenceExcerpt    *string        `json:"evidence_excerpt"`
}

func (claim *Claim) UnmarshalJSON(data []byte) error {
	type alias Claim
	decoded := struct {
		SnapshotAt *wireTimestamp `json:"snapshot_at"`
		*alias
	}{alias: (*alias)(claim)}
	if err := decodeJSON(data, &decoded); err != nil {
		return err
	}
	claim.SnapshotAt = (*time.Time)(decoded.SnapshotAt)
	return nil
}

type SourceRecord struct {
	LocalRef         string         `json:"local_ref"`
	ArtifactLocalRef string         `json:"artifact_local_ref"`
	RecordType       string         `json:"record_type"`
	ExternalID       *string        `json:"external_id"`
	SchemaVersion    string         `json:"schema_version"`
	RawData          map[string]any `json:"raw_data"`
}

type SourceResult struct {
	JobID                string              `json:"job_id"`
	AttemptNo            int                 `json:"attempt_no"`
	Outcome              string              `json:"outcome"`
	RecordCount          int                 `json:"record_count"`
	AdapterVersion       string              `json:"adapter_version"`
	ParserVersion        string              `json:"parser_version"`
	MappingVersion       string              `json:"mapping_version"`
	NormalizationVersion string              `json:"normalization_version"`
	Digest               string              `json:"digest"`
	Artifacts            []ArtifactReference `json:"artifacts"`
	Manifest             *ArtifactReference  `json:"manifest"`
	Records              []SourceRecord      `json:"records"`
	Claims               []Claim             `json:"claims"`
	Warnings             []string            `json:"warnings"`
}

func (result *SourceResult) UnmarshalJSON(data []byte) error {
	type sourceResultAlias SourceResult
	decoded := struct {
		AttemptNo   wireInt  `json:"attempt_no"`
		RecordCount *wireInt `json:"record_count"`
		*sourceResultAlias
	}{sourceResultAlias: (*sourceResultAlias)(result)}
	if err := decodeJSON(data, &decoded); err != nil {
		return err
	}
	if decoded.RecordCount == nil {
		return fmt.Errorf("record_count is required")
	}
	result.AttemptNo = int(decoded.AttemptNo)
	result.RecordCount = int(*decoded.RecordCount)
	return nil
}

type ProcessingRunInput struct {
	ArtifactLocalRef     string
	ProcessingScope      string
	SchemaVersion        string
	ParserVersion        string
	MappingVersion       string
	NormalizationVersion string
	Records              []SourceRecord
}

type SourceResultManifest struct {
	SchemaVersion int            `json:"schema_version"`
	JobID         string         `json:"job_id"`
	AttemptNo     int            `json:"attempt_no"`
	RecordCount   int            `json:"record_count"`
	Records       []SourceRecord `json:"records"`
	Claims        []Claim        `json:"claims"`
}

type SourceCollectError struct {
	Code              string `json:"code"`
	Classification    string `json:"classification"`
	Message           string `json:"message"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
}

func (failure *SourceCollectError) UnmarshalJSON(data []byte) error {
	type alias SourceCollectError
	decoded := struct {
		RetryAfterSeconds *wireInt `json:"retry_after_seconds"`
		*alias
	}{alias: (*alias)(failure)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	failure.RetryAfterSeconds = (*int)(decoded.RetryAfterSeconds)
	return nil
}

type SourceCollectFailedPayload struct {
	JobID          string              `json:"job_id"`
	AttemptNo      int                 `json:"attempt_no"`
	AdapterVersion string              `json:"adapter_version"`
	Artifacts      []ArtifactReference `json:"artifacts"`
	Error          SourceCollectError  `json:"error"`
}

func (failure *SourceCollectFailedPayload) UnmarshalJSON(data []byte) error {
	type alias SourceCollectFailedPayload
	decoded := struct {
		AttemptNo wireInt `json:"attempt_no"`
		*alias
	}{alias: (*alias)(failure)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	failure.AttemptNo = int(decoded.AttemptNo)
	return nil
}

func (manifest *SourceResultManifest) UnmarshalJSON(data []byte) error {
	type sourceResultManifestAlias SourceResultManifest
	decoded := struct {
		SchemaVersion wireInt  `json:"schema_version"`
		AttemptNo     wireInt  `json:"attempt_no"`
		RecordCount   *wireInt `json:"record_count"`
		*sourceResultManifestAlias
	}{sourceResultManifestAlias: (*sourceResultManifestAlias)(manifest)}
	if err := decodeJSON(data, &decoded); err != nil {
		return err
	}
	if decoded.RecordCount == nil {
		return fmt.Errorf("record_count is required")
	}
	manifest.SchemaVersion = int(decoded.SchemaVersion)
	manifest.AttemptNo = int(decoded.AttemptNo)
	manifest.RecordCount = int(*decoded.RecordCount)
	return nil
}

type wireInt int

func (integer *wireInt) UnmarshalJSON(raw []byte) error {
	value, err := decodeExactJSONInteger(raw)
	if err != nil {
		return err
	}
	if int64(int(value)) != value {
		return fmt.Errorf("value is outside the supported integer range")
	}
	*integer = wireInt(value)
	return nil
}

type wireTimestamp time.Time

func (timestamp *wireTimestamp) UnmarshalJSON(raw []byte) error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return err
	}
	value, err := time.Parse(time.RFC3339Nano, strings.ToUpper(text))
	if err != nil {
		return err
	}
	*timestamp = wireTimestamp(value)
	return nil
}

func decodeExactJSONInteger(raw json.RawMessage) (int64, error) {
	const maximumSafeInteger int64 = 1<<53 - 1

	var number json.Number
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return 0, fmt.Errorf("value is required")
	}
	if (raw[0] < '0' || raw[0] > '9') && raw[0] != '-' {
		return 0, fmt.Errorf("value must be a JSON number")
	}
	if err := json.Unmarshal(raw, &number); err != nil {
		return 0, fmt.Errorf("value must be a JSON number: %w", err)
	}
	value, ok := new(big.Rat).SetString(number.String())
	if !ok || !value.IsInt() {
		return 0, fmt.Errorf("value must be an exact integer")
	}
	numerator := value.Num()
	if !numerator.IsInt64() || numerator.Sign() < 0 || numerator.Cmp(big.NewInt(maximumSafeInteger)) > 0 {
		return 0, fmt.Errorf("value is outside the non-negative safe integer range")
	}
	return numerator.Int64(), nil
}
