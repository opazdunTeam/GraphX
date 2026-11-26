package ingestion

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
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
		RecordCount json.RawMessage `json:"record_count"`
		*sourceResultAlias
	}{sourceResultAlias: (*sourceResultAlias)(result)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	recordCount, err := decodeExactJSONInteger(decoded.RecordCount)
	if err != nil {
		return fmt.Errorf("decode record_count: %w", err)
	}
	result.RecordCount = recordCount
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

type SourceCollectFailedPayload struct {
	JobID          string              `json:"job_id"`
	AttemptNo      int                 `json:"attempt_no"`
	AdapterVersion string              `json:"adapter_version"`
	Artifacts      []ArtifactReference `json:"artifacts"`
	Error          SourceCollectError  `json:"error"`
}

func (manifest *SourceResultManifest) UnmarshalJSON(data []byte) error {
	type sourceResultManifestAlias SourceResultManifest
	decoded := struct {
		RecordCount json.RawMessage `json:"record_count"`
		*sourceResultManifestAlias
	}{sourceResultManifestAlias: (*sourceResultManifestAlias)(manifest)}
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	recordCount, err := decodeExactJSONInteger(decoded.RecordCount)
	if err != nil {
		return fmt.Errorf("decode record_count: %w", err)
	}
	manifest.RecordCount = recordCount
	return nil
}

func decodeExactJSONInteger(raw json.RawMessage) (int, error) {
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
	integer64 := numerator.Int64()
	integer := int(integer64)
	if int64(integer) != integer64 {
		return 0, fmt.Errorf("value is outside the supported integer range")
	}
	return integer, nil
}
