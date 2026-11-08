package ingestion

import "time"

type ArtifactReference struct {
	LocalRef     string            `json:"local_ref"`
	Kind         string            `json:"kind"`
	ObjectKey    string            `json:"object_key"`
	SHA256       string            `json:"sha256"`
	MediaType    string            `json:"media_type"`
	ByteSize     int64             `json:"byte_size"`
	SourceURI    string            `json:"source_uri"`
	RetrievedAt  time.Time         `json:"retrieved_at"`
	PublishedAt  *time.Time        `json:"published_at"`
	HTTPMetadata map[string]string `json:"http_metadata"`
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

type SourceResultManifest struct {
	SchemaVersion int            `json:"schema_version"`
	JobID         string         `json:"job_id"`
	AttemptNo     int            `json:"attempt_no"`
	RecordCount   int            `json:"record_count"`
	Records       []SourceRecord `json:"records"`
	Claims        []Claim        `json:"claims"`
}
