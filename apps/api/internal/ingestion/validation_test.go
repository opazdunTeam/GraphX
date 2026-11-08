package ingestion

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestSharedCompletedFixturesPassSemanticValidation(t *testing.T) {
	root := repositoryRoot(t)
	for _, name := range []string{
		"source-collect-completed.v1.json",
		"source-collect-completed-manifest.v1.json",
	} {
		t.Run(name, func(t *testing.T) {
			raw, readErr := os.ReadFile(filepath.Join(root, "contracts", "examples", "messages", name))
			if readErr != nil {
				t.Fatal(readErr)
			}
			var envelope struct {
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatal(err)
			}
			var result SourceResult
			if err := json.Unmarshal(envelope.Payload, &result); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSourceResult(result, envelope.Payload); err != nil {
				t.Fatal(err)
			}
			if result.Manifest != nil {
				manifest, manifestErr := os.ReadFile(filepath.Join(
					root, "contracts", "examples", "ingestion", "source-result-manifest.v1.json",
				))
				if manifestErr != nil {
					t.Fatal(manifestErr)
				}
				var manifestDocument SourceResultManifest
				if manifestErr = json.Unmarshal(manifest, &manifestDocument); manifestErr != nil {
					t.Fatal(manifestErr)
				}
				if manifestErr = ValidateManifest(result, manifestDocument, manifest); manifestErr != nil {
					t.Fatal(manifestErr)
				}
			}
		})
	}
}

func TestCompletedDigestMismatchIsRejected(t *testing.T) {
	root := repositoryRoot(t)
	raw, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "messages", "source-collect-completed.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	payload := value["payload"].(map[string]any)
	payload["digest"] = "0000000000000000000000000000000000000000000000000000000000000000"
	mutatedPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var result SourceResult
	if err := json.Unmarshal(mutatedPayload, &result); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSourceResult(result, mutatedPayload); err == nil {
		t.Fatal("expected digest mismatch")
	}
}

func TestSemanticValidationRejectsInvalidReferencesAndClaims(t *testing.T) {
	artifacts := []ArtifactReference{{LocalRef: "artifact:1"}}
	records := []SourceRecord{{LocalRef: "person:1", ArtifactLocalRef: "missing"}}
	if err := validateContent(records, nil, artifacts); err == nil {
		t.Fatal("expected unknown artifact reference error")
	}
	records[0].ArtifactLocalRef = "artifact:1"
	object := "person:1"
	claims := []Claim{{
		ClaimKind: "attribute", SubjectLocalRef: "person:1", ObjectLocalRef: &object, RawValue: "name",
		ValidFromPrecision: "unknown", ValidToPrecision: "unknown", TemporalBasis: "unknown",
	}}
	if err := validateContent(records, claims, artifacts); err == nil {
		t.Fatal("expected invalid attribute object error")
	}
}

func TestSemanticValidationRejectsDuplicateRecords(t *testing.T) {
	artifacts := []ArtifactReference{{LocalRef: "artifact:1"}}
	records := []SourceRecord{
		{LocalRef: "person:1", ArtifactLocalRef: "artifact:1"},
		{LocalRef: "person:1", ArtifactLocalRef: "artifact:1"},
	}
	if err := validateContent(records, nil, artifacts); err == nil {
		t.Fatal("expected duplicate source record local_ref error")
	}
}

func TestClaimRejectsRequiredNegativeSemantics(t *testing.T) {
	date := "2020-01-01"
	tests := []struct {
		name  string
		claim Claim
	}{
		{"relationship without object", Claim{
			ClaimKind: "relationship", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
		}},
		{"empty event", Claim{
			ClaimKind: "event", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
		}},
		{"unknown with date", Claim{
			ClaimKind: "event", RawValue: true, ValidFrom: &date, ValidFromPrecision: "day",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateClaim(test.claim); err == nil {
				t.Fatal("expected invalid claim")
			}
		})
	}
}

func TestClaimTemporalSemantics(t *testing.T) {
	badAnchor := "2020-05-15"
	claim := Claim{
		ClaimKind:          "event",
		SubjectLocalRef:    "person:1",
		RawValue:           true,
		ValidFrom:          &badAnchor,
		ValidFromPrecision: "month",
		ValidToPrecision:   "unknown",
		TemporalBasis:      "explicit",
	}
	if err := validateClaim(claim); err == nil {
		t.Fatal("expected invalid month anchor")
	}
	claim.ValidFrom = nil
	claim.ValidFromPrecision = "unknown"
	claim.TemporalBasis = "snapshot"
	if err := validateClaim(claim); err == nil {
		t.Fatal("expected invalid snapshot form")
	}
}

func TestClaimTemporalOrderUsesPrecisionRanges(t *testing.T) {
	tests := []struct {
		name          string
		validFrom     string
		fromPrecision string
		validTo       string
		toPrecision   string
		valid         bool
	}{
		{"ordered days", "2020-01-01", "day", "2020-01-02", "day", true},
		{"equal days", "2020-01-01", "day", "2020-01-01", "day", false},
		{"same year", "2020-01-01", "year", "2020-01-01", "year", true},
		{"same month", "2020-05-01", "month", "2020-05-01", "month", true},
		{"reversed years", "2021-01-01", "year", "2020-01-01", "year", false},
		{"reversed months", "2021-02-01", "month", "2021-01-01", "month", false},
		{"year to first day", "2021-01-01", "year", "2021-01-01", "day", false},
		{"approximate bounds", "2021-01-01", "approximate", "1900-01-01", "approximate", true},
		{"approximate start", "2021-01-01", "approximate", "1900-01-01", "day", true},
		{"approximate end", "2021-01-01", "day", "1900-01-01", "approximate", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			claim := Claim{
				ClaimKind: "event", SubjectLocalRef: "person:1", RawValue: true,
				ValidFrom: &test.validFrom, ValidFromPrecision: test.fromPrecision,
				ValidTo: &test.validTo, ValidToPrecision: test.toPrecision,
				TemporalBasis: "explicit",
			}
			err := validateClaim(claim)
			if test.valid && err != nil {
				t.Fatalf("expected valid claim: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("expected invalid interval order")
			}
		})
	}
}

func TestSourceResultRejectsDuplicateArtifactsInEveryMode(t *testing.T) {
	for _, manifestMode := range []bool{false, true} {
		name := "inline"
		var manifest *ArtifactReference
		if manifestMode {
			name = "manifest"
			manifest = &ArtifactReference{LocalRef: "artifact:manifest"}
		}
		t.Run(name, func(t *testing.T) {
			result := SourceResult{
				Outcome: "succeeded", Artifacts: []ArtifactReference{
					{LocalRef: "artifact:1"}, {LocalRef: "artifact:1"},
				}, Manifest: manifest,
			}
			raw := sourceResultPayload(t, &result)
			if err := ValidateSourceResult(result, raw); err == nil {
				t.Fatal("expected duplicate artifact local_ref error")
			}
		})
	}
}

func sourceResultPayload(t *testing.T, result *SourceResult) []byte {
	t.Helper()
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := sourceResultDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	result.Digest = digest
	raw, err = json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSourceResultDigestUsesRFC8785NumberSerialization(t *testing.T) {
	raw := []byte(`{"digest":"ignored","numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001]}`)
	got, err := sourceResultDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("%x", sha256.Sum256(
		[]byte(`{"numbers":[333333333.3333333,1e+30,4.5,0.002,1e-27]}`),
	))
	if got != want {
		t.Fatalf("unexpected digest: got %s, want %s", got, want)
	}
}

func TestSourceResultDigestRejectsIntegerOutsideJCSSafeDomain(t *testing.T) {
	raw := []byte(`{"digest":"ignored","raw_data":{"identifier":9007199254740992}}`)
	if _, err := sourceResultDigest(raw); err == nil {
		t.Fatal("expected unsafe integer to be rejected")
	}
}

func TestManifestValidationRejectsEveryMetadataMismatch(t *testing.T) {
	baseResult := SourceResult{
		JobID: "00000000-0000-4000-8000-000000000001", AttemptNo: 1, RecordCount: 1,
		Manifest: &ArtifactReference{SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte("manifest"))), ByteSize: 8},
	}
	baseManifest := SourceResultManifest{
		JobID: baseResult.JobID, AttemptNo: 1, RecordCount: 1,
		Records: []SourceRecord{{LocalRef: "record:1", ArtifactLocalRef: "artifact:1"}},
	}
	for _, mutate := range []struct {
		name string
		fn   func(*SourceResultManifest)
	}{
		{"job id", func(value *SourceResultManifest) { value.JobID = "different" }},
		{"attempt", func(value *SourceResultManifest) { value.AttemptNo++ }},
		{"record count", func(value *SourceResultManifest) { value.RecordCount++ }},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			manifest := baseManifest
			mutate.fn(&manifest)
			if err := ValidateManifest(baseResult, manifest, []byte("manifest")); err == nil {
				t.Fatal("expected manifest metadata mismatch")
			}
		})
	}
}
