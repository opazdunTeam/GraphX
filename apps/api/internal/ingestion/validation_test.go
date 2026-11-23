package ingestion

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestResultRecordCountDecodesEquivalentExactIntegerTokens(t *testing.T) {
	valid := []struct {
		token string
		want  int
	}{
		{"0", 0},
		{"1", 1},
		{"1.0", 1},
		{"1e0", 1},
		{"2.00e1", 20},
		{"9007199254740991.0", 9_007_199_254_740_991},
	}
	for _, test := range valid {
		t.Run(test.token, func(t *testing.T) {
			for name, target := range map[string]any{
				"inline":   &SourceResult{},
				"manifest": &SourceResultManifest{},
			} {
				t.Run(name, func(t *testing.T) {
					raw := []byte(fmt.Sprintf(`{"record_count":%s}`, test.token))
					if err := json.Unmarshal(raw, target); err != nil {
						t.Fatalf("decode equivalent integer token: %v", err)
					}
					switch value := target.(type) {
					case *SourceResult:
						if value.RecordCount != test.want {
							t.Fatalf("record_count = %d, want %d", value.RecordCount, test.want)
						}
					case *SourceResultManifest:
						if value.RecordCount != test.want {
							t.Fatalf("record_count = %d, want %d", value.RecordCount, test.want)
						}
					}
				})
			}
		})
	}
}

func TestResultRecordCountRejectsInexactTokens(t *testing.T) {
	for _, token := range []string{
		"-1", "1.5", "1.00000000000000001", "9007199254740992.0", `"1"`, "true", "null",
	} {
		t.Run(token, func(t *testing.T) {
			raw := []byte(fmt.Sprintf(`{"record_count":%s}`, token))
			if err := json.Unmarshal(raw, &SourceResult{}); err == nil {
				t.Fatal("expected inexact record_count to be rejected")
			}
			if err := json.Unmarshal(raw, &SourceResultManifest{}); err == nil {
				t.Fatal("expected inexact manifest record_count to be rejected")
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
	records := []SourceRecord{{
		LocalRef: "person:1", ArtifactLocalRef: "missing", RecordType: "person",
		SchemaVersion: "person/1",
	}}
	if err := validateContent(records, nil, artifacts); err == nil || !strings.Contains(err.Error(), "unknown artifact") {
		t.Fatal("expected unknown artifact reference error")
	}
	records[0].ArtifactLocalRef = "artifact:1"
	object := "person:1"
	claims := []Claim{{
		ClaimKind: "attribute", SubjectLocalRef: "person:1", Predicate: "name",
		ObjectLocalRef: &object, RawValue: "name",
		ValidFromPrecision: "unknown", ValidToPrecision: "unknown", TemporalBasis: "unknown",
		EvidenceLocator: map[string]any{"json_pointer": "/name"},
	}}
	if err := validateContent(records, claims, artifacts); err == nil || !strings.Contains(err.Error(), "attribute claim") {
		t.Fatal("expected invalid attribute object error")
	}
}

func TestSemanticValidationRejectsDuplicateRecords(t *testing.T) {
	artifacts := []ArtifactReference{{LocalRef: "artifact:1"}}
	records := []SourceRecord{
		{LocalRef: "person:1", ArtifactLocalRef: "artifact:1", RecordType: "person", SchemaVersion: "person/1"},
		{LocalRef: "person:1", ArtifactLocalRef: "artifact:1", RecordType: "person", SchemaVersion: "person/1"},
	}
	if err := validateContent(records, nil, artifacts); err == nil || !strings.Contains(err.Error(), "duplicate source record") {
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
			ClaimKind: "relationship", SubjectLocalRef: "record:1", Predicate: "related_to",
			ValidFromPrecision: "unknown",
			ValidToPrecision:   "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"json_pointer": "/relationship"},
		}},
		{"empty event", Claim{
			ClaimKind: "event", SubjectLocalRef: "record:1", Predicate: "event",
			ValidFromPrecision: "unknown",
			ValidToPrecision:   "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"csv_row": 2},
		}},
		{"control-only attribute", Claim{
			ClaimKind: "attribute", SubjectLocalRef: "record:1", Predicate: "name",
			RawValue: "\u001f", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"json_pointer": "/name"},
		}},
		{"bell-only attribute", Claim{
			ClaimKind: "attribute", SubjectLocalRef: "record:1", Predicate: "name",
			RawValue: "\u0007", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"json_pointer": "/name"},
		}},
		{"delete-only event", Claim{
			ClaimKind: "event", SubjectLocalRef: "record:1", Predicate: "event",
			RawValue: "\u007f", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"json_pointer": "/event"},
		}},
		{"control-only event", Claim{
			ClaimKind: "event", SubjectLocalRef: "record:1", Predicate: "event",
			RawValue: "\u001c", ValidFromPrecision: "unknown",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"json_pointer": "/event"},
		}},
		{"unknown with date", Claim{
			ClaimKind: "event", SubjectLocalRef: "record:1", Predicate: "event",
			RawValue: true, ValidFrom: &date, ValidFromPrecision: "day",
			ValidToPrecision: "unknown", TemporalBasis: "unknown",
			EvidenceLocator: map[string]any{"page_number": 3, "fragment": "table 1"},
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
		Predicate:          "event",
		RawValue:           true,
		ValidFrom:          &badAnchor,
		ValidFromPrecision: "month",
		ValidToPrecision:   "unknown",
		TemporalBasis:      "explicit",
		EvidenceLocator:    map[string]any{"json_pointer": "/event"},
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
				ClaimKind: "event", SubjectLocalRef: "person:1", Predicate: "event",
				RawValue:  true,
				ValidFrom: &test.validFrom, ValidFromPrecision: test.fromPrecision,
				ValidTo: &test.validTo, ValidToPrecision: test.toPrecision,
				TemporalBasis:   "explicit",
				EvidenceLocator: map[string]any{"json_pointer": "/event"},
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

func TestEvidenceLocatorSupportsOnlyPortableForms(t *testing.T) {
	valid := []map[string]any{
		{"json_pointer": ""},
		{"json_pointer": "/people/0/name~1display"},
		{"json_pointer": "/name", "column": "name"},
		{"csv_row": float64(2)},
		{"csv_row": float64(9_007_199_254_740_991)},
		{"page_number": 3},
		{"fragment": "section-2"},
		{"page_number": 3, "fragment": "table 1"},
		{"page_number": 3, "section": "appendix"},
		{"csv_row": json.Number("2e0")},
	}
	for _, locator := range valid {
		if err := validateEvidenceLocator(locator); err != nil {
			t.Fatalf("expected valid locator %v: %v", locator, err)
		}
	}
	invalid := []map[string]any{
		{},
		{"json_pointer": "people/0"},
		{"json_pointer": "/bad~2escape"},
		{"csv_row": 0},
		{"csv_row": 1.5},
		{"csv_row": float64(9_007_199_254_740_992)},
		{"page_number": -1},
		{"page_number": nil, "fragment": "section-2"},
		{"fragment": ""},
		{"fragment": " \t"},
		{"fragment": "\u001c"},
		{"page_number": 1, "fragment": nil},
		{"json_pointer": "/name", "csv_row": 1},
		{"offset": 42},
		{"csv_row": json.Number("1e20")},
	}
	for _, locator := range invalid {
		if err := validateEvidenceLocator(locator); err == nil {
			t.Fatalf("expected invalid locator %v", locator)
		}
	}
}

func TestRawLocatorNumbersPreserveExactDecimalSemantics(t *testing.T) {
	valid := []string{"2", "2.0", "2e0", "9007199254740991", "2.00e1"}
	for _, token := range valid {
		document := []byte(`{"claims":[{"evidence_locator":{"csv_row":` + token + `}}]}`)
		if err := validateRawLocatorNumbers(document); err != nil {
			t.Fatalf("expected %s to be valid: %v", token, err)
		}
	}
	invalid := []string{
		"0", "1.5", "1.00000000000000001", "9007199254740991.1",
		"9007199254740992", "1e20", "-1", "1e-1",
		"1e-9223372036854775808", "1e9223372036854775807",
	}
	for _, token := range invalid {
		document := []byte(`{"claims":[{"evidence_locator":{"page_number":` + token + `}}]}`)
		if err := validateRawLocatorNumbers(document); err == nil {
			t.Fatalf("expected %s to be invalid", token)
		}
	}
}

func TestClaimRejectsBlankPersistenceText(t *testing.T) {
	base := Claim{
		ClaimKind: "attribute", SubjectLocalRef: "record:1", Predicate: "name",
		RawValue: "Example", ValidFromPrecision: "unknown", ValidToPrecision: "unknown",
		TemporalBasis: "unknown", EvidenceLocator: map[string]any{"json_pointer": "/name"},
	}
	blank := " \t"
	mutations := []func(*Claim){
		func(claim *Claim) { claim.SubjectLocalRef = blank },
		func(claim *Claim) { claim.Predicate = blank },
		func(claim *Claim) { claim.ObjectLocalRef = &blank },
	}
	for _, mutate := range mutations {
		claim := base
		mutate(&claim)
		if err := validateClaim(claim); err == nil {
			t.Fatal("expected blank claim persistence field to be rejected")
		}
	}
}

func TestClaimRejectsPostgresIncompatibleValues(t *testing.T) {
	base := Claim{
		ClaimKind: "attribute", SubjectLocalRef: "record:1", Predicate: "name",
		RawValue: "Example", ValidFromPrecision: "unknown", ValidToPrecision: "unknown",
		TemporalBasis: "unknown", EvidenceLocator: map[string]any{"json_pointer": "/name"},
	}
	mutations := []func(*Claim){
		func(claim *Claim) { claim.RawValue = map[string]any{"nested": "bad\x00value"} },
		func(claim *Claim) { claim.Qualifiers = map[string]any{"bad\x00key": true} },
		func(claim *Claim) {
			claim.EvidenceLocator = map[string]any{"json_pointer": "/bad\x00value"}
		},
		func(claim *Claim) {
			value := "bad\x00value"
			claim.EvidenceExcerpt = &value
		},
	}
	for _, mutate := range mutations {
		claim := base
		mutate(&claim)
		if err := validateClaim(claim); err == nil {
			t.Fatal("expected PostgreSQL-incompatible claim value to be rejected")
		}
	}
}

func TestBuildProcessingRunInputsPreservesVersionsAndArtifactGrouping(t *testing.T) {
	result := SourceResult{
		JobID: "22222222-2222-4222-8222-222222222222", AttemptNo: 1,
		Outcome: "succeeded", RecordCount: 2,
		ParserVersion: "parser/3", MappingVersion: "mapping/4",
		NormalizationVersion: "normalization/5",
		Artifacts:            []ArtifactReference{{LocalRef: "artifact:1"}, {LocalRef: "artifact:2"}},
		Records: []SourceRecord{
			{LocalRef: "record:1", ArtifactLocalRef: "artifact:2", RecordType: "person", SchemaVersion: "fixture/3"},
			{LocalRef: "record:2", ArtifactLocalRef: "artifact:1", RecordType: "person", SchemaVersion: "fixture/2"},
		},
	}
	runs, err := BuildProcessingRunInputs(result, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].ArtifactLocalRef != "artifact:1" ||
		runs[1].ArtifactLocalRef != "artifact:2" {
		t.Fatalf("unexpected deterministic artifact grouping: %#v", runs)
	}
	wantSchemas := map[string]string{"artifact:1": "fixture/2", "artifact:2": "fixture/3"}
	for _, run := range runs {
		if run.ProcessingScope != "source_collection" ||
			run.SchemaVersion != wantSchemas[run.ArtifactLocalRef] ||
			run.ParserVersion != "parser/3" || run.MappingVersion != "mapping/4" ||
			run.NormalizationVersion != "normalization/5" || len(run.Records) != 1 {
			t.Fatalf("unexpected processing run input: %#v", run)
		}
	}
}

func TestSourceArtifactsRejectManifestKind(t *testing.T) {
	artifacts := []ArtifactReference{{LocalRef: "artifact:1", Kind: "manifest"}}
	if err := validateSourceArtifactKinds(artifacts); err == nil {
		t.Fatal("expected manifest kind in source artifacts to be rejected")
	}
	result := SourceResult{
		RecordCount: 0, ParserVersion: "parser/1", MappingVersion: "mapping/1",
		NormalizationVersion: "normalization/1", Artifacts: artifacts,
	}
	if _, err := BuildProcessingRunInputs(result, nil); err == nil {
		t.Fatal("expected processing run builder to reject manifest source artifact")
	}
}

func TestBuildProcessingRunInputsRejectsMixedSchemaVersionsForOneArtifact(t *testing.T) {
	result := SourceResult{
		Outcome: "succeeded", RecordCount: 2,
		ParserVersion: "parser/1", MappingVersion: "mapping/1",
		NormalizationVersion: "normalization/1",
		Artifacts:            []ArtifactReference{{LocalRef: "artifact:1"}},
		Records: []SourceRecord{
			{LocalRef: "record:1", ArtifactLocalRef: "artifact:1", RecordType: "person", SchemaVersion: "schema/1"},
			{LocalRef: "record:2", ArtifactLocalRef: "artifact:1", RecordType: "person", SchemaVersion: "schema/2"},
		},
	}
	if _, err := BuildProcessingRunInputs(result, nil); err == nil {
		t.Fatal("expected mixed record schema versions to be rejected")
	}
}

func TestSemanticValidationRejectsBlankProcessingVersions(t *testing.T) {
	base := SourceResult{
		AdapterVersion: "adapter/1", ParserVersion: "parser/1",
		MappingVersion: "mapping/1", NormalizationVersion: "normalization/1",
	}
	mutations := []struct {
		name string
		set  func(*SourceResult)
	}{
		{"adapter", func(result *SourceResult) { result.AdapterVersion = " \t" }},
		{"parser", func(result *SourceResult) { result.ParserVersion = " \t" }},
		{"mapping", func(result *SourceResult) { result.MappingVersion = " \t" }},
		{"normalization", func(result *SourceResult) { result.NormalizationVersion = " \t" }},
	}
	for _, value := range []string{" \t", "\u001c", "\u00a0", "v\x00x"} {
		for _, mutation := range mutations {
			t.Run(mutation.name, func(t *testing.T) {
				result := base
				mutation.set(&result)
				switch mutation.name {
				case "adapter":
					result.AdapterVersion = value
				case "parser":
					result.ParserVersion = value
				case "mapping":
					result.MappingVersion = value
				case "normalization":
					result.NormalizationVersion = value
				}
				raw := sourceResultPayload(t, &result)
				if err := ValidateSourceResult(result, raw); err == nil {
					t.Fatal("expected invalid version to be rejected")
				}
			})
		}
	}
}

func TestSemanticValidationRejectsBlankRecordPersistenceFields(t *testing.T) {
	artifacts := []ArtifactReference{{LocalRef: "artifact:1"}}
	base := SourceRecord{
		LocalRef: "record:1", ArtifactLocalRef: "artifact:1", RecordType: "person",
		SchemaVersion: "schema/1",
	}
	for _, value := range []string{" \t", "\u001c", "\u00a0", "v\x00x"} {
		mutations := []func(*SourceRecord){
			func(record *SourceRecord) { record.LocalRef = value },
			func(record *SourceRecord) { record.ArtifactLocalRef = value },
			func(record *SourceRecord) { record.RecordType = value },
			func(record *SourceRecord) { record.ExternalID = &value },
			func(record *SourceRecord) { record.SchemaVersion = value },
		}
		for _, mutate := range mutations {
			record := base
			mutate(&record)
			if err := validateContent([]SourceRecord{record}, nil, artifacts); err == nil {
				t.Fatal("expected invalid record persistence field to be rejected")
			}
		}
	}
	record := base
	record.RawData = map[string]any{"nested": []any{"safe", "bad\x00value"}}
	if err := validateContent([]SourceRecord{record}, nil, artifacts); err == nil {
		t.Fatal("expected PostgreSQL-incompatible raw_data to be rejected")
	}
}

func TestBuildProcessingRunInputsUsesValidatedManifestContent(t *testing.T) {
	result := SourceResult{
		JobID: "22222222-2222-4222-8222-222222222222", AttemptNo: 2,
		Outcome: "succeeded", RecordCount: 1,
		ParserVersion: "parser/1", MappingVersion: "mapping/1",
		NormalizationVersion: "normalization/1",
		Artifacts:            []ArtifactReference{{LocalRef: "artifact:1"}},
		Manifest:             &ArtifactReference{LocalRef: "artifact:manifest"},
	}
	manifest := &SourceResultManifest{
		SchemaVersion: 1, JobID: result.JobID, AttemptNo: result.AttemptNo, RecordCount: 1,
		Records: []SourceRecord{{
			LocalRef: "record:1", ArtifactLocalRef: "artifact:1", RecordType: "person",
			SchemaVersion: "schema/1",
		}},
	}
	runs, err := BuildProcessingRunInputs(result, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].SchemaVersion != "schema/1" {
		t.Fatalf("unexpected manifest processing run input: %#v", runs)
	}
	if _, err = BuildProcessingRunInputs(result, nil); err == nil {
		t.Fatal("expected missing manifest content to be rejected")
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
				Outcome: "succeeded", AdapterVersion: "adapter/1", ParserVersion: "parser/1",
				MappingVersion: "mapping/1", NormalizationVersion: "normalization/1",
				Artifacts: []ArtifactReference{
					{LocalRef: "artifact:1"}, {LocalRef: "artifact:1"},
				}, Manifest: manifest,
			}
			raw := sourceResultPayload(t, &result)
			if err := ValidateSourceResult(result, raw); err == nil || !strings.Contains(err.Error(), "duplicate artifact") {
				t.Fatal("expected duplicate artifact local_ref error")
			}
		})
	}
}

func TestArtifactParentReferencesMustResolveWithoutCycles(t *testing.T) {
	parent := "artifact:parent"
	child := "artifact:child"
	valid := []ArtifactReference{
		{LocalRef: parent},
		{LocalRef: child, ParentLocalRef: &parent},
	}
	if _, err := validateArtifactRefs(valid); err != nil {
		t.Fatalf("expected valid artifact lineage: %v", err)
	}

	missing := "artifact:missing"
	if _, err := validateArtifactRefs([]ArtifactReference{
		{LocalRef: child, ParentLocalRef: &missing},
	}); err == nil {
		t.Fatal("expected missing artifact parent to be rejected")
	}

	cyclic := []ArtifactReference{
		{LocalRef: parent, ParentLocalRef: &child},
		{LocalRef: child, ParentLocalRef: &parent},
	}
	if _, err := validateArtifactRefs(cyclic); err == nil {
		t.Fatal("expected artifact parent cycle to be rejected")
	}
}

func TestArtifactTransportBoundaryRejectsUnsafeMetadataAndForeignKeys(t *testing.T) {
	jobID := "22222222-2222-4222-8222-222222222222"
	filename := "response.json"
	base := ArtifactReference{
		LocalRef:         "artifact:raw:1",
		ObjectKey:        "artifacts/v1/22222222-2222-4222-8222-222222222222/1/YXJ0aWZhY3Q6cmF3OjE",
		MediaType:        "application/json",
		SourceURI:        "https://example.test/source/1",
		HTTPMetadata:     map[string]string{"etag": "source-v1"},
		OriginalFilename: &filename,
	}
	tests := []struct {
		name   string
		mutate func(*ArtifactReference)
	}{
		{"foreign key", func(value *ArtifactReference) {
			value.ObjectKey = "artifacts/v1/00000000-0000-4000-8000-000000000099/1/YXJ0aWZhY3Q6cmF3OjE"
		}},
		{"userinfo", func(value *ArtifactReference) {
			value.SourceURI = "https://user:password@example.test/source"
		}},
		{"secret query", func(value *ArtifactReference) {
			value.SourceURI = "https://example.test/source?token=secret"
		}},
		{"file URI", func(value *ArtifactReference) { value.SourceURI = "file:///etc/passwd" }},
		{"out of range port", func(value *ArtifactReference) {
			value.SourceURI = "https://example.test:99999/source"
		}},
		{"non-canonical port", func(value *ArtifactReference) {
			value.SourceURI = "https://example.test:00080/source"
		}},
		{"authorization", func(value *ArtifactReference) {
			value.HTTPMetadata = map[string]string{"authorization": "Bearer secret"}
		}},
		{"blank local ref", func(value *ArtifactReference) { value.LocalRef = " \t" }},
		{"blank media type", func(value *ArtifactReference) { value.MediaType = " \t" }},
		{"NUL metadata", func(value *ArtifactReference) {
			value.HTTPMetadata = map[string]string{"etag": "bad\x00value"}
		}},
		{"CRLF", func(value *ArtifactReference) {
			value.HTTPMetadata = map[string]string{"etag": "bad\r\nheader"}
		}},
		{"unsafe filename", func(value *ArtifactReference) {
			unsafe := " response.json "
			value.OriginalFilename = &unsafe
		}},
		{"C1 local ref", func(value *ArtifactReference) {
			value.LocalRef = "artifact:bad\u0085ref"
		}},
		{"C1 filename", func(value *ArtifactReference) {
			unsafe := "bad\u0085name.json"
			value.OriginalFilename = &unsafe
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			artifact := base
			test.mutate(&artifact)
			if err := ValidateArtifactRefs([]ArtifactReference{artifact}, jobID, 1); err == nil {
				t.Fatal("expected unsafe artifact transport value to be rejected")
			}
		})
	}
	ipv6 := base
	ipv6.SourceURI = "https://[::1]/source/1"
	if err := ValidateArtifactRefs([]ArtifactReference{ipv6}, jobID, 1); err != nil {
		t.Fatalf("expected bracketed IPv6 source URI to be valid: %v", err)
	}
	maximumPort := base
	maximumPort.SourceURI = "https://example.test:65535/source/1"
	if err := ValidateArtifactRefs([]ArtifactReference{maximumPort}, jobID, 1); err != nil {
		t.Fatalf("expected maximum source URI port to be valid: %v", err)
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
		Artifacts: []ArtifactReference{{LocalRef: "artifact:1"}},
		Manifest:  &ArtifactReference{},
	}
	baseManifest := SourceResultManifest{
		SchemaVersion: 1, JobID: baseResult.JobID, AttemptNo: 1, RecordCount: 1,
		Records: []SourceRecord{{
			LocalRef: "record:1", ArtifactLocalRef: "artifact:1", RecordType: "person",
			SchemaVersion: "person/1",
		}},
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
			stored, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			result := baseResult
			result.Manifest = &ArtifactReference{
				SHA256: fmt.Sprintf("%x", sha256.Sum256(stored)), ByteSize: int64(len(stored)),
			}
			if err = ValidateManifest(result, manifest, stored); err == nil || !strings.Contains(err.Error(), "metadata") {
				t.Fatal("expected manifest metadata mismatch")
			}
		})
	}
}
