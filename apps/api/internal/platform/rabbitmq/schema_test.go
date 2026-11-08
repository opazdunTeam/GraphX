package rabbitmq

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
)

func testRepositoryRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestSchemaRegistryAcceptsSharedMessageFixtures(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(root, "contracts", "examples", "messages", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no shared message fixtures found")
	}
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, readErr := os.ReadFile(path)
			if readErr != nil {
				t.Fatal(readErr)
			}
			message, validateErr := registry.ValidateMessage(raw)
			if validateErr != nil {
				t.Fatal(validateErr)
			}
			switch message.Type {
			case SourceCollectRequestedType:
				_, validateErr = Decode[SourceCollectRequestedPayload](message, message.Type)
			case SourceCollectCompletedType:
				_, validateErr = Decode[ingestion.SourceResult](message, message.Type)
			case SourceCollectFailedType:
				_, validateErr = Decode[SourceCollectFailedPayload](message, message.Type)
			}
			if validateErr != nil {
				t.Fatal(validateErr)
			}
		})
	}
}

func TestSchemaRegistryRejectsUnknownVersionAndInvalidFormats(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "messages", "source-collect-requested.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	tests := map[string]func(map[string]any){
		"unknown type":      func(value map[string]any) { value["type"] = "source.collect.unknown.v1" },
		"unknown version":   func(value map[string]any) { value["schema_version"] = float64(2) },
		"invalid uuid":      func(value map[string]any) { value["message_id"] = "not-a-uuid" },
		"invalid date-time": func(value map[string]any) { value["occurred_at"] = "not-a-date" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			copyValue := make(map[string]any, len(original))
			for key, value := range original {
				copyValue[key] = value
			}
			mutate(copyValue)
			invalid, marshalErr := json.Marshal(copyValue)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if _, validateErr := registry.ValidateMessage(invalid); validateErr == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSchemaRegistryChecksURIFormat(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
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
	artifacts := payload["artifacts"].([]any)
	artifacts[0].(map[string]any)["source_uri"] = "not a URI"
	invalid, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ValidateMessage(invalid); err == nil {
		t.Fatal("expected URI format validation error")
	}
}

func TestSchemaRegistryRejectsDuplicateObjectKeysAtEveryLevel(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	completed, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "messages", "source-collect-completed.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	requested, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "messages", "source-collect-requested.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "ingestion", "source-result-manifest.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		raw      []byte
		manifest bool
	}{
		{
			name: "envelope", raw: duplicateJSONField(
				t, completed, `"schema_version": 1,`, `"schema_version": 1,`),
		},
		{
			name: "payload", raw: duplicateJSONField(
				t, completed, `"record_count": 1,`, `"record_count": 1,`),
		},
		{
			name: "nested object", raw: duplicateJSONField(
				t, requested, `"iin": "900101300000"`, `"iin": "900101300000",`),
		},
		{
			name: "manifest", raw: duplicateJSONField(
				t, manifest, `"record_count": 1,`, `"record_count": 1,`), manifest: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.manifest {
				if err := registry.ValidateManifest(test.raw); err == nil {
					t.Fatal("expected duplicate object key error")
				}
				return
			}
			if _, err := registry.ValidateMessage(test.raw); err == nil {
				t.Fatal("expected duplicate object key error")
			}
		})
	}
}

func TestManifestUsesSharedSafeIntegerDomain(t *testing.T) {
	root := testRepositoryRoot(t)
	registry, err := NewSchemaRegistry(filepath.Join(root, "contracts", "schemas"))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(
		root, "contracts", "examples", "ingestion", "source-result-manifest.v1.json",
	))
	if err != nil {
		t.Fatal(err)
	}
	var original map[string]any
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		value int64
		valid bool
	}{
		{"positive maximum", 9_007_199_254_740_991, true},
		{"positive overflow", 9_007_199_254_740_992, false},
		{"negative minimum", -9_007_199_254_740_991, true},
		{"negative overflow", -9_007_199_254_740_992, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := cloneJSONMap(t, original)
			records := value["records"].([]any)
			rawData := records[0].(map[string]any)["raw_data"].(map[string]any)
			rawData["numeric_identifier"] = test.value
			encoded, marshalErr := json.Marshal(value)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			err := registry.ValidateManifest(encoded)
			if test.valid && err != nil {
				t.Fatalf("expected valid manifest: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("expected unsafe integer to be rejected")
			}
		})
	}
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var clone map[string]any
	if err := json.Unmarshal(raw, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

func duplicateJSONField(t *testing.T, raw []byte, field string, duplicate string) []byte {
	t.Helper()
	needle := []byte(field)
	if bytes.Count(raw, needle) != 1 {
		t.Fatalf("expected exactly one %q field", field)
	}
	replacement := append([]byte(duplicate+"\n    "), needle...)
	return bytes.Replace(raw, needle, replacement, 1)
}
