package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/goccy/go-yaml"
)

type openAPIContract struct {
	Components struct {
		Schemas map[string]struct {
			Properties map[string]struct {
				Maximum *int `yaml:"maximum"`
			} `yaml:"properties"`
		} `yaml:"schemas"`
	} `yaml:"components"`
}

func TestInvestigationMaximumsMatchOpenAPI(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test source path")
	}
	specPath := filepath.Join(filepath.Dir(filename), "..", "..", "..", "..", "..", "contracts", "openapi.yaml")
	spec, err := os.ReadFile(specPath)
	if err != nil {
		t.Fatalf("read OpenAPI contract: %v", err)
	}

	var contract openAPIContract
	if err := yaml.Unmarshal(spec, &contract); err != nil {
		t.Fatalf("parse OpenAPI contract: %v", err)
	}
	limitsSchema, ok := contract.Components.Schemas["InvestigationLimits"]
	if !ok {
		t.Fatal("locate InvestigationLimits schema in OpenAPI")
	}

	tests := []struct {
		property string
		want     int
	}{
		{property: "depth", want: maxInvestigationDepth},
		{property: "entity_limit", want: maxInvestigationEntities},
	}
	for _, tt := range tests {
		t.Run(tt.property, func(t *testing.T) {
			property, ok := limitsSchema.Properties[tt.property]
			if !ok || property.Maximum == nil {
				t.Fatalf("locate maximum for %s", tt.property)
			}
			if got := *property.Maximum; got != tt.want {
				t.Fatalf("OpenAPI maximum = %d, configuration maximum = %d", got, tt.want)
			}
		})
	}
}
