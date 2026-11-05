package config

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

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

	limitsSchema := extractSchema(t, string(spec), "InvestigationLimits", "CreateInvestigationRequest")
	tests := []struct {
		property string
		want     int
	}{
		{property: "depth", want: maxInvestigationDepth},
		{property: "entity_limit", want: maxInvestigationEntities},
	}

	for _, tt := range tests {
		t.Run(tt.property, func(t *testing.T) {
			if got := propertyMaximum(t, limitsSchema, tt.property); got != tt.want {
				t.Fatalf("OpenAPI maximum = %d, configuration maximum = %d", got, tt.want)
			}
		})
	}
}

func extractSchema(t *testing.T, spec string, name string, nextName string) string {
	t.Helper()
	startMarker := "    " + name + ":"
	endMarker := "    " + nextName + ":"
	start := strings.Index(spec, startMarker)
	end := strings.Index(spec, endMarker)
	if start < 0 || end <= start {
		t.Fatalf("locate %s schema in OpenAPI", name)
	}
	return spec[start:end]
}

func propertyMaximum(t *testing.T, schema string, property string) int {
	t.Helper()
	propertyPattern := regexp.MustCompile(
		`(?ms)^        ` + regexp.QuoteMeta(property) + `:\s*$` +
			`(.*?)(?:^        [a-zA-Z_][a-zA-Z0-9_]*:\s*$|\z)`,
	)
	propertyMatch := propertyPattern.FindStringSubmatch(schema)
	if len(propertyMatch) != 2 {
		t.Fatalf("locate %s property in InvestigationLimits", property)
	}
	maximumPattern := regexp.MustCompile(`(?m)^          maximum:\s*([0-9]+)\s*$`)
	maximumMatch := maximumPattern.FindStringSubmatch(propertyMatch[1])
	if len(maximumMatch) != 2 {
		t.Fatalf("locate maximum for %s", property)
	}
	maximum, err := strconv.Atoi(maximumMatch[1])
	if err != nil {
		t.Fatalf("parse maximum for %s: %v", property, err)
	}
	return maximum
}
