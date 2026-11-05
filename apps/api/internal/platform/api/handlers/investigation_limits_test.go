package handlers

import (
	"reflect"
	"testing"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func investigationHandler() *Handler {
	return &Handler{investigation: config.Investigation{
		DefaultDepth: 1, MaxDepth: 2, DefaultEntityLimit: 50, MaxEntityLimit: 100,
	}}
}

func TestResolveInvestigationLimitsDefaults(t *testing.T) {
	got, err := investigationHandler().ResolveInvestigationLimits(nil)
	if err != nil {
		t.Fatalf("ResolveInvestigationLimits() error = %v", err)
	}

	if got.Depth != 1 || got.EntityLimit != 50 || got.Sources != nil {
		t.Fatalf("limits = %#v, want depth=1 entity_limit=50 and nil sources", got)
	}
}

func TestResolveInvestigationLimitsOverridesAndCopiesSources(t *testing.T) {
	depth := 2
	entityLimit := 75
	sources := []string{"fixture", "icij"}
	input := &generated.InvestigationLimits{
		Depth:       &depth,
		EntityLimit: &entityLimit,
		Sources:     &sources,
	}

	got, err := investigationHandler().ResolveInvestigationLimits(input)
	if err != nil {
		t.Fatalf("ResolveInvestigationLimits() error = %v", err)
	}
	sources[0] = "changed"

	if got.Depth != depth || got.EntityLimit != entityLimit {
		t.Fatalf("limits = %#v, want explicit numeric values", got)
	}
	if !reflect.DeepEqual(got.Sources, []string{"fixture", "icij"}) {
		t.Fatalf("sources = %v, want independent copy", got.Sources)
	}
}

func TestResolveInvestigationLimitsRejectsValuesOutsidePolicy(t *testing.T) {
	tests := []struct {
		name        string
		depth       *int
		entityLimit *int
	}{
		{name: "negative depth", depth: intPointer(-1)},
		{name: "depth over configured maximum", depth: intPointer(3)},
		{name: "non-positive entity limit", entityLimit: intPointer(0)},
		{name: "entity limit over configured maximum", entityLimit: intPointer(101)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := investigationHandler().ResolveInvestigationLimits(&generated.InvestigationLimits{
				Depth: tt.depth, EntityLimit: tt.entityLimit,
			})
			if err == nil {
				t.Fatal("ResolveInvestigationLimits() error = nil, want validation error")
			}
		})
	}
}

func TestResolveInvestigationLimitsRejectsInvalidSources(t *testing.T) {
	tests := []struct {
		name    string
		sources []string
	}{
		{name: "empty list", sources: []string{}},
		{name: "empty value", sources: []string{""}},
		{name: "duplicate value", sources: []string{"fixture", "fixture"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := investigationHandler().ResolveInvestigationLimits(&generated.InvestigationLimits{
				Sources: &tt.sources,
			})
			if err == nil {
				t.Fatal("ResolveInvestigationLimits() error = nil, want sources validation error")
			}
		})
	}
}

func intPointer(value int) *int { return &value }
