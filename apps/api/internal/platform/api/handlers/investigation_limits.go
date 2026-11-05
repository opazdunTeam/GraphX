package handlers

import (
	"fmt"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
)

const (
	minimumInvestigationDepth       = 0
	minimumInvestigationEntityLimit = 1
)

type InvestigationLimits struct {
	Depth       int
	EntityLimit int
	Sources     []string
}

func (h *Handler) ResolveInvestigationLimits(input *generated.InvestigationLimits) (InvestigationLimits, error) {
	resolved := InvestigationLimits{
		Depth:       h.investigation.DefaultDepth,
		EntityLimit: h.investigation.DefaultEntityLimit,
	}
	if input == nil {
		return resolved, nil
	}

	if input.Depth != nil {
		if *input.Depth < minimumInvestigationDepth {
			return InvestigationLimits{}, fmt.Errorf("depth must not be less than %d", minimumInvestigationDepth)
		}
		if *input.Depth > h.investigation.MaxDepth {
			return InvestigationLimits{}, fmt.Errorf("depth must not exceed configured maximum of %d", h.investigation.MaxDepth)
		}
		resolved.Depth = *input.Depth
	}
	if input.EntityLimit != nil {
		if *input.EntityLimit < minimumInvestigationEntityLimit {
			return InvestigationLimits{}, fmt.Errorf("entity_limit must not be less than %d", minimumInvestigationEntityLimit)
		}
		if *input.EntityLimit > h.investigation.MaxEntityLimit {
			return InvestigationLimits{}, fmt.Errorf("entity_limit must not exceed configured maximum of %d", h.investigation.MaxEntityLimit)
		}
		resolved.EntityLimit = *input.EntityLimit
	}
	if input.Sources != nil {
		if len(*input.Sources) == 0 {
			return InvestigationLimits{}, fmt.Errorf("sources must contain at least one item")
		}
		seen := make(map[string]struct{}, len(*input.Sources))
		for _, source := range *input.Sources {
			if source == "" {
				return InvestigationLimits{}, fmt.Errorf("sources must not contain empty values")
			}
			if _, exists := seen[source]; exists {
				return InvestigationLimits{}, fmt.Errorf("sources must contain unique values")
			}
			seen[source] = struct{}{}
		}
		resolved.Sources = append([]string(nil), (*input.Sources)...)
	}

	return resolved, nil
}
