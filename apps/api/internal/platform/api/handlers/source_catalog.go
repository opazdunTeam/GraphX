package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/utils"
)

func (h *Handler) ListSources(c *gin.Context, _ generated.ListSourcesParams) {
	if h.sources == nil {
		h.notImplemented(c)
		return
	}
	sources, err := h.sources.ListSources(c.Request.Context())
	if err != nil {
		utils.WriteProblem(c, http.StatusInternalServerError, "INTERNAL_ERROR", "Internal server error", "The source catalog could not be loaded.")
		return
	}

	items := make([]generated.SourceCatalogItem, 0, len(sources))
	for _, source := range sources {
		items = append(items, generated.SourceCatalogItem{
			Code: source.Code, Name: source.Name, Mode: generated.SourceCatalogItemMode(source.Mode), Enabled: source.Enabled,
			EntityTypes: []generated.EntityType{generated.EntityTypePerson},
			Roles:       []generated.SourceCatalogItemRoles{generated.SourceCatalogItemRolesDiscovery},
			Health:      generated.SourceHealth{State: generated.SourceHealthStateUnknown},
		})
	}
	c.JSON(http.StatusOK, generated.SourceCatalog{Items: items, UpdatedAt: time.Now().UTC()})
}
