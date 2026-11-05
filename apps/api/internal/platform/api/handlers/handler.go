package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/utils"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

type readinessChecker interface {
	Ping(context.Context) error
}

type Handler struct {
	readiness        readinessChecker
	readinessTimeout time.Duration
	investigation    config.Investigation
}

func New(readiness readinessChecker, readinessTimeout time.Duration, investigation config.Investigation) *Handler {
	return &Handler{
		readiness:        readiness,
		readinessTimeout: readinessTimeout,
		investigation:    investigation,
	}
}

func (h *Handler) Liveness(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) Readiness(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), h.readinessTimeout)
	defer cancel()

	if err := h.readiness.Ping(ctx); err != nil {
		utils.WriteProblem(
			c,
			http.StatusServiceUnavailable,
			"SERVICE_UNAVAILABLE",
			"Service unavailable",
			"A required dependency is not ready.",
		)
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "ready",
		"checks": gin.H{"postgres": "ready"},
	})
}
