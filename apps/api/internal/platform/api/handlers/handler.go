package handlers

import (
	"GraphX/internal/platform/application"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Handler struct {
	application *application.Application
}

func NewHandler(application *application.Application) *Handler {
	return &Handler{
		application: application,
	}
}

func (h *Handler) Template(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "work"})
}

func (h *Handler) HealthCheckHadler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) IsReadyHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ready", "checks": gin.H{}})
}
