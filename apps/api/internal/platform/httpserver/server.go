package httpserver

import (
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func New(cfg config.API, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           NewRouter(logger),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func NewRouter(logger *slog.Logger) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery(), requestLogger(logger))

	router.GET("/health/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/health/ready", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ready", "checks": gin.H{}})
	})

	return router
}

func requestLogger(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		traceID := c.GetHeader("X-Request-ID")
		if traceID == "" {
			traceID = newTraceID()
		}
		c.Header("X-Request-ID", traceID)
		c.Next()
		logger.InfoContext(c.Request.Context(), "http request",
			"trace_id", traceID,
			"method", c.Request.Method,
			"path", c.FullPath(),
			"status", c.Writer.Status(),
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}
}

func newTraceID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "trace-id-unavailable"
	}
	return hex.EncodeToString(value)
}
