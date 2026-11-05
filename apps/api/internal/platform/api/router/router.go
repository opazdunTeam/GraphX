package router

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/middlewares"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

func NewServer(cfg config.Config, logger *slog.Logger, h *handlers.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           NewRouter(logger, h),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
	}
}

func NewRouter(logger *slog.Logger, h *handlers.Handler) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(
		middlewares.RequestIDMiddleware(),
		middlewares.LoggerMiddleware(logger),
		middlewares.RecoverMiddleware(logger),
	)

	registerServiceRoutes(router, h)
	registerBusinessRoutes(router.Group("/api/v1"), h)

	return router
}

func registerServiceRoutes(router gin.IRoutes, h *handlers.Handler) {
	router.GET("/health/live", h.Liveness)
	router.GET("/health/ready", h.Readiness)
}

func registerBusinessRoutes(router gin.IRoutes, h *handlers.Handler) {
	router.POST("/investigations", h.NotImplemented)
	router.GET("/investigations/:investigation_id", h.NotImplemented)
	router.GET("/investigations/:investigation_id/status", h.NotImplemented)
	router.POST("/investigations/:investigation_id/cancel", h.NotImplemented)
	router.GET("/claims/:claim_id/evidence", h.NotImplemented)
	router.GET("/artifacts/:artifact_id/content", h.NotImplemented)
	router.GET("/sources", h.NotImplemented)
}
