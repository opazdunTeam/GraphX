package router

import (
	"GraphX/internal/platform/api/handlers"
	"GraphX/internal/platform/api/middlewares"
	"GraphX/internal/platform/config"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

func NewServer(cfg config.Config, logger *slog.Logger, h *handlers.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.Server.Host,
		Handler:           NewRouter(logger, h),
		ReadHeaderTimeout: time.Duration(cfg.Server.Read_header_timeout) * time.Second,
		ReadTimeout:       time.Duration(cfg.Server.Read_timeout) * time.Second,
		WriteTimeout:      time.Duration(cfg.Server.Write_timeout) * time.Second,
		IdleTimeout:       time.Duration(cfg.Server.Idle_timeout) * time.Second,
	}
}

func NewRouter(logger *slog.Logger, h *handlers.Handler) http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(
		middlewares.RequestIDMiddleware(),
		middlewares.RecoverMiddleware(logger),
		middlewares.LoggerMiddleware(logger),
	)

	router.GET("/health/live", h.HealthCheckHadler)
	router.GET("/health/ready", h.IsReadyHandler)

	router.POST("/investigations", h.Template)
	router.GET("/investigations/{id}", h.Template)
	router.GET("/investigations/{id}/status", h.Template)
	router.POST("POST /investigations/{id}/cancel", h.Template)
	router.GET("/claims/{id}/evidence", h.Template)
	router.GET("/artifacts/{id}/content", h.Template)
	router.GET("/sources")

	return router
}
