package router

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/generated"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/middlewares"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/utils"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
)

const businessBaseURL = "/api/v1"

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
	registerBusinessRoutes(router, h)

	return router
}

func registerServiceRoutes(router gin.IRoutes, h *handlers.Handler) {
	router.GET("/health/live", h.Liveness)
	router.GET("/health/ready", h.Readiness)
}

func registerBusinessRoutes(router gin.IRouter, h generated.ServerInterface) {
	generated.RegisterHandlersWithOptions(router, h, generated.GinServerOptions{
		BaseURL:      businessBaseURL,
		ErrorHandler: generatedErrorHandler,
	})
}

func generatedErrorHandler(c *gin.Context, _ error, statusCode int) {
	utils.WriteProblem(
		c,
		statusCode,
		"INVALID_REQUEST",
		http.StatusText(statusCode),
		"The request does not match the HTTP API contract.",
	)
}
