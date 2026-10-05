package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/httpserver"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/logging"
)

func main() {
	cfg, err := config.LoadAPI()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	logger := logging.New("graphx-api", cfg.LogLevel)
	slog.SetDefault(logger)

	server := httpserver.New(cfg, logger)
	errCh := make(chan error, 1)
	go func() {
		logger.Info("api started", "address", cfg.HTTPAddr, "environment", cfg.Environment)
		errCh <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("api shutdown failed", "error", err)
			os.Exit(1)
		}
		logger.Info("api stopped")
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("api stopped unexpectedly", "error", err)
			os.Exit(1)
		}
	}
}
