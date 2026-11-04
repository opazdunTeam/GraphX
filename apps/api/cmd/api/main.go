package main

// import (
// 	"GraphX/internal/platform/api/router"
// 	"GraphX/internal/platform/config"
// 	"GraphX/internal/platform/logging"
// 	"context"
// 	"errors"
// 	"log/slog"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"syscall"
// )

// func main() {
// 	cfg, err := config.LoadAPI()
// 	if err != nil {
// 		slog.Error("invalid configuration", "error", err)
// 		os.Exit(1)
// 	}

// 	logger := logging.New("graphx-api", cfg.LogLevel)
// 	slog.SetDefault(logger)

// 	server := router.New(cfg, logger)
// 	errCh := make(chan error, 1)
// 	go func() {
// 		logger.Info("api started", "address", cfg.HTTPAddr, "environment", cfg.Env)
// 		errCh <- server.ListenAndServe()
// 	}()

// 	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
// 	defer stop()

// 	select {
// 	case <-ctx.Done():
// 		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
// 		defer cancel()
// 		if err := server.Shutdown(shutdownCtx); err != nil {
// 			logger.Error("api shutdown failed", "error", err)
// 			os.Exit(1)
// 		}
// 		logger.Info("api stopped")
// 	case err := <-errCh:
// 		if err != nil && !errors.Is(err, http.ErrServerClosed) {
// 			logger.Error("api stopped unexpectedly", "error", err)
// 			os.Exit(1)
// 		}
// 	}
// }
