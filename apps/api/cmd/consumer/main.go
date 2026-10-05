package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/logging"
)

func main() {
	cfg, err := config.LoadProcess()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}

	logger := logging.New("graphx-consumer", cfg.LogLevel)
	logger.Info("consumer scaffold started", "environment", cfg.Environment, "mode", "idle_until_messaging_runtime_is_added")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	logger.Info("consumer scaffold stopped")
}
