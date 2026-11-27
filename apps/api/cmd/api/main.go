package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/opazdunTeam/GraphX/apps/api/internal/investigations"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/handlers"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/api/router"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/logging"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/rabbitmq"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/repository/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadAPI()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	logger := logging.New("graphx-api", cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)

	processCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Open(processCtx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	defer pool.Close()

	investigationStore := postgres.NewInvestigationStore(pool)
	schemaDir := strings.TrimSpace(os.Getenv("CONTRACTS_SCHEMA_DIR"))
	if schemaDir == "" {
		schemaDir = filepath.Join("..", "..", "contracts", "schemas")
	}
	schemaRegistry, err := rabbitmq.NewSchemaRegistry(schemaDir)
	if err != nil {
		return fmt.Errorf("load RabbitMQ message schemas: %w", err)
	}
	outboxFactory, err := rabbitmq.NewSourceCollectOutboxFactory(schemaRegistry)
	if err != nil {
		return fmt.Errorf("initialize source collect outbox factory: %w", err)
	}
	investigationCreator, err := investigations.NewCreator(investigationStore, outboxFactory)
	if err != nil {
		return fmt.Errorf("initialize investigation creator: %w", err)
	}
	handler := handlers.New(pool, cfg.Postgres.HealthTimeout, cfg.Investigation, investigationStore, investigationCreator)
	server := router.NewServer(cfg, logger, handler)

	errCh := make(chan error, 1)
	go func() {
		logger.Info(
			"api started",
			"address", cfg.HTTP.Addr,
			"environment", cfg.Environment,
		)
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-processCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		logger.Info("api stopped")
		return nil

	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	}
}
