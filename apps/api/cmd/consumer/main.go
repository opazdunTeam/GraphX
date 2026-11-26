package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/logging"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/objectstore"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/rabbitmq"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/repository/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("source result consumer stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadConsumer()
	if err != nil {
		return fmt.Errorf("invalid source result consumer configuration: %w", err)
	}

	logger := logging.New("graphx-consumer", cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Open(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	defer pool.Close()

	schemaRoot := strings.TrimSpace(os.Getenv("CONTRACT_SCHEMA_ROOT"))
	if schemaRoot == "" {
		schemaRoot = filepath.Join("..", "..", "contracts", "schemas")
	}
	registry, err := rabbitmq.NewSchemaRegistry(schemaRoot)
	if err != nil {
		return fmt.Errorf("load result message schemas: %w", err)
	}

	manifestReader, err := objectstore.NewManifestReader(cfg.ObjectStore)
	if err != nil {
		return fmt.Errorf("initialize result manifest reader: %w", err)
	}
	defer manifestReader.Close()

	resultConsumer, err := ingestion.NewResultConsumer(
		registry,
		manifestReader,
		postgres.NewStore(pool),
		ingestion.ResultConsumerConfig{
			MaxMessageBytes:  cfg.Consumer.MaxMessageBytes,
			MaxManifestBytes: cfg.ObjectStore.MaxManifestBytes,
		},
	)
	if err != nil {
		return fmt.Errorf("initialize result ingestion: %w", err)
	}
	broker, err := rabbitmq.NewSourceResultConsumer(
		cfg.RabbitMQ.URL,
		cfg.Consumer.ConnectTimeout,
		cfg.Consumer.Prefetch,
		resultConsumer,
		logger,
	)
	if err != nil {
		return fmt.Errorf("initialize RabbitMQ source result consumer: %w", err)
	}

	logger.Info(
		"source result consumer started",
		"prefetch", cfg.Consumer.Prefetch,
	)
	if err := broker.Run(ctx); err != nil {
		if ctx.Err() != nil {
			logger.Info("source result consumer stopping")
			return nil
		}
		return fmt.Errorf("run source result consumer: %w", err)
	}
	if ctx.Err() != nil {
		logger.Info("source result consumer stopped")
		return nil
	}
	return errors.New("source result consumer stopped unexpectedly")
}
