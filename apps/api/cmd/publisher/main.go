package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/config"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/logging"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/rabbitmq"
	"github.com/opazdunTeam/GraphX/apps/api/internal/platform/repository/postgres"
)

const publisherPollInterval = time.Second

func main() {
	if err := run(); err != nil {
		slog.Error("outbox publisher stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadPublisher()
	if err != nil {
		return fmt.Errorf("invalid publisher configuration: %w", err)
	}

	logger := logging.New("graphx-publisher", cfg.LogLevel, cfg.LogFormat)
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := postgres.Open(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("initialize PostgreSQL: %w", err)
	}
	defer pool.Close()

	broker, err := rabbitmq.NewConfirmPublisher(ctx, cfg.RabbitMQ.URL, rabbitmq.CommandExchange)
	if err != nil {
		return fmt.Errorf("initialize RabbitMQ publisher: %w", err)
	}
	defer func() {
		if err := broker.Close(); err != nil {
			logger.Error("close RabbitMQ publisher", "error", err)
		}
	}()

	schemaDir := strings.TrimSpace(os.Getenv("CONTRACTS_SCHEMA_DIR"))
	if schemaDir == "" {
		schemaDir = filepath.Join("..", "..", "contracts", "schemas")
	}
	registry, err := rabbitmq.NewSchemaRegistry(schemaDir)
	if err != nil {
		return fmt.Errorf("load RabbitMQ message schemas: %w", err)
	}
	validatingBroker, err := rabbitmq.NewSchemaValidatingPublisher(registry, broker)
	if err != nil {
		return fmt.Errorf("initialize schema-validating publisher: %w", err)
	}

	publisher, err := outbox.NewPublisher(
		postgres.NewOutboxStore(pool),
		validatingBroker,
		outbox.DefaultConfig(),
	)
	if err != nil {
		return fmt.Errorf("initialize outbox publisher: %w", err)
	}

	logger.Info("outbox publisher started")
	ticker := time.NewTicker(publisherPollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			logger.Info("outbox publisher stopping")
			return nil
		}
		processed, err := publisher.PublishOne(ctx)
		if err != nil {
			if outbox.IsDeferred(err) {
				logger.Warn("outbox message deferred", "error", err)
				continue
			}
			return fmt.Errorf("publish outbox message: %w", err)
		}
		if processed {
			continue
		}

		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}
