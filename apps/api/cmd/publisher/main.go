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
			logger.Error("close RabbitMQ publisher", publisherCloseLogFields()...)
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

	metrics, err := newPublisherMetrics()
	if err != nil {
		return fmt.Errorf("initialize publisher metrics: %w", err)
	}
	metricsServer, metricsServerErr, err := startPublisherMetricsServer(
		cfg.PublisherMetricsAddr,
		metrics,
	)
	if err != nil {
		return fmt.Errorf("start publisher metrics server: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("stop publisher metrics server", "error", err)
		}
	}()

	logger.Info("outbox publisher started")
	ticker := time.NewTicker(publisherPollInterval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			logger.Info("outbox publisher stopping")
			return nil
		}
		select {
		case serveErr := <-metricsServerErr:
			if serveErr == nil || errors.Is(serveErr, http.ErrServerClosed) {
				return errors.New("publisher metrics server stopped unexpectedly")
			}
			return fmt.Errorf("publisher metrics server failed: %w", serveErr)
		default:
		}
		publishStartedAt := time.Now()
		result, err := publisher.PublishOne(ctx)
		publishDuration := time.Since(publishStartedAt)
		if err != nil {
			if ctx.Err() != nil {
				logger.Info("outbox publisher stopping")
				return nil
			}
			if outbox.IsDeferred(err) {
				metrics.record("deferred", publishDuration, result)
				metrics.deferred++
				logger.Warn("outbox message deferred", publisherLogFields(
					result,
					deferredReason(err),
					metrics,
				)...)
				continue
			}
			metrics.record("failed", publishDuration, result)
			metrics.failed++
			logger.Error("outbox publisher processing failed", publisherLogFields(
				result,
				"internal_failure",
				metrics,
			)...)
			return errors.New("outbox publisher processing failed")
		}
		if result.Published {
			metrics.record("published", publishDuration, result)
			metrics.published++
			logger.Info("outbox message published", publisherLogFields(
				result,
				"published",
				metrics,
			)...)
		}
		if result.Processed {
			continue
		}

		select {
		case <-ctx.Done():
		case <-ticker.C:
		case serveErr := <-metricsServerErr:
			if serveErr == nil || errors.Is(serveErr, http.ErrServerClosed) {
				return errors.New("publisher metrics server stopped unexpectedly")
			}
			return fmt.Errorf("publisher metrics server failed: %w", serveErr)
		}
	}
}

func deferredReason(err error) string {
	if errors.Is(err, outbox.ErrLeaseLost) {
		return "lease_lost"
	}
	if errors.Is(err, outbox.ErrProcessingTimeout) {
		return "processing_timeout"
	}
	return "delivery_failed"
}

func publisherLogFields(
	result outbox.PublishResult,
	outcome string,
	metrics *publisherMetrics,
) []any {
	return []any{
		"outcome", outcome,
		"message_id", result.MessageID,
		"job_id", result.JobID,
		"investigation_id", result.InvestigationID,
		"correlation_id", result.CorrelationID,
		"retry_after", result.RetryAfter,
		"outbox_published_total", metrics.published,
		"outbox_deferred_total", metrics.deferred,
		"outbox_failed_total", metrics.failed,
	}
}

func publisherCloseLogFields() []any {
	return []any{"outcome", "close_failed"}
}
