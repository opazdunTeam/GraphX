package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/ingestion"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	EventsExchange          = "events.topic"
	DeadLetterExchange      = "dead-letter.topic"
	CoreEventsQueue         = "core.events.q"
	CoreEventsDeadLetter    = "core.events.dlq"
	CoreEventsDeadLetterKey = "core.events.dlq"
)

type SourceResultHandler interface {
	Handle(
		context.Context,
		ingestion.ResultDeliveryMetadata,
		[]byte,
	) (ingestion.ResultConsumerOutcome, error)
}

type SourceResultConsumer struct {
	url            string
	connectTimeout time.Duration
	prefetch       int
	handler        SourceResultHandler
	logger         *slog.Logger
}

func NewSourceResultConsumer(
	url string,
	connectTimeout time.Duration,
	prefetch int,
	handler SourceResultHandler,
	logger *slog.Logger,
) (*SourceResultConsumer, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("RabbitMQ URL is required")
	}
	if connectTimeout <= 0 {
		return nil, errors.New("RabbitMQ connect timeout must be positive")
	}
	if prefetch < 1 || prefetch > 65535 {
		return nil, errors.New("RabbitMQ prefetch must be between 1 and 65535")
	}
	if handler == nil {
		return nil, errors.New("source result handler is required")
	}
	if logger == nil {
		return nil, errors.New("source result logger is required")
	}
	return &SourceResultConsumer{
		url:            url,
		connectTimeout: connectTimeout,
		prefetch:       prefetch,
		handler:        handler,
		logger:         logger,
	}, nil
}

func (c *SourceResultConsumer) Run(ctx context.Context) error {
	connection, err := amqp.DialConfig(c.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Dial: func(network, address string) (net.Conn, error) {
			return net.DialTimeout(network, address, c.connectTimeout)
		},
	})
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ consumer channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("set RabbitMQ source result prefetch: %w", err)
	}
	if err := channel.ExchangeDeclare(EventsExchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ events exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(DeadLetterExchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ dead-letter exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(CoreEventsDeadLetter, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare RabbitMQ core events dead-letter queue: %w", err)
	}
	if err := channel.QueueBind(CoreEventsDeadLetter, CoreEventsDeadLetterKey, DeadLetterExchange, false, nil); err != nil {
		return fmt.Errorf("bind RabbitMQ core events dead-letter queue: %w", err)
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    DeadLetterExchange,
		"x-dead-letter-routing-key": CoreEventsDeadLetterKey,
	}
	if _, err := channel.QueueDeclare(CoreEventsQueue, true, false, false, false, args); err != nil {
		return fmt.Errorf("declare RabbitMQ core events queue: %w", err)
	}
	for _, routingKey := range []string{
		SourceCollectCompletedType,
		SourceCollectFailedType,
	} {
		if err := channel.QueueBind(CoreEventsQueue, routingKey, EventsExchange, false, nil); err != nil {
			return fmt.Errorf("bind RabbitMQ core events queue: %w", err)
		}
	}
	deliveries, err := channel.ConsumeWithContext(
		ctx, CoreEventsQueue, "graphx-source-result-consumer", false, false, false, false, nil,
	)
	if err != nil {
		return fmt.Errorf("start RabbitMQ source result consumer: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return errors.New("RabbitMQ source result delivery channel closed")
			}
			if err := c.handleDelivery(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (c *SourceResultConsumer) handleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	outcome, err := c.handler.Handle(ctx, ingestion.ResultDeliveryMetadata{
		MessageID:  delivery.MessageId,
		Type:       delivery.Type,
		RoutingKey: delivery.RoutingKey,
	}, delivery.Body)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		permanent := ingestion.IsPermanentResultError(err)
		if permanent {
			c.logger.Warn(
				"source result rejected and sent to dead-letter queue",
				"message_id", delivery.MessageId,
				"message_type", delivery.Type,
				"routing_key", delivery.RoutingKey,
				"reason_code", ingestion.PermanentResultErrorCode(err),
			)
		} else {
			c.logger.Error(
				"source result processing failed and will be retried",
				"message_id", delivery.MessageId,
				"message_type", delivery.Type,
				"routing_key", delivery.RoutingKey,
			)
		}
		if nackErr := delivery.Nack(false, !permanent); nackErr != nil {
			return fmt.Errorf("reject RabbitMQ source result delivery: %w", nackErr)
		}
		return nil
	}

	if err := delivery.Ack(false); err != nil {
		return fmt.Errorf("acknowledge RabbitMQ source result delivery: %w", err)
	}
	c.logger.Info(
		"source result committed",
		"message_id", outcome.MessageID,
		"investigation_id", outcome.CorrelationID,
		"job_id", outcome.JobID,
		"attempt_no", outcome.AttemptNo,
		"status", outcome.Status,
		"applied", outcome.Applied,
		"duplicate", outcome.Duplicate,
		"ignored_reason", outcome.IgnoredReason,
	)
	return nil
}
