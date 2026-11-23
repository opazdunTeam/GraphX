package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/opazdunTeam/GraphX/apps/api/internal/outbox"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	CommandExchange       = "commands.topic"
	publisherDialTimeout  = 5 * time.Second
	publisherCloseTimeout = time.Second
)

type ConfirmPublisher struct {
	mu       sync.Mutex
	url      string
	exchange string
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
	closed   <-chan *amqp.Error
}

func NewConfirmPublisher(ctx context.Context, url, exchange string) (*ConfirmPublisher, error) {
	if strings.TrimSpace(url) == "" {
		return nil, errors.New("RabbitMQ URL is required")
	}
	if strings.TrimSpace(exchange) == "" {
		return nil, errors.New("RabbitMQ command exchange is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &ConfirmPublisher{url: url, exchange: exchange}, nil
}

type SchemaValidatingPublisher struct {
	registry *SchemaRegistry
	next     outbox.Broker
}

func NewSchemaValidatingPublisher(registry *SchemaRegistry, next outbox.Broker) (*SchemaValidatingPublisher, error) {
	if registry == nil {
		return nil, errors.New("RabbitMQ schema registry is required")
	}
	if next == nil {
		return nil, errors.New("RabbitMQ publisher is required")
	}
	return &SchemaValidatingPublisher{registry: registry, next: next}, nil
}

func (p *SchemaValidatingPublisher) Publish(ctx context.Context, message outbox.Message) error {
	validated, err := p.registry.ValidateMessage(message.Payload)
	if err != nil {
		return fmt.Errorf("validate outbox message %s: %w", message.ID, err)
	}
	if validated.Type != message.Type {
		return fmt.Errorf(
			"outbox message type %q does not match payload type %q",
			message.Type,
			validated.Type,
		)
	}
	return p.next.Publish(ctx, message)
}

func (p *ConfirmPublisher) Publish(ctx context.Context, message outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	if p.channel == nil || p.channel.IsClosed() || p.conn == nil || p.conn.IsClosed() {
		p.reset()
		if err := p.connect(ctx); err != nil {
			return err
		}
	}

	channel := p.channel
	connection := p.conn
	seqNo := channel.GetNextPublishSeqNo()
	stopCloseOnCancel := context.AfterFunc(ctx, func() {
		abortConnection(connection)
	})
	publishErr := channel.PublishWithContext(
		ctx,
		p.exchange,
		message.Type,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    message.ID,
			Type:         message.Type,
			AppId:        "graphx-api",
			Body:         message.Payload,
		},
	)
	if publishErr != nil {
		stopCloseOnCancel()
		p.reset()
		return fmt.Errorf("send RabbitMQ message: %w", publishErr)
	}

	var returned *amqp.Return
	for {
		select {
		case confirmation, ok := <-p.confirms:
			if !ok {
				stopCloseOnCancel()
				p.reset()
				return errors.New("RabbitMQ publisher confirmation channel closed")
			}
			if confirmation.DeliveryTag != seqNo {
				stopCloseOnCancel()
				p.reset()
				return fmt.Errorf(
					"RabbitMQ confirmation tag %d does not match message tag %d",
					confirmation.DeliveryTag,
					seqNo,
				)
			}
			select {
			case deliveryReturn, ok := <-p.returns:
				if !ok {
					stopCloseOnCancel()
					p.reset()
					return errors.New("RabbitMQ returned-message channel closed")
				}
				if deliveryReturn.MessageId == message.ID {
					copy := deliveryReturn
					returned = &copy
				}
			default:
			}
			if !confirmation.Ack {
				stopCloseOnCancel()
				return errors.New("RabbitMQ rejected message with a negative publisher confirmation")
			}
			if returned != nil {
				stopCloseOnCancel()
				return fmt.Errorf(
					"RabbitMQ could not route message %s: %s",
					message.ID,
					returned.ReplyText,
				)
			}
			stopCloseOnCancel()
			return nil
		case deliveryReturn, ok := <-p.returns:
			if !ok {
				stopCloseOnCancel()
				p.reset()
				return errors.New("RabbitMQ returned-message channel closed")
			}
			if deliveryReturn.MessageId == message.ID {
				copy := deliveryReturn
				returned = &copy
			}
		case closeErr, ok := <-p.closed:
			stopCloseOnCancel()
			p.reset()
			if ok && closeErr != nil {
				return fmt.Errorf("RabbitMQ connection closed before confirmation: %s", closeErr.Reason)
			}
			return errors.New("RabbitMQ connection closed before confirmation")
		case <-ctx.Done():
			stopCloseOnCancel()
			p.reset()
			return ctx.Err()
		}
	}
}

func (p *ConfirmPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var closeErrors []error
	if p.conn != nil {
		if err := p.conn.CloseDeadline(time.Now().Add(publisherCloseTimeout)); err != nil &&
			!errors.Is(err, amqp.ErrClosed) {
			closeErrors = append(closeErrors, fmt.Errorf("close RabbitMQ publisher connection: %w", err))
		}
	}
	p.channel = nil
	p.conn = nil
	p.confirms = nil
	p.returns = nil
	p.closed = nil
	return errors.Join(closeErrors...)
}

func (p *ConfirmPublisher) connect(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: publisherDialTimeout}
	var transportMu sync.Mutex
	var transport net.Conn
	stopCloseOnCancel := context.AfterFunc(ctx, func() {
		transportMu.Lock()
		defer transportMu.Unlock()
		if transport != nil {
			_ = transport.Close()
		}
	})
	defer stopCloseOnCancel()
	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			opened, dialErr := dialer.DialContext(ctx, network, address)
			if dialErr != nil {
				return nil, dialErr
			}
			if err := opened.SetDeadline(connectionDeadline(ctx)); err != nil {
				_ = opened.Close()
				return nil, err
			}
			transportMu.Lock()
			transport = opened
			transportMu.Unlock()
			if err := ctx.Err(); err != nil {
				_ = opened.Close()
				return nil, err
			}
			return opened, nil
		},
	})
	if err != nil {
		transportMu.Lock()
		if transport != nil {
			_ = transport.Close()
			transport = nil
		}
		transportMu.Unlock()
		return fmt.Errorf("connect RabbitMQ publisher: %w", err)
	}
	if err := ctx.Err(); err != nil {
		abortConnection(conn)
		return err
	}
	transportMu.Lock()
	if transport != nil {
		err = transport.SetDeadline(connectionDeadline(ctx))
	}
	transportMu.Unlock()
	if err != nil {
		abortConnection(conn)
		return fmt.Errorf("bound RabbitMQ channel setup: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		abortConnection(conn)
		return fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		abortConnection(conn)
		return fmt.Errorf("enable RabbitMQ publisher confirmations: %w", err)
	}
	transportMu.Lock()
	if transport != nil {
		err = transport.SetDeadline(time.Time{})
	}
	transportMu.Unlock()
	if err != nil {
		abortConnection(conn)
		return fmt.Errorf("clear RabbitMQ setup deadline: %w", err)
	}
	if err := ctx.Err(); err != nil {
		abortConnection(conn)
		return err
	}

	p.conn = conn
	p.channel = channel
	p.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	p.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	p.closed = conn.NotifyClose(make(chan *amqp.Error, 1))
	return nil
}

func (p *ConfirmPublisher) reset() {
	if p.conn != nil {
		_ = p.conn.CloseDeadline(time.Now().Add(publisherCloseTimeout))
	}
	p.channel = nil
	p.conn = nil
	p.confirms = nil
	p.returns = nil
	p.closed = nil
}

func connectionDeadline(ctx context.Context) time.Time {
	deadline := time.Now().Add(publisherDialTimeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		return contextDeadline
	}
	return deadline
}

func abortConnection(connection *amqp.Connection) {
	if connection == nil || connection.IsClosed() {
		return
	}
	_ = connection.CloseDeadline(time.Now())
}
