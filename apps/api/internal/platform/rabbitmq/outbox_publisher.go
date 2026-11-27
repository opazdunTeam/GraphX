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

var (
	errNegativeConfirmation = errors.New("RabbitMQ rejected message with a negative publisher confirmation")
	errUnroutableMessage    = errors.New("RabbitMQ could not route message")
)

type ConfirmPublisher struct {
	mu        sync.Mutex
	url       string
	exchange  string
	conn      *amqp.Connection
	transport net.Conn
	channel   *amqp.Channel
	confirms  <-chan amqp.Confirmation
	returns   <-chan amqp.Return
	closed    <-chan *amqp.Error
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
	transport := p.transport
	seqNo := channel.GetNextPublishSeqNo()
	stopCloseOnCancel := context.AfterFunc(ctx, func() {
		_ = transport.Close()
	})
	defer stopCloseOnCancel()
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
		p.reset()
		return fmt.Errorf("send RabbitMQ message: %w", publishErr)
	}

	var returned *amqp.Return
	for {
		select {
		case confirmation, ok := <-p.confirms:
			if !ok {
				p.reset()
				return errors.New("RabbitMQ publisher confirmation channel closed")
			}
			if confirmation.DeliveryTag != seqNo {
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
				return errNegativeConfirmation
			}
			if returned != nil {
				return fmt.Errorf(
					"%w %s: %s",
					errUnroutableMessage,
					message.ID,
					returned.ReplyText,
				)
			}
			return nil
		case deliveryReturn, ok := <-p.returns:
			if !ok {
				p.reset()
				return errors.New("RabbitMQ returned-message channel closed")
			}
			if deliveryReturn.MessageId == message.ID {
				copy := deliveryReturn
				returned = &copy
			}
		case closeErr, ok := <-p.closed:
			p.reset()
			if ok && closeErr != nil {
				return fmt.Errorf("RabbitMQ connection closed before confirmation: %s", closeErr.Reason)
			}
			return errors.New("RabbitMQ connection closed before confirmation")
		case <-ctx.Done():
			p.reset()
			return ctx.Err()
		}
	}
}

func (p *ConfirmPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.reset()

	if p.conn == nil {
		return nil
	}
	transport := p.transport
	forceClose := time.AfterFunc(publisherCloseTimeout, func() {
		_ = transport.Close()
	})
	defer forceClose.Stop()
	if err := p.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		return fmt.Errorf("close RabbitMQ publisher connection: %w", err)
	}
	return nil
}

func (p *ConfirmPublisher) connect(ctx context.Context) error {
	setupCtx, cancel := context.WithTimeout(ctx, publisherDialTimeout)
	defer cancel()
	dialer := &net.Dialer{}
	var transport net.Conn
	var stopAbort func() bool
	connected := false
	defer func() {
		if stopAbort != nil {
			stopAbort()
		}
		if !connected && transport != nil {
			_ = transport.Close()
		}
	}()

	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial: func(network, address string) (net.Conn, error) {
			opened, err := dialer.DialContext(setupCtx, network, address)
			if err != nil {
				return nil, err
			}
			transport = opened
			stopAbort = context.AfterFunc(setupCtx, func() {
				_ = opened.Close()
			})
			return opened, nil
		},
	})
	if err != nil {
		return fmt.Errorf("connect RabbitMQ publisher: %w", err)
	}
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("open RabbitMQ publisher channel: %w", err)
	}
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable RabbitMQ publisher confirmations: %w", err)
	}
	stopAbort()
	if err := setupCtx.Err(); err != nil {
		return err
	}

	p.conn = conn
	p.transport = transport
	p.channel = channel
	p.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	p.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	p.closed = conn.NotifyClose(make(chan *amqp.Error, 1))
	connected = true
	return nil
}

func (p *ConfirmPublisher) reset() {
	if p.transport != nil {
		_ = p.transport.Close()
	}
	p.transport = nil
	p.channel = nil
	p.conn = nil
	p.confirms = nil
	p.returns = nil
	p.closed = nil
}
