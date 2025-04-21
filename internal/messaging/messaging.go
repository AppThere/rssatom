// Package messaging defines interfaces and implementations for interacting
// with a message queue broker.
package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/appthere/social-ingest-rssatom/internal/config" // To use config types

	amqp "github.com/rabbitmq/amqp091-go" // RabbitMQ client library
)

// HandlerFunc defines the function signature for processing received messages.
// It should return nil if the message was processed successfully (for ACK),
// or an error if processing failed (for NACK).
type HandlerFunc func(ctx context.Context, msgBody []byte) error

// Client defines the interface for message queue operations.
type Client interface {
	// Publish sends a message to a specific topic/routing key.
	// The interpretation of topicOrRoutingKey depends on the underlying MQ implementation
	// and exchange configuration (e.g., queue name for default exchange, topic pattern for topic exchange).
	Publish(ctx context.Context, topicOrRoutingKey string, message []byte) error

	// Subscribe listens for messages on a specific queue and calls the handler for each message.
	// This function typically starts a background goroutine and returns immediately.
	// The provided context should be used to signal when the subscription should stop.
	Subscribe(ctx context.Context, queueName string, handler HandlerFunc) error

	// Close gracefully shuts down the connection to the message queue.
	Close() error
}

// --- RabbitMQ Implementation ---

// rabbitMQClient implements the Client interface using RabbitMQ.
type rabbitMQClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	cfg     config.MessageQueueConfig
	logger  *slog.Logger
	mu      sync.RWMutex // Protects channel/connection state if needed (e.g., during reconnect attempts - not implemented here)
}

// NewClient creates a new message queue client (RabbitMQ implementation).
// It establishes a connection and channel.
func NewClient(cfg config.MessageQueueConfig, logger *slog.Logger) (Client, error) {
	log := logger.With(slog.String("component", "messaging_client"), slog.String("broker_type", "rabbitmq"))
	log.Info("Connecting to RabbitMQ", slog.String("url", cfg.URL))

	// TODO: Add connection retry logic for production robustness.
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		log.Error("Failed to connect to RabbitMQ", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	// TODO: Add channel recovery logic for production robustness.
	channel, err := conn.Channel()
	if err != nil {
		log.Error("Failed to open a channel", slog.Any("error", err))
		// Attempt to close connection if channel opening failed
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	log.Info("Successfully connected to RabbitMQ and opened channel")

	// Optional: Handle connection/channel closures asynchronously
	// go func() {
	// 	<-conn.NotifyClose(make(chan *amqp.Error))
	// 	log.Warn("RabbitMQ connection closed")
	// 	// Trigger reconnection logic here
	// }()
	// go func() {
	// 	<-channel.NotifyClose(make(chan *amqp.Error))
	// 	log.Warn("RabbitMQ channel closed")
	// 	// Trigger channel reopening or full reconnection here
	// }()

	return &rabbitMQClient{
		conn:    conn,
		channel: channel,
		cfg:     cfg,
		logger:  log,
	}, nil
}

// Publish sends a message using the RabbitMQ channel.
// Assumes default exchange ("") where routingKey is the queue name,
// unless exchange configuration is added later.
func (c *rabbitMQClient) Publish(ctx context.Context, routingKey string, message []byte) error {
	c.mu.RLock() // Read lock might be sufficient if channel isn't replaced often
	ch := c.channel
	c.mu.RUnlock()

	if ch == nil {
		return errors.New("cannot publish: RabbitMQ channel is not open")
	}

	// TODO: Make exchange name configurable if not using default exchange.
	exchangeName := "" // Default exchange

	c.logger.Debug("Publishing message",
		slog.String("exchange", exchangeName),
		slog.String("routing_key", routingKey),
		slog.Int("size_bytes", len(message)),
	)

	err := ch.PublishWithContext(ctx,
		exchangeName, // exchange
		routingKey,   // routing key (often queue name for default exchange)
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "application/json", // Assume JSON, make configurable if needed
			Body:         message,
			DeliveryMode: amqp.Persistent, // Make messages persistent
			Timestamp:    time.Now(),
		})

	if err != nil {
		c.logger.Error("Failed to publish message",
			slog.String("routing_key", routingKey),
			slog.Any("error", err),
		)
		// TODO: Consider adding retry logic here for specific publish errors.
		return fmt.Errorf("failed to publish message to %s: %w", routingKey, err)
	}

	return nil
}

// Subscribe starts consuming messages from a specified queue.
// It runs the handler in a separate goroutine and handles ACKs/NACKs.
func (c *rabbitMQClient) Subscribe(ctx context.Context, queueName string, handler HandlerFunc) error {
	c.mu.RLock()
	ch := c.channel
	c.mu.RUnlock()

	if ch == nil {
		return errors.New("cannot subscribe: RabbitMQ channel is not open")
	}

	// Declare the queue to ensure it exists. Make it durable.
	// TODO: Make durability and other queue arguments configurable.
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		c.logger.Error("Failed to declare queue", slog.String("queue", queueName), slog.Any("error", err))
		return fmt.Errorf("failed to declare queue %s: %w", queueName, err)
	}
	c.logger.Info("Declared queue for subscription", slog.String("queue", q.Name), slog.Int("messages", q.Messages), slog.Int("consumers", q.Consumers))

	// Set Quality of Service (prefetch count). Limits the number of unacknowledged messages per consumer.
	// TODO: Make prefetch count configurable.
	prefetchCount := 1 // Process one message at a time
	err = ch.Qos(
		prefetchCount, // prefetch count
		0,             // prefetch size (0 means no specific limit)
		false,         // global (false applies per consumer)
	)
	if err != nil {
		c.logger.Error("Failed to set QoS", slog.String("queue", queueName), slog.Any("error", err))
		return fmt.Errorf("failed to set QoS for queue %s: %w", queueName, err)
	}
	c.logger.Info("Set QoS", slog.String("queue", q.Name), slog.Int("prefetch_count", prefetchCount))

	// Start consuming messages
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer tag (empty for auto-generated)
		false,  // auto-ack (MANUAL ACKNOWLEDGEMENT)
		false,  // exclusive
		false,  // no-local (not supported by RabbitMQ)
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		c.logger.Error("Failed to register consumer", slog.String("queue", queueName), slog.Any("error", err))
		return fmt.Errorf("failed to register consumer for queue %s: %w", queueName, err)
	}

	c.logger.Info("Started consumer", slog.String("queue", q.Name))

	// Start a goroutine to process deliveries
	go func() {
		subLog := c.logger.With(slog.String("queue", q.Name))
		subLog.Info("Subscription goroutine started, waiting for messages...")
		for {
			select {
			case <-ctx.Done(): // Check if the context has been cancelled
				subLog.Info("Context cancelled, stopping subscription goroutine.", slog.Any("error", ctx.Err()))
				// Optional: Attempt to gracefully cancel the consumer?
				// err := ch.Cancel(consumerTag, false) // Need to store consumerTag if using this
				return // Exit the goroutine

			case d, ok := <-msgs:
				if !ok {
					subLog.Warn("Message channel closed unexpectedly. Stopping subscription goroutine.")
					// This might happen if the channel or connection closes.
					// Reconnection logic would be needed here.
					return // Exit the goroutine
				}

				subLog.Debug("Received message", slog.Uint64("delivery_tag", d.DeliveryTag), slog.Int("body_size", len(d.Body)))

				// Create a context for the handler, potentially with a timeout
				// handlerCtx, handlerCancel := context.WithTimeout(ctx, 30*time.Second) // Example timeout
				handlerCtx := ctx // Or just use the main subscription context

				// Process the message using the provided handler
				err := handler(handlerCtx, d.Body)

				// handlerCancel() // Cancel handler context if timeout was used

				if err == nil {
					// Message processed successfully, acknowledge it
					ackErr := d.Ack(false) // false = ack single message
					if ackErr != nil {
						subLog.Error("Failed to ACK message", slog.Uint64("delivery_tag", d.DeliveryTag), slog.Any("error", ackErr))
						// Consider how to handle ACK failures (e.g., log, potentially retry later?)
					} else {
						subLog.Debug("Message ACKed", slog.Uint64("delivery_tag", d.DeliveryTag))
					}
				} else {
					// Message processing failed, negatively acknowledge it
					subLog.Error("Handler failed to process message", slog.Uint64("delivery_tag", d.DeliveryTag), slog.Any("error", err))
					// Decide whether to requeue the message. Requeuing can lead to infinite loops
					// if the error is permanent. Usually set to false for permanent errors.
					// TODO: Make requeue logic configurable or based on error type.
					shouldRequeue := false
					nackErr := d.Nack(false, shouldRequeue) // false = nack single message
					if nackErr != nil {
						subLog.Error("Failed to NACK message", slog.Uint64("delivery_tag", d.DeliveryTag), slog.Any("error", nackErr))
					} else {
						subLog.Debug("Message NACKed", slog.Uint64("delivery_tag", d.DeliveryTag), slog.Bool("requeue", shouldRequeue))
					}
				}
			}
		}
	}()

	return nil // Subscription setup successful, handler runs in background
}

// Close shuts down the channel and connection.
func (c *rabbitMQClient) Close() error {
	c.mu.Lock() // Use exclusive lock for closing
	defer c.mu.Unlock()

	var firstErr error

	// Close the channel
	if c.channel != nil {
		c.logger.Info("Closing RabbitMQ channel...")
		if err := c.channel.Close(); err != nil {
			c.logger.Error("Failed to close RabbitMQ channel", slog.Any("error", err))
			firstErr = fmt.Errorf("failed to close channel: %w", err)
		}
		c.channel = nil // Mark as closed
	}

	// Close the connection
	if c.conn != nil {
		c.logger.Info("Closing RabbitMQ connection...")
		if err := c.conn.Close(); err != nil {
			c.logger.Error("Failed to close RabbitMQ connection", slog.Any("error", err))
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to close connection: %w", err)
			}
		}
		c.conn = nil // Mark as closed
	}

	if firstErr == nil {
		c.logger.Info("RabbitMQ connection and channel closed successfully.")
	}

	return firstErr
}

// Ensure implementation satisfies the interface
var _ Client = (*rabbitMQClient)(nil)
