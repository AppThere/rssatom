// Package subscriber handles incoming subscription requests, typically via a message queue.
package subscriber

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/appthere/social-ingest-rssatom/internal/messaging"
	"github.com/appthere/social-ingest-rssatom/internal/subscription" // Corrected import path
)

// subscriptionRequest defines the expected structure of a message requesting a new subscription.
type subscriptionRequest struct {
	FeedURL string `json:"feed_url"` // The URL of the RSS/Atom feed to subscribe to.
	UserID  int64  `json:"user_id"`  // The ID of the user requesting the subscription.
	// Add other fields if needed, e.g., requested_interval, metadata...
}

// SubscriptionHandler listens for and processes subscription requests.
type SubscriptionHandler struct {
	subManager *subscription.Manager // Use the subscription manager
	mqClient   messaging.Client      // MQ client interface
	queueName  string                // Name of the queue to listen on
	logger     *slog.Logger
}

// NewSubscriptionHandler creates a new handler for subscription requests.
func NewSubscriptionHandler(
	subManager *subscription.Manager,
	mqClient messaging.Client,
	queueName string, // Pass the specific queue name from config
	logger *slog.Logger,
) *SubscriptionHandler {
	if queueName == "" {
		// Log a warning or handle as fatal depending on requirements
		logger.Warn("Subscription queue name is empty, subscriber might not function correctly")
	}
	return &SubscriptionHandler{
		subManager: subManager,
		mqClient:   mqClient,
		queueName:  queueName,
		logger:     logger.With(slog.String("component", "subscriber")),
	}
}

// Start begins listening for subscription requests on the configured message queue.
// It blocks until the context is cancelled or an unrecoverable error occurs during setup.
func (h *SubscriptionHandler) Start(ctx context.Context) error {
	h.logger.Info("Starting subscription handler", slog.String("queue", h.queueName))

	// Use the mqClient's Subscribe method. This method should handle the
	// underlying consumption loop and call our handleSubscriptionRequest for each message.
	// It's expected to run the handler processing in background goroutines.
	err := h.mqClient.Subscribe(ctx, h.queueName, h.handleSubscriptionRequest)
	if err != nil {
		h.logger.Error("Failed to start subscription consumer", slog.String("queue", h.queueName), slog.Any("error", err))
		return fmt.Errorf("failed to subscribe to queue %s: %w", h.queueName, err)
	}

	h.logger.Info("Subscription consumer started successfully", slog.String("queue", h.queueName))

	// Wait for the context to be cancelled, signaling shutdown.
	// The actual message processing happens in the goroutine(s) managed by mqClient.Subscribe.
	<-ctx.Done()

	h.logger.Info("Context cancelled, subscription handler shutting down.", slog.Any("error", ctx.Err()))
	// mqClient.Close() is handled by the defer in main.go
	return ctx.Err() // Return context error to indicate why it stopped
}

// handleSubscriptionRequest is the callback function passed to mqClient.Subscribe.
// It processes a single incoming subscription request message.
// Returning an error will typically cause the message to be NACKed (negatively acknowledged).
// Returning nil will typically cause the message to be ACKed (acknowledged).
func (h *SubscriptionHandler) handleSubscriptionRequest(ctx context.Context, msgBody []byte) error {
	msgLog := h.logger.With(slog.String("queue", h.queueName)) // Add queue context
	msgLog.Debug("Received subscription request message", slog.Int("body_size", len(msgBody)))

	var req subscriptionRequest
	if err := json.Unmarshal(msgBody, &req); err != nil {
		// If the message is malformed, we cannot process it.
		// Log the error but ACK the message (return nil) to remove it from the queue.
		// Requeuing malformed messages usually leads to infinite loops.
		msgLog.Error("Failed to unmarshal subscription request JSON", slog.Any("error", err), slog.String("raw_body", string(msgBody)))
		return nil // ACK malformed message
	}

	// Add request details to logger for subsequent messages
	msgLog = msgLog.With(slog.String("feed_url", req.FeedURL), slog.Int64("user_id", req.UserID))

	// Basic validation
	if req.FeedURL == "" {
		msgLog.Warn("Received subscription request with empty feed URL. Discarding.")
		return nil // ACK invalid message
	}
	if req.UserID <= 0 {
		msgLog.Warn("Received subscription request with invalid user ID. Discarding.")
		return nil // ACK invalid message
	}

	msgLog.Info("Processing parsed subscription request")

	// Call the subscription manager to handle the core logic
	feedID, isNew, err := h.subManager.Subscribe(ctx, req.FeedURL, req.UserID)
	if err != nil {
		// An error occurred during the subscription process (e.g., storage error, invalid URL after normalization).
		// Log the error and NACK the message (return error).
		// The messaging client implementation should decide whether to requeue (usually false for such errors).
		msgLog.Error("Failed to process subscription via manager", slog.Any("error", err))
		return err // NACK the message
	}

	// Subscription processed successfully (either new or existing)
	if isNew {
		msgLog.Info("Successfully processed new feed subscription", slog.Int64("feed_id", feedID))
	} else {
		msgLog.Info("Successfully processed subscription for existing feed", slog.Int64("feed_id", feedID))
	}

	return nil // ACK the message
}
