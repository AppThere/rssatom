// Package translation handles formatting new items and notifying downstream services,
// such as a Protocol Translation Service.
package translation

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/appthere/social-ingest-rssatom/internal/config"    // To use TranslationConfig
	"github.com/appthere/social-ingest-rssatom/internal/messaging" // To use the MQ client interface/type
	"github.com/appthere/social-ingest-rssatom/internal/parsing"
)

// Notifier defines the interface for sending notifications about new items.
type Notifier interface {
	// NotifyNewItems formats and sends the provided items to the configured downstream service.
	NotifyNewItems(ctx context.Context, items []*parsing.ParsedItem) error
}

// mqNotifier implements the Notifier interface using a message queue.
type mqNotifier struct {
	cfg      config.TranslationConfig
	mqClient messaging.Client // Use the specific type or interface from your messaging package
	logger   *slog.Logger
}

// NewNotifier creates a new Notifier instance that uses a message queue.
// It returns the Notifier interface.
func NewNotifier(cfg config.TranslationConfig, mqClient messaging.Client, logger *slog.Logger) (Notifier, error) {
	if !cfg.Enabled {
		logger.Info("Translation notifier is disabled via configuration.")
		// Return a no-op notifier if disabled
		return &noopNotifier{logger: logger}, nil
	}

	if mqClient == nil {
		// If MQ is essential for notifications when enabled, treat this as an error.
		// If MQ is optional overall, the caller (main.go) might handle nil mqClient earlier.
		return nil, fmt.Errorf("message queue client is required but was not provided")
	}
	if cfg.TargetTopic == "" {
		return nil, fmt.Errorf("translation target topic is required but not configured")
	}

	return &mqNotifier{
		cfg:      cfg,
		mqClient: mqClient,
		logger:   logger.With(slog.String("component", "translation_notifier")),
	}, nil
}

// NotifyNewItems implements the Notifier interface for mqNotifier.
func (n *mqNotifier) NotifyNewItems(ctx context.Context, items []*parsing.ParsedItem) error {
	if len(items) == 0 {
		n.logger.Debug("No new items to notify.")
		return nil
	}

	n.logger.Info("Notifying downstream service about new items",
		slog.Int("count", len(items)),
		slog.String("topic", n.cfg.TargetTopic),
	)

	publishedCount := 0
	var firstErr error // Keep track of the first error encountered

	// Potential Optimization: Publish items in batches instead of one by one.
	// This would require changing the message format (e.g., JSON array)
	// and potentially the MQ client's publish method signature or usage.

	for _, item := range items {
		// Check for context cancellation within the loop
		select {
		case <-ctx.Done():
			n.logger.Warn("Context cancelled during item notification loop", slog.Any("error", ctx.Err()))
			if firstErr == nil {
				firstErr = ctx.Err()
			}
			// Return the context error, potentially wrapped
			return fmt.Errorf("notification cancelled: %w", firstErr)
		default:
			// Proceed with publishing
		}

		// Format the item (currently using JSON)
		msgBytes, err := json.Marshal(item)
		if err != nil {
			n.logger.Error("Failed to marshal item for notification", slog.String("guid", item.GUID), slog.Any("error", err))
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to marshal item %s: %w", item.GUID, err)
			}
			continue // Skip this item, try the next one
		}

		// Publish to MQ using the configured topic
		err = n.mqClient.Publish(ctx, n.cfg.TargetTopic, msgBytes)
		if err != nil {
			n.logger.Error("Failed to publish item notification to message queue",
				slog.String("guid", item.GUID),
				slog.String("topic", n.cfg.TargetTopic),
				slog.Any("error", err),
			)
			if firstErr == nil {
				firstErr = fmt.Errorf("failed to publish item %s: %w", item.GUID, err)
			}
			// Decide whether to stop on first error or continue publishing others
			// continue // Try publishing next item
			// return firstErr // Stop processing on first publish error
		} else {
			publishedCount++
			n.logger.Debug("Published item notification", slog.String("guid", item.GUID))
		}
	}

	if firstErr != nil {
		n.logger.Error("Encountered errors during item notification",
			slog.Int("published_count", publishedCount),
			slog.Int("total_items", len(items)),
			slog.Any("first_error", firstErr), // Log the first error encountered
		)
		// Return the first error to signal that something went wrong.
		return firstErr
	}

	n.logger.Info("Finished notifying items", slog.Int("published_count", publishedCount))
	return nil // All items attempted (possibly with skipped items due to marshal errors)
}

// --- No-Op Notifier ---

// noopNotifier implements Notifier but does nothing. Used when translation is disabled.
type noopNotifier struct {
	logger *slog.Logger
}

func (n *noopNotifier) NotifyNewItems(ctx context.Context, items []*parsing.ParsedItem) error {
	if len(items) > 0 {
		n.logger.Debug("Skipping notification of new items because notifier is disabled.", slog.Int("count", len(items)))
	}
	return nil
}

// Ensure implementations satisfy the interface
var _ Notifier = (*mqNotifier)(nil)
var _ Notifier = (*noopNotifier)(nil)
