// Package subscription provides the business logic for managing feed subscriptions.
// It interacts with the storage layer to persist subscription data.
package subscription

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url" // For basic URL validation/normalization
	"strings"

	"github.com/appthere/rssatom/internal/storage"
)

// Manager handles the business logic for feed subscriptions.
type Manager struct {
	store  storage.Store
	logger *slog.Logger
}

// NewManager creates a new subscription Manager.
func NewManager(store storage.Store, logger *slog.Logger) *Manager {
	return &Manager{
		store:  store,
		logger: logger.With(slog.String("component", "subscriptions_manager")),
	}
}

// Subscribe handles a request for a user to subscribe to a feed URL.
// It implements the logic for:
// - Checking if the feed is already tracked (C).
// - Adding the feed if it's new and scheduling its initial fetch (D).
// - Ensuring the feed is active if it already exists (part of E).
// It returns the feed's unique ID, whether the feed subscription was newly created
// in the store by *this* call, and any error encountered.
// Note: 'isNew' refers to the feed record itself, not necessarily the user's link to it
// if a more complex user-feed relationship exists.
func (m *Manager) Subscribe(ctx context.Context, feedURL string, userID int64) (feedID int64, isNew bool, err error) {
	subLog := m.logger.With(slog.String("feed_url", feedURL), slog.Int64("user_id", userID))

	// Optional: Basic validation or normalization of the URL
	normalizedURL, err := normalizeURL(feedURL)
	if err != nil {
		subLog.Warn("Invalid feed URL provided", slog.Any("error", err))
		return 0, false, fmt.Errorf("invalid feed URL '%s': %w", feedURL, err)
	}
	if normalizedURL != feedURL {
		subLog.Debug("Normalized URL", slog.String("original", feedURL), slog.String("normalized", normalizedURL))
	}

	subLog.Info("Processing subscription request")

	// Use the storage layer's AddSubscription method.
	// This method handles the core logic of inserting the feed if it doesn't exist
	// (with ON CONFLICT) and setting next_check_at to NOW() for immediate scheduling.
	// It should also return whether the record was newly inserted.
	feedID, err = m.store.AddSubscription(ctx, normalizedURL, userID)
	if err != nil {
		// Don't wrap ErrSubscriptionExists if storage returns it, let caller handle it.

		if errors.Is(err, storage.ErrSubscriptionExists) {
			subLog.Info("Feed subscription already exists, ensuring active")
			// The ON CONFLICT clause in storage should ideally handle re-activation.
			// Return false for isNew as it wasn't inserted by *this* call.
			return feedID, false, nil // Not an error condition for the caller usually
		}
		// Handle other potential storage errors
		subLog.Error("Failed to add or update subscription in storage", slog.Any("error", err))
		return 0, false, fmt.Errorf("storage error during subscription: %w", err)
	}

	if err == nil && feedID > 0 {
		subLog.Info("Successfully added new feed subscription", slog.Int64("feed_id", feedID))
		isNew = true
	} else if err == nil {
		// This case might occur if AddSubscription uses ON CONFLICT DO UPDATE
		// and doesn't return ErrSubscriptionExists explicitly.
		subLog.Info("Feed subscription already existed, ensured active", slog.Int64("feed_id", feedID))
		isNew = false
	}

	// --- Future Enhancement: User-Feed Linking ---
	// If you have a separate user_feed_subscriptions table (many-to-many):
	// err = m.store.LinkUserToFeed(ctx, userID, feedID)
	// if err != nil {
	//     // Handle error linking user, potentially compensating the AddSubscription if critical
	//     subLog.Error("Failed to link user to feed after ensuring feed exists", slog.Any("error", err))
	//     return feedID, isNew, fmt.Errorf("failed to link user %d to feed %d: %w", userID, feedID, err)
	// }
	// subLog.Info("Successfully linked user to feed")
	// --- End Enhancement ---

	return feedID, isNew, nil
}

// IsTracked checks if a feed URL is currently being tracked in the system.
// It returns true and the feed ID if tracked, false otherwise.
// Implements C{Is Feed Tracked?}.
func (m *Manager) IsTracked(ctx context.Context, feedURL string) (tracked bool, feedID int64, err error) {
	checkLog := m.logger.With(slog.String("feed_url", feedURL))

	normalizedURL, err := normalizeURL(feedURL)
	if err != nil {
		checkLog.Warn("Invalid feed URL provided for tracking check", slog.Any("error", err))
		return false, 0, fmt.Errorf("invalid feed URL '%s': %w", feedURL, err)
	}

	feedMeta, err := m.store.GetFeedByURL(ctx, normalizedURL)
	if err != nil {
		if errors.Is(err, storage.ErrFeedNotFound) {
			checkLog.Debug("Feed URL not found in storage")
			return false, 0, nil // Not tracked, no error
		}
		// Other storage error
		checkLog.Error("Failed to check feed tracking status in storage", slog.Any("error", err))
		return false, 0, fmt.Errorf("storage error checking feed tracking: %w", err)
	}

	// Found the feed
	checkLog.Debug("Feed URL is tracked", slog.Int64("feed_id", feedMeta.ID), slog.Bool("is_active", feedMeta.IsActive))
	// Consider if IsActive matters for "tracked". Usually, we just care if the record exists.
	return true, feedMeta.ID, nil
}

// Unsubscribe handles removing a user's interest in a feed.
// NOTE: This is a placeholder. Full implementation requires decisions on
// whether to deactivate/delete the feed if no users are left subscribed,
// which typically needs a many-to-many user-feed linking table.
func (m *Manager) Unsubscribe(ctx context.Context, feedURL string, userID int64) error {
	unsubLog := m.logger.With(slog.String("feed_url", feedURL), slog.Int64("user_id", userID))
	unsubLog.Warn("Unsubscribe function called, but not fully implemented")

	// 1. Normalize URL
	normalizedURL, err := normalizeURL(feedURL)
	if err != nil {
		return fmt.Errorf("invalid feed URL '%s': %w", feedURL, err)
	}

	// 2. Find Feed ID
	tracked, _, err := m.IsTracked(ctx, normalizedURL)
	if err != nil {
		return fmt.Errorf("failed to check feed status before unsubscribe: %w", err)
	}
	if !tracked {
		unsubLog.Info("User tried to unsubscribe from a feed that is not tracked")
		return nil // Or return a specific "not subscribed" error?
	}

	// --- Requires User-Feed Linking Table ---
	// 3. Remove the link between the user and the feed in the linking table.
	// err = m.store.UnlinkUserFromFeed(ctx, userID, feedID)
	// if err != nil {
	//     return fmt.Errorf("failed to unlink user %d from feed %d: %w", userID, feedID, err)
	// }
	//
	// 4. Check if any other users are subscribed to this feed.
	// count, err := m.store.GetFeedSubscriberCount(ctx, feedID)
	// if err != nil {
	//     return fmt.Errorf("failed to check subscriber count for feed %d: %w", feedID, err)
	// }
	//
	// 5. If count is 0, optionally deactivate or delete the feed record.
	// if count == 0 {
	//     unsubLog.Info("No remaining subscribers for feed, deactivating.", slog.Int64("feed_id", feedID))
	//     err = m.store.DeactivateFeed(ctx, feedID) // Or DeleteFeed
	//     if err != nil {
	//         return fmt.Errorf("failed to deactivate feed %d: %w", feedID, err)
	//     }
	// }
	// --- End Requires User-Feed Linking Table ---

	// If only using the basic `feeds` table, unsubscribing is harder to model correctly.
	// You might just log it or do nothing.
	return errors.New("unsubscribe functionality requires user-feed linking implementation")
}

// normalizeURL performs basic cleaning/validation of a URL string.
// Example: ensures scheme exists, trims space. Add more rules as needed.
func normalizeURL(rawURL string) (string, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", errors.New("URL cannot be empty")
	}

	// Ensure a scheme is present, default to http if missing? Or require https?
	// For simplicity, let's require http or https for now.
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		// Defaulting can be risky if the feed is only available on one.
		// return "http://" + rawURL, nil // Option 1: Default to http
		return "", fmt.Errorf("URL scheme (http:// or https://) is missing") // Option 2: Require scheme
	}

	parsed, err := url.ParseRequestURI(rawURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL format: %w", err)
	}

	// Further normalization could include:
	// - Lowercasing scheme and host
	// - Removing default ports (e.g., :80, :443)
	// - Removing trailing slash from path if path is not just "/"
	// - Sorting query parameters? (Can break some feeds)

	// Return the parsed URL as a string (rebuilds it consistently)
	return parsed.String(), nil
}

// --- Helper needed by normalizeURL ---
//import "strings"
