// Package scheduler manages the timing and execution of feed fetching tasks.
package scheduler

import (
	"context"
	// "encoding/json" // No longer needed here if formatting is in translation pkg
	"fmt"
	"log/slog"
	"math"
	"math/rand" // For jitter
	"sync"
	"time"

	"github.com/appthere/social-ingest-rssatom/internal/config"
	"github.com/appthere/social-ingest-rssatom/internal/fetching"
	"github.com/appthere/social-ingest-rssatom/internal/parsing"
	"github.com/appthere/social-ingest-rssatom/internal/storage"
	"github.com/appthere/social-ingest-rssatom/internal/translation" // Import the translation package
)

const (
	// How often the scheduler checks the database for feeds that are due.
	// This is NOT the fetch interval for individual feeds.
	defaultCheckInterval = 1 * time.Minute
	// Jitter factor to add randomness to next check times, preventing thundering herds.
	// 0.1 means +/- 10% of the calculated interval.
	defaultJitterFactor = 0.1
	// Base delay for exponential backoff on fetch errors.
	defaultErrorBackoffBase = 2 * time.Minute
	// Maximum delay for exponential backoff.
	defaultErrorBackoffMax = 1 * time.Hour
	// Timeout for processing a single feed (fetch + parse + store).
	defaultFeedProcessTimeout = 2 * time.Minute
)

// Scheduler orchestrates the fetching of feeds based on their schedules.
type Scheduler struct {
	cfg            config.SchedulerConfig
	store          storage.Store
	fetcher        *fetching.Fetcher
	parser         *parsing.Parser
	notifier       translation.Notifier // Use the Notifier interface
	logger         *slog.Logger
	checkInterval  time.Duration
	processTimeout time.Duration

	// Concurrency control
	workerTokens chan struct{} // Acts as a semaphore

	// Shutdown signalling
	wg     sync.WaitGroup
	cancel context.CancelFunc // To stop internal operations if needed (rarely used directly)
}

// New creates a new Scheduler instance.
func New(
	cfg config.SchedulerConfig,
	store storage.Store,
	// mqClient messaging.Client, // Removed: No longer needed directly if only used for notifications
	fetcher *fetching.Fetcher,
	parser *parsing.Parser,
	notifier translation.Notifier, // Add notifier parameter
	logger *slog.Logger,
) *Scheduler {

	// Use defaults or values from config if available
	checkInterval := defaultCheckInterval
	// if cfg.CheckInterval > 0 { checkInterval = cfg.CheckInterval } // Add CheckInterval to config if needed

	processTimeout := defaultFeedProcessTimeout
	// if cfg.FeedProcessTimeout > 0 { processTimeout = cfg.FeedProcessTimeout } // Add to config if needed

	return &Scheduler{
		cfg:   cfg,
		store: store,
		// mqClient:       mqClient, // Removed
		fetcher:        fetcher,
		parser:         parser,
		notifier:       notifier, // Assign notifier
		logger:         logger.With(slog.String("component", "scheduler")),
		checkInterval:  checkInterval,
		processTimeout: processTimeout,
		workerTokens:   make(chan struct{}, cfg.MaxConcurrentFetches), // Buffered channel limits concurrency
	}
}

// Start begins the scheduler's main loop. It blocks until the context is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	s.logger.Info("Starting scheduler",
		slog.Duration("check_interval", s.checkInterval),
		slog.Int("max_concurrent_fetches", s.cfg.MaxConcurrentFetches),
	)

	// Create an internal context that can be cancelled by the parent context
	// or potentially by an internal critical error (though less common).
	internalCtx, internalCancel := context.WithCancel(ctx)
	s.cancel = internalCancel // Store cancel func if needed elsewhere
	defer internalCancel()    // Ensure cleanup

	ticker := time.NewTicker(s.checkInterval)
	defer ticker.Stop()

	// Run an initial check immediately
	s.runCheck(internalCtx)

	for {
		select {
		case <-ticker.C:
			s.logger.Debug("Scheduler tick: running check for due feeds")
			s.runCheck(internalCtx) // Check for feeds to process

		case <-internalCtx.Done():
			s.logger.Info("Scheduler context cancelled. Shutting down...")
			// Wait for any active processFeed goroutines to finish
			s.logger.Info("Waiting for active feed processing tasks to complete...")
			s.wg.Wait()
			s.logger.Info("All feed processing tasks finished.")
			return // Exit the Start method
		}
	}
}

// runCheck queries the store for feeds due for checking and schedules them for processing.
func (s *Scheduler) runCheck(ctx context.Context) {
	now := time.Now().UTC()
	limit := s.cfg.MaxConcurrentFetches * 2 // Fetch slightly more than concurrency limit in case some fail fast

	feeds, err := s.store.GetFeedsToCheck(ctx, limit, now)
	if err != nil {
		s.logger.Error("Failed to get feeds to check from store", slog.Any("error", err))
		return // Try again on the next tick
	}

	if len(feeds) == 0 {
		s.logger.Debug("No feeds due for checking at this time")
		return
	}

	s.logger.Info("Found feeds due for checking", slog.Int("count", len(feeds)))

	for _, feed := range feeds {
		select {
		case s.workerTokens <- struct{}{}: // Acquire a token (blocks if channel is full)
			s.wg.Add(1)
			go func(f storage.FeedMetadata) {
				// Create a context with timeout for this specific feed processing task
				processCtx, processCancel := context.WithTimeout(ctx, s.processTimeout)
				defer processCancel()

				s.processFeed(processCtx, f) // Process the feed

				// Release token and signal WaitGroup completion in defer
				defer func() {
					<-s.workerTokens // Release the token
					s.wg.Done()
				}()
			}(feed) // Pass feed by value to the goroutine
		case <-ctx.Done():
			s.logger.Info("Context cancelled during feed scheduling loop. Aborting check.", slog.Any("error", ctx.Err()))
			return // Stop trying to schedule more feeds if context is cancelled
		}
	}
}

// processFeed handles the fetching, parsing, and storing logic for a single feed.
func (s *Scheduler) processFeed(ctx context.Context, feed storage.FeedMetadata) {
	feedLog := s.logger.With(slog.Int64("feed_id", feed.ID), slog.String("feed_url", feed.URL))
	feedLog.Info("Processing feed")

	// --- 1. Fetch ---
	fetchResult, fetchErr := s.fetcher.Fetch(ctx, feed.URL, feed.ETag, feed.LastModified)
	fetchedAt := time.Now().UTC() // Record time after fetch attempt concludes

	// --- 2. Handle Fetch Result ---
	if fetchErr != nil {
		feedLog.Warn("Fetch failed", slog.Any("error", fetchErr))
		nextCheck := s.calculateNextCheck(feed, s.cfg.DefaultInterval, fetchErr)
		err := s.store.UpdateFeedFetchError(ctx, feed.ID, fetchErr, fetchedAt, nextCheck)
		if err != nil {
			feedLog.Error("Failed to update feed fetch error status in store", slog.Any("error", err))
		}
		return // Stop processing this feed
	}

	// --- 3. Handle Not Modified (304) ---
	if fetchResult.NotModified {
		feedLog.Info("Feed content not modified (304)")
		nextCheck := s.calculateNextCheck(feed, s.cfg.DefaultInterval, nil) // No error, calculate normal next check
		// Update ETag/LastModified even if not modified, as server might have sent new ones
		err := s.store.UpdateFeedFetchSuccess(ctx, feed.ID, fetchResult.ETag, fetchResult.LastModified, fetchedAt, nextCheck, nil) // Pass nil for feedInfo
		if err != nil {
			feedLog.Error("Failed to update feed fetch success status (304) in store", slog.Any("error", err))
		}
		return // Done processing this feed
	}

	// --- 4. Handle Success (200 OK with Content) ---
	feedLog.Info("Fetch successful (200 OK)", slog.Int("content_length", len(fetchResult.Content)))

	// --- 5. Get Known Items ---
	knownGUIDs, err := s.store.GetItemGUIDsForFeed(ctx, feed.ID)
	if err != nil {
		feedLog.Error("Failed to get known item GUIDs from store", slog.Any("error", err))
		// Decide how to proceed. Maybe try to parse anyway but risk duplicates?
		// Or update as error? For now, log and stop.
		nextCheck := s.calculateNextCheck(feed, s.cfg.DefaultInterval, err) // Treat as error for scheduling
		_ = s.store.UpdateFeedFetchError(ctx, feed.ID, fmt.Errorf("failed to get known GUIDs: %w", err), fetchedAt, nextCheck)
		return
	}

	// --- 6. Parse & Compare ---
	parseResult, parseErr := s.parser.ParseAndCompare(ctx, feed.URL, fetchResult.Content, knownGUIDs)
	if parseErr != nil {
		feedLog.Error("Failed to parse feed content", slog.Any("error", parseErr))
		nextCheck := s.calculateNextCheck(feed, s.cfg.DefaultInterval, parseErr) // Treat as error for scheduling
		_ = s.store.UpdateFeedFetchError(ctx, feed.ID, fmt.Errorf("parsing failed: %w", parseErr), fetchedAt, nextCheck)
		return
	}

	// --- 7. Save New Items & Notify --- // Step name updated
	if len(parseResult.NewItems) > 0 {
		feedLog.Info("Found new items", slog.Int("count", len(parseResult.NewItems)))

		// Save items to the database
		saveErr := s.store.SaveNewItems(ctx, feed.ID, parseResult.NewItems)
		if saveErr != nil {
			feedLog.Error("Failed to save new items to store", slog.Any("error", saveErr))
			// Don't necessarily mark the feed fetch as failed, but log the error.
			// The items won't be notified if saving failed.
			// Consider if partial saves are possible/problematic based on store implementation.
		} else {
			feedLog.Info("Successfully saved new items", slog.Int("count", len(parseResult.NewItems)))
			// Notify downstream service about the successfully saved items
			notifyErr := s.notifier.NotifyNewItems(ctx, parseResult.NewItems) // Use the notifier
			if notifyErr != nil {
				// Log the notification error, but don't treat it as a feed processing failure.
				// The feed was processed successfully up to this point.
				feedLog.Warn("Failed to notify downstream service about new items", slog.Any("error", notifyErr))
			}
		}
	} else {
		feedLog.Info("No new items found after parsing")
	}

	// --- 8. Update Feed Success Status ---
	nextCheck := s.calculateNextCheck(feed, s.cfg.DefaultInterval, nil) // No error
	// Update ETag/LastModified and potentially feed title/description from parsing
	updateErr := s.store.UpdateFeedFetchSuccess(ctx, feed.ID, fetchResult.ETag, fetchResult.LastModified, fetchedAt, nextCheck, parseResult.Feed)
	if updateErr != nil {
		feedLog.Error("Failed to update feed fetch success status (200) in store", slog.Any("error", updateErr))
	} else {
		feedLog.Info("Successfully processed feed", slog.Time("next_check_at", nextCheck))
	}
}

/* // Removed: This logic is now handled by the translation.Notifier
// publishNewItems sends newly found items to the message queue.
func (s *Scheduler) publishNewItems(ctx context.Context, feedLog *slog.Logger, items []*parsing.ParsedItem) {
	// Check if MQ client is configured/available
	if s.mqClient == nil {
		feedLog.Debug("MQ client not configured, skipping item publishing")
		return
	}

	// TODO: Get topic name from config (e.g., cfg.MessageQueue.FetchedItemsTopic)
	topic := "social.content.new" // Example topic

	publishedCount := 0
	for _, item := range items {
		// Select allows context cancellation check during potentially long loops
		select {
		case <-ctx.Done():
			feedLog.Warn("Context cancelled during item publishing loop", slog.Any("error", ctx.Err()))
			return
		default:
			// Proceed with publishing
		}

		// Marshal item to JSON (or other format)
		// Consider a more optimized encoding like protobuf if performance is critical
		msgBytes, err := json.Marshal(item)
		if err != nil {
			feedLog.Error("Failed to marshal item for publishing", slog.String("guid", item.GUID), slog.Any("error", err))
			continue // Skip this item
		}

		// Publish to MQ
		// Add retries or specific error handling for MQ publish if needed
		err = s.mqClient.Publish(ctx, topic, msgBytes) // Assuming Publish exists
		if err != nil {
			feedLog.Error("Failed to publish item to message queue", slog.String("guid", item.GUID), slog.String("topic", topic), slog.Any("error", err))
			// Decide whether to stop publishing other items for this feed
			// continue // Try publishing next item
			// return // Stop publishing for this feed run
		} else {
			publishedCount++
			feedLog.Debug("Published item", slog.String("guid", item.GUID), slog.String("topic", topic))
		}
	}
	feedLog.Info("Finished publishing items", slog.Int("published_count", publishedCount), slog.Int("total_new", len(items)))
}
*/

// calculateNextCheck determines the time for the next fetch attempt.
// It applies exponential backoff on errors and adds jitter.
func (s *Scheduler) calculateNextCheck(feed storage.FeedMetadata, defaultInterval time.Duration, fetchErr error) time.Time {
	var interval time.Duration

	if fetchErr != nil {
		// Exponential backoff: base * 2^failures (capped)
		backoffFactor := math.Pow(2, float64(feed.FailureCount))
		interval = time.Duration(float64(defaultErrorBackoffBase) * backoffFactor)

		if interval > defaultErrorBackoffMax {
			interval = defaultErrorBackoffMax
		}
		s.logger.Debug("Calculated error backoff interval",
			slog.Int64("feed_id", feed.ID),
			slog.Int("failure_count", feed.FailureCount),
			slog.Duration("interval", interval),
		)
	} else {
		// Use feed-specific interval if set, otherwise default
		interval = feed.FetchInterval
		if interval <= 0 {
			interval = defaultInterval
		}
	}

	// Add jitter: +/- (jitterFactor * interval)
	jitter := time.Duration(float64(interval) * defaultJitterFactor * (rand.Float64()*2 - 1)) // Random number between -jitterFactor and +jitterFactor
	nextInterval := interval + jitter

	// Ensure interval doesn't go below a minimum reasonable value (e.g., 1 minute)
	minInterval := 1 * time.Minute
	if nextInterval < minInterval {
		nextInterval = minInterval
	}

	return time.Now().UTC().Add(nextInterval)
}

// Removed messaging.Client helper type comment as client is removed
