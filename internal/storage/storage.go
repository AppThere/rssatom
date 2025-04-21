// Package storage defines interfaces and implementations for interacting
// with the data store for feeds, items, and related metadata.
package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	// Import your internal parsing package to use its types
	"github.com/appthere/rssatom/internal/parsing"
	// Import your chosen SQL driver (e.g., postgres) in main.go or here if only used here
	// _ "github.com/lib/pq"
)

var (
	// ErrFeedNotFound is returned when a specific feed is not found.
	ErrFeedNotFound = errors.New("feed not found")
	// ErrItemNotFound is returned when a specific item is not found (less common usage).
	ErrItemNotFound = errors.New("item not found")
	// ErrSubscriptionExists is returned when trying to add a feed URL that already exists.
	ErrSubscriptionExists = errors.New("feed subscription already exists for this URL")
)

// FeedMetadata holds essential information about a feed retrieved from storage,
// primarily used by the scheduler.
type FeedMetadata struct {
	ID             int64         // Unique identifier for the feed in the database.
	URL            string        // The URL of the feed to fetch.
	ETag           string        // Last known ETag header value.
	LastModified   string        // Last known Last-Modified header value.
	LastFetchedAt  sql.NullTime  // When the feed was last successfully fetched or attempted.
	NextCheckAt    time.Time     // When the feed should be checked next.
	FetchInterval  time.Duration // How often this feed should ideally be checked. (Optional: Can be calculated from NextCheckAt)
	FailureCount   int           // Consecutive fetch failure count. (Optional: For backoff/deactivation)
	IsActive       bool          // Flag to enable/disable fetching for this feed.
	SubscribedByID int64         // ID of the user/service that initially subscribed (Optional)
}

// Store defines the operations needed for managing feed and item data.
type Store interface {
	// GetFeedsToCheck retrieves a batch of active feeds that are due for fetching.
	GetFeedsToCheck(ctx context.Context, limit int, now time.Time) ([]FeedMetadata, error)

	// UpdateFeedFetchSuccess updates feed metadata after a successful fetch (200 OK or 304 Not Modified).
	// It updates ETag, LastModified, fetch times, and optionally feed metadata discovered during parsing.
	UpdateFeedFetchSuccess(ctx context.Context, feedID int64, etag, lastModified string, fetchedAt time.Time, nextCheckAt time.Time, feedInfo *parsing.ParsedFeed) error

	// UpdateFeedFetchError updates feed metadata after a failed fetch attempt.
	// It logs the error, updates fetch times, potentially increments failure count, and schedules the next check.
	UpdateFeedFetchError(ctx context.Context, feedID int64, fetchError error, fetchedAt time.Time, nextCheckAt time.Time) error

	// GetItemGUIDsForFeed retrieves the set of known item GUIDs for a specific feed.
	// Used by the parser to identify new items. Returns a map where keys are GUIDs.
	GetItemGUIDsForFeed(ctx context.Context, feedID int64) (map[string]struct{}, error)

	// SaveNewItems stores newly parsed items and associates them with the feed.
	// It should handle potential duplicates gracefully (e.g., using ON CONFLICT).
	SaveNewItems(ctx context.Context, feedID int64, items []*parsing.ParsedItem) error

	// AddSubscription adds a new feed URL to be tracked.
	// It should check for existing URLs and return ErrSubscriptionExists if found.
	// Returns the ID of the newly created or existing feed subscription.
	AddSubscription(ctx context.Context, feedURL string, subscribedByID int64) (feedID int64, err error)

	// GetFeedByURL retrieves feed metadata based on its URL. (Optional, useful for subscriber)
	GetFeedByURL(ctx context.Context, feedURL string) (*FeedMetadata, error)

	// Close gracefully closes the underlying database connection(s).
	Close() error
}

// --- SQL Implementation ---

// SQLStore provides a SQL-based implementation of the Store interface.
type SQLStore struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewSQLStore creates a new SQLStore instance.
func NewSQLStore(db *sql.DB, logger *slog.Logger) *SQLStore {
	return &SQLStore{
		db:     db,
		logger: logger.With(slog.String("component", "storage")),
	}
}

// Close closes the database connection pool.
func (s *SQLStore) Close() error {
	s.logger.Info("Closing database connection pool...")
	return s.db.Close()
}

// GetFeedsToCheck retrieves a batch of active feeds due for fetching.
func (s *SQLStore) GetFeedsToCheck(ctx context.Context, limit int, now time.Time) ([]FeedMetadata, error) {
	// NOTE: Replace with your actual SQL query and table/column names.
	// Assumes a 'feeds' table with columns like:
	// id, feed_url, etag, last_modified, last_fetched_at, next_check_at, fetch_interval_seconds, failure_count, is_active
	query := `
		SELECT
			id, feed_url, etag, last_modified, last_fetched_at,
			next_check_at, fetch_interval_seconds, failure_count, is_active
		FROM feeds
		WHERE is_active = TRUE AND next_check_at <= $1
		ORDER BY next_check_at ASC
		LIMIT $2;
	`
	rows, err := s.db.QueryContext(ctx, query, now, limit)
	if err != nil {
		return nil, fmt.Errorf("querying feeds to check failed: %w", err)
	}
	defer rows.Close()

	feeds := make([]FeedMetadata, 0, limit) // Pre-allocate slice capacity
	for rows.Next() {
		var meta FeedMetadata
		var intervalSeconds sql.NullInt64 // Handle potential NULL interval

		err := rows.Scan(
			&meta.ID,
			&meta.URL,
			&meta.ETag,         // Assumes etag column allows NULLs or defaults to ''
			&meta.LastModified, // Assumes last_modified column allows NULLs or defaults to ''
			&meta.LastFetchedAt,
			&meta.NextCheckAt,
			&intervalSeconds,
			&meta.FailureCount,
			&meta.IsActive,
		)
		if err != nil {
			s.logger.Error("Failed to scan feed row", slog.Any("error", err))
			// Decide whether to return partial results or fail entirely
			continue // Skip this row and log
			// return nil, fmt.Errorf("scanning feed row failed: %w", err) // Fail fast
		}
		if intervalSeconds.Valid {
			meta.FetchInterval = time.Duration(intervalSeconds.Int64) * time.Second
		} else {
			// Handle case where interval is NULL - use a default?
			// meta.FetchInterval = config.DefaultFetchInterval // Get from config
		}
		feeds = append(feeds, meta)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating feed rows: %w", err)
	}

	s.logger.Debug("Fetched feeds to check", slog.Int("count", len(feeds)), slog.Time("cutoff_time", now))
	return feeds, nil
}

// UpdateFeedFetchSuccess updates feed metadata after a successful fetch.
func (s *SQLStore) UpdateFeedFetchSuccess(ctx context.Context, feedID int64, etag, lastModified string, fetchedAt time.Time, nextCheckAt time.Time, feedInfo *parsing.ParsedFeed) error {
	// NOTE: Replace with your actual SQL query.
	// Reset failure count on success. Optionally update title/description/link from parsed feed.
	query := `
		UPDATE feeds
		SET
			etag = $2,
			last_modified = $3,
			last_fetched_at = $4,
			next_check_at = $5,
			last_error = NULL,
			failure_count = 0,
			feed_title = COALESCE(NULLIF($6, ''), feed_title), -- Update title if new one is not empty
			feed_description = COALESCE(NULLIF($7, ''), feed_description),
			feed_link = COALESCE(NULLIF($8, ''), feed_link),
			updated_at = NOW() -- Or use fetchedAt
		WHERE id = $1;
	`
	// Handle potential nil feedInfo
	var title, description, link sql.NullString
	if feedInfo != nil {
		title = sql.NullString{String: feedInfo.Title, Valid: feedInfo.Title != ""}
		description = sql.NullString{String: feedInfo.Description, Valid: feedInfo.Description != ""}
		link = sql.NullString{String: feedInfo.Link, Valid: feedInfo.Link != ""}
	}

	result, err := s.db.ExecContext(ctx, query, feedID, etag, lastModified, fetchedAt, nextCheckAt, title, description, link)
	if err != nil {
		return fmt.Errorf("updating feed success info for feed %d failed: %w", feedID, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrFeedNotFound
	}

	s.logger.Debug("Updated feed fetch success info", slog.Int64("feed_id", feedID))
	return nil
}

// UpdateFeedFetchError updates feed metadata after a failed fetch attempt.
func (s *SQLStore) UpdateFeedFetchError(ctx context.Context, feedID int64, fetchError error, fetchedAt time.Time, nextCheckAt time.Time) error {
	// NOTE: Replace with your actual SQL query.
	// Increment failure count. Store the error message.
	// Consider adding logic to deactivate the feed after N failures (e.g., in a trigger or here).
	query := `
		UPDATE feeds
		SET
			last_fetched_at = $2,
			next_check_at = $3,
			last_error = $4,
			failure_count = failure_count + 1,
			updated_at = NOW() -- Or use fetchedAt
		WHERE id = $1;
	`
	errorMsg := fetchError.Error()
	// Truncate error message if needed to fit DB column size
	maxLen := 255 // Example limit
	if len(errorMsg) > maxLen {
		errorMsg = errorMsg[:maxLen]
	}

	result, err := s.db.ExecContext(ctx, query, feedID, fetchedAt, nextCheckAt, errorMsg)
	if err != nil {
		return fmt.Errorf("updating feed error info for feed %d failed: %w", feedID, err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return ErrFeedNotFound
	}

	s.logger.Warn("Updated feed fetch error info", slog.Int64("feed_id", feedID), slog.Any("error", fetchError))
	return nil
}

// GetItemGUIDsForFeed retrieves the set of known item GUIDs for a specific feed.
func (s *SQLStore) GetItemGUIDsForFeed(ctx context.Context, feedID int64) (map[string]struct{}, error) {
	// NOTE: Replace with your actual SQL query.
	// Assumes a join table 'feed_items' with (feed_id, item_guid) or similar.
	// Using item_guid directly avoids joining the 'items' table if you only need the identifier.
	query := `SELECT item_guid FROM feed_items WHERE feed_id = $1;`

	rows, err := s.db.QueryContext(ctx, query, feedID)
	if err != nil {
		return nil, fmt.Errorf("querying item GUIDs for feed %d failed: %w", feedID, err)
	}
	defer rows.Close()

	guids := make(map[string]struct{})
	for rows.Next() {
		var guid string
		if err := rows.Scan(&guid); err != nil {
			s.logger.Error("Failed to scan item GUID row", slog.Int64("feed_id", feedID), slog.Any("error", err))
			// Continue processing other rows? Or return error immediately?
			continue
			// return nil, fmt.Errorf("scanning item GUID for feed %d failed: %w", feedID, err)
		}
		guids[guid] = struct{}{} // Add GUID to the set
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating item GUIDs for feed %d: %w", feedID, err)
	}

	s.logger.Debug("Retrieved known item GUIDs", slog.Int64("feed_id", feedID), slog.Int("count", len(guids)))
	return guids, nil
}

// SaveNewItems stores newly parsed items and associates them with the feed.
func (s *SQLStore) SaveNewItems(ctx context.Context, feedID int64, items []*parsing.ParsedItem) error {
	if len(items) == 0 {
		return nil // Nothing to save
	}

	// Use a transaction for atomicity
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for saving items (feed %d): %w", feedID, err)
	}
	defer tx.Rollback() // Rollback if commit is not called

	// NOTE: Replace with your actual SQL queries.
	// Assumes 'items' table (guid, link, title, content, published_at, updated_at, author_name, ...)
	// Assumes 'feed_items' join table (feed_id, item_id, item_guid, added_at)

	// Prepare statements for efficiency within the loop
	// Insert/update item details (handle conflicts on GUID)
	itemStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO items (guid, link, title, content, published_at, updated_at, author_name, author_email, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), NOW())
		ON CONFLICT (guid) DO UPDATE SET
			link = EXCLUDED.link,
			title = EXCLUDED.title,
			content = EXCLUDED.content,
			published_at = EXCLUDED.published_at,
			updated_at = EXCLUDED.updated_at,
			author_name = EXCLUDED.author_name,
			author_email = EXCLUDED.author_email,
			updated_at = NOW()
		RETURNING id; -- Return the ID of the inserted or updated item
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare item insert statement (feed %d): %w", feedID, err)
	}
	defer itemStmt.Close()

	// Link item to feed (handle conflicts on feed_id, item_id/guid)
	feedItemStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO feed_items (feed_id, item_id, item_guid, added_at)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (feed_id, item_id) DO NOTHING; -- Or ON CONFLICT (feed_id, item_guid)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare feed_item link statement (feed %d): %w", feedID, err)
	}
	defer feedItemStmt.Close()

	savedCount := 0
	for _, item := range items {
		// 1. Insert or Update the item globally
		var itemID int64
		err = itemStmt.QueryRowContext(ctx,
			item.GUID, item.Link, item.Title, item.Content,
			sql.NullTime{Time: item.Published, Valid: !item.Published.IsZero()},
			sql.NullTime{Time: item.Updated, Valid: !item.Updated.IsZero()},
			sql.NullString{String: item.AuthorName, Valid: item.AuthorName != ""},
			sql.NullString{String: item.AuthorEmail, Valid: item.AuthorEmail != ""},
		).Scan(&itemID)

		if err != nil {
			s.logger.Error("Failed to insert/update item", slog.String("guid", item.GUID), slog.Any("error", err))
			// Decide: continue with other items or rollback transaction?
			// Continuing might lead to partial saves. Rollback ensures all-or-nothing.
			return fmt.Errorf("failed to save item with GUID %s (feed %d): %w", item.GUID, feedID, err) // Fail fast
		}

		// 2. Link the item to this specific feed
		_, err = feedItemStmt.ExecContext(ctx, feedID, itemID, item.GUID)
		if err != nil {
			s.logger.Error("Failed to link item to feed", slog.Int64("feed_id", feedID), slog.Int64("item_id", itemID), slog.String("guid", item.GUID), slog.Any("error", err))
			return fmt.Errorf("failed to link item %d (GUID %s) to feed %d: %w", itemID, item.GUID, feedID, err) // Fail fast
		}
		savedCount++
	}

	// Commit the transaction if all items were processed successfully
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction for saving items (feed %d): %w", feedID, err)
	}

	s.logger.Info("Successfully saved new items", slog.Int64("feed_id", feedID), slog.Int("count", savedCount))
	return nil
}

// AddSubscription adds a new feed URL to be tracked.
func (s *SQLStore) AddSubscription(ctx context.Context, feedURL string, subscribedByID int64) (int64, error) {
	// NOTE: Replace with your actual SQL query.
	// Use ON CONFLICT to handle race conditions and existing URLs gracefully.
	// Set next_check_at to NOW() to trigger an immediate fetch.
	query := `
		INSERT INTO feeds (feed_url, subscribed_by_id, next_check_at, fetch_interval_seconds, is_active, created_at, updated_at)
		VALUES ($1, $2, NOW(), $3, TRUE, NOW(), NOW())
		ON CONFLICT (feed_url) DO UPDATE
			-- Optionally update subscribed_by_id or just do nothing if it exists
			-- SET updated_at = NOW() -- Update timestamp even if conflicting
			SET is_active = TRUE -- Ensure it's active if re-added
		RETURNING id, created_at = updated_at; -- Return ID and whether it was newly inserted (created_at == updated_at)
	`
	// TODO: Get default interval from config
	defaultIntervalSeconds := 15 * 60 // 15 minutes

	var feedID int64
	var wasInserted bool // True if INSERT happened, false if ON CONFLICT UPDATE happened

	err := s.db.QueryRowContext(ctx, query, feedURL, subscribedByID, defaultIntervalSeconds).Scan(&feedID, &wasInserted)
	if err != nil {
		// Check for specific DB errors if needed
		return 0, fmt.Errorf("failed to add subscription for URL %s: %w", feedURL, err)
	}

	if !wasInserted {
		s.logger.Info("Subscription already exists for URL, returning existing ID", slog.String("url", feedURL), slog.Int64("feed_id", feedID))
		// Optionally return ErrSubscriptionExists if the caller needs to know it wasn't new
		// return feedID, ErrSubscriptionExists
	} else {
		s.logger.Info("Successfully added new subscription", slog.String("url", feedURL), slog.Int64("feed_id", feedID))
	}

	return feedID, nil
}

// GetFeedByURL retrieves feed metadata by URL.
func (s *SQLStore) GetFeedByURL(ctx context.Context, feedURL string) (*FeedMetadata, error) {
	// NOTE: Replace with your actual SQL query.
	query := `
		SELECT
			id, feed_url, etag, last_modified, last_fetched_at,
			next_check_at, fetch_interval_seconds, failure_count, is_active, subscribed_by_id
		FROM feeds
		WHERE feed_url = $1;
	`
	row := s.db.QueryRowContext(ctx, query, feedURL)

	var meta FeedMetadata
	var intervalSeconds sql.NullInt64
	var subID sql.NullInt64

	err := row.Scan(
		&meta.ID,
		&meta.URL,
		&meta.ETag,
		&meta.LastModified,
		&meta.LastFetchedAt,
		&meta.NextCheckAt,
		&intervalSeconds,
		&meta.FailureCount,
		&meta.IsActive,
		&subID,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrFeedNotFound
		}
		return nil, fmt.Errorf("querying feed by URL %s failed: %w", feedURL, err)
	}

	if intervalSeconds.Valid {
		meta.FetchInterval = time.Duration(intervalSeconds.Int64) * time.Second
	}
	if subID.Valid {
		meta.SubscribedByID = subID.Int64
	}

	return &meta, nil
}

// Ensure SQLStore implements Store interface
var _ Store = (*SQLStore)(nil)
