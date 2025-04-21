// Package parsing handles parsing RSS and Atom feed content (XML) and
// identifying new items compared to a set of known item identifiers.
package parsing

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/mmcdole/gofeed" // Library for parsing RSS, Atom, JSON feeds
)

// ParsedItem represents a single feed item (article, post, etc.)
// extracted in a standardized format.
type ParsedItem struct {
	GUID        string    // Unique identifier (Atom ID, RSS GUID, or Link if others are missing)
	Link        string    // Primary URL for the item
	Title       string    // Title of the item
	Published   time.Time // Publication time (best effort, UTC)
	Updated     time.Time // Last updated time (best effort, UTC)
	Content     string    // Main content (Atom content or RSS description)
	AuthorName  string    // Author's name, if available
	AuthorEmail string    // Author's email, if available
	Categories  []string  // Tags or categories associated with the item
	// Add other fields as needed, e.g., ImageURL, Enclosures
}

// ParseResult holds the outcome of parsing a feed and comparing its items.
type ParseResult struct {
	Feed     *ParsedFeed   // Metadata about the parsed feed.
	NewItems []*ParsedItem // Slice containing only the items identified as new.
}

// ParsedFeed holds standardized metadata about the feed itself.
type ParsedFeed struct {
	Title       string
	Description string
	Link        string // Link to the HTML version of the feed website
	FeedLink    string // Link to the feed file itself (e.g., the XML URL)
	Updated     time.Time
	AuthorName  string
	AuthorEmail string
	// Add Image, Language etc. if needed
}

// Parser is responsible for parsing feed data.
// Currently stateless, but could hold configuration (e.g., sanitization rules) later.
type Parser struct {
	logger *slog.Logger
}

// NewParser creates a new Parser instance.
func NewParser(logger *slog.Logger) *Parser {
	return &Parser{
		logger: logger.With(slog.String("component", "parser")),
	}
}

// ParseAndCompare parses the feed data and returns only the items whose GUIDs
// are not present in the knownItemGUIDs map.
// knownItemGUIDs should be a map where keys are the GUIDs of items already seen/processed.
func (p *Parser) ParseAndCompare(ctx context.Context, feedURL string, feedData []byte, knownItemGUIDs map[string]struct{}) (*ParseResult, error) {
	parseLog := p.logger.With(slog.String("feed_url", feedURL)) // Logger specific to this parse operation

	// Use gofeed parser
	fp := gofeed.NewParser()
	// fp.UserAgent = "Your-Custom-User-Agent" // Can customize if needed, but fetching layer should handle this primarily

	// Parse the feed content
	// Using Parse instead of ParseURL as we already have the data from the fetching layer.
	gfFeed, err := fp.Parse(bytes.NewReader(feedData))
	if err != nil {
		parseLog.Error("Failed to parse feed data", slog.Any("error", err))
		return nil, fmt.Errorf("parsing error for %s: %w", feedURL, err)
	}

	if gfFeed == nil {
		parseLog.Warn("Parser returned nil feed object without error")
		return nil, fmt.Errorf("parsing resulted in nil feed for %s", feedURL)
	}

	parseLog.Info("Feed parsed successfully", slog.String("feed_title", gfFeed.Title), slog.Int("total_items_parsed", len(gfFeed.Items)))

	result := &ParseResult{
		Feed:     convertFeedMetadata(gfFeed),
		NewItems: make([]*ParsedItem, 0), // Initialize slice
	}

	// Compare items
	newCount := 0
	for _, item := range gfFeed.Items {
		select {
		case <-ctx.Done():
			parseLog.Warn("Context cancelled during item processing", slog.Any("error", ctx.Err()))
			return nil, ctx.Err() // Stop processing if context is cancelled
		default:
			// continue processing
		}

		if item == nil {
			parseLog.Warn("Encountered nil item in feed item list")
			continue
		}

		guid := extractGUID(item)
		if guid == "" {
			parseLog.Warn("Skipping item without usable GUID or Link", slog.String("item_title", item.Title))
			continue // Cannot track items without a unique identifier
		}

		// Check if item GUID is already known
		if _, exists := knownItemGUIDs[guid]; !exists {
			// This is a new item
			newCount++
			parsedItem := convertItem(item, guid)
			result.NewItems = append(result.NewItems, parsedItem)
			parseLog.Debug("New item found", slog.String("guid", guid), slog.String("title", parsedItem.Title))
		} else {
			parseLog.Debug("Skipping known item", slog.String("guid", guid))
		}
	}

	parseLog.Info("Comparison complete", slog.Int("new_items_found", newCount))

	return result, nil
}

// extractGUID determines the best unique identifier for a feed item.
// Prefers Atom ID or RSS GUID, falls back to Link.
func extractGUID(item *gofeed.Item) string {
	if item.GUID != "" {
		return item.GUID
	}
	// Atom feeds use <id> which gofeed maps to GUID field as well.
	// If GUID is empty, fallback to the item's primary link.
	if item.Link != "" {
		return item.Link
	}
	return "" // No usable identifier found
}

// convertItem transforms a gofeed.Item into our standardized ParsedItem struct.
func convertItem(item *gofeed.Item, guid string) *ParsedItem {
	parsed := &ParsedItem{
		GUID:       guid, // Use the already extracted GUID
		Link:       item.Link,
		Title:      item.Title,
		Content:    item.Content, // Atom <content> often here
		Categories: item.Categories,
	}

	// Use Description if Content is empty (common in RSS)
	if parsed.Content == "" {
		parsed.Content = item.Description
	}

	// Handle Author
	if item.Author != nil {
		parsed.AuthorName = item.Author.Name
		parsed.AuthorEmail = item.Author.Email
	}

	// Handle Timestamps (gofeed provides parsed versions)
	// Prefer Updated over Published if available
	if item.UpdatedParsed != nil {
		parsed.Updated = item.UpdatedParsed.UTC()
	} else if item.PublishedParsed != nil {
		// If no Updated, use Published as Updated time as well
		parsed.Updated = item.PublishedParsed.UTC()
	}

	if item.PublishedParsed != nil {
		parsed.Published = item.PublishedParsed.UTC()
	} else if item.UpdatedParsed != nil {
		// If no Published, use Updated as Published time
		// This is less ideal, but better than zero time
		parsed.Published = item.UpdatedParsed.UTC()
	}

	// Ensure non-zero times if possible
	if parsed.Updated.IsZero() && !parsed.Published.IsZero() {
		parsed.Updated = parsed.Published
	}
	if parsed.Published.IsZero() && !parsed.Updated.IsZero() {
		parsed.Published = parsed.Updated
	}

	// Add more field conversions here (Images, Enclosures, etc.) if needed

	return parsed
}

// convertFeedMetadata transforms gofeed.Feed metadata into our ParsedFeed struct.
func convertFeedMetadata(gfFeed *gofeed.Feed) *ParsedFeed {
	if gfFeed == nil {
		return nil
	}
	parsed := &ParsedFeed{
		Title:       gfFeed.Title,
		Description: gfFeed.Description,
		Link:        gfFeed.Link,
		FeedLink:    gfFeed.FeedLink,
	}
	if gfFeed.UpdatedParsed != nil {
		parsed.Updated = gfFeed.UpdatedParsed.UTC()
	} else if gfFeed.PublishedParsed != nil {
		// Fallback to published if updated is missing
		parsed.Updated = gfFeed.PublishedParsed.UTC()
	}

	if gfFeed.Author != nil {
		parsed.AuthorName = gfFeed.Author.Name
		parsed.AuthorEmail = gfFeed.Author.Email
	}

	return parsed
}
