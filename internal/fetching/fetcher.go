// Package fetching implements the logic for fetching feed content over HTTP,
// including handling conditional requests (ETag, Last-Modified) and retries.
package fetching

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"time"

	// It's good practice to define a user agent
	"runtime"
)

// DefaultUserAgent is used if no specific user agent is configured.
// It's polite to identify your client.
var DefaultUserAgent = fmt.Sprintf("SocialAggregatorIngester/1.0 (+https://your-project-url.com; %s)", runtime.Version())

// --- Configuration ---

// Config holds configuration specific to the Fetcher.
type Config struct {
	RequestTimeout    time.Duration // Timeout for the entire HTTP request lifecycle.
	MaxRetries        int           // Maximum number of retry attempts for transient errors.
	InitialRetryDelay time.Duration // Base delay for exponential backoff.
	MaxRetryDelay     time.Duration // Maximum delay between retries.
	UserAgent         string        // Custom User-Agent string.
}

// DefaultConfig returns a reasonable default configuration for the Fetcher.
func DefaultConfig() Config {
	return Config{
		RequestTimeout:    30 * time.Second,
		MaxRetries:        3,
		InitialRetryDelay: 500 * time.Millisecond,
		MaxRetryDelay:     5 * time.Second,
		UserAgent:         DefaultUserAgent,
	}
}

// --- Fetch Result ---

// FetchResult encapsulates the outcome of a fetch operation.
type FetchResult struct {
	// Content holds the raw bytes of the fetched feed if successful and modified.
	// Nil if the status was 304 Not Modified or an error occurred.
	Content []byte
	// ETag is the ETag header value from the response, if present.
	ETag string
	// LastModified is the Last-Modified header value from the response, if present.
	LastModified string
	// NotModified is true if the server responded with HTTP 304 Not Modified.
	NotModified bool
	// FinalURL is the URL of the resource after any redirects.
	FinalURL string
	// FetchedAt is the timestamp when the fetch attempt concluded (successfully or not).
	FetchedAt time.Time
}

// --- Fetcher Implementation ---

// Fetcher is responsible for fetching feed content.
type Fetcher struct {
	httpClient *http.Client
	config     Config
	logger     *slog.Logger
}

// NewFetcher creates a new Fetcher instance.
func NewFetcher(cfg Config, logger *slog.Logger) *Fetcher {
	// Customize the transport if needed (e.g., MaxIdleConns, IdleConnTimeout)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Example customization:
	// transport.MaxIdleConns = 100
	// transport.MaxIdleConnsPerHost = 10
	// transport.IdleConnTimeout = 90 * time.Second

	client := &http.Client{
		Timeout:   cfg.RequestTimeout, // Overall request timeout
		Transport: transport,
		// Prevent infinite redirect loops
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return http.ErrUseLastResponse // Or a custom error: fmt.Errorf("stopped after 10 redirects")
			}
			return nil // Allow redirect
		},
	}

	return &Fetcher{
		httpClient: client,
		config:     cfg,
		logger:     logger.With(slog.String("component", "fetcher")), // Add component context to logger
	}
}

// Fetch attempts to retrieve content from the given URL.
// It uses the provided currentETag and currentLastModified for conditional GET requests.
// It handles retries for transient network errors and specific server-side errors (5xx).
func (f *Fetcher) Fetch(ctx context.Context, url string, currentETag string, currentLastModified string) (*FetchResult, error) {
	fetchLog := f.logger.With(slog.String("url", url)) // Logger specific to this fetch attempt
	var lastErr error

	for attempt := 0; attempt <= f.config.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			fetchLog.Warn("Context cancelled before fetch attempt", slog.Int("attempt", attempt), slog.Any("error", ctx.Err()))
			if lastErr == nil {
				lastErr = ctx.Err() // Make sure we return the context error if it's the reason we stopped
			}
			return nil, fmt.Errorf("fetch cancelled: %w", lastErr)
		default:
			// Proceed with fetch attempt
		}

		if attempt > 0 {
			delay := calculateBackoff(attempt, f.config.InitialRetryDelay, f.config.MaxRetryDelay)
			fetchLog.Info("Retrying fetch",
				slog.Int("attempt", attempt),
				slog.Duration("delay", delay),
				slog.Any("last_error", lastErr), // Log the error that triggered the retry
			)
			select {
			case <-time.After(delay):
				// Wait complete
			case <-ctx.Done():
				fetchLog.Warn("Context cancelled during retry backoff", slog.Int("attempt", attempt), slog.Any("error", ctx.Err()))
				return nil, fmt.Errorf("fetch cancelled during retry backoff: %w", ctx.Err())
			}
		}

		// --- Create Request ---
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			// This error is unlikely unless URL is fundamentally invalid or context setup failed
			fetchLog.Error("Failed to create HTTP request", slog.Any("error", err))
			return nil, fmt.Errorf("failed to create request for %s: %w", url, err) // Non-retryable
		}

		// --- Set Headers ---
		req.Header.Set("User-Agent", f.config.UserAgent)
		req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7")
		if currentETag != "" {
			req.Header.Set("If-None-Match", currentETag)
		}
		if currentLastModified != "" {
			// Note: Ensure currentLastModified is in the correct RFC1123 format if setting manually.
			// http.TimeFormat = "Mon, 02 Jan 2006 15:04:05 GMT"
			req.Header.Set("If-Modified-Since", currentLastModified)
		}

		fetchLog.Debug("Executing HTTP GET", slog.Int("attempt", attempt+1))

		// --- Execute Request ---
		resp, err := f.httpClient.Do(req)
		fetchedAt := time.Now().UTC() // Record time immediately after Do returns

		if err != nil {
			lastErr = fmt.Errorf("http client error on attempt %d: %w", attempt+1, err)
			fetchLog.Warn("HTTP request failed", slog.Int("attempt", attempt+1), slog.Any("error", err))
			// Decide if retryable (network errors, timeouts usually are)
			// Simple check: if context wasn't cancelled, assume potentially transient
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, lastErr // Don't retry if context is done
			}
			// Continue to next retry iteration
			continue
		}

		// --- Process Response ---
		// Ensure body is always closed
		defer resp.Body.Close()

		finalURL := url
		if resp.Request != nil && resp.Request.URL != nil {
			finalURL = resp.Request.URL.String() // Capture URL after redirects
		}

		result := &FetchResult{
			ETag:         resp.Header.Get("ETag"),
			LastModified: resp.Header.Get("Last-Modified"),
			FinalURL:     finalURL,
			FetchedAt:    fetchedAt,
		}

		fetchLog = fetchLog.With(
			slog.Int("status_code", resp.StatusCode),
			slog.String("etag", result.ETag),
			slog.String("last_modified", result.LastModified),
			slog.String("final_url", result.FinalURL),
		)

		switch resp.StatusCode {
		case http.StatusOK: // 200
			bodyBytes, err := io.ReadAll(resp.Body)
			if err != nil {
				lastErr = fmt.Errorf("failed to read response body (status %d) on attempt %d: %w", resp.StatusCode, attempt+1, err)
				fetchLog.Error("Failed to read response body", slog.Any("error", err))
				// This could be transient (e.g., connection reset during read), so allow retry
				continue // Retry
			}
			result.Content = bodyBytes
			result.NotModified = false
			fetchLog.Info("Fetch successful", slog.Int("content_length", len(result.Content)))
			return result, nil // Success!

		case http.StatusNotModified: // 304
			result.NotModified = true
			fetchLog.Info("Fetch successful: Content not modified")
			// Drain the body to allow connection reuse, even though it should be empty for 304
			_, _ = io.Copy(io.Discard, resp.Body)
			return result, nil // Success (but no new content)!

		case http.StatusNotFound: // 404
			lastErr = fmt.Errorf("fetch failed for %s: %w", url, ErrNotFound)
			fetchLog.Warn("Fetch failed: Not Found (404)")
			return nil, lastErr // Permanent error, don't retry

		case http.StatusForbidden: // 403
			lastErr = fmt.Errorf("fetch failed for %s: %w", url, ErrForbidden)
			fetchLog.Warn("Fetch failed: Forbidden (403)")
			return nil, lastErr // Permanent error, don't retry

		case http.StatusUnauthorized: // 401
			lastErr = fmt.Errorf("fetch failed for %s: %w", url, ErrUnauthorized)
			fetchLog.Warn("Fetch failed: Unauthorized (401)")
			return nil, lastErr // Permanent error, don't retry

		// Add other 4xx codes you want to handle specifically (e.g., 410 Gone)

		default:
			// Includes 5xx server errors and unexpected 4xx errors
			lastErr = fmt.Errorf("unexpected HTTP status %d for %s on attempt %d", resp.StatusCode, url, attempt+1)
			fetchLog.Warn("Fetch failed: Unexpected status code", slog.Any("error", lastErr))
			// Retry for 5xx or potentially transient issues
			if resp.StatusCode >= 500 && resp.StatusCode < 600 {
				// Drain body for potential connection reuse
				_, _ = io.Copy(io.Discard, resp.Body)
				continue // Retry on 5xx
			} else {
				// Treat other unexpected codes (e.g., weird 4xx) as permanent for now
				return nil, lastErr
			}
		}
	}

	// If loop finishes, all retries failed
	fetchLog.Error("Fetch failed after all retries", slog.Int("max_retries", f.config.MaxRetries), slog.Any("last_error", lastErr))
	return nil, fmt.Errorf("fetch failed for %s after %d retries: %w", url, f.config.MaxRetries, lastErr)
}

// calculateBackoff computes the delay for the next retry attempt using exponential backoff.
func calculateBackoff(attempt int, initialDelay, maxDelay time.Duration) time.Duration {
	// Exponential backoff: initialDelay * 2^(attempt-1)
	// attempt starts at 1 for the first *retry*
	backoff := float64(initialDelay) * math.Pow(2, float64(attempt-1))
	delay := time.Duration(backoff)

	// Cap the delay at maxDelay
	if delay > maxDelay {
		delay = maxDelay
	}
	// Add some jitter? (e.g., +/- 10%) - Optional but good practice
	// jitter := time.Duration(rand.Intn(int(delay)/5)) - (delay / 10)
	// delay += jitter
	// if delay < 0 { delay = 0 }

	return delay
}

// --- Custom Errors ---
// Define sentinel errors for common permanent fetch failures.
var (
	ErrNotFound     = fmt.Errorf("resource not found (404)")
	ErrForbidden    = fmt.Errorf("access forbidden (403)")
	ErrUnauthorized = fmt.Errorf("authentication required (401)")
	// Add others as needed (e.g., ErrGone for 410)
)

// Helper function to read body safely (example - incorporated above now)
func readBody(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	// Consider limiting read size to prevent OOM on massive responses
	// limitedReader := &io.LimitedReader{R: resp.Body, N: 10 * 1024 * 1024} // 10 MB limit
	// bodyBytes, err := io.ReadAll(limitedReader)
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed reading response body: %w", err)
	}
	return bodyBytes, nil
}

// Helper function to check if an error suggests a retry is warranted
// (Example - logic incorporated directly into Fetch loop now)
// func isRetryableError(err error, statusCode int) bool {
// 	if err != nil {
// 		// Network errors, timeouts etc. are generally retryable
// 		// Check for specific non-retryable net errors if needed
// 		return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
// 	}
// 	// Retry on 5xx server errors
// 	return statusCode >= 500 && statusCode < 600
// }
