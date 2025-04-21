// Package config handles loading and validating application configuration.
// Configuration values are primarily sourced from environment variables.
package config

import (
	"fmt"
	"time"

	// Import fetching config if you want to embed it directly, otherwise remove if not used here
	// "github.com/appthere/social-ingest-rssatom/internal/fetching"

	"github.com/kelseyhightower/envconfig" // For loading config from env vars
)

// Config holds the application's configuration settings.
type Config struct {
	Database     DatabaseConfig
	MessageQueue MessageQueueConfig
	Scheduler    SchedulerConfig
	Translation  TranslationConfig // Added Translation configuration
	// Fetcher      fetching.Config // Example: If you embed fetcher config
	LogLevel string `envconfig:"LOG_LEVEL" default:"info"` // e.g., debug, info, warn, error
}

// DatabaseConfig holds database-related configuration.
type DatabaseConfig struct {
	// DSN is the Data Source Name for connecting to the database.
	// Example: "postgres://user:password@host:port/dbname?sslmode=disable"
	DSN string `envconfig:"DATABASE_DSN" required:"true"`
	// MaxOpenConns sets the maximum number of open connections to the database.
	MaxOpenConns int `envconfig:"DATABASE_MAX_OPEN_CONNS" default:"25"`
	// MaxIdleConns sets the maximum number of connections in the idle connection pool.
	MaxIdleConns int `envconfig:"DATABASE_MAX_IDLE_CONNS" default:"10"`
	// ConnMaxLifetime sets the maximum amount of time a connection may be reused.
	ConnMaxLifetime time.Duration `envconfig:"DATABASE_CONN_MAX_LIFETIME" default:"5m"`
}

// MessageQueueConfig holds message queue related configuration.
type MessageQueueConfig struct {
	// URL is the connection string for the message queue broker.
	// Example: "amqp://guest:guest@localhost:5672/" for RabbitMQ
	// Example: "nats://localhost:4222" for NATS
	URL string `envconfig:"MQ_URL" required:"true"`
	// Add other MQ specific settings if needed, e.g., queue names
	SubscriptionQueue string `envconfig:"MQ_SUBSCRIPTION_QUEUE" default:"ingester.subscriptions"`
	// FetchedItemsTopic string `envconfig:"MQ_FETCHED_ITEMS_TOPIC" default:"social.content.new"` // Note: This is superseded by Translation.TargetTopic
}

// SchedulerConfig holds configuration for the feed fetching scheduler.
type SchedulerConfig struct {
	// DefaultInterval specifies the default time between fetch attempts for a feed
	// if not otherwise specified by the feed's subscription details.
	DefaultInterval time.Duration `envconfig:"SCHEDULER_DEFAULT_INTERVAL" default:"15m"`
	// MaxConcurrentFetches limits how many feeds can be fetched simultaneously.
	MaxConcurrentFetches int `envconfig:"SCHEDULER_MAX_CONCURRENT_FETCHES" default:"10"`
	// Add other scheduler settings like jitter, timeout per fetch, etc.
	// CheckInterval time.Duration `envconfig:"SCHEDULER_CHECK_INTERVAL" default:"1m"`
	// FeedProcessTimeout time.Duration `envconfig:"SCHEDULER_FEED_PROCESS_TIMEOUT" default:"2m"`
	// FetchTimeout time.Duration `envconfig:"SCHEDULER_FETCH_TIMEOUT" default:"30s"` // Consider moving FetchTimeout to a FetcherConfig struct
}

// TranslationConfig holds settings for notifying downstream services (translation layer).
type TranslationConfig struct {
	// TargetTopic is the message queue topic where new item notifications are sent.
	TargetTopic string `envconfig:"TRANSLATION_TARGET_TOPIC" default:"social.content.new"`
	// Enabled determines if notifications should be sent.
	Enabled bool `envconfig:"TRANSLATION_ENABLED" default:"true"`
	// Format specifies the serialization format (e.g., "json"). Future use.
	// Format string `envconfig:"TRANSLATION_FORMAT" default:"json"`
}

// Load reads configuration from environment variables and returns a Config struct.
// It uses the prefix "INGESTER" for environment variables (e.g., INGESTER_DATABASE_DSN).
func Load() (*Config, error) {
	var cfg Config

	// Process environment variables with the prefix "INGESTER"
	// Example: INGESTER_DATABASE_DSN, INGESTER_MQ_URL, INGESTER_SCHEDULER_DEFAULT_INTERVAL
	err := envconfig.Process("ingester", &cfg) // Lowercase prefix is conventional
	if err != nil {
		return nil, fmt.Errorf("failed to process environment configuration: %w", err)
	}

	// --- Add Custom Validations Here ---
	if cfg.Scheduler.MaxConcurrentFetches <= 0 {
		return nil, fmt.Errorf("invalid configuration: SCHEDULER_MAX_CONCURRENT_FETCHES must be positive (got %d)", cfg.Scheduler.MaxConcurrentFetches)
	}
	if cfg.Scheduler.DefaultInterval <= 0 {
		return nil, fmt.Errorf("invalid configuration: SCHEDULER_DEFAULT_INTERVAL must be positive (got %s)", cfg.Scheduler.DefaultInterval)
	}
	if cfg.Database.MaxOpenConns < 0 {
		return nil, fmt.Errorf("invalid configuration: DATABASE_MAX_OPEN_CONNS cannot be negative (got %d)", cfg.Database.MaxOpenConns)
	}
	if cfg.Database.MaxIdleConns < 0 {
		return nil, fmt.Errorf("invalid configuration: DATABASE_MAX_IDLE_CONNS cannot be negative (got %d)", cfg.Database.MaxIdleConns)
	}
	if cfg.Database.MaxIdleConns > cfg.Database.MaxOpenConns && cfg.Database.MaxOpenConns > 0 {
		// Only validate if MaxOpenConns is positive (0 means unlimited)
		return nil, fmt.Errorf("invalid configuration: DATABASE_MAX_IDLE_CONNS (%d) cannot be greater than DATABASE_MAX_OPEN_CONNS (%d)", cfg.Database.MaxIdleConns, cfg.Database.MaxOpenConns)
	}
	if cfg.Database.ConnMaxLifetime < 0 {
		return nil, fmt.Errorf("invalid configuration: DATABASE_CONN_MAX_LIFETIME cannot be negative (got %s)", cfg.Database.ConnMaxLifetime)
	}

	// Validate Translation config
	if cfg.Translation.Enabled && cfg.Translation.TargetTopic == "" {
		return nil, fmt.Errorf("invalid configuration: TRANSLATION_TARGET_TOPIC must be set when TRANSLATION_ENABLED is true")
	}

	// Validate MessageQueue config
	if cfg.MessageQueue.SubscriptionQueue == "" {
		// Assuming subscriber uses this queue and is always enabled
		return nil, fmt.Errorf("invalid configuration: MQ_SUBSCRIPTION_QUEUE must be set")
	}

	// Could add validation for LogLevel against known values ("debug", "info", "warn", "error")

	return &cfg, nil
}
