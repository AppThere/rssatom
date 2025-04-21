package main

import (
	"context"
	"database/sql" // Import standard SQL package
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	// --- Internal Dependencies ---
	// Ensure these paths match your actual go module structure
	"github.com/appthere/social-ingest-rssatom/internal/config"
	"github.com/appthere/social-ingest-rssatom/internal/fetching"
	"github.com/appthere/social-ingest-rssatom/internal/messaging" // Using this for MQ client
	"github.com/appthere/social-ingest-rssatom/internal/parsing"
	"github.com/appthere/social-ingest-rssatom/internal/scheduler" // Using this
	"github.com/appthere/social-ingest-rssatom/internal/storage"
	"github.com/appthere/social-ingest-rssatom/internal/subscriber"   // Using this
	"github.com/appthere/social-ingest-rssatom/internal/subscription" // Using this (package name is subscription)
	"github.com/appthere/social-ingest-rssatom/internal/translation"  // Added translation notifier

	// --- External Dependencies ---
	// _ "github.com/joho/godotenv/autoload" // Example: For .env files
	_ "github.com/lib/pq" // Example: Postgres driver (ensure this matches your DSN)
	// _ "github.com/go-sql-driver/mysql" // Example: MySQL driver
	// _ "github.com/mattn/go-sqlite3"    // Example: SQLite driver
	// _ "github.com/rabbitmq/amqp091-go" // Example: RabbitMQ client (Imported in messaging pkg)
)

const (
	// Default shutdown timeout allows ongoing operations to complete.
	defaultShutdownTimeout = 15 * time.Second
	// Default DB connection verification timeout
	dbPingTimeout = 5 * time.Second
)

func main() {
	// 1. Initialize Logger
	// TODO: Implement log level parsing from config if desired
	logLevel := slog.LevelInfo
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)
	logger.Info("Starting ingester service...")

	// 2. Load Configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Error("Failed to load configuration", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("Configuration loaded successfully")
	// Example: Log a config value (be careful with secrets)
	logger.Info("Database DSN loaded", slog.String("dsn_prefix", cfg.Database.DSN[:min(15, len(cfg.Database.DSN))]+"...")) // Log prefix only
	logger.Info("MQ URL loaded", slog.String("mq_url", cfg.MessageQueue.URL))
	logger.Info("Translation config",
		slog.Bool("enabled", cfg.Translation.Enabled),
		slog.String("topic", cfg.Translation.TargetTopic),
	)
	logger.Info("Subscription queue", slog.String("queue", cfg.MessageQueue.SubscriptionQueue))

	// 3. Initialize Dependencies
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupSignalHandler(logger, cancel)

	// --- Initialize Database Connection Pool ---
	logger.Info("Initializing database connection pool...", slog.String("driver", "postgres"))
	dbPool, err := sql.Open("postgres", cfg.Database.DSN)
	if err != nil {
		logger.Error("Failed to open database connection pool", slog.Any("error", err))
		os.Exit(1)
	}

	dbPool.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	dbPool.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	dbPool.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)
	logger.Info("Database pool configured",
		slog.Int("max_open_conns", cfg.Database.MaxOpenConns),
		slog.Int("max_idle_conns", cfg.Database.MaxIdleConns),
		slog.Duration("conn_max_lifetime", cfg.Database.ConnMaxLifetime),
	)

	pingCtx, pingCancel := context.WithTimeout(ctx, dbPingTimeout)
	defer pingCancel()
	if err = dbPool.PingContext(pingCtx); err != nil {
		logger.Error("Failed to ping database", slog.Any("error", err))
		_ = dbPool.Close()
		os.Exit(1)
	}
	logger.Info("Database connection pool established and verified")

	defer func() {
		logger.Info("Closing database connection pool...")
		if err := dbPool.Close(); err != nil {
			logger.Error("Error closing database connection pool", slog.Any("error", err))
		} else {
			logger.Info("Database connection pool closed.")
		}
	}()

	// --- Initialize Storage Component ---
	dbStore := storage.NewSQLStore(dbPool, logger)
	logger.Info("Storage component initialized (SQLStore)")

	// --- Initialize Message Queue Client ---
	// CORRECTED: Pass the whole MessageQueueConfig struct and logger
	mqClient, err := messaging.NewClient(cfg.MessageQueue, logger)
	if err != nil {
		logger.Error("Failed to initialize message queue client", slog.Any("error", err))
		os.Exit(1) // Assuming MQ is critical
	}
	defer func() {
		logger.Info("Closing message queue connection...")
		if err := mqClient.Close(); err != nil {
			logger.Error("Error closing message queue connection", slog.Any("error", err))
		} else {
			logger.Info("Message queue connection closed.")
		}
	}()
	logger.Info("Message queue client connected")

	// --- Initialize Fetcher ---
	fetcherCfg := fetching.DefaultConfig()
	// TODO: Populate fetcherCfg from main cfg if needed
	fetcher := fetching.NewFetcher(fetcherCfg, logger)
	logger.Info("Fetcher initialized")

	// --- Initialize Parser ---
	parser := parsing.NewParser(logger)
	logger.Info("Parser initialized")

	// --- Initialize Subscription Manager ---
	subManager := subscription.NewManager(dbStore, logger)
	logger.Info("Subscription manager initialized")

	// --- Initialize Translation Notifier ---
	notifier, err := translation.NewNotifier(cfg.Translation, mqClient, logger)
	if err != nil {
		logger.Error("Failed to initialize translation notifier", slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("Translation notifier initialized")

	// 4. Initialize Application Components (using initialized dependencies)

	// Initialize the subscription handler (needs Subscription Manager, MQ Client, Queue Name)
	// CORRECTED: Added cfg.MessageQueue.SubscriptionQueue argument
	subscriptionHandler := subscriber.NewSubscriptionHandler(
		subManager,
		mqClient,
		cfg.MessageQueue.SubscriptionQueue, // Pass the queue name from config
		logger,
	)
	logger.Info("Subscription handler initialized")

	// Initialize the Scheduler (needs Config, Store, Fetcher, Parser, Notifier)
	feedScheduler := scheduler.New(
		cfg.Scheduler,
		dbStore,
		fetcher,
		parser,
		notifier,
		logger,
	)
	logger.Info("Feed scheduler initialized")

	// 5. Start Services (using goroutines and WaitGroup)
	var wg sync.WaitGroup

	// Start the subscription request listener
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Starting subscription handler...")
		if err := subscriptionHandler.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Subscription handler failed", slog.Any("error", err))
			cancel() // Trigger shutdown if critical
		}
		logger.Info("Subscription handler stopped.")
	}()

	// Start the feed fetching scheduler
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("Starting feed scheduler...")
		feedScheduler.Start(ctx)
		logger.Info("Feed scheduler stopped.")
	}()

	// 6. Wait for Shutdown Signal (blocks here)
	<-ctx.Done()
	logger.Info("Shutdown signal received.")

	// 7. Graceful Shutdown
	logger.Info("Initiating graceful shutdown...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All background services stopped gracefully.")
	case <-shutdownCtx.Done():
		logger.Warn("Shutdown timeout exceeded. Forcing exit.")
	}

	// Deferred cleanup functions (dbPool.Close(), mqClient.Close()) will run now.

	logger.Info("Ingester service shut down successfully.")
	os.Exit(0)
}

// setupSignalHandler registers for SIGINT and SIGTERM and cancels the main context.
func setupSignalHandler(logger *slog.Logger, cancel context.CancelFunc) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signalChan
		logger.Info("Received signal, initiating shutdown...", slog.String("signal", sig.String()))
		cancel()
	}()
	logger.Debug("Signal handler set up.")
}

// Helper for logging DSN prefix safely
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// --- Placeholder Signatures (For Reference) ---
// package config (Ensure fields used above exist)
// package messaging
// func NewClient(cfg config.MessageQueueConfig, logger *slog.Logger) (Client, error)
// package subscriber
// func NewSubscriptionHandler(subManager *subscription.Manager, mq messaging.Client, queueName string, log *slog.Logger) *SubscriptionHandler
// package scheduler
// func New(cfg config.SchedulerConfig, store storage.Store, fetcher *fetching.Fetcher, parser *parsing.Parser, notifier translation.Notifier, log *slog.Logger) *Scheduler
