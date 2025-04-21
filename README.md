# RSS/Atom Ingest Service (github.com/appthere/rssatom)

This service is responsible for ingesting content from RSS and Atom feeds for a larger social media aggregation platform. It handles feed subscriptions, fetches content periodically, parses feeds, identifies new items, stores relevant metadata, and notifies downstream services about new content.

## Overview

The `rssatom` ingester performs the following core functions:

1.  **Subscription Management:** Listens for requests (typically via a message queue) to subscribe to new feed URLs. It manages the list of tracked feeds and their metadata in a database.
2.  **Scheduled Fetching:** Periodically checks the database for feeds that are due to be fetched based on configurable intervals.
3.  **Content Fetching:** Retrieves feed content over HTTP, respecting conditional GET headers (`ETag`, `If-Modified-Since`) to minimize bandwidth and processing. Includes retry logic for transient errors.
4.  **Parsing & Comparison:** Parses the fetched RSS/Atom XML content, identifies items (posts/articles), and compares them against previously seen items for that feed to detect new content.
5.  **Storage:** Stores feed metadata (URL, fetch schedule, ETag, etc.) and information about processed items (GUIDs, potentially full item details) in a persistent data store (e.g., PostgreSQL).
6.  **Notification:** Publishes newly detected items to a downstream service (e.g., a "Protocol Translation Service") via a message queue for further processing and aggregation.

## Architecture

The service follows a modular design, with components interacting primarily through interfaces and well-defined responsibilities:

+---------------------+ +-----------------+ +-----------------------+ +----------------+ | A: Subscription Req | ---> | Subscriber | ---> | B,C,D,E: Subscription | ---> | F: Feed Meta | | (e.g., MQ Message) | | (internal/ | | Manager | | Storage | +---------------------+ | subscriber) | | (internal/ | | (internal/ | +-----------------+ | subscription) | | storage) | +-----------------------+ +-------+--------+ | | (Feed List) V +-----------------------+ +-----------------+ +-----------------+ +-----------+----+ | K: Notify Protocol | <--- | Notifier | <--- | J: Parser | <--- | H: Fetcher | <--- G: Scheduler | | Translation Service | | (internal/ | | (internal/ | | (internal/ | | (internal/ | | (e.g., MQ Consumer) | | translation) | | parsing) | | fetching) | | scheduler) | +-----------------------+ +-------+---------+ +--------+--------+ +----------------+ +--------------+ | (New Items) | (Feed XML) V V +--------------------------------+ | I: Item Storage (GUIDs, etc.) | | (internal/storage) | +--------------------------------+


**Flow:**

1.  **(A) Subscription Request:** An external request (e.g., via MQ) arrives at the `subscriber`.
2.  The `subscriber` parses the request and calls the `subscription.Manager` **(B, C, D, E)**.
3.  The `subscription.Manager` interacts with `storage` **(F)** to add or update the feed record, potentially scheduling an immediate check.
4.  **(G) Scheduler:** Periodically queries `storage` **(F)** for feeds due for checking.
5.  For each due feed, the `scheduler` coordinates with the `fetching` component **(H)**.
6.  The `fetcher` retrieves the feed content (XML) using HTTP, handling conditional requests and retries.
7.  The fetched content is passed to the `parsing` component **(J)**.
8.  The `parser` reads the XML, compares item GUIDs against known GUIDs retrieved from `storage` **(I)**, and identifies new items.
9.  New items are saved via the `storage` component **(I)**.
10. Successfully saved new items are passed to the `translation.Notifier` **(K)**.
11. The `notifier` formats the items and publishes them to a message queue topic for downstream consumption.
12. The `scheduler` updates the feed's metadata (last checked time, next check time, ETag, etc.) in `storage` **(F)**.

## Features

*   Supports RSS and Atom feed formats (via `gofeed`).
*   Handles feed subscriptions via message queue.
*   Conditional fetching using `ETag` and `Last-Modified` headers.
*   Configurable fetch intervals with jitter and exponential backoff on errors.
*   Limits concurrent fetch operations.
*   Detects new items based on unique identifiers (GUID/Link).
*   Stores feed metadata and item identifiers persistently (SQL database).
*   Notifies downstream services of new items via message queue.
*   Configurable via environment variables.
*   Graceful shutdown handling.
*   Structured logging (`slog`).

## Configuration

The service is configured primarily through environment variables, processed using `github.com/kelseyhightower/envconfig`. All variables are prefixed with `INGESTER_`.

Refer to `internal/config/config.go` for the complete list of options and their defaults.

**Key Environment Variables:**

*   `INGESTER_LOG_LEVEL`: Logging level (`debug`, `info`, `warn`, `error`). Default: `info`.
*   **Database:**
    *   `INGESTER_DATABASE_DSN`: **Required.** Data Source Name for the SQL database (e.g., `postgres://user:pass@host:port/db?sslmode=disable`).
    *   `INGESTER_DATABASE_MAX_OPEN_CONNS`: Max open DB connections. Default: `25`.
    *   `INGESTER_DATABASE_MAX_IDLE_CONNS`: Max idle DB connections. Default: `10`.
    *   `INGESTER_DATABASE_CONN_MAX_LIFETIME`: Max connection reuse time. Default: `5m`.
*   **Message Queue (RabbitMQ Example):**
    *   `INGESTER_MQ_URL`: **Required.** Connection URL for the message broker (e.g., `amqp://guest:guest@localhost:5672/`).
    *   `INGESTER_MQ_SUBSCRIPTION_QUEUE`: **Required.** Queue name for receiving new subscription requests. Default: `ingester.subscriptions`.
*   **Scheduler:**
    *   `INGESTER_SCHEDULER_DEFAULT_INTERVAL`: Default time between fetches for a feed. Default: `15m`.
    *   `INGESTER_SCHEDULER_MAX_CONCURRENT_FETCHES`: Max number of feeds to fetch simultaneously. Default: `10`.
*   **Translation/Notifier:**
    *   `INGESTER_TRANSLATION_ENABLED`: Enable sending notifications. Default: `true`.
    *   `INGESTER_TRANSLATION_TARGET_TOPIC`: **Required if enabled.** MQ topic for publishing new items. Default: `social.content.new`.

## Prerequisites

*   **Go:** Version 1.21 or later (due to `log/slog`).
*   **Database:** A running SQL database instance (e.g., PostgreSQL) compatible with the chosen driver (`github.com/lib/pq` is used in examples).
*   **Message Queue:** A running message broker instance (e.g., RabbitMQ) compatible with the chosen client (`github.com/rabbitmq/amqp091-go` is used in examples).
*   **Database Schema:** The required database tables must be created. Use a migration tool (e.g., `golang-migrate/migrate`, `sql-migrate`) to manage the schema based on SQL files (not included in this README, assumed to exist).

## Building

To build the binary:

```bash
go build -o rssatom_ingester ./cmd/ingester
```

This will create an executable file named rssatom_ingester in the current directory.

## Running

Set Environment Variables: Export the required environment variables listed in the Configuration section.

```bash
export INGESTER_DATABASE_DSN="postgres://user:password@localhost:5432/ingesterdb?sslmode=disable"
export INGESTER_MQ_URL="amqp://guest:guest@localhost:5672/"
export INGESTER_MQ_SUBSCRIPTION_QUEUE="ingester.subscriptions"
export INGESTER_TRANSLATION_TARGET_TOPIC="social.content.new"
# Add other variables as needed...
```

Run the Binary:

```bash
./rssatom_ingester
```

Or, run directly using go run:

```bash
go run ./cmd/ingester/main.go
```

The service will start, initialize components, and begin listening for subscription requests and scheduling feed checks. Logs will be printed to standard output in JSON format.

## Running with Docker (Example)

## Create a Dockerfile:

```dockerfile
# Stage 1: Build the application
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary statically linked
RUN CGO_ENABLED=0 go build -ldflags="-w -s" -o /rssatom_ingester ./cmd/ingester

# Stage 2: Create the final small image
FROM alpine:latest

# Optional: Add ca-certificates for HTTPS fetching
RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /rssatom_ingester /app/rssatom_ingester

# Expose any ports if the service had an HTTP API (not currently the case)
# EXPOSE 8080

# Set the entrypoint
ENTRYPOINT ["/app/rssatom_ingester"]
```

Build the Docker Image:

```bash
docker build -t rssatom-ingester:latest .
```

Run the Docker Container: Pass configuration via environment variables.

```bash
docker run --rm -it \
  --name rssatom_ingester_app \
  -e INGESTER_DATABASE_DSN="postgres://user:password@docker_host_ip:5432/ingesterdb?sslmode=disable" \
  -e INGESTER_MQ_URL="amqp://guest:guest@docker_host_ip:5672/" \
  -e INGESTER_MQ_SUBSCRIPTION_QUEUE="ingester.subscriptions" \
  -e INGESTER_TRANSLATION_TARGET_TOPIC="social.content.new" \
  # Add other necessary env vars... \
  rssatom-ingester:latest
```

(Note: Replace docker_host_ip with the appropriate IP address accessible from within the Docker container, or use Docker networking features like --network host or custom networks.)

## Dependencies

Key external libraries used:

* github.com/kelseyhightower/envconfig: For loading configuration from environment variables.
* github.com/mmcdole/gofeed: For parsing RSS and Atom feeds.
* github.com/rabbitmq/amqp091-go: RabbitMQ client library (example MQ implementation).
* github.com/lib/pq: PostgreSQL driver (example DB implementation).

Standard library packages like database/sql, net/http, context, log/slog, sync, time, encoding/json are also heavily used.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request. (Add more specific guidelines if desired).

### License

This project is licensed under the Apache License 2.0.