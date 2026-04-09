# curator

Curator is a content downloader and organizer built using Rust

## Features

- Ingest links for processing
- Track link processing state
- Manage subscriptions for automated polling
- RESTful API for all operations
- SQLite database with automated backups
- Configurable via TOML

## Setup

1. Create a `config.toml` file:

```toml
[database]
name = "curator.db"
max_backups = 5

[processor]
thread_pool_size = 4
enabled_on_start = true

[server]
host = "0.0.0.0"
port = 8080
```

2. Build and run:

```bash
cargo build
cargo run
```

The server will start on `http://0.0.0.0:8080`

## API Usage

### Health Check

```bash
GET /health
```

Returns server status.

### Ingest Links

```bash
POST /ingest
Content-Type: application/json

{
  "links": ["https://example.com", "https://example.org"],
  "subscription_id": "optional-subscription-id"
}
```

Returns ingested link IDs and duplicate count.

### Check Link Exists

```bash
GET /check?url=https://example.com
```

Returns whether a URL exists in the database.

### Link Processing

**Get processing status:**
```bash
GET /processing/status
```

**Toggle processing on/off:**
```bash
POST /processing/toggle
```

**Get unprocessed links count:**
```bash
GET /links/unprocessed
```

### Subscriptions

**List all subscriptions:**
```bash
GET /subscriptions
```

**Create a subscription:**
```bash
POST /subscriptions
Content-Type: application/json

{
  "name": "My Feed",
  "url": "https://example.com/feed",
  "polling_interval_seconds": 3600
}
```

## Database

The database is automatically initialized on first run with the following structure:

- `links` - Stores URLs to be processed
  - `id` - Unique identifier
  - `url` - Link URL
  - `created_at` - Creation timestamp
  - `processed_at` - Processing completion timestamp (null if unprocessed)
  - `subscription_id` - Associated subscription (optional)

- `subscriptions` - Manages content sources
  - `id` - Unique identifier
  - `name` - Subscription name
  - `url` - Source URL to poll
  - `polling_interval_seconds` - How often to check for new content
  - `last_polled_at` - Last polling timestamp
  - `created_at` - Creation timestamp
  - `active` - Whether subscription is active

Database backups are automatically created before migrations and stored in the `backups/` directory.
