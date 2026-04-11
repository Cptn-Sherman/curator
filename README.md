# Curator - Content Management System

A complete content management solution with a Rust backend server and Firefox browser extension.

## Project Structure

```
curator/
├── server/          # Rust backend API server
└── extension/       # Firefox browser extension
```

## Quick Start

### Server Setup

```bash
cd server
cargo run
```

The server will start on `http://localhost:8080` by default.

Configuration is managed via `config.toml`:
- Database settings
- Server host and port
- Processor thread pool and startup settings

### Extension Setup

1. Navigate to Firefox's extension debugging page: `about:debugging#/runtime/this-firefox`
2. Click "Load Temporary Add-on"
3. Select `extension/manifest.json`

**Configure the extension URL** in the extension settings to point to your server (default: `http://localhost:8080`)

## Features

### Server
- **RESTful API** for all operations
- **Link Management**: Ingest, track, and process links
- **Subscriptions**: Manage content subscriptions with automated polling
- **Database**: SQLite with automatic backups
- **Processing**: Configurable background link processor

### Extension
- **Save Links**: Quickly save the current page or any link
- **Manage Subscriptions**: View and manage content subscriptions
- **Check Status**: Real-time stats about links, processing, and subscriptions
- **Processing Control**: Enable/disable processing from extension
- **Context Menu**: Right-click to save links/pages

## API Endpoints

- `GET /` - Service info
- `GET /health` - Health check
- `GET /graphql` - GraphQL health endpoint
- `POST /ingest` - Ingest links
- `GET /check` - Check if URL exists
- `GET /processing/status` - Get processing status
- `POST /processing/toggle` - Toggle processing
- `GET /links/unprocessed` - Get unprocessed count
- `GET /subscriptions` - List subscriptions
- `POST /subscriptions` - Create subscription

## Development

### Requirements
- Rust 1.70+
- Firefox (for extension testing)

### Building

```bash
cd server
cargo build --release
```

The release binary will be in `server/target/release/curator`.

## License

See `server/LICENSE` for details.
