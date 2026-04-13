use axum::{
    routing::{get, post},
    response::Html,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;
use tracing::info;
use uuid::Uuid;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Clone)]
struct Link {
    id: String,
    url: String,
    created_at: String,
    processed_at: Option<String>,
    subscription_id: Option<String>,
    source_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Source {
    id: String,
    name: String,
    domain: String,
    rate_limit_per_minute: Option<i32>,
    rate_limit_per_day: Option<i32>,
    api_key: Option<String>,
    user_id: Option<String>,
    notes: Option<String>,
    created_at: String,
    active: bool,
}

#[derive(Deserialize)]
struct CreateSourceRequest {
    name: String,
    domain: String,
    rate_limit_per_minute: Option<i32>,
    rate_limit_per_day: Option<i32>,
    api_key: Option<String>,
    user_id: Option<String>,
    notes: Option<String>,
}

#[derive(Deserialize)]
struct UpdateSourceRequest {
    name: String,
    domain: String,
    rate_limit_per_minute: Option<i32>,
    rate_limit_per_day: Option<i32>,
    api_key: Option<String>,
    user_id: Option<String>,
    notes: Option<String>,
    active: bool,
}

struct SourceCredentials {
    domain: String,
    api_key: Option<String>,
    user_id: Option<String>,
}

#[derive(Serialize)]
struct SourcesListResponse {
    sources: Vec<Source>,
    total_count: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct CreatorTag {
    name: String,
    tag_type: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Creator {
    id: String,
    name: String,
    created_at: String,
    aliases: Vec<String>,
    tags: Vec<CreatorTag>,
}

#[derive(Deserialize)]
struct CreateCreatorRequest {
    name: String,
    #[serde(default)]
    aliases: Vec<String>,
}

#[derive(Deserialize)]
struct UpdateCreatorRequest {
    name: String,
}

#[derive(Deserialize)]
struct AddAliasRequest {
    alias: String,
}

#[derive(Serialize)]
struct CreatorsListResponse {
    creators: Vec<Creator>,
    total_count: i64,
}

#[derive(Serialize, Deserialize, Clone)]
struct TagParent {
    id: String,
    name: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct Tag {
    id: String,
    name: String,
    tag_type: String,
    source_id: Option<String>,
    content_count: i64,
    created_at: String,
    parents: Vec<TagParent>,
}

#[derive(Deserialize)]
struct CreateTagRequest {
    name: String,
    #[serde(default)]
    tag_type: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    #[serde(default)]
    content_count: i64,
}

#[derive(Deserialize)]
struct UpdateTagRequest {
    name: String,
    tag_type: Option<String>,
    source_id: Option<String>,
}

#[derive(Deserialize)]
struct AddTagParentRequest {
    /// Name of an existing tag to set as parent
    parent_name: String,
}

#[derive(Serialize)]
struct TagsListResponse {
    tags: Vec<Tag>,
    total_count: i64,
    page: i64,
    per_page: i64,
}

#[derive(Deserialize)]
struct TagsQuery {
    #[serde(default)]
    search: String,
    #[serde(default)]
    sort_by: Option<String>,
    #[serde(default)]
    sort_dir: Option<String>,
    #[serde(default = "default_tags_page")]
    page: i64,
    #[serde(default = "default_tags_per_page")]
    per_page: i64,
}

fn default_tags_page() -> i64 { 1 }
fn default_tags_per_page() -> i64 { 50 }

#[derive(Serialize)]
struct ContentRecord {
    id: String,
    file_path: Option<String>,
    post_date: Option<String>,
    uploader: Option<String>,
    rating: Option<String>,
    score: Option<i64>,
    source_url: Option<String>,
    source_id: Option<String>,
    created_at: String,
    favorite: bool,
    tags: Vec<String>,
}

#[derive(Serialize)]
struct ContentListResponse {
    content: Vec<ContentRecord>,
    total_count: i64,
    page: i64,
    per_page: i64,
}

#[derive(Deserialize)]
struct ContentQuery {
    #[serde(default)]
    search: String,
    #[serde(default)]
    sort_by: Option<String>,
    #[serde(default)]
    sort_dir: Option<String>,
    #[serde(default = "default_content_page")]
    page: i64,
    #[serde(default = "default_content_per_page")]
    per_page: i64,
}

fn default_content_page() -> i64 { 1 }
fn default_content_per_page() -> i64 { 50 }

#[derive(Serialize, Deserialize, Clone)]
struct Subscription {
    id: String,
    name: String,
    url: String,
    polling_interval_seconds: i32,
    last_polled_at: Option<String>,
    created_at: String,
    active: bool,
}

#[derive(Deserialize)]
struct CreateSubscriptionRequest {
    name: String,
    url: String,
    polling_interval_seconds: i32,
}

#[derive(Deserialize)]
struct UpdateSubscriptionRequest {
    name: Option<String>,
    url: Option<String>,
    polling_interval_seconds: Option<i32>,
    active: Option<bool>,
}

#[derive(Serialize)]
struct SubscriptionResponse {
    id: String,
    name: String,
    url: String,
    polling_interval_seconds: i32,
    last_polled_at: Option<String>,
    created_at: String,
    active: bool,
}

#[derive(Serialize)]
struct SubscriptionsListResponse {
    subscriptions: Vec<SubscriptionResponse>,
    total_count: i64,
}

#[derive(Deserialize)]
struct Config {
    database: DatabaseConfig,
    processor: ProcessorConfig,
    server: ServerConfig,
    storage: StorageConfig,
    #[serde(default)]
    flaresolverr: FlareSolverrConfig,
}

#[derive(Deserialize, Default)]
struct FlareSolverrConfig {
    /// Base URL of a running FlareSolverr instance, e.g. "http://localhost:8191".
    /// Leave empty to disable (curl is used instead, which will fail on Cloudflare-protected sites).
    #[serde(default)]
    url: String,
}

#[derive(Deserialize)]
struct StorageConfig {
    path: String,
}

#[derive(Deserialize)]
struct DatabaseConfig {
    name: String,
    max_backups: Option<usize>,
}

#[derive(Deserialize)]
struct ProcessorConfig {
    thread_pool_size: usize,
    enabled_on_start: bool,
}

#[derive(Deserialize)]
struct ServerConfig {
    host: String,
    port: u16,
}

#[derive(Deserialize)]
struct IngestRequest {
    links: Vec<String>,
    #[serde(default)]
    subscription_id: Option<String>,
    #[serde(default)]
    source_id: Option<String>,
    /// Direct media URL resolved by the extension at save time (bypasses Cloudflare)
    #[serde(default)]
    file_url: Option<String>,
}

#[derive(Serialize)]
struct IngestResponse {
    ingested_count: usize,
    ids: Vec<String>,
    duplicate_count: usize,
    duplicates: Vec<String>,
}

#[derive(Serialize)]
struct CheckResponse {
    url: String,
    exists: bool,
}

#[derive(Serialize)]
struct ProcessingStatusResponse {
    processing_enabled: bool,
}

#[derive(Serialize)]
struct ToggleProcessingResponse {
    processing_enabled: bool,
    message: String,
}

#[derive(Serialize)]
struct UnprocessedLinksResponse {
    unprocessed_count: i64,
}

#[derive(Serialize)]
struct LinkRecord {
    id: String,
    url: String,
    created_at: String,
    processed_at: Option<String>,
    source_id: Option<String>,
    subscription_id: Option<String>,
    error: Option<String>,
}

#[derive(Serialize)]
struct LinksListResponse {
    links: Vec<LinkRecord>,
    total: i64,
}

#[derive(Deserialize, Default)]
struct LinksQuery {
    #[serde(default = "default_limit")]
    limit: i64,
    #[serde(default)]
    offset: i64,
    /// Column to sort by. Allowed: created_at, processed_at, url, source_id, subscription_id
    #[serde(default)]
    sort_by: Option<String>,
    /// Sort direction: asc or desc. Default: desc
    #[serde(default)]
    sort_dir: Option<String>,
}

fn default_limit() -> i64 { 100 }

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    info!("Starting curator service");

    // Load configuration
    let config_content = std::fs::read_to_string("config.toml")
        .expect("Failed to read config.toml");
    let config: Config = toml::from_str(&config_content)
        .expect("Failed to parse config.toml");

    // Log configuration neatly
    info!("=== Configuration ===");
    info!("Database:");
    info!("  Name: {}", config.database.name);
    info!("  Max Backups: {}", config.database.max_backups.unwrap_or(5));
    info!("Processor:");
    info!("  Thread Pool Size: {}", config.processor.thread_pool_size);
    info!("  Enabled on Startup: {}", config.processor.enabled_on_start);
    info!("Server:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!("Storage:");
    info!("  Path: {}", config.storage.path);
    info!("====================");

    // Initialize storage directories
    for subfolder in &["image", "video", "audio", "comic", "document", "thumbnails"] {
        let dir = std::path::Path::new(&config.storage.path).join(subfolder);
        std::fs::create_dir_all(&dir)
            .unwrap_or_else(|e| panic!("Failed to create storage directory {}: {}", dir.display(), e));
    }
    info!("Storage directories initialized at {}", config.storage.path);

    // Initialize database
    let db_pool = init_database(&config.database)
        .await
        .expect("Failed to initialize database");

    info!("Database initialized successfully");

    // Get startup stats
    let links_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links")
        .fetch_one(&db_pool)
        .await
        .unwrap_or(0);

    let subscriptions_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM subscriptions")
        .fetch_one(&db_pool)
        .await
        .unwrap_or(0);

    info!("=== Database Stats ===");
    info!("  Name: {}", config.database.name);
    info!("  Links: {}", links_count);
    info!("  Subscriptions: {}", subscriptions_count);
    info!("====================");

    let thread_pool_size = config.processor.thread_pool_size;

    // Create processing enabled state from config
    let processing_enabled = Arc::new(Mutex::new(config.processor.enabled_on_start));
    
    if config.processor.enabled_on_start {
        info!("Link processing enabled on startup");
    } else {
        info!("Link processing disabled on startup");
    }

    // Spawn the processor task
    let processor_pool = db_pool.clone();
    let processor_state = processing_enabled.clone();
    let storage_path = config.storage.path.clone();
    let flaresolverr_url = config.flaresolverr.url.clone();
    if !flaresolverr_url.is_empty() {
        info!("FlareSolverr enabled at {}", flaresolverr_url);
    } else {
        info!("FlareSolverr not configured — Cloudflare-protected sources will fail");
    }
    let storage_path_state = storage_path.clone();
    tokio::spawn(async move {
        processor_loop(processor_pool, thread_pool_size, processor_state, storage_path, flaresolverr_url).await;
    });

    let backup_pool = db_pool.clone();
    tokio::spawn(async move {
        backup_scheduler_loop(backup_pool).await;
    });

    // Build router
    let app = Router::new()
        .route("/", get(handler_root))
        .route("/health", get(handler_health))
        .route("/graphql", get(handler_graphql).post(handler_graphql))
        .route("/ingest", post(handler_ingest))
        .route("/check", get(handler_check))
        .route("/processing/status", get(handler_processing_status))
        .route("/processing/toggle", post(handler_toggle_processing))
        .route("/links/unprocessed", get(handler_unprocessed_links))
        .route("/links", get(handler_links_list))
        .route("/links/:id", axum::routing::delete(handler_delete_link))
        .route("/links/:id/retry", post(handler_retry_link))
        .route("/subscriptions", get(handler_subscriptions_list).post(handler_create_subscription))
        .route("/sources", get(handler_sources_list).post(handler_create_source))
        .route("/sources/:id", get(handler_get_source).put(handler_update_source).delete(handler_delete_source))
        .route("/sources/:id/ping", post(handler_ping_source))
        .route("/sources/:id/refresh", post(handler_refresh_source))
        .route("/sources/resolve-media", get(handler_resolve_media))
        .route("/creators", get(handler_creators_list).post(handler_create_creator))
        .route("/creators/:id", get(handler_get_creator).put(handler_update_creator).delete(handler_delete_creator))
        .route("/creators/:id/aliases", post(handler_add_alias))
        .route("/creators/:id/aliases/:alias", axum::routing::delete(handler_remove_alias))
        .route("/tags", get(handler_tags_list).post(handler_create_tag))
        .route("/tags/:id", get(handler_get_tag).put(handler_update_tag).delete(handler_delete_tag))
        .route("/tags/:id/parents", post(handler_add_tag_parent))
        .route("/tags/:id/parents/:parent_id", axum::routing::delete(handler_remove_tag_parent))
        .route("/content", get(handler_content_list))
        .route("/content/stats", get(handler_content_stats))
        .route("/content/:id/file", get(handler_content_file))
        .route("/content/:id/thumbnail", get(handler_content_thumbnail))
        .route("/content/:id/favorite", post(handler_toggle_favorite))
        .route("/ignored-domains", get(handler_ignored_domains_list).post(handler_add_ignored_domain))
        .route("/ignored-domains/:domain", axum::routing::delete(handler_remove_ignored_domain))
        .route("/links/stats", get(handler_links_stats))
        .route("/backup", post(handler_backup))
        .route("/backup/settings", get(handler_backup_settings_get).put(handler_backup_settings_put))
        .route("/backup/list", get(handler_backup_list))
        .route("/debug/reset", post(handler_debug_reset))
        .with_state((db_pool, processing_enabled, storage_path_state));

    // Run server
    let server_addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = tokio::net::TcpListener::bind(&server_addr)
        .await
        .expect("Failed to bind to server address");

    info!("Server listening on http://{}", server_addr);

    axum::serve(listener, app)
        .await
        .expect("Server error");
}

async fn init_database(db_config: &DatabaseConfig) -> Result<SqlitePool, sqlx::Error> {
    let db_name = &db_config.name;
    let max_backups = db_config.max_backups.unwrap_or(5);
    
    let database_url = format!("sqlite:{}", db_name);
    let db_path = std::path::Path::new(db_name);
    let db_existed = db_path.exists();

    // Create connection string with auto-create enabled
    let connect_options = SqliteConnectOptions::from_str(&database_url)?
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await?;

    if db_existed {
        info!("Connected to existing database: {}", db_name);
    } else {
        info!("Created new database: {}", db_name);
    }

    // Initialize version tracking table
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS database_version (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            version INTEGER NOT NULL,
            last_migration_at TEXT NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;

    // Get current database version
    let current_version: Option<i32> = sqlx::query_scalar(
        "SELECT version FROM database_version WHERE id = 1"
    )
    .fetch_optional(&pool)
    .await?;

    let current_version = current_version.unwrap_or(0);

    // Run migrations if needed
    if current_version < 1 {
        // Backup before migration if database already existed
        if db_existed && current_version > 0 {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        // Migration 1: Create links table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS links (
                id TEXT PRIMARY KEY,
                url TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                processed_at TEXT
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Update version
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 1, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 1: Created links table");
    }

    // Run migration 2: Add processed_at column
    if current_version < 2 {
        // Backup before migration
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        // Check if processed_at column exists
        let processed_at_exists: bool = sqlx::query(
            "PRAGMA table_info(links)" 
        )
        .fetch_all(&pool)
        .await?
        .iter()
        .any(|row| {
            row.get::<String, _>(1) == "processed_at"
        });

        if !processed_at_exists {
            sqlx::query(
                "ALTER TABLE links ADD COLUMN processed_at TEXT"
            )
            .execute(&pool)
            .await?;
            info!("Added 'processed_at' column to links table");
        }

        // Update version
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 2, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 2: Added processed_at column");
    }

    // Run migration 3: Create subscriptions table
    if current_version < 3 {
        // Backup before migration
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                polling_interval_seconds INTEGER NOT NULL,
                last_polled_at TEXT,
                created_at TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT 1
            )
            "#,
        )
        .execute(&pool)
        .await?;

        // Update version
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 3, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 3: Created subscriptions table");
    }

    // Run migration 4: Add subscription_id to links table
    if current_version < 4 {
        // Backup before migration
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        // Check if subscription_id column exists
        let subscription_id_exists: bool = sqlx::query(
            "PRAGMA table_info(links)" 
        )
        .fetch_all(&pool)
        .await?
        .iter()
        .any(|row| {
            row.get::<String, _>(1) == "subscription_id"
        });

        if !subscription_id_exists {
            sqlx::query(
                "ALTER TABLE links ADD COLUMN subscription_id TEXT"
            )
            .execute(&pool)
            .await?;
            info!("Added 'subscription_id' column to links table");
        }

        // Update version
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 4, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 4: Added subscription_id column");
    }

    // Run migration 5: Create sources table
    if current_version < 5 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sources (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                domain TEXT NOT NULL UNIQUE,
                rate_limit_per_minute INTEGER,
                rate_limit_per_day INTEGER,
                api_key TEXT,
                notes TEXT,
                created_at TEXT NOT NULL,
                active BOOLEAN NOT NULL DEFAULT 1
            )
            "#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 5, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 5: Created sources table");
    }

    // Run migration 6: Add source_id to links table
    if current_version < 6 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        let source_id_exists: bool = sqlx::query("PRAGMA table_info(links)")
            .fetch_all(&pool)
            .await?
            .iter()
            .any(|row| row.get::<String, _>(1) == "source_id");

        if !source_id_exists {
            sqlx::query("ALTER TABLE links ADD COLUMN source_id TEXT REFERENCES sources(id)")
                .execute(&pool)
                .await?;
            info!("Added 'source_id' column to links table");
        }

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 6, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 6: Added source_id column to links");
    }

    // Migration 7: Create artists and artist_aliases tables
    if current_version < 7 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS artists (
                id         TEXT PRIMARY KEY,
                name       TEXT NOT NULL,
                created_at TEXT NOT NULL
            )"#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS artist_aliases (
                id        TEXT PRIMARY KEY,
                artist_id TEXT NOT NULL REFERENCES artists(id),
                alias     TEXT NOT NULL,
                UNIQUE(artist_id, alias)
            )"#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 7, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 7: Created artists and artist_aliases tables");
    }

    // Migration 8: Create tags and tag_parents tables
    if current_version < 8 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS tags (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL UNIQUE,
                source_id     TEXT REFERENCES sources(id),
                content_count INTEGER NOT NULL DEFAULT 0,
                created_at    TEXT NOT NULL
            )"#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            r#"CREATE TABLE IF NOT EXISTS tag_parents (
                tag_id        TEXT NOT NULL REFERENCES tags(id),
                parent_tag_id TEXT NOT NULL REFERENCES tags(id),
                PRIMARY KEY (tag_id, parent_tag_id),
                CHECK (tag_id != parent_tag_id)
            )"#,
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 8, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 8: Created tags and tag_parents tables");
    }

    // Migration 9: Add user_id column to sources
    if current_version < 9 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query("ALTER TABLE sources ADD COLUMN user_id TEXT")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 9, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 9: Added user_id column to sources");
    }

    if current_version < 10 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query("ALTER TABLE links ADD COLUMN file_url TEXT")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 10, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 10: Added file_url column to links");
    }

    if current_version < 11 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS content (
                id          TEXT PRIMARY KEY,
                link_id     TEXT NOT NULL REFERENCES links(id),
                source_id   TEXT REFERENCES sources(id),
                file_path   TEXT,
                post_date   TEXT,
                uploader    TEXT,
                rating      TEXT,
                score       INTEGER,
                source_url  TEXT,
                created_at  TEXT NOT NULL
            )"
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS content_tags (
                content_id  TEXT NOT NULL REFERENCES content(id),
                tag_id      TEXT NOT NULL REFERENCES tags(id),
                PRIMARY KEY (content_id, tag_id)
            )"
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 11, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 11: Created content and content_tags tables");
    }

    if current_version < 12 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query("ALTER TABLE tags ADD COLUMN tag_type TEXT NOT NULL DEFAULT 'default'")
            .execute(&pool)
            .await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 12, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 12: Added tag_type column to tags");
    }

    // Migration 13: Rename artists/artist_aliases to creators/creator_aliases
    if current_version < 13 {
        if db_existed {
            create_backup(db_name, &format!("v{}", current_version), max_backups).await.ok();
            info!("Backup created before migration");
        }

        sqlx::query("ALTER TABLE artists RENAME TO creators")
            .execute(&pool).await?;
        sqlx::query("ALTER TABLE artist_aliases RENAME TO creator_aliases")
            .execute(&pool).await?;
        sqlx::query("ALTER TABLE creator_aliases RENAME COLUMN artist_id TO creator_id")
            .execute(&pool).await?;

        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 13, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;

        info!("Applied migration 13: Renamed artists to creators");
    }

    // Migration 14: Add error column to links
    if current_version < 14 {
        sqlx::query("ALTER TABLE links ADD COLUMN error TEXT")
            .execute(&pool).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 14, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;
        info!("Applied migration 14: Added error column to links");
    }

    if current_version < 15 {
        sqlx::query("ALTER TABLE content ADD COLUMN favorite INTEGER NOT NULL DEFAULT 0")
            .execute(&pool).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 15, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;
        info!("Applied migration 15: Added favorite column to content");
    }

    if current_version < 16 {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS ignored_domains (
                domain     TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            )"
        )
        .execute(&pool).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 16, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;
        info!("Applied migration 16: Created ignored_domains table");
    }

    if current_version < 17 {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS backup_settings (
                id               INTEGER PRIMARY KEY CHECK (id = 1),
                max_backups      INTEGER NOT NULL DEFAULT 5,
                schedule_minutes INTEGER NOT NULL DEFAULT 0,
                last_backup_at   TEXT,
                last_backup_path TEXT
            )"
        )
        .execute(&pool).await?;
        sqlx::query("INSERT OR IGNORE INTO backup_settings (id, max_backups, schedule_minutes) VALUES (1, 5, 0)")
            .execute(&pool).await?;
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 17, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;
        info!("Applied migration 17: Created backup_settings table");
    }

    if current_version < 18 {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS creator_tags (
                creator_id TEXT NOT NULL REFERENCES creators(id),
                tag_id     TEXT NOT NULL REFERENCES tags(id),
                PRIMARY KEY (creator_id, tag_id)
            )"
        )
        .execute(&pool)
        .await?;
        sqlx::query(
            "INSERT OR REPLACE INTO database_version (id, version, last_migration_at) VALUES (1, 18, ?)"
        )
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&pool)
        .await?;
        info!("Applied migration 18: Created creator_tags table");
    }

    info!("Database is at version {}", 18);

    Ok(pool)
}

async fn create_backup(db_name: &str, label: &str, max_backups: usize) -> Result<String, std::io::Error> {
    use std::fs;
    use chrono::Local;
    use std::path::Path;

    // Create backups directory if it doesn't exist
    fs::create_dir_all("backups")?;

    // Get just the filename without the path
    let db_filename = Path::new(db_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(db_name);

    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let backup_name = format!("backups/{}.backup.{}.{}", db_filename, label, timestamp);

    fs::copy(db_name, &backup_name)?;
    info!("Database backed up to: {}", backup_name);

    // Clean up old backups if we exceed the limit
    if max_backups > 0 {
        cleanup_old_backups(db_name, max_backups)?;
    }

    Ok(backup_name)
}

fn cleanup_old_backups(db_name: &str, max_backups: usize) -> Result<(), std::io::Error> {
    use std::fs;
    use std::path::{Path, PathBuf};

    let backup_dir = std::path::Path::new("backups");
    
    // Create backups directory if it doesn't exist
    if !backup_dir.exists() {
        return Ok(());
    }

    // Get just the filename without the path
    let db_filename = Path::new(db_name)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(db_name);

    let backup_prefix = format!("{}.backup.v", db_filename);

    // Find all backup files
    let mut backups: Vec<PathBuf> = fs::read_dir(backup_dir)?
        .filter_map(|entry| {
            entry.ok().and_then(|e| {
                let path = e.path();
                if path.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with(&backup_prefix))
                    .unwrap_or(false)
                {
                    Some(path)
                } else {
                    None
                }
            })
        })
        .collect();

    // Sort by modification time (newest first)
    backups.sort_by(|a, b| {
        let a_time = fs::metadata(a)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        let b_time = fs::metadata(b)
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        b_time.cmp(&a_time)
    });

    // Delete old backups beyond the limit
    for backup_path in backups.iter().skip(max_backups) {
        match fs::remove_file(backup_path) {
            Ok(_) => {
                if let Some(filename) = backup_path.file_name() {
                    info!("Deleted old backup: {:?}", filename);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to delete old backup {:?}: {}", backup_path, e);
            }
        }
    }

    Ok(())
}

async fn handler_root() -> Html<&'static str> {
    info!("GET /");
    Html(include_str!("dashboard.html"))
}

async fn handler_links_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<LinksQuery>,
) -> Json<LinksListResponse> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);

    // Whitelist sort column to prevent SQL injection
    let sort_col = match params.sort_by.as_deref().unwrap_or("created_at") {
        "processed_at"   => "processed_at",
        "url"            => "url",
        "source_id"      => "source_id",
        "subscription_id" => "subscription_id",
        _                => "created_at",
    };
    let sort_dir = match params.sort_dir.as_deref().unwrap_or("desc") {
        "asc" => "ASC",
        _     => "DESC",
    };

    let query_str = format!(
        "SELECT id, url, created_at, processed_at, source_id, subscription_id, error FROM links ORDER BY {} {} LIMIT ? OFFSET ?",
        sort_col, sort_dir
    );

    let rows = sqlx::query(&query_str)
        .bind(params.limit)
        .bind(params.offset)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    let links = rows.iter().map(|r| LinkRecord {
        id: r.get("id"),
        url: r.get("url"),
        created_at: r.get("created_at"),
        processed_at: r.get("processed_at"),
        source_id: r.get("source_id"),
        subscription_id: r.get("subscription_id"),
        error: r.get("error"),
    }).collect();

    info!("GET /links - total={} limit={} offset={} sort={}:{}", total, params.limit, params.offset, sort_col, sort_dir);
    Json(LinksListResponse { links, total })
}

async fn handler_delete_link(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let _ = sqlx::query(
        "DELETE FROM content_tags WHERE content_id IN (SELECT id FROM content WHERE link_id = ?)"
    ).bind(&id).execute(&pool).await;
    let _ = sqlx::query("DELETE FROM content WHERE link_id = ?")
        .bind(&id).execute(&pool).await;
    match sqlx::query("DELETE FROM links WHERE id = ?")
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            info!("DELETE /links/{} - removed", id);
            Json(json!({ "ok": true }))
        }
        Ok(_) => Json(json!({ "ok": false, "error": "Link not found" })),
        Err(e) => {
            tracing::warn!("Failed to delete link {}: {}", id, e);
            Json(json!({ "ok": false, "error": e.to_string() }))
        }
    }
}

async fn handler_retry_link(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query("UPDATE links SET error = NULL, processed_at = NULL WHERE id = ?")
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => {
            info!("POST /links/{}/retry - reset for retry", id);
            Json(json!({ "ok": true }))
        }
        Ok(_) => Json(json!({ "ok": false, "error": "Link not found" })),
        Err(e) => {
            tracing::warn!("Failed to reset link {} for retry: {}", id, e);
            Json(json!({ "ok": false, "error": e.to_string() }))
        }
    }
}

async fn handler_health() -> Json<serde_json::Value> {
    info!("GET /health");
    Json(json!({
        "status": "healthy"
    }))
}

async fn handler_graphql() -> Json<serde_json::Value> {
    info!("GET /graphql");
    Json(json!({
        "status": "healthy"
    }))
}

/// Strip query parameters that are not part of the content identity.
/// Removes credentials (api_key, user_id), Cloudflare tokens (__cf_*, cf_*),
/// tracking params (utm_*, ref, …), and booru search context (tags, sort, …).
/// For booru post pages (page=post&s=view&id=N) only the three identity params are kept.
fn clean_url(url: &str) -> String {
    const STRIP_PARAMS: &[&str] = &[
        // credentials
        "api_key", "user_id", "api_secret", "token", "access_token", "auth",
        // booru search context
        "tags", "sort", "order", "filter", "q", "query", "search",
        // generic tracking
        "ref", "referrer", "from", "source",
    ];

    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, q),
        None => return url.to_string(),
    };

    // For booru tag-list pages keep only page + s + tags (drop pid, credentials, etc.)
    let is_booru_list = query.contains("page=post") && query.contains("s=list");
    if is_booru_list {
        const KEEP_LIST: &[&str] = &["page", "s", "tags"];
        let kept: Vec<&str> = query
            .split('&')
            .filter(|p| {
                let key = p.split('=').next().unwrap_or("").to_lowercase();
                KEEP_LIST.contains(&key.as_str())
            })
            .collect();
        return if kept.is_empty() {
            base.to_string()
        } else {
            format!("{}?{}", base, kept.join("&"))
        };
    }

    // For booru post pages keep only the three identity params
    let is_booru_post = query.contains("page=post") && query.contains("s=view") && query.contains("id=");
    if is_booru_post {
        const KEEP: &[&str] = &["page", "s", "id"];
        let kept: Vec<&str> = query
            .split('&')
            .filter(|p| {
                let key = p.split('=').next().unwrap_or("").to_lowercase();
                KEEP.contains(&key.as_str())
            })
            .collect();
        return if kept.is_empty() {
            base.to_string()
        } else {
            format!("{}?{}", base, kept.join("&"))
        };
    }

    let cleaned: Vec<&str> = query
        .split('&')
        .filter(|param| {
            let key = param.split('=').next().unwrap_or("").to_lowercase();
            !STRIP_PARAMS.contains(&key.as_str())
                && !key.starts_with("utm_")
                && !key.starts_with("__cf")
                && !key.starts_with("cf_")
        })
        .collect();

    if cleaned.is_empty() {
        base.to_string()
    } else {
        format!("{}?{}", base, cleaned.join("&"))
    }
}

fn root_domain(url: &str) -> Option<String> {
    let without_scheme = url
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let host = without_scheme.split('/').next()?.split('?').next()?;
    // drop port
    let host = host.split(':').next()?;
    // strip leading www.
    let host = host.strip_prefix("www.").unwrap_or(host);
    if host.is_empty() { return None; }
    Some(host.to_lowercase())
}

async fn get_or_create_source(pool: &SqlitePool, domain: &str) -> Option<String> {
    // Return existing source id if one already exists for this domain
    if let Ok(Some(row)) = sqlx::query("SELECT id FROM sources WHERE domain = ?")
        .bind(domain)
        .fetch_optional(pool)
        .await
    {
        return Some(row.get("id"));
    }

    // Create a new source for this domain
    let id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();
    // Capitalise the first letter of the domain as a default name
    let name: String = {
        let mut chars = domain.chars();
        match chars.next() {
            None => domain.to_string(),
            Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
        }
    };

    match sqlx::query(
        "INSERT INTO sources (id, name, domain, created_at, active) VALUES (?, ?, ?, ?, 1)"
    )
    .bind(&id)
    .bind(&name)
    .bind(domain)
    .bind(&created_at)
    .execute(pool)
    .await
    {
        Ok(_) => {
            info!("Auto-created source '{}' for domain '{}'", name, domain);
            Some(id)
        }
        Err(e) => {
            tracing::warn!("Failed to auto-create source for domain '{}': {}", domain, e);
            None
        }
    }
}

async fn handler_ingest(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<IngestRequest>,
) -> Json<IngestResponse> {
    info!("POST /ingest - Processing {} links", payload.links.len());

    let mut ingested_count: usize = 0;
    let mut ids: Vec<String> = Vec::new();
    let mut duplicate_count: usize = 0;
    let mut duplicates: Vec<String> = Vec::new();

    for url in payload.links {
        let url = clean_url(&url);
        // Check if URL already exists
        let existing = sqlx::query("SELECT id FROM links WHERE url = ?")
            .bind(&url)
            .fetch_optional(&pool)
            .await;

        match existing {
            Ok(Some(_)) => {
                duplicate_count += 1;
                duplicates.push(url.clone());
                tracing::info!("Link already exists: {}", url);
            }
            Ok(None) => {
                let id: String = Uuid::new_v4().to_string();
                let created_at: String = chrono::Utc::now().to_rfc3339();

                // Use the provided source_id or resolve/create one from the URL's domain
                let source_id = if payload.source_id.is_some() {
                    payload.source_id.clone()
                } else {
                    match root_domain(&url) {
                        Some(domain) => get_or_create_source(&pool, &domain).await,
                        None => None,
                    }
                };

                match sqlx::query(
                    "INSERT INTO links (id, url, created_at, subscription_id, source_id, file_url) VALUES (?, ?, ?, ?, ?, ?)"
                )
                .bind(&id)
                .bind(&url)
                .bind(&created_at)
                .bind(&payload.subscription_id)
                .bind(&source_id)
                .bind(&payload.file_url)
                .execute(&pool)
                .await
                {
                    Ok(_) => {
                        ingested_count += 1;
                        ids.push(id);
                        info!("Ingested link: {} (file_url={})", url, payload.file_url.as_deref().unwrap_or("none"));
                    }
                    Err(e) => {
                        tracing::warn!("Failed to ingest link {}: {}", url, e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Database error checking link {}: {}", url, e);
            }
        }
    }

    info!(
        "Ingest complete: {} links stored, {} duplicates",
        ingested_count, duplicate_count
    );

    Json(IngestResponse {
        ingested_count,
        ids,
        duplicate_count,
        duplicates,
    })
}

async fn handler_check(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<CheckResponse> {
    let url = params
        .get("url")
        .cloned()
        .unwrap_or_else(|| String::new());

    info!("GET /check?url={}", url);

    let exists = sqlx::query("SELECT id FROM links WHERE url = ?")
        .bind(&url)
        .fetch_optional(&pool)
        .await
        .map(|result| result.is_some())
        .unwrap_or(false);

    info!("Link check: {} exists={}", url, exists);

    Json(CheckResponse { url, exists })
}

async fn handler_processing_status(
    axum::extract::State((_, processing_enabled, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<ProcessingStatusResponse> {
    let enabled = *processing_enabled.lock().await;
    info!("GET /processing/status - enabled={}", enabled);
    Json(ProcessingStatusResponse {
        processing_enabled: enabled,
    })
}

async fn handler_toggle_processing(
    axum::extract::State((_, processing_enabled, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<ToggleProcessingResponse> {
    let mut enabled = processing_enabled.lock().await;
    *enabled = !*enabled;
    let new_state = *enabled;
    
    let message = if new_state {
        "Link processing enabled".to_string()
    } else {
        "Link processing disabled".to_string()
    };
    
    info!("POST /processing/toggle - new_state={}", new_state);
    
    Json(ToggleProcessingResponse {
        processing_enabled: new_state,
        message,
    })
}

async fn handler_unprocessed_links(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<UnprocessedLinksResponse> {
    let unprocessed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM links WHERE processed_at IS NULL"
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    info!("GET /links/unprocessed - count={}", unprocessed_count);

    Json(UnprocessedLinksResponse { unprocessed_count })
}

async fn handler_subscriptions_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<SubscriptionsListResponse> {
    let subscriptions = sqlx::query_as::<_, (String, String, String, i32, Option<String>, String, bool)>(
        "SELECT id, name, url, polling_interval_seconds, last_polled_at, created_at, active FROM subscriptions ORDER BY created_at DESC"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let total_count = subscriptions.len() as i64;
    let subs = subscriptions
        .into_iter()
        .map(|(id, name, url, polling_interval_seconds, last_polled_at, created_at, active)| {
            SubscriptionResponse {
                id,
                name,
                url,
                polling_interval_seconds,
                last_polled_at,
                created_at,
                active,
            }
        })
        .collect();

    info!("GET /subscriptions - count={}", total_count);

    Json(SubscriptionsListResponse {
        subscriptions: subs,
        total_count,
    })
}

async fn handler_create_subscription(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<CreateSubscriptionRequest>,
) -> Json<SubscriptionResponse> {
    let id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();

    match sqlx::query(
        "INSERT INTO subscriptions (id, name, url, polling_interval_seconds, created_at, active) VALUES (?, ?, ?, ?, ?, 1)"
    )
    .bind(&id)
    .bind(&payload.name)
    .bind(&payload.url)
    .bind(payload.polling_interval_seconds)
    .bind(&created_at)
    .execute(&pool)
    .await
    {
        Ok(_) => {
            info!("Created subscription: {} - {}", id, payload.name);
            Json(SubscriptionResponse {
                id,
                name: payload.name,
                url: payload.url,
                polling_interval_seconds: payload.polling_interval_seconds,
                last_polled_at: None,
                created_at,
                active: true,
            })
        }
        Err(e) => {
            tracing::warn!("Failed to create subscription: {}", e);
            // Return the new subscription anyway for consistency
            Json(SubscriptionResponse {
                id,
                name: payload.name,
                url: payload.url,
                polling_interval_seconds: payload.polling_interval_seconds,
                last_polled_at: None,
                created_at,
                active: true,
            })
        }
    }
}

async fn handler_sources_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<SourcesListResponse> {
    let rows = sqlx::query(
        "SELECT id, name, domain, rate_limit_per_minute, rate_limit_per_day, api_key, user_id, notes, created_at, active FROM sources ORDER BY name ASC"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let sources: Vec<Source> = rows
        .iter()
        .map(|row| Source {
            id: row.get("id"),
            name: row.get("name"),
            domain: row.get("domain"),
            rate_limit_per_minute: row.get("rate_limit_per_minute"),
            rate_limit_per_day: row.get("rate_limit_per_day"),
            api_key: row.get("api_key"),
            user_id: row.get("user_id"),
            notes: row.get("notes"),
            created_at: row.get("created_at"),
            active: row.get("active"),
        })
        .collect();

    let total_count = sources.len() as i64;
    info!("GET /sources - count={}", total_count);
    Json(SourcesListResponse { sources, total_count })
}

async fn handler_create_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<CreateSourceRequest>,
) -> Json<serde_json::Value> {
    let id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();

    match sqlx::query(
        "INSERT INTO sources (id, name, domain, rate_limit_per_minute, rate_limit_per_day, api_key, user_id, notes, created_at, active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)"
    )
    .bind(&id)
    .bind(&payload.name)
    .bind(&payload.domain)
    .bind(payload.rate_limit_per_minute)
    .bind(payload.rate_limit_per_day)
    .bind(&payload.api_key)
    .bind(&payload.user_id)
    .bind(&payload.notes)
    .bind(&created_at)
    .execute(&pool)
    .await
    {
        Ok(_) => {
            info!("POST /sources - created source: {} ({})", payload.name, payload.domain);
            Json(json!({
                "id": id,
                "name": payload.name,
                "domain": payload.domain,
                "rate_limit_per_minute": payload.rate_limit_per_minute,
                "rate_limit_per_day": payload.rate_limit_per_day,
                "api_key": payload.api_key,
                "user_id": payload.user_id,
                "notes": payload.notes,
                "created_at": created_at,
                "active": true,
            }))
        }
        Err(e) => {
            tracing::warn!("Failed to create source: {}", e);
            Json(json!({ "error": format!("Failed to create source: {}", e) }))
        }
    }
}

async fn handler_get_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query(
        "SELECT id, name, domain, rate_limit_per_minute, rate_limit_per_day, api_key, user_id, notes, created_at, active FROM sources WHERE id = ?"
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await
    {
        Ok(Some(row)) => {
            info!("GET /sources/{}", id);
            Json(json!({
                "id": row.get::<String, _>("id"),
                "name": row.get::<String, _>("name"),
                "domain": row.get::<String, _>("domain"),
                "rate_limit_per_minute": row.get::<Option<i32>, _>("rate_limit_per_minute"),
                "rate_limit_per_day": row.get::<Option<i32>, _>("rate_limit_per_day"),
                "api_key": row.get::<Option<String>, _>("api_key"),
                "user_id": row.get::<Option<String>, _>("user_id"),
                "notes": row.get::<Option<String>, _>("notes"),
                "created_at": row.get::<String, _>("created_at"),
                "active": row.get::<bool, _>("active"),
            }))
        }
        Ok(None) => Json(json!({ "error": "Source not found" })),
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_update_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<UpdateSourceRequest>,
) -> Json<serde_json::Value> {
    match sqlx::query(
        "UPDATE sources SET name = ?, domain = ?, rate_limit_per_minute = ?, rate_limit_per_day = ?, api_key = ?, user_id = ?, notes = ?, active = ? WHERE id = ?"
    )
    .bind(&payload.name)
    .bind(&payload.domain)
    .bind(payload.rate_limit_per_minute)
    .bind(payload.rate_limit_per_day)
    .bind(&payload.api_key)
    .bind(&payload.user_id)
    .bind(&payload.notes)
    .bind(payload.active)
    .bind(&id)
    .execute(&pool)
    .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Source not found" })),
        Ok(_) => {
            info!("PUT /sources/{} - updated", id);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn source_credentials(pool: &SqlitePool, id: &str) -> Option<SourceCredentials> {
    sqlx::query("SELECT domain, api_key, user_id FROM sources WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|row| SourceCredentials {
            domain: row.get("domain"),
            api_key: row.get("api_key"),
            user_id: row.get("user_id"),
        })
}

async fn handler_ping_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let creds = match source_credentials(&pool, &id).await {
        Some(c) => c,
        None => return Json(json!({ "error": "Source not found" })),
    };

    let base = format!("https://{}", creds.domain);
    let authenticated = creds.api_key.is_some();

    // When credentials are present, test against the real API endpoint;
    // otherwise fall back to a plain connectivity check on the domain root.
    let url = match (&creds.api_key, &creds.user_id) {
        (Some(key), Some(uid)) => format!(
            "{}/index.php?page=dapi&s=post&q=index&limit=1&json=1&api_key={}&user_id={}",
            base, key, uid
        ),
        (Some(key), None) => format!(
            "{}/index.php?page=dapi&s=post&q=index&limit=1&json=1&api_key={}",
            base, key
        ),
        _ => base.clone(),
    };

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers({
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert("Accept", "application/json, text/html, */*".parse().unwrap());
            headers.insert("Accept-Language", "en-US,en;q=0.9".parse().unwrap());
            headers.insert("Accept-Encoding", "gzip, deflate, br".parse().unwrap());
            headers
        })
        .build()
    {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Client build error: {}", e) })),
    };

    let start = std::time::Instant::now();

    let (reachable, status_code, latency_ms, error) = match client.get(&url).header("Referer", &base).send().await {
        Ok(resp) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            let status_code = resp.status().as_u16();
            (true, Some(status_code), latency_ms, None)
        }
        Err(e) => {
            let latency_ms = start.elapsed().as_millis() as u64;
            (false, None, latency_ms, Some(e.to_string()))
        }
    };

    let active = status_code == Some(200);
    sqlx::query("UPDATE sources SET active = ? WHERE id = ?")
        .bind(active)
        .bind(&id)
        .execute(&pool)
        .await
        .ok();

    info!("POST /sources/{}/ping - authenticated={} reachable={} status={:?} {}ms active={}", id, authenticated, reachable, status_code, latency_ms, active);
    Json(json!({ "reachable": reachable, "status_code": status_code, "latency_ms": latency_ms, "active": active, "authenticated": authenticated, "error": error }))
}

async fn handler_refresh_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let creds = match source_credentials(&pool, &id).await {
        Some(c) => c,
        None => return Json(json!({ "error": "Source not found" })),
    };

    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .user_agent("Mozilla/5.0 (compatible; curator/1.0)")
        .build()
    {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Client build error: {}", e) })),
    };

    let base = format!("https://{}", creds.domain);
    let url = match (&creds.api_key, &creds.user_id) {
        (Some(key), Some(uid)) => format!("{}?api_key={}&user_id={}", base, key, uid),
        (Some(key), None)      => format!("{}?api_key={}", base, key),
        _                      => base,
    };
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => return Json(json!({ "error": e.to_string(), "updated": false })),
    };

    // Collect all response headers for inspection
    let headers = resp.headers().clone();
    let mut found: Vec<String> = Vec::new();
    let mut rate_per_minute: Option<i32> = None;
    let mut rate_per_day: Option<i32> = None;

    for (name, val) in &headers {
        let n = name.as_str().to_lowercase();
        if n.contains("ratelimit") || n.contains("rate-limit") || n.contains("rate_limit") || n.contains("x-rate") {
            let v = val.to_str().unwrap_or("").trim().to_string();
            found.push(format!("{}: {}", name.as_str(), v));
            if let Ok(num) = v.parse::<i32>() {
                if n.contains("minute") || n.contains("-min") || n.contains("_min") {
                    rate_per_minute = Some(num);
                } else if n.contains("day") {
                    rate_per_day = Some(num);
                } else if (n.contains("limit") || n.contains("allowed")) && !n.contains("remaining") && !n.contains("reset") {
                    // Generic limit with no time unit — treat as per-minute
                    rate_per_minute.get_or_insert(num);
                }
            }
        }
    }

    if found.is_empty() {
        info!("POST /sources/{}/refresh - no rate limit headers found", id);
        return Json(json!({ "updated": false, "message": "No rate limit headers found in response", "headers_found": [] }));
    }

    let mut updated_parts: Vec<String> = Vec::new();
    if let Some(rpm) = rate_per_minute {
        sqlx::query("UPDATE sources SET rate_limit_per_minute = ? WHERE id = ?")
            .bind(rpm).bind(&id).execute(&pool).await.ok();
        updated_parts.push(format!("{}/min", rpm));
    }
    if let Some(rpd) = rate_per_day {
        sqlx::query("UPDATE sources SET rate_limit_per_day = ? WHERE id = ?")
            .bind(rpd).bind(&id).execute(&pool).await.ok();
        updated_parts.push(format!("{}/day", rpd));
    }

    info!("POST /sources/{}/refresh - found: {:?}, updated: {:?}", id, found, updated_parts);

    if updated_parts.is_empty() {
        Json(json!({
            "updated": false,
            "message": format!("Found headers but could not parse rate values: {}", found.join(", ")),
            "headers_found": found,
        }))
    } else {
        Json(json!({
            "updated": true,
            "message": format!("Updated: {}", updated_parts.join(", ")),
            "headers_found": found,
        }))
    }
}

/// Returns the credentialed dapi URL for a given post page URL so the browser
/// extension can fetch it (with Cloudflare cookies) and report back the file_url.
async fn handler_resolve_media(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let url = match params.get("url") {
        Some(u) => u.clone(),
        None => return Json(json!({ "error": "Missing url parameter" })),
    };

    // Currently only handles booru-style post pages
    if !(url.contains("page=post") && url.contains("s=view")) {
        return Json(json!({ "error": "URL type not supported" }));
    }

    let post_id = match extract_query_param(&url, "id") {
        Some(id) => id,
        None => return Json(json!({ "error": "Could not extract post id" })),
    };

    let base = match extract_domain_base(&url) {
        Some(b) => b,
        None => return Json(json!({ "error": "Could not extract domain" })),
    };

    let domain = base.trim_start_matches("https://").trim_start_matches("http://").to_string();

    let row = sqlx::query("SELECT api_key, user_id FROM sources WHERE domain = ?")
        .bind(&domain)
        .fetch_optional(&pool)
        .await
        .ok()
        .flatten();

    let dapi_url = match row {
        Some(r) => {
            let api_key: Option<String> = r.get("api_key");
            let user_id: Option<String> = r.get("user_id");
            match (&api_key, &user_id) {
                (Some(key), Some(uid)) => format!(
                    "{}/index.php?page=dapi&s=post&q=index&id={}&json=1&api_key={}&user_id={}",
                    base, post_id, key, uid
                ),
                (Some(key), None) => format!(
                    "{}/index.php?page=dapi&s=post&q=index&id={}&json=1&api_key={}",
                    base, post_id, key
                ),
                _ => format!("{}/index.php?page=dapi&s=post&q=index&id={}&json=1", base, post_id),
            }
        }
        None => format!("{}/index.php?page=dapi&s=post&q=index&id={}&json=1", base, post_id),
    };

    info!("GET /sources/resolve-media - post_id={} domain={}", post_id, domain);
    Json(json!({ "dapi_url": dapi_url, "post_id": post_id }))
}

async fn handler_delete_source(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query("DELETE FROM sources WHERE id = ?")
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Source not found" })),
        Ok(_) => {
            info!("DELETE /sources/{}", id);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

fn row_to_creator(r: &sqlx::sqlite::SqliteRow) -> Creator {
    let aliases_str: Option<String> = r.get("aliases");
    let aliases = aliases_str
        .unwrap_or_default()
        .split('|')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    let tags_str: Option<String> = r.try_get("tags").ok().flatten();
    let tags = tags_str
        .unwrap_or_default()
        .split('|')
        .filter(|s| !s.is_empty())
        .map(|s| {
            let mut parts = s.splitn(2, '\x1f');
            let name = parts.next().unwrap_or("").to_string();
            let tag_type = parts.next().unwrap_or("default").to_string();
            CreatorTag { name, tag_type }
        })
        .collect();
    Creator {
        id: r.get("id"),
        name: r.get("name"),
        created_at: r.get("created_at"),
        aliases,
        tags,
    }
}

async fn handler_creators_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<CreatorsListResponse> {
    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM creators")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);

    let rows = sqlx::query(
        r#"SELECT a.id, a.name, a.created_at,
                  GROUP_CONCAT(aa.alias, '|') AS aliases,
                  (SELECT GROUP_CONCAT(tg.nm, '|')
                   FROM (
                     SELECT DISTINCT t.name || char(31) || COALESCE(t.tag_type, 'default') AS nm
                     FROM tags t
                     JOIN creator_tags ct ON ct.tag_id = t.id
                     WHERE ct.creator_id = a.id
                     UNION
                     SELECT DISTINCT t.name || char(31) || COALESCE(t.tag_type, 'default') AS nm
                     FROM content c
                     JOIN content_tags ct ON ct.content_id = c.id
                     JOIN tags t ON t.id = ct.tag_id
                     WHERE c.uploader = a.name
                        OR c.uploader IN (SELECT alias FROM creator_aliases ca2 WHERE ca2.creator_id = a.id)
                     ORDER BY nm
                   ) tg
                  ) AS tags
           FROM creators a
           LEFT JOIN creator_aliases aa ON aa.creator_id = a.id
           GROUP BY a.id
           ORDER BY a.name"#,
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let creators = rows.iter().map(row_to_creator).collect();
    info!("GET /creators - count={}", total_count);
    Json(CreatorsListResponse { creators, total_count })
}

async fn handler_create_creator(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<CreateCreatorRequest>,
) -> Json<serde_json::Value> {
    let id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();

    match sqlx::query("INSERT INTO creators (id, name, created_at) VALUES (?, ?, ?)")
        .bind(&id)
        .bind(&payload.name)
        .bind(&created_at)
        .execute(&pool)
        .await
    {
        Ok(_) => {
            for alias in &payload.aliases {
                let alias = alias.trim();
                if alias.is_empty() { continue; }
                let alias_id = Uuid::new_v4().to_string();
                sqlx::query("INSERT OR IGNORE INTO creator_aliases (id, creator_id, alias) VALUES (?, ?, ?)")
                    .bind(&alias_id)
                    .bind(&id)
                    .bind(alias)
                    .execute(&pool)
                    .await
                    .ok();
            }
            info!("POST /creators - created {} ({})", payload.name, id);
            Json(json!({ "id": id, "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_get_creator(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query(
        r#"SELECT a.id, a.name, a.created_at,
                  GROUP_CONCAT(aa.alias, '|') AS aliases,
                  (SELECT GROUP_CONCAT(tg.nm, '|')
                   FROM (
                     SELECT DISTINCT t.name || char(31) || COALESCE(t.tag_type, 'default') AS nm
                     FROM tags t
                     JOIN creator_tags ct ON ct.tag_id = t.id
                     WHERE ct.creator_id = a.id
                     UNION
                     SELECT DISTINCT t.name || char(31) || COALESCE(t.tag_type, 'default') AS nm
                     FROM content c
                     JOIN content_tags ct ON ct.content_id = c.id
                     JOIN tags t ON t.id = ct.tag_id
                     WHERE c.uploader = a.name
                        OR c.uploader IN (SELECT alias FROM creator_aliases ca2 WHERE ca2.creator_id = a.id)
                     ORDER BY nm
                   ) tg
                  ) AS tags
           FROM creators a
           LEFT JOIN creator_aliases aa ON aa.creator_id = a.id
           WHERE a.id = ?
           GROUP BY a.id"#,
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await
    {
        Ok(Some(row)) => Json(json!(row_to_creator(&row))),
        Ok(None) => Json(json!({ "error": "Creator not found" })),
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_update_creator(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<UpdateCreatorRequest>,
) -> Json<serde_json::Value> {
    match sqlx::query("UPDATE creators SET name = ? WHERE id = ?")
        .bind(&payload.name)
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Creator not found" })),
        Ok(_) => {
            info!("PUT /creators/{} - updated name to '{}'", id, payload.name);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_delete_creator(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    // Remove aliases first (no ON DELETE CASCADE without PRAGMA foreign_keys)
    sqlx::query("DELETE FROM creator_aliases WHERE creator_id = ?")
        .bind(&id)
        .execute(&pool)
        .await
        .ok();

    match sqlx::query("DELETE FROM creators WHERE id = ?")
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Creator not found" })),
        Ok(_) => {
            info!("DELETE /creators/{}", id);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_add_alias(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<AddAliasRequest>,
) -> Json<serde_json::Value> {
    let alias = payload.alias.trim().to_string();
    if alias.is_empty() {
        return Json(json!({ "error": "Alias cannot be empty" }));
    }
    let alias_id = Uuid::new_v4().to_string();
    match sqlx::query("INSERT OR IGNORE INTO creator_aliases (id, creator_id, alias) VALUES (?, ?, ?)")
        .bind(&alias_id)
        .bind(&id)
        .bind(&alias)
        .execute(&pool)
        .await
    {
        Ok(_) => {
            info!("POST /creators/{}/aliases - added '{}'", id, alias);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_remove_alias(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path((id, alias)): axum::extract::Path<(String, String)>,
) -> Json<serde_json::Value> {
    match sqlx::query("DELETE FROM creator_aliases WHERE creator_id = ? AND alias = ?")
        .bind(&id)
        .bind(&alias)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Alias not found" })),
        Ok(_) => {
            info!("DELETE /creators/{}/aliases/{}", id, alias);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

// ── Tag helpers ──────────────────────────────────────────────────────────────

fn parse_tag_parents(raw: Option<String>) -> Vec<TagParent> {
    raw.unwrap_or_default()
        .split('|')
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            let mut parts = s.splitn(2, '\x1e'); // ASCII record separator
            Some(TagParent {
                id: parts.next()?.to_string(),
                name: parts.next()?.to_string(),
            })
        })
        .collect()
}

const TAG_SELECT: &str = r#"
    SELECT t.id, t.name, t.tag_type, t.source_id, t.content_count, t.created_at,
           GROUP_CONCAT(pt.id || char(30) || pt.name, '|') AS parents
    FROM tags t
    LEFT JOIN tag_parents tp ON tp.tag_id = t.id
    LEFT JOIN tags pt ON pt.id = tp.parent_tag_id
"#;

fn row_to_tag(r: &sqlx::sqlite::SqliteRow) -> Tag {
    Tag {
        id: r.get("id"),
        name: r.get("name"),
        tag_type: r.get::<Option<String>, _>("tag_type").unwrap_or_else(|| "default".to_string()),
        source_id: r.get("source_id"),
        content_count: r.get("content_count"),
        created_at: r.get("created_at"),
        parents: parse_tag_parents(r.get("parents")),
    }
}

fn normalize_tag_type(raw: Option<&str>) -> &'static str {
    match raw.unwrap_or("").to_lowercase().trim() {
        "artist"            => "artist",
        "publisher"         => "publisher",
        "copyright"         => "copyright",
        "character"         => "character",
        "metadata" | "meta" => "metadata",
        _                   => "default",
    }
}

async fn handler_tags_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<TagsQuery>,
) -> Json<TagsListResponse> {
    let search_pattern = format!("%{}%", params.search.trim());

    let total_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tags t WHERE t.name LIKE ?"
    )
    .bind(&search_pattern)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let sort_col = match params.sort_by.as_deref().unwrap_or("name") {
        "tag_type"      => "t.tag_type",
        "source_id"     => "t.source_id",
        "content_count" => "t.content_count",
        "created_at"    => "t.created_at",
        _               => "t.name",
    };
    let sort_dir = if params.sort_dir.as_deref() == Some("desc") { "DESC" } else { "ASC" };
    let per_page = params.per_page.max(1).min(200);
    let offset = (params.page.max(1) - 1) * per_page;

    let query = format!(
        "{} WHERE t.name LIKE ? GROUP BY t.id ORDER BY {} {} LIMIT ? OFFSET ?",
        TAG_SELECT, sort_col, sort_dir
    );
    let rows = sqlx::query(&query)
        .bind(&search_pattern)
        .bind(per_page)
        .bind(offset)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();
    let tags = rows.iter().map(row_to_tag).collect();

    info!("GET /tags - count={} search='{}' sort={}:{} page={}", total_count, params.search, sort_col, sort_dir, params.page);
    Json(TagsListResponse { tags, total_count, page: params.page.max(1), per_page })
}

async fn handler_content_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<ContentQuery>,
) -> Json<ContentListResponse> {
    let search_pattern = format!("%{}%", params.search.trim());

    let total_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(DISTINCT c.id) FROM content c
         LEFT JOIN content_tags ct ON ct.content_id = c.id
         LEFT JOIN tags t ON t.id = ct.tag_id
         WHERE c.file_path LIKE ? OR c.uploader LIKE ? OR c.source_url LIKE ? OR t.name LIKE ?"
    )
    .bind(&search_pattern).bind(&search_pattern).bind(&search_pattern).bind(&search_pattern)
    .fetch_one(&pool)
    .await
    .unwrap_or(0);

    let sort_col = match params.sort_by.as_deref().unwrap_or("created_at") {
        "file_path"  => "c.file_path",
        "post_date"  => "c.post_date",
        "uploader"   => "c.uploader",
        "rating"     => "c.rating",
        "score"      => "c.score",
        "source_id"  => "c.source_id",
        "favorite"   => "c.favorite",
        _            => "c.created_at",
    };
    let sort_dir = if params.sort_dir.as_deref() == Some("asc") { "ASC" } else { "DESC" };
    let per_page = params.per_page.max(1).min(200);
    let offset = (params.page.max(1) - 1) * per_page;

    let query = format!(
        "SELECT c.id, c.file_path, c.post_date, c.uploader, c.rating, c.score,
                c.source_url, c.source_id, c.created_at, c.favorite,
                GROUP_CONCAT(t.name, '|') AS tag_names
         FROM content c
         LEFT JOIN content_tags ct ON ct.content_id = c.id
         LEFT JOIN tags t ON t.id = ct.tag_id
         WHERE c.id IN (
             SELECT DISTINCT c2.id FROM content c2
             LEFT JOIN content_tags ct2 ON ct2.content_id = c2.id
             LEFT JOIN tags t2 ON t2.id = ct2.tag_id
             WHERE c2.file_path LIKE ? OR c2.uploader LIKE ? OR c2.source_url LIKE ? OR t2.name LIKE ?
         )
         GROUP BY c.id
         ORDER BY {} {}
         LIMIT ? OFFSET ?",
        sort_col, sort_dir
    );

    let rows = sqlx::query(&query)
        .bind(&search_pattern).bind(&search_pattern).bind(&search_pattern).bind(&search_pattern)
        .bind(per_page)
        .bind(offset)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    let content = rows.iter().map(|r| {
        let tag_names: Vec<String> = r.get::<Option<String>, _>("tag_names")
            .unwrap_or_default()
            .split('|')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        ContentRecord {
            id: r.get("id"),
            file_path: r.get("file_path"),
            post_date: r.get("post_date"),
            uploader: r.get("uploader"),
            rating: r.get("rating"),
            score: r.get("score"),
            source_url: r.get("source_url"),
            source_id: r.get("source_id"),
            created_at: r.get("created_at"),
            favorite: r.get::<i64, _>("favorite") != 0,
            tags: tag_names,
        }
    }).collect();

    info!("GET /content - total={} search='{}' sort={}:{} page={}", total_count, params.search, sort_col, sort_dir, params.page);
    Json(ContentListResponse { content, total_count, page: params.page.max(1), per_page })
}

async fn handler_toggle_favorite(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query_scalar::<_, i64>("SELECT favorite FROM content WHERE id = ?")
        .bind(&id)
        .fetch_optional(&pool)
        .await
    {
        Ok(Some(current)) => {
            let new_val = if current == 0 { 1i64 } else { 0i64 };
            match sqlx::query("UPDATE content SET favorite = ? WHERE id = ?")
                .bind(new_val)
                .bind(&id)
                .execute(&pool)
                .await
            {
                Ok(_) => Json(json!({ "ok": true, "favorite": new_val != 0 })),
                Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
            }
        }
        Ok(None) => Json(json!({ "ok": false, "error": "Not found" })),
        Err(e)   => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn handler_ignored_domains_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    match sqlx::query_as::<_, (String, String)>(
        "SELECT domain, created_at FROM ignored_domains ORDER BY domain ASC"
    )
    .fetch_all(&pool)
    .await
    {
        Ok(rows) => {
            let domains: Vec<_> = rows.into_iter().map(|(d, c)| json!({ "domain": d, "created_at": c })).collect();
            Json(json!({ "domains": domains }))
        }
        Err(e) => Json(json!({ "domains": [], "error": e.to_string() })),
    }
}

async fn handler_add_ignored_domain(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let domain = match payload.get("domain").and_then(|v| v.as_str()).map(|s| s.trim().to_string()) {
        Some(d) if !d.is_empty() => d,
        _ => return Json(json!({ "ok": false, "error": "domain is required" })),
    };
    let created_at = chrono::Utc::now().to_rfc3339();
    match sqlx::query("INSERT OR IGNORE INTO ignored_domains (domain, created_at) VALUES (?, ?)")
        .bind(&domain)
        .bind(&created_at)
        .execute(&pool)
        .await
    {
        Ok(_)  => Json(json!({ "ok": true, "domain": domain })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn handler_remove_ignored_domain(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(domain): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    match sqlx::query("DELETE FROM ignored_domains WHERE domain = ?")
        .bind(&domain)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() > 0 => Json(json!({ "ok": true })),
        Ok(_)  => Json(json!({ "ok": false, "error": "Domain not found" })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn handler_create_tag(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<CreateTagRequest>,
) -> Json<serde_json::Value> {
    let id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();

    let tag_type = normalize_tag_type(payload.tag_type.as_deref());
    match sqlx::query(
        "INSERT INTO tags (id, name, tag_type, source_id, content_count, created_at) VALUES (?, ?, ?, ?, ?, ?)"
    )
    .bind(&id)
    .bind(&payload.name)
    .bind(&tag_type)
    .bind(&payload.source_id)
    .bind(payload.content_count)
    .bind(&created_at)
    .execute(&pool)
    .await
    {
        Ok(_) => {
            info!("POST /tags - created '{}' type='{}' ({})", payload.name, tag_type, id);
            Json(json!({ "id": id, "success": true }))
        }
        Err(e) if e.to_string().contains("UNIQUE") => {
            Json(json!({ "error": format!("Tag '{}' already exists", payload.name) }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_get_tag(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    let query = format!("{} WHERE t.id = ? GROUP BY t.id", TAG_SELECT);
    match sqlx::query(&query).bind(&id).fetch_optional(&pool).await {
        Ok(Some(row)) => Json(json!(row_to_tag(&row))),
        Ok(None) => Json(json!({ "error": "Tag not found" })),
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_update_tag(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<UpdateTagRequest>,
) -> Json<serde_json::Value> {
    let tag_type = normalize_tag_type(payload.tag_type.as_deref());
    match sqlx::query(
        "UPDATE tags SET name = ?, tag_type = ?, source_id = ? WHERE id = ?"
    )
    .bind(&payload.name)
    .bind(&tag_type)
    .bind(&payload.source_id)
    .bind(&id)
    .execute(&pool)
    .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Tag not found" })),
        Ok(_) => {
            info!("PUT /tags/{} - updated", id);
            Json(json!({ "success": true }))
        }
        Err(e) if e.to_string().contains("UNIQUE") => {
            Json(json!({ "error": format!("Tag '{}' already exists", payload.name) }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_delete_tag(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Json<serde_json::Value> {
    // Remove parent relationships (both directions) before deleting
    sqlx::query("DELETE FROM tag_parents WHERE tag_id = ? OR parent_tag_id = ?")
        .bind(&id).bind(&id).execute(&pool).await.ok();

    // Remove content associations before deleting
    sqlx::query("DELETE FROM content_tags WHERE tag_id = ?")
        .bind(&id).execute(&pool).await.ok();

    match sqlx::query("DELETE FROM tags WHERE id = ?")
        .bind(&id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Tag not found" })),
        Ok(_) => {
            info!("DELETE /tags/{}", id);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_add_tag_parent(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(payload): Json<AddTagParentRequest>,
) -> Json<serde_json::Value> {
    let parent_name = payload.parent_name.trim().to_string();
    if parent_name.is_empty() {
        return Json(json!({ "error": "Parent tag name cannot be empty" }));
    }

    // Resolve parent tag by name
    let parent_row = sqlx::query("SELECT id FROM tags WHERE name = ?")
        .bind(&parent_name)
        .fetch_optional(&pool)
        .await;

    let parent_id: String = match parent_row {
        Ok(Some(row)) => row.get("id"),
        Ok(None) => return Json(json!({ "error": format!("Tag '{}' not found", parent_name) })),
        Err(e) => return Json(json!({ "error": format!("Database error: {}", e) })),
    };

    if parent_id == id {
        return Json(json!({ "error": "A tag cannot be its own parent" }));
    }

    match sqlx::query("INSERT OR IGNORE INTO tag_parents (tag_id, parent_tag_id) VALUES (?, ?)")
        .bind(&id)
        .bind(&parent_id)
        .execute(&pool)
        .await
    {
        Ok(_) => {
            info!("POST /tags/{}/parents - added '{}'", id, parent_name);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_remove_tag_parent(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path((id, parent_id)): axum::extract::Path<(String, String)>,
) -> Json<serde_json::Value> {
    match sqlx::query("DELETE FROM tag_parents WHERE tag_id = ? AND parent_tag_id = ?")
        .bind(&id)
        .bind(&parent_id)
        .execute(&pool)
        .await
    {
        Ok(r) if r.rows_affected() == 0 => Json(json!({ "error": "Parent relationship not found" })),
        Ok(_) => {
            info!("DELETE /tags/{}/parents/{}", id, parent_id);
            Json(json!({ "success": true }))
        }
        Err(e) => Json(json!({ "error": format!("Database error: {}", e) })),
    }
}

async fn handler_links_stats(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    let unprocessed: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links WHERE processed_at IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap_or(0);
    info!("GET /links/stats - total={} unprocessed={}", total, unprocessed);
    Json(json!({
        "total": total,
        "unprocessed": unprocessed,
        "processed": total - unprocessed,
    }))
}

async fn handler_content_stats(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    let file_paths: Vec<String> = sqlx::query_scalar(
        "SELECT file_path FROM content WHERE file_path IS NOT NULL"
    )
    .fetch_all(&pool)
    .await
    .unwrap_or_default();

    let mut by_type: std::collections::HashMap<String, (u64, u64)> = std::collections::HashMap::new();
    let mut total_size: u64 = 0;

    for path in &file_paths {
        let ext = path.rsplit('.').next().unwrap_or("unknown").to_lowercase();
        let size = tokio::fs::metadata(path).await.map(|m| m.len()).unwrap_or(0);
        total_size += size;
        let entry = by_type.entry(ext).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += size;
    }

    let mut type_list: Vec<serde_json::Value> = by_type
        .into_iter()
        .map(|(ext, (count, size))| json!({ "ext": ext, "count": count, "size": size }))
        .collect();
    type_list.sort_by(|a, b| {
        b["count"].as_u64().unwrap_or(0).cmp(&a["count"].as_u64().unwrap_or(0))
    });

    info!("GET /content/stats - {} files, {} bytes", file_paths.len(), total_size);
    Json(json!({
        "total_files": file_paths.len(),
        "total_size": total_size,
        "by_type": type_list,
    }))
}

async fn handler_backup(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    info!("POST /backup - Creating manual backup");
    run_backup(&pool, "manual").await
}

async fn run_backup(pool: &SqlitePool, label: &str) -> Json<serde_json::Value> {
    let config_content = match std::fs::read_to_string("config.toml") {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Failed to read config: {}", e) })),
    };
    let config: Config = match toml::from_str(&config_content) {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Failed to parse config: {}", e) })),
    };

    // Use max_backups from db settings, falling back to config
    let max_backups: i64 = sqlx::query_scalar("SELECT max_backups FROM backup_settings WHERE id = 1")
        .fetch_optional(pool).await.ok().flatten()
        .unwrap_or(config.database.max_backups.unwrap_or(5) as i64);

    match create_backup(&config.database.name, label, max_backups as usize).await {
        Ok(path) => {
            let now = chrono::Utc::now().to_rfc3339();
            let _ = sqlx::query("UPDATE backup_settings SET last_backup_at = ?, last_backup_path = ? WHERE id = 1")
                .bind(&now).bind(&path).execute(pool).await;
            info!("Backup created: {}", path);
            Json(json!({ "success": true, "path": path }))
        }
        Err(e) => Json(json!({ "error": format!("Backup failed: {}", e) })),
    }
}

async fn handler_backup_settings_get(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    match sqlx::query(
        "SELECT max_backups, schedule_minutes, last_backup_at, last_backup_path FROM backup_settings WHERE id = 1"
    )
    .fetch_optional(&pool).await
    {
        Ok(Some(r)) => Json(json!({
            "max_backups":      r.get::<i64, _>("max_backups"),
            "schedule_minutes": r.get::<i64, _>("schedule_minutes"),
            "last_backup_at":   r.get::<Option<String>, _>("last_backup_at"),
            "last_backup_path": r.get::<Option<String>, _>("last_backup_path"),
        })),
        _ => Json(json!({ "max_backups": 5, "schedule_minutes": 0, "last_backup_at": null, "last_backup_path": null })),
    }
}

async fn handler_backup_settings_put(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let max_backups = payload.get("max_backups").and_then(|v| v.as_i64()).unwrap_or(5).max(1).min(100);
    let schedule_minutes = payload.get("schedule_minutes").and_then(|v| v.as_i64()).unwrap_or(0).max(0);
    match sqlx::query("UPDATE backup_settings SET max_backups = ?, schedule_minutes = ? WHERE id = 1")
        .bind(max_backups).bind(schedule_minutes).execute(&pool).await
    {
        Ok(_)  => Json(json!({ "ok": true })),
        Err(e) => Json(json!({ "ok": false, "error": e.to_string() })),
    }
}

async fn handler_backup_list(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
) -> Json<serde_json::Value> {
    let row = sqlx::query(
        "SELECT max_backups, schedule_minutes, last_backup_at, last_backup_path FROM backup_settings WHERE id = 1"
    )
    .fetch_optional(&pool).await.ok().flatten();

    let (max_backups, schedule_minutes, last_backup_at, last_backup_path) = match row {
        Some(r) => (
            r.get::<i64, _>("max_backups"),
            r.get::<i64, _>("schedule_minutes"),
            r.get::<Option<String>, _>("last_backup_at"),
            r.get::<Option<String>, _>("last_backup_path"),
        ),
        None => (5, 0, None, None),
    };

    // Count actual backup files on disk
    let backup_count = std::fs::read_dir("backups")
        .map(|rd| rd.filter_map(|e| e.ok()).filter(|e| e.path().is_file()).count())
        .unwrap_or(0);

    Json(json!({
        "max_backups": max_backups,
        "schedule_minutes": schedule_minutes,
        "last_backup_at": last_backup_at,
        "last_backup_path": last_backup_path,
        "backup_count": backup_count,
    }))
}

/// Recursively delete all regular files under `dir`, counting deletions.
/// Directories themselves are left in place (cheap to keep, avoids race conditions).
async fn delete_files_recursive(dir: &std::path::Path, count: &mut u64, error: &mut Option<String>) {
    let mut rd = match tokio::fs::read_dir(dir).await {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            *error = Some(format!("read_dir {}: {}", dir.display(), e));
            return;
        }
    };
    while let Ok(Some(entry)) = rd.next_entry().await {
        let path = entry.path();
        if let Ok(ft) = entry.file_type().await {
            if ft.is_dir() {
                Box::pin(delete_files_recursive(&path, count, error)).await;
            } else if tokio::fs::remove_file(&path).await.is_ok() {
                *count += 1;
            }
        }
    }
}

async fn handler_debug_reset(
    axum::extract::State((pool, _, storage_path)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Json<serde_json::Value> {
    // Default true for backward compat (bare POST with no params clears everything)
    let flag = |key: &str| params.get(key).map(|v| v != "false").unwrap_or(true);
    let clear_links         = flag("clear_links");
    let clear_content       = flag("clear_content");
    let clear_subscriptions = flag("clear_subscriptions");
    let clear_sources       = flag("clear_sources");
    let clear_creators      = flag("clear_creators");
    let clear_tags          = flag("clear_tags");
    let delete_files        = params.get("delete_files").map(|v| v == "true").unwrap_or(false);

    tracing::warn!(
        "POST /debug/reset - links={} content={} subs={} sources={} creators={} tags={} files={}",
        clear_links, clear_content, clear_subscriptions, clear_sources, clear_creators, clear_tags, delete_files
    );

    async fn del(pool: &SqlitePool, q: &'static str) -> u64 {
        sqlx::query(q).execute(pool).await.map(|r| r.rows_affected()).unwrap_or(0)
    }

    // Junction tables must be cleared before their parents to respect FK constraints.
    // content_tags links content↔tags; clear it if either side is being cleared.
    let content_tags_deleted = if clear_content || clear_tags {
        del(&pool, "DELETE FROM content_tags").await
    } else { 0 };
    // tag_parents is self-referencing on tags; clear it when tags are cleared.
    let tag_parents_deleted = if clear_tags {
        del(&pool, "DELETE FROM tag_parents").await
    } else { 0 };

    let content_deleted       = if clear_content       { del(&pool, "DELETE FROM content")          .await } else { 0 };
    let links_deleted         = if clear_links         { del(&pool, "DELETE FROM links")             .await } else { 0 };
    let subscriptions_deleted = if clear_subscriptions { del(&pool, "DELETE FROM subscriptions")     .await } else { 0 };
    let creator_aliases_del   = if clear_creators      { del(&pool, "DELETE FROM creator_aliases")   .await } else { 0 };
    let creators_deleted      = if clear_creators      { del(&pool, "DELETE FROM creators")          .await } else { 0 };
    let tags_deleted          = if clear_tags          { del(&pool, "DELETE FROM tags")              .await } else { 0 };
    let sources_deleted       = if clear_sources       { del(&pool, "DELETE FROM sources")           .await } else { 0 };

    tracing::warn!(
        "Debug reset complete: {} links, {} subscriptions, {} sources, {} creators, {} tags, {} content deleted",
        links_deleted, subscriptions_deleted, sources_deleted, creators_deleted + creator_aliases_del,
        tags_deleted + tag_parents_deleted, content_deleted + content_tags_deleted
    );

    // Optionally wipe downloaded files (recursive — sharded layout has 3 levels)
    let mut files_deleted: u64 = 0;
    let mut files_error: Option<String> = None;
    if delete_files {
        for subfolder in &["image", "video", "audio", "document", "unknown"] {
            let dir = format!("{}/{}", storage_path, subfolder);
            delete_files_recursive(std::path::Path::new(&dir), &mut files_deleted, &mut files_error).await;
        }
        tracing::warn!("Deleted {} downloaded files from {}", files_deleted, storage_path);
    }

    Json(json!({
        "links_deleted": links_deleted,
        "subscriptions_deleted": subscriptions_deleted,
        "sources_deleted": sources_deleted,
        "creators_deleted": creators_deleted,
        "tags_deleted": tags_deleted,
        "content_deleted": content_deleted,
        "files_deleted": files_deleted,
        "files_error": files_error,
    }))
}

async fn backup_scheduler_loop(pool: SqlitePool) {
    // Check every minute whether a scheduled backup is due.
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;

        let row = sqlx::query(
            "SELECT schedule_minutes, last_backup_at FROM backup_settings WHERE id = 1"
        )
        .fetch_optional(&pool).await;

        let (schedule_minutes, last_backup_at): (i64, Option<String>) = match row {
            Ok(Some(r)) => (r.get("schedule_minutes"), r.get("last_backup_at")),
            _ => continue,
        };

        if schedule_minutes <= 0 { continue; }

        let due = match last_backup_at {
            None => true,
            Some(ts) => {
                match chrono::DateTime::parse_from_rfc3339(&ts) {
                    Ok(last) => {
                        let elapsed = chrono::Utc::now().signed_duration_since(last.with_timezone(&chrono::Utc));
                        elapsed.num_minutes() >= schedule_minutes
                    }
                    Err(_) => true,
                }
            }
        };

        if due {
            info!("Scheduled backup triggered (interval: {}m)", schedule_minutes);
            run_backup(&pool, "scheduled").await;
        }
    }
}

async fn processor_loop(pool: SqlitePool, thread_pool_size: usize, processing_enabled: Arc<Mutex<bool>>, storage_path: String, flaresolverr_url: String) {
    // Pre-create all download subdirectories
    for subfolder in &["image", "video", "audio", "document", "unknown"] {
        let dir = format!("{}/{}", storage_path, subfolder);
        if let Err(e) = tokio::fs::create_dir_all(&dir).await {
            tracing::error!("Failed to create download directory {}: {}", dir, e);
        }
    }

    let http_client = reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .default_headers({
            let mut h = reqwest::header::HeaderMap::new();
            h.insert("Accept", "application/json, text/html, */*".parse().unwrap());
            h.insert("Accept-Language", "en-US,en;q=0.9".parse().unwrap());
            h.insert("Accept-Encoding", "gzip, deflate, br".parse().unwrap());
            h
        })
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    // Shared CF session cache — keyed by domain base URL so a solved Cloudflare
    // clearance can be reused for every subsequent link on the same site.
    let session_cache: SessionCache = Arc::new(Mutex::new(HashMap::new()));

    let mut tasks = Vec::new();

    loop {
        // Check if processing is enabled
        let enabled = *processing_enabled.lock().await;
        
        if !enabled {
            // Processing is disabled, wait before checking again
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        // Fetch links from database
        match sqlx::query("SELECT id, url, file_url, source_id FROM links WHERE processed_at IS NULL AND error IS NULL LIMIT ?")
            .bind(thread_pool_size as i32)
            .fetch_all(&pool)
            .await
        {
            Ok(rows) => {
                if rows.is_empty() {
                    // No links to process, wait before trying again
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }

                info!("Processing {} links", rows.len());

                // Spawn tasks for each link
                for row in rows {
                    let id: String = row.get("id");
                    let url: String = row.get("url");
                    let file_url: Option<String> = row.get("file_url");
                    let source_id: Option<String> = row.get("source_id");
                    let client = http_client.clone();
                    let pool_clone = pool.clone();
                    let id_clone = id.clone();
                    let storage_clone = storage_path.clone();
                    let fs_url = flaresolverr_url.clone();
                    let cache_clone = session_cache.clone();

                    let task = tokio::spawn(async move {
                        process_link(&client, &url, file_url.as_deref(), source_id.as_deref(), &id_clone, &pool_clone, &storage_clone, &fs_url, &cache_clone).await;
                    });

                    tasks.push(task);
                }

                // Wait for all tasks to complete
                for task in tasks.drain(..) {
                    let _ = task.await;
                }

                info!("Batch processing complete");
            }
            Err(e) => {
                tracing::error!("Failed to fetch links: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }

        // Small delay between batches
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn process_link(
    client: &reqwest::Client,
    url: &str,
    stored_file_url: Option<&str>,
    source_id: Option<&str>,
    link_id: &str,
    pool: &SqlitePool,
    storage_path: &str,
    flaresolverr_url: &str,
    session_cache: &SessionCache,
) {
    info!("Processing link: {}", url);

    // Booru tag-list pages → expand to individual post links, do not download directly
    if is_booru_tag_list(url) {
        expand_booru_tag_list(client, url, link_id, source_id, pool, flaresolverr_url, session_cache).await;
        return;
    }

    // Use the file_url already resolved by the extension, or fall back to dapi/HEAD resolution
    let (file_url, cf_session, meta) = if let Some(fu) = stored_file_url {
        info!("Using pre-resolved file_url: {}", fu);
        (fu.to_string(), None, PostMetadata::default())
    } else {
        // Look up source credentials attached to this link
        let creds = sqlx::query(
            "SELECT s.domain, s.api_key, s.user_id \
             FROM links l JOIN sources s ON l.source_id = s.id \
             WHERE l.id = ?"
        )
        .bind(link_id)
        .fetch_optional(pool)
        .await
        .ok()
        .flatten()
        .map(|row| SourceCredentials {
            domain: row.get("domain"),
            api_key: row.get("api_key"),
            user_id: row.get("user_id"),
        });

        match resolve_file_url(client, url, creds.as_ref(), flaresolverr_url, session_cache).await {
            Some(r) => r,
            None => {
                tracing::warn!("Could not resolve a downloadable file URL for {}", url);
                mark_link_error(pool, link_id, "Could not resolve a downloadable file URL").await;
                return;
            }
        }
    };

    // Polite random delay: 1–5 s
    let delay = rand::random::<f64>() * 4.0 + 1.0;
    tokio::time::sleep(Duration::from_secs_f64(delay)).await;

    match download_file(client, &file_url, storage_path, url, cf_session.as_ref()).await {
        Ok(saved_path) => {
            info!("Saved {} -> {}", url, saved_path);
            create_content(pool, link_id, source_id, &saved_path, &meta, storage_path).await;
            mark_link_processed(pool, link_id).await;
        }
        Err(e) => {
            tracing::warn!("Download failed for {}: {}", file_url, e);
            mark_link_error(pool, link_id, &format!("Download failed: {}", e)).await;
        }
    }
}

/// Mark a link as processed right now (and clear any previous error).
async fn mark_link_processed(pool: &SqlitePool, link_id: &str) {
    let ts = chrono::Utc::now().to_rfc3339();
    if let Err(e) = sqlx::query("UPDATE links SET processed_at = ?, error = NULL WHERE id = ?")
        .bind(&ts)
        .bind(link_id)
        .execute(pool)
        .await
    {
        tracing::warn!("Failed to mark link {} as processed: {}", link_id, e);
    }
}

/// Record a processing error on a link (leaves processed_at NULL so it stays retryable).
async fn mark_link_error(pool: &SqlitePool, link_id: &str, msg: &str) {
    if let Err(e) = sqlx::query("UPDATE links SET error = ? WHERE id = ?")
        .bind(msg)
        .bind(link_id)
        .execute(pool)
        .await
    {
        tracing::warn!("Failed to record error for link {}: {}", link_id, e);
    }
}

/// Stream the original downloaded file for a content item.
async fn handler_content_file(
    axum::extract::State((pool, _, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    if id.len() > 64 || id.chars().any(|c| !c.is_ascii_alphanumeric() && c != '-') {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    let file_path = match sqlx::query_scalar::<_, Option<String>>(
        "SELECT file_path FROM content WHERE id = ?"
    )
    .bind(&id)
    .fetch_optional(&pool)
    .await
    {
        Ok(Some(Some(p))) => p,
        _ => return axum::http::StatusCode::NOT_FOUND.into_response(),
    };

    let ext = file_path.rsplit('.').next().unwrap_or("").to_lowercase();
    let content_type = match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png"          => "image/png",
        "gif"          => "image/gif",
        "webp"         => "image/webp",
        "avif"         => "image/avif",
        "bmp"          => "image/bmp",
        "tiff"         => "image/tiff",
        "mp4"          => "video/mp4",
        "webm"         => "video/webm",
        "mkv"          => "video/x-matroska",
        "avi"          => "video/x-msvideo",
        "mov"          => "video/quicktime",
        "flv"          => "video/x-flv",
        "mp3"          => "audio/mpeg",
        "flac"         => "audio/flac",
        "ogg"          => "audio/ogg",
        "wav"          => "audio/wav",
        "aac"          => "audio/aac",
        "opus"         => "audio/opus",
        "pdf"          => "application/pdf",
        _              => "application/octet-stream",
    };

    let file = match tokio::fs::File::open(&file_path).await {
        Ok(f) => f,
        Err(_) => return axum::http::StatusCode::NOT_FOUND.into_response(),
    };
    let stream = tokio_util::io::ReaderStream::new(file);
    axum::response::Response::builder()
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(axum::http::header::CACHE_CONTROL, "public, max-age=31536000, immutable")
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
}

/// Serve a pre-generated JPEG thumbnail for a content item.
async fn handler_content_thumbnail(
    axum::extract::State((_, _, storage_path)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>, String)>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Reject anything that isn't a plain UUID to prevent path traversal
    if id.len() > 64 || id.chars().any(|c| !c.is_ascii_alphanumeric() && c != '-') {
        return axum::http::StatusCode::BAD_REQUEST.into_response();
    }
    let id_chars: Vec<char> = id.chars().collect();
    let s1 = format!("{}{}", id_chars.get(0).copied().unwrap_or('0'), id_chars.get(1).copied().unwrap_or('0'));
    let s2 = format!("{}{}", id_chars.get(2).copied().unwrap_or('0'), id_chars.get(3).copied().unwrap_or('0'));
    let thumb_path = format!("{}/thumbnails/{}/{}/{}.jpg", storage_path, s1, s2, id);
    match tokio::fs::read(&thumb_path).await {
        Ok(data) => (
            [
                (axum::http::header::CONTENT_TYPE, "image/jpeg"),
                (axum::http::header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
            ],
            data,
        ).into_response(),
        Err(_) => axum::http::StatusCode::NOT_FOUND.into_response(),
    }
}

/// Generate a JPEG thumbnail for an image file (CPU-bound, run in spawn_blocking).
fn generate_image_thumbnail(file_path: &str, thumb_path: &str) -> Result<(), String> {
    let img = image::open(file_path).map_err(|e| e.to_string())?;
    let thumb = img.thumbnail(400, 400);
    thumb.save(thumb_path).map_err(|e| e.to_string())
}

/// Generate a JPEG thumbnail for a video file using ffmpeg.
async fn generate_video_thumbnail(file_path: &str, thumb_path: &str) {
    let out = tokio::process::Command::new("ffmpeg")
        .args(["-y", "-ss", "00:00:01", "-i", file_path,
               "-vframes", "1", "-vf", "scale=400:-2", "-q:v", "3", thumb_path])
        .output()
        .await;
    match out {
        Ok(o) if o.status.success() => {}
        Ok(o) => tracing::warn!("ffmpeg thumbnail failed: {}", String::from_utf8_lossy(&o.stderr)),
        Err(e) => tracing::warn!("ffmpeg not available for thumbnail: {}", e),
    }
}

/// Generate and save a thumbnail for the given file, keyed by content_id.
async fn generate_thumbnail(content_id: &str, file_path: &str, storage_path: &str) {
    let ext = file_path.rsplit('.').next().unwrap_or("").to_lowercase();
    let id_chars: Vec<char> = content_id.chars().collect();
    let s1 = format!("{}{}", id_chars.get(0).copied().unwrap_or('0'), id_chars.get(1).copied().unwrap_or('0'));
    let s2 = format!("{}{}", id_chars.get(2).copied().unwrap_or('0'), id_chars.get(3).copied().unwrap_or('0'));
    let thumb_dir = format!("{}/thumbnails/{}/{}", storage_path, s1, s2);
    let thumb_path = format!("{}/{}.jpg", thumb_dir, content_id);
    if let Err(e) = tokio::fs::create_dir_all(&thumb_dir).await {
        tracing::warn!("Could not create thumbnail dir {}: {}", thumb_dir, e);
        return;
    }
    if tokio::fs::metadata(&thumb_path).await.is_ok() {
        return; // already generated
    }
    match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "bmp" | "tiff" => {
            let fp = file_path.to_string();
            let tp = thumb_path.clone();
            match tokio::task::spawn_blocking(move || generate_image_thumbnail(&fp, &tp)).await {
                Ok(Ok(())) => info!("Thumbnail generated for content {}", content_id),
                Ok(Err(e)) => tracing::warn!("Image thumbnail failed for {}: {}", content_id, e),
                Err(e) => tracing::warn!("Thumbnail task panicked for {}: {}", content_id, e),
            }
        }
        "mp4" | "webm" | "mkv" | "avi" | "mov" | "flv" => {
            generate_video_thumbnail(file_path, &thumb_path).await;
            info!("Video thumbnail generated for content {}", content_id);
        }
        _ => {} // audio, documents, unknown — no thumbnail
    }
}

/// Create a content row and link its tags, finding or creating each tag as needed.
async fn create_content(
    pool: &SqlitePool,
    link_id: &str,
    source_id: Option<&str>,
    file_path: &str,
    meta: &PostMetadata,
    storage_path: &str,
) {
    let content_id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();

    let res = sqlx::query(
        "INSERT INTO content (id, link_id, source_id, file_path, post_date, uploader, rating, score, source_url, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(&content_id)
    .bind(link_id)
    .bind(source_id)
    .bind(file_path)
    .bind(meta.post_date.as_deref())
    .bind(meta.uploader.as_deref())
    .bind(meta.rating.as_deref())
    .bind(meta.score)
    .bind(meta.source_url.as_deref())
    .bind(&created_at)
    .execute(pool)
    .await;

    if let Err(e) = res {
        tracing::warn!("Failed to create content for link {}: {}", link_id, e);
        return;
    }

    info!("Created content {} for link {} ({} tags)", content_id, link_id, meta.tags.len());

    // Find or create each tag, then link it to this content entry
    for (tag_name, tag_type) in &meta.tags {
        let tag_id = find_or_create_tag(pool, tag_name, tag_type, source_id).await;
        if let Some(ref tid) = tag_id {
            let _ = sqlx::query(
                "INSERT OR IGNORE INTO content_tags (content_id, tag_id) VALUES (?, ?)"
            )
            .bind(&content_id)
            .bind(tid)
            .execute(pool)
            .await;

            // Increment the tag's content count
            let _ = sqlx::query(
                "UPDATE tags SET content_count = content_count + 1 WHERE id = ?"
            )
            .bind(tid)
            .execute(pool)
            .await;

            // Auto-create/link a creator entry for any artist-type tag
            if tag_type == "artist" {
                ensure_creator_for_artist_tag(pool, tag_name, tid).await;
            }
        }
    }

    generate_thumbnail(&content_id, file_path, storage_path).await;
}

/// For an artist tag, ensure a creator entry exists and the tag is linked in creator_tags.
async fn ensure_creator_for_artist_tag(pool: &SqlitePool, tag_name: &str, tag_id: &str) {
    // Resolve existing creator id by name or alias
    let creator_id: Option<String> = sqlx::query_scalar(
        "SELECT id FROM creators WHERE name = ?
         UNION
         SELECT creator_id FROM creator_aliases WHERE alias = ?
         LIMIT 1"
    )
    .bind(tag_name)
    .bind(tag_name)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    let creator_id = match creator_id {
        Some(id) => id,
        None => {
            // Create a new creator
            let new_id = Uuid::new_v4().to_string();
            let created_at = chrono::Utc::now().to_rfc3339();
            match sqlx::query(
                "INSERT OR IGNORE INTO creators (id, name, created_at) VALUES (?, ?, ?)"
            )
            .bind(&new_id)
            .bind(tag_name)
            .bind(&created_at)
            .execute(pool)
            .await
            {
                Ok(_) => info!("Auto-created creator '{}' from artist tag", tag_name),
                Err(e) => {
                    tracing::warn!("Failed to auto-create creator '{}': {}", tag_name, e);
                    return;
                }
            }
            new_id
        }
    };

    // Link the tag to the creator
    let _ = sqlx::query(
        "INSERT OR IGNORE INTO creator_tags (creator_id, tag_id) VALUES (?, ?)"
    )
    .bind(&creator_id)
    .bind(tag_id)
    .execute(pool)
    .await;
}

/// Return an existing tag's id by name+source, or create a new tag and return its id.
/// `tag_type` is only applied when creating a new tag; existing tags are not modified.
async fn find_or_create_tag(pool: &SqlitePool, name: &str, tag_type: &str, source_id: Option<&str>) -> Option<String> {
    // Try to find existing tag (match on name; prefer same source if available)
    let existing: Option<String> = if let Some(sid) = source_id {
        sqlx::query_scalar("SELECT id FROM tags WHERE name = ? AND source_id = ?")
            .bind(name)
            .bind(sid)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .or_else(|| None)
    } else {
        None
    };

    // Fall back to any tag with that name regardless of source
    let existing = if existing.is_none() {
        sqlx::query_scalar("SELECT id FROM tags WHERE name = ?")
            .bind(name)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
    } else {
        existing
    };

    if let Some(id) = existing {
        return Some(id);
    }

    // Create new tag
    let tag_id = Uuid::new_v4().to_string();
    let created_at = chrono::Utc::now().to_rfc3339();
    match sqlx::query(
        "INSERT INTO tags (id, name, tag_type, source_id, content_count, created_at) VALUES (?, ?, ?, ?, 0, ?)"
    )
    .bind(&tag_id)
    .bind(name)
    .bind(tag_type)
    .bind(source_id)
    .bind(&created_at)
    .execute(pool)
    .await
    {
        Ok(_) => Some(tag_id),
        Err(e) => {
            tracing::warn!("Failed to create tag '{}': {}", name, e);
            None
        }
    }
}

/// Try to obtain a direct download URL from `page_url`.
///
/// For booru-style post pages (`page=post&s=view&id=N`) the dapi is used to
/// retrieve the `file_url`.  For anything else a HEAD request is made; if the
/// Content-Type is a media type the original URL is returned as-is.
async fn resolve_file_url(
    client: &reqwest::Client,
    page_url: &str,
    _creds: Option<&SourceCredentials>,
    flaresolverr_url: &str,
    session_cache: &SessionCache,
) -> Option<(String, Option<CfSession>, PostMetadata)> {
    // Booru post page?
    if page_url.contains("page=post") && page_url.contains("s=view") {
        if let Some(post_id) = extract_query_param(page_url, "id") {
            return resolve_booru_file_url(page_url, &post_id, flaresolverr_url, session_cache).await;
        }
    }

    // Fall back: check whether the URL itself is already a media file
    let domain_base = extract_domain_base(page_url).unwrap_or_default();
    match client
        .head(page_url)
        .header("Referer", &domain_base)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            let ct = resp
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if ct.starts_with("image/") || ct.starts_with("video/") || ct.starts_with("audio/") {
                return Some((page_url.to_string(), None, PostMetadata::default()));
            }
        }
        _ => {}
    }

    None
}

/// Returns true if `s` looks like a Cloudflare challenge page rather than real API JSON.
/// Used to decide whether to retry via FlareSolverr.
fn looks_like_cf(s: &str) -> bool {
    // A Cloudflare challenge page contains one or more of these strings.
    // Do NOT check starts_with('<') — every valid HTML page also starts with '<'.
    s.contains("Just a moment")
        || s.contains("cf-browser-verification")
        || s.contains("Checking if the site connection is secure")
        || s.contains("Enable JavaScript and cookies to continue")
        || s.contains("cf_chl_opt")
        || s.contains("challenges.cloudflare.com")
}

/// Fetch a booru post page, reusing or acquiring a Cloudflare clearance session.
///
/// Strategy:
/// 1. If the domain has a cached CF session, try a plain curl request with those cookies.
/// 2. If FlareSolverr is configured (and the cached session is missing/stale), solve via FS
///    and cache the resulting session for future requests to the same domain.
/// 3. If FlareSolverr is not configured, attempt a plain curl request and fail clearly on CF.
async fn fetch_page_with_session(
    url: &str,
    base: &str,
    flaresolverr_url: &str,
    cache: &SessionCache,
) -> Option<(String, Option<CfSession>)> {
    // 1. Try cached session first (fast path — avoids another FlareSolverr solve)
    let cached = cache.lock().await.get(base).cloned();
    if let Some(ref sess) = cached {
        match curl_get(url, base, Some(sess)).await {
            Ok(html) if !looks_like_cf(&html) => {
                info!("Reused cached CF session for {}", base);
                return Some((html, cached));
            }
            _ => {
                info!("Cached CF session for {} is stale — re-solving", base);
            }
        }
    }

    // 2. FlareSolverr path (preferred when configured)
    if !flaresolverr_url.is_empty() {
        match flaresolverr_get(flaresolverr_url, url).await {
            Ok((html, sess)) => {
                info!("FlareSolverr solved CF for {} — session cached", base);
                cache.lock().await.insert(base.to_string(), sess.clone());
                return Some((html, Some(sess)));
            }
            Err(e) => {
                tracing::warn!("FlareSolverr failed for {}: {}", url, e);
                return None;
            }
        }
    }

    // 3. Plain curl fallback (no FlareSolverr configured)
    match curl_get(url, base, None).await {
        Ok(html) if !looks_like_cf(&html) => Some((html, None)),
        Ok(_) | Err(_) => {
            tracing::warn!(
                "Post page {} is Cloudflare-protected. Configure flaresolverr_url to bypass it.",
                url
            );
            None
        }
    }
}

/// Fetch a booru post page and parse the file URL + tags directly from HTML.
/// Single request, no dapi calls.
async fn resolve_booru_file_url(
    page_url: &str,
    post_id: &str,
    flaresolverr_url: &str,
    session_cache: &SessionCache,
) -> Option<(String, Option<CfSession>, PostMetadata)> {
    let base = extract_domain_base(page_url)?;

    let (html, cf_session) = fetch_page_with_session(page_url, &base, flaresolverr_url, session_cache).await?;

    let file_url = match extract_file_url_from_html(&html) {
        Some(u) => u,
        None => {
            tracing::warn!("Could not find file URL in post page for post {}", post_id);
            return None;
        }
    };

    let tags = extract_tags_from_html(&html);
    let (post_date, uploader, rating, score) = extract_post_metadata_from_html(&html);
    info!(
        "Post {}: file_url found, {} tags, uploader={:?} rating={:?} score={:?} date={:?}",
        post_id, tags.len(), uploader, rating, score, post_date
    );

    Some((file_url, cf_session, PostMetadata { tags, post_date, uploader, rating, score, ..Default::default() }))
}

/// Extract the highest-quality file URL from a booru post page's HTML.
///
/// Priority for images:
///   1. `<a id="image-link" href="…">` — the anchor wrapping the post image
///      always points to the original full-resolution file on Gelbooru-compatible
///      sites, even when the visible `<img>` shows a downscaled sample.
///   2. `<img id="image" src="…">` — fallback when no wrapping anchor is found.
///
/// For video posts: `<video …><source src="…">`.
fn extract_file_url_from_html(html: &str) -> Option<String> {
    let lower = html.to_ascii_lowercase();

    // Strategy 1: anchor wrapping the main image (original full-res on Gelbooru)
    // Gelbooru uses <a id="image-link" href="//cdn/images/…">
    // Some compatible software uses <a id="original-image" href="…">
    // Strategy 1 (highest priority): <a href="…">Original image</a>
    // Gelbooru renders this as a bold link in the post sidebar, e.g.:
    //   <a href="https://img2.gelbooru.com/images/…/abc.jpg" …>Original image</a>
    // Find the closing `>` that immediately precedes the label text, then walk
    // back to the opening `<a` to extract the href — avoids fragile tag counting.
    for label in &[">original image<", ">original file<"] {
        if let Some(rel) = lower.find(label) {
            // rel points to the `>` before the text; the `<a` is somewhere before it
            if let Some(a_open) = lower[..rel].rfind("<a") {
                let tag_end = lower[a_open..].find('>').map(|e| a_open + e).unwrap_or(html.len());
                let tag = &html[a_open..tag_end];
                if let Some(href) = html_attr_value(tag, "href") {
                    if let Some(url) = normalize_url(&href) {
                        info!("file URL via \"Original image\" link (original)");
                        return Some(url);
                    }
                }
            }
        }
    }

    // Strategy 2: <a id="image-link|original-image|image-view-link" href="…">
    for anchor_id in &["image-link", "original-image", "image-view-link"] {
        let needle = format!(r#"id="{}""#, anchor_id);
        if let Some(a_pos) = lower.find(&needle) {
            if let Some(open) = lower[..a_pos].rfind("<a") {
                let tag_end = lower[open..].find('>').map(|e| open + e).unwrap_or(html.len());
                let tag = &html[open..tag_end];
                if let Some(href) = html_attr_value(tag, "href") {
                    if let Some(url) = normalize_url(&href) {
                        info!("file URL via <a id=\"{}\"> href (original)", anchor_id);
                        return Some(url);
                    }
                }
            }
        }
    }

    // Strategy 3: <a href> immediately wrapping <img id="image">
    if let Some(img_rel) = lower.find(r#"id="image""#) {
        if let Some(img_open) = lower[..img_rel].rfind("<img") {
            if let Some(a_open) = lower[..img_open].rfind("<a") {
                let between = &lower[a_open..img_open];
                if !between.contains("</a>") {
                    let tag_end = lower[a_open..].find('>').map(|e| a_open + e).unwrap_or(html.len());
                    let tag = &html[a_open..tag_end];
                    if let Some(href) = html_attr_value(tag, "href") {
                        if let Some(url) = normalize_url(&href) {
                            info!("file URL via <a> wrapping <img id=\"image\"> (original)");
                            return Some(url);
                        }
                    }
                }
            }
            // Fall back to the img src itself
            let tag_end = lower[img_open..].find('>').map(|e| img_open + e + 1).unwrap_or(html.len());
            let tag = &html[img_open..tag_end];
            if let Some(src) = html_attr_value(tag, "src") {
                if let Some(url) = normalize_url(&src) {
                    info!("file URL via <img id=\"image\"> src (may be sample)");
                    return Some(url);
                }
            }
        }
    }

    // Strategy 4: <video …><source src="URL"> (video posts)
    let mut pos = 0;
    while let Some(rel) = lower[pos..].find("<video") {
        let abs = pos + rel;
        let video_end = lower[abs..].find("</video>").map(|e| abs + e).unwrap_or(html.len().min(abs + 20_000));
        let video_html = &html[abs..video_end];
        let vl = video_html.to_ascii_lowercase();
        let mut spos = 0;
        while let Some(srel) = vl[spos..].find("<source") {
            let sabs = spos + srel;
            let stag_end = vl[sabs..].find('>').map(|e| sabs + e + 1).unwrap_or(video_html.len());
            let stag = &video_html[sabs..stag_end];
            if let Some(src) = html_attr_value(stag, "src") {
                if let Some(url) = normalize_url(&src) {
                    info!("file URL via <video><source> src");
                    return Some(url);
                }
            }
            spos = sabs + 7;
        }
        pos = abs + 6;
    }

    None
}

/// Extract post metadata (date, uploader, rating, score) from a booru post page.
///
/// Targets the statistics sidebar present on Gelbooru-compatible sites:
/// ```html
/// <li>Posted: <a href="...">2024-01-15 10:30:00</a> by <a href="...">username</a></li>
/// <li>Rating: <span>General</span></li>
/// <li>Score: <span id="score">42</span></li>
/// ```
fn extract_post_metadata_from_html(html: &str) -> (Option<String>, Option<String>, Option<String>, Option<i64>) {
    let lower = html.to_ascii_lowercase();

    // ── Score ──────────────────────────────────────────────────────────────
    // <span id="score">42</span>
    let score: Option<i64> = lower.find("id=\"score\"").and_then(|pos| {
        let after = &html[pos..];
        let gt = after.find('>')?;
        let text = &after[gt + 1..];
        let end = text.find('<').unwrap_or(text.len().min(20));
        text[..end].trim().parse().ok()
    });

    // ── Rating ─────────────────────────────────────────────────────────────
    // "Rating: " followed by plain text or wrapped in a tag, e.g.:
    //   <li>Rating: General</li>
    //   <li>Rating: <span>Safe</span></li>
    let rating: Option<String> = lower.find("rating: ").and_then(|pos| {
        let after = &html[pos + 8..];
        let trimmed = after.trim_start();
        let slice = if trimmed.starts_with('<') {
            let gt = trimmed.find('>')?;
            &trimmed[gt + 1..]
        } else {
            trimmed
        };
        let end = slice.find(|c: char| c == '<' || c == '\n').unwrap_or(slice.len().min(40));
        let r = decode_html_entities(slice[..end].trim());
        if r.is_empty() || r.len() > 40 { None } else { Some(r) }
    });

    // ── Posted date ────────────────────────────────────────────────────────
    // Pattern: "Posted: 2023-03-30 12:15:35<br>"  (plain text, no wrapping anchor)
    let post_date: Option<String> = lower.find("posted:").and_then(|pos| {
        let after = &html[pos + 7..]; // skip "posted:"
        let trimmed = after.trim_start();
        let end = trimmed.find(|c: char| c == '<' || c == '\n').unwrap_or(trimmed.len().min(30));
        let d = trimmed[..end].trim().to_string();
        if d.is_empty() { None } else { Some(d) }
    });

    // ── Uploader ───────────────────────────────────────────────────────────
    // Pattern: "Uploader: <a href="...">username</a>"
    let uploader: Option<String> = lower.find("uploader:").and_then(|pos| {
        let after = &html[pos + 9..]; // skip "uploader:"
        let lower_after = after.to_ascii_lowercase();
        let a_rel = lower_after.find("<a ").or_else(|| lower_after.find("<a\t"))?;
        let a_chunk = &after[a_rel..];
        let gt = a_chunk.find('>')?;
        let text = &a_chunk[gt + 1..];
        let end = text.find('<').unwrap_or(text.len().min(80));
        let u = decode_html_entities(text[..end].trim());
        if u.is_empty() { None } else { Some(u) }
    });

    (post_date, uploader, rating, score)
}

/// Extract `(tag_name, tag_type)` pairs from a booru post page's HTML sidebar.
/// Recognises the `tag-type-{artist,copyright,character,metadata,general}` CSS classes
/// used by Gelbooru and compatible software.
fn extract_tags_from_html(html: &str) -> Vec<(String, String)> {
    let mut tags = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut pos = 0;

    loop {
        let rel = match html[pos..].find("tag-type-") {
            Some(r) => r,
            None => break,
        };
        let abs = pos + rel;
        pos = abs + 1; // advance past this hit before the next iteration

        let rest = &html[abs..];

        // Extract the type word directly after "tag-type-"
        let pfx = "tag-type-".len();
        let type_len = rest[pfx..].find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(0);
        let type_raw = &rest[pfx..pfx + type_len];
        let tag_type = match type_raw {
            "artist"            => "artist",
            "copyright"         => "copyright",
            "character"         => "character",
            "metadata" | "meta" => "metadata",
            _                   => "default",
        }.to_string();

        // Bound the search to the current <li> element
        let li_end = rest.find("</li>").unwrap_or(rest.len().min(2000));
        let chunk = &rest[..li_end];
        let chunk_lower = chunk.to_ascii_lowercase();

        // Find the <a> whose href contains `tags=` — that's the tag name link.
        // The first <a> in the <li> is typically a search/question button (?), not the tag.
        let mut name = None;
        let mut search_pos = 0;
        while let Some(rel) = chunk_lower[search_pos..].find("<a ").or_else(|| chunk_lower[search_pos..].find("<a\t")) {
            let a_start = search_pos + rel;
            let tag_end = chunk_lower[a_start..].find('>').map(|e| a_start + e).unwrap_or(chunk.len());
            let a_tag = &chunk[a_start..tag_end];
            if let Some(href) = html_attr_value(a_tag, "href") {
                let href = decode_html_entities(&href);
                if let Some(p) = href.find("tags=") {
                    let s = &href[p + 5..];
                    let e = s.find(|c| c == '&' || c == '#').unwrap_or(s.len());
                    let decoded = url_decode_query(&s[..e]);
                    if !decoded.is_empty() {
                        name = Some(decoded);
                        break;
                    }
                }
            }
            search_pos = a_start + 3;
        }

        let name = match name {
            Some(n) => n.trim().to_string(),
            None => continue,
        };
        if name.is_empty() || name.len() > 200 || seen.contains(&name) {
            continue;
        }
        seen.insert(name.clone());
        tags.push((name, tag_type));
    }

    tags
}

/// Extract an HTML attribute value from a tag string (handles `"` and `'` quoting).
fn html_attr_value(tag: &str, attr: &str) -> Option<String> {
    let tag_lower = tag.to_ascii_lowercase();
    let attr_lower = attr.to_ascii_lowercase();
    for quote in ['"', '\''] {
        let needle = format!("{}={}", attr_lower, quote);
        if let Some(start) = tag_lower.find(&needle) {
            let val_start = start + needle.len();
            if let Some(end) = tag[val_start..].find(quote) {
                return Some(tag[val_start..val_start + end].to_string());
            }
        }
    }
    None
}

/// Ensure a URL is absolute with an explicit https scheme.
fn normalize_url(url: &str) -> Option<String> {
    let url = url.trim();
    if url.starts_with("https://") || url.starts_with("http://") {
        Some(url.to_string())
    } else if url.starts_with("//") {
        Some(format!("https:{}", url))
    } else {
        None
    }
}

/// Percent-decode a URL query-string value (`+` → space, `%XX` → char).
fn url_decode_query(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => { out.push(' '); i += 1; }
            b'%' if i + 2 < bytes.len() => {
                if let Ok(n) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                    out.push(n as char);
                    i += 3;
                } else {
                    out.push('%');
                    i += 1;
                }
            }
            b => { out.push(b as char); i += 1; }
        }
    }
    out
}

/// Decode the most common HTML character entities.
fn decode_html_entities(s: &str) -> String {
    s.replace("&amp;", "&")
     .replace("&lt;", "<")
     .replace("&gt;", ">")
     .replace("&quot;", "\"")
     .replace("&#39;", "'")
     .replace("&apos;", "'")
     .replace("&nbsp;", " ")
}

/// Download `file_url` and write it to `{storage_path}/{subfolder}/{filename}`.
/// Returns the path it was saved to.
async fn download_file(
    _client: &reqwest::Client,
    file_url: &str,
    storage_path: &str,
    referer_page: &str,
    cf_session: Option<&CfSession>,
) -> Result<String, String> {
    // Use the full page URL as Referer — CDNs with hotlink protection often
    // require the exact referring page, not just the domain.
    let referer_base = referer_page.to_string();

    // Derive filename from URL path before making any requests
    let filename = file_url
        .split('/')
        .last()
        .unwrap_or("file")
        .split('?')
        .next()
        .unwrap_or("file");
    let filename = if filename.is_empty() { "file" } else { filename };

    // Infer type subfolder from the file extension; fall back to "unknown"
    let ext = filename.rsplit('.').next().unwrap_or("").to_lowercase();
    let type_folder = match ext.as_str() {
        "jpg" | "jpeg" | "png" | "gif" | "webp" | "avif" | "bmp" | "tiff" => "image",
        "mp4" | "webm" | "mkv" | "avi" | "mov" | "flv"                    => "video",
        "mp3" | "flac" | "ogg" | "wav" | "aac" | "opus"                   => "audio",
        "pdf" | "cbz" | "cbr" | "epub"                                     => "document",
        _                                                                   => "unknown",
    };

    // Two-level hex-prefix sharding to avoid 10k+ files in a single directory.
    // Uses the first 4 hex characters of the filename stem (or a fallback).
    // e.g. "9214cce2…abc.jpg" → image/92/14/9214cce2…abc.jpg
    // Non-hex characters are normalised to '0' so every filename gets a valid shard.
    let stem = filename.rsplit('.').nth(1).unwrap_or(filename);
    let chars: Vec<char> = stem.chars()
        .map(|c| if c.is_ascii_hexdigit() { c.to_ascii_lowercase() } else { '0' })
        .collect();
    let shard1 = chars.get(0).copied().unwrap_or('0').to_string()
        + &chars.get(1).copied().unwrap_or('0').to_string();
    let shard2 = chars.get(2).copied().unwrap_or('0').to_string()
        + &chars.get(3).copied().unwrap_or('0').to_string();

    let dir = format!("{}/{}/{}/{}", storage_path, type_folder, shard1, shard2);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| format!("mkdir {}: {}", dir, e))?;

    let file_path = format!("{}/{}", dir, filename);

    // Skip if already downloaded
    if tokio::fs::metadata(&file_path).await.is_ok() {
        info!("Already exists, skipping: {}", file_path);
        return Ok(file_path);
    }

    curl_download(file_url, &referer_base, &file_path, cf_session).await
}

/// Fetch `url` with curl and return the response body as a String.
/// Appends the HTTP status code as the final line (via `-w "\n%{http_code}"`)
/// so we can log it without a separate request.
/// Fetch `url` via a FlareSolverr instance, which runs headless Chrome to solve
/// Cloudflare JS challenges automatically.  Returns the response body.
/// Cloudflare clearance session obtained from FlareSolverr.
/// Carries the cookies and user-agent needed to make subsequent requests
/// to the same Cloudflare zone without triggering the JS challenge again.
/// Metadata extracted from a booru dapi post response.
/// Each tag is stored as `(name, tag_type)` where tag_type is one of the
/// normalized values: "artist", "copyright", "metadata", or "default".
#[derive(Default)]
struct PostMetadata {
    tags: Vec<(String, String)>,
    post_date: Option<String>,
    uploader: Option<String>,
    rating: Option<String>,
    score: Option<i64>,
    source_url: Option<String>,
}

/// Reusable Cloudflare clearance session obtained via FlareSolverr.
/// Clone-able so it can be stored in the session cache and also returned to callers.
#[derive(Clone)]
struct CfSession {
    /// `name=value; name2=value2` string ready for curl's `--cookie` flag
    cookies: String,
    user_agent: String,
}

/// Domain-keyed cache of CF clearance sessions, shared across the processor loop.
type SessionCache = Arc<Mutex<HashMap<String, CfSession>>>;

/// Fetch `url` via FlareSolverr and return the response body plus the
/// Cloudflare session (cookies + user-agent) so downstream downloads can
/// reuse the clearance without solving the challenge again.
async fn flaresolverr_get(flaresolverr_base: &str, url: &str) -> Result<(String, CfSession), String> {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "cmd": "request.get",
        "url": url,
        "maxTimeout": 60000
    });

    let resp = client
        .post(format!("{}/v1", flaresolverr_base))
        .json(&payload)
        .timeout(Duration::from_secs(90))
        .send()
        .await
        .map_err(|e| format!("FlareSolverr request: {}", e))?;

    let data: serde_json::Value = resp.json().await
        .map_err(|e| format!("FlareSolverr parse: {}", e))?;

    if data["status"].as_str() != Some("ok") {
        return Err(format!("FlareSolverr: {}", data["message"].as_str().unwrap_or("unknown error")));
    }

    let body = data["solution"]["response"]
        .as_str()
        .ok_or_else(|| "FlareSolverr: no response body in solution".to_string())?;

    // FlareSolverr wraps plain API responses in an HTML shell when Chrome renders them.
    // Extract the content of the <pre> tag if present, otherwise return as-is.
    let body = if let (Some(start), Some(end)) = (body.find("<pre>"), body.rfind("</pre>")) {
        body[start + 5..end].to_string()
    } else {
        body.to_string()
    };

    // Collect cookies for reuse in subsequent requests to the same CF zone
    let cookies = data["solution"]["cookies"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    let name = c["name"].as_str()?;
                    let value = c["value"].as_str()?;
                    Some(format!("{}={}", name, value))
                })
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default();

    let user_agent = data["solution"]["userAgent"]
        .as_str()
        .unwrap_or("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        .to_string();

    Ok((body, CfSession { cookies, user_agent }))
}

async fn curl_get(url: &str, referer: &str, cf_session: Option<&CfSession>) -> Result<String, String> {
    let default_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
    let user_agent = cf_session.map(|s| s.user_agent.as_str()).unwrap_or(default_ua);

    let mut cmd = tokio::process::Command::new("curl");
    cmd.args([
        "--silent",
        "--location",
        "--http2",
        "--max-time", "30",
        "--user-agent", user_agent,
        "--header", "Accept: application/json, text/html, */*",
        "--header", "Accept-Language: en-US,en;q=0.9",
        "--header", &format!("Referer: {}", referer),
        "--write-out", "\n%{http_code}",
    ]);
    if let Some(sess) = cf_session {
        if !sess.cookies.is_empty() {
            cmd.args(["--cookie", &sess.cookies]);
        }
    }
    cmd.arg(url);
    let output = cmd
        .output()
        .await
        .map_err(|e| format!("curl spawn: {}", e))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("curl process exit {:?}: {}", output.status.code(), stderr.trim()));
    }

    let raw = String::from_utf8(output.stdout).map_err(|e| format!("curl utf8: {}", e))?;

    // Last line is the status code written by --write-out
    let (body, status_line) = raw.rsplit_once('\n').unwrap_or((&raw, ""));
    let status: u16 = status_line.trim().parse().unwrap_or(0);

    if status < 200 || status >= 300 {
        let snippet = &body[..body.len().min(500)];
        return Err(format!("HTTP {} — body: {}", status, snippet));
    }

    Ok(body.to_string())
}

/// Download `url` with curl directly to `dest_path`.
async fn curl_download(url: &str, referer: &str, dest_path: &str, cf_session: Option<&CfSession>) -> Result<String, String> {
    let default_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
    let user_agent = cf_session.map(|s| s.user_agent.as_str()).unwrap_or(default_ua);

    let mut cmd = tokio::process::Command::new("curl");
    cmd.args([
        "--silent",
        "--location",
        "--http2",
        "--max-time", "300",
        "--user-agent", user_agent,
        "--header", "Accept: image/avif,image/webp,image/apng,*/*",
        "--header", "Accept-Language: en-US,en;q=0.9",
        "--header", &format!("Referer: {}", referer),
        "--output", dest_path,
        "--write-out", "%{http_code}",
    ]);

    // Reuse Cloudflare clearance cookies from the dapi request so the CDN
    // (which shares the same CF zone) doesn't challenge us again
    if let Some(sess) = cf_session {
        if !sess.cookies.is_empty() {
            cmd.args(["--cookie", &sess.cookies]);
        }
    }

    cmd.arg(url);

    let output = cmd.output().await.map_err(|e| format!("curl spawn: {}", e))?;

    let status_code: u16 = String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0);

    if status_code < 200 || status_code >= 300 {
        let _ = tokio::fs::remove_file(dest_path).await;
        return Err(format!("HTTP {}", status_code));
    }

    if !output.status.success() {
        let _ = tokio::fs::remove_file(dest_path).await;
        return Err(format!("curl exit {:?}", output.status.code()));
    }

    Ok(dest_path.to_string())
}

/// Extract a single query parameter value from a URL string.
/// Returns true when `url` is a booru tag-list page (e.g. Gelbooru `?page=post&s=list&tags=…`).
fn is_booru_tag_list(url: &str) -> bool {
    let q = url.split_once('?').map_or("", |(_, q)| q);
    q.contains("page=post")
        && q.contains("s=list")
        && extract_query_param(url, "tags").map_or(false, |t| !t.is_empty())
}

/// Minimal percent-encoder for a URL query-string value.
/// Spaces become `+`; all other non-unreserved ASCII bytes are `%XX`-encoded.
fn url_encode_query_value(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
            | b'-' | b'_' | b'.' | b'~' | b'+' => out.push(byte as char),
            b' ' => out.push('+'),
            b => { let _ = write!(out, "%{:02X}", b); }
        }
    }
    out
}

/// Page through a booru tag-list via the dapi (`?page=dapi&s=post&q=index&tags=…`),
/// insert each discovered post as a new link in the queue, then mark the list link processed.
async fn expand_booru_tag_list(
    _client: &reqwest::Client,
    list_url: &str,
    link_id: &str,
    source_id: Option<&str>,
    pool: &SqlitePool,
    flaresolverr_url: &str,
    session_cache: &SessionCache,
) {
    let base = match extract_domain_base(list_url) {
        Some(b) => b,
        None => {
            mark_link_error(pool, link_id, "Could not parse domain from tag list URL").await;
            return;
        }
    };
    let tags = match extract_query_param(list_url, "tags") {
        Some(t) if !t.is_empty() => t,
        _ => {
            mark_link_error(pool, link_id, "Tag list URL has no tags parameter").await;
            return;
        }
    };

    // Look up API credentials from the source record (if one is linked)
    let creds: Option<SourceCredentials> = if let Some(sid) = source_id {
        sqlx::query("SELECT domain, api_key, user_id FROM sources WHERE id = ?")
            .bind(sid)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten()
            .map(|row| SourceCredentials {
                domain: row.get("domain"),
                api_key: row.get("api_key"),
                user_id: row.get("user_id"),
            })
    } else {
        None
    };

    info!("Expanding booru tag list: base='{}' tags='{}'", base, tags);

    let encoded_tags = url_encode_query_value(&tags);
    const LIMIT: u64 = 100;
    const MAX_PAGES: u64 = 500; // safety ceiling (~50 000 posts)
    let mut page: u64 = 0;
    let mut total_queued: usize = 0;
    let mut total_skipped: usize = 0;

    loop {
        if page >= MAX_PAGES {
            tracing::warn!("Reached page limit ({}) expanding tag list '{}'", MAX_PAGES, tags);
            break;
        }

        // Build the dapi URL, including credentials when available
        let dapi_url = match &creds {
            Some(c) if c.api_key.is_some() && c.user_id.is_some() => format!(
                "{}/index.php?page=dapi&s=post&q=index&tags={}&limit={}&pid={}&json=1&api_key={}&user_id={}",
                base, encoded_tags, LIMIT, page,
                c.api_key.as_deref().unwrap_or(""),
                c.user_id.as_deref().unwrap_or("")
            ),
            Some(c) if c.api_key.is_some() => format!(
                "{}/index.php?page=dapi&s=post&q=index&tags={}&limit={}&pid={}&json=1&api_key={}",
                base, encoded_tags, LIMIT, page,
                c.api_key.as_deref().unwrap_or("")
            ),
            _ => format!(
                "{}/index.php?page=dapi&s=post&q=index&tags={}&limit={}&pid={}&json=1",
                base, encoded_tags, LIMIT, page
            ),
        };

        info!("Tag list page {}: {}", page, dapi_url);

        let body = match fetch_page_with_session(&dapi_url, &base, flaresolverr_url, session_cache).await {
            Some((b, _)) => b,
            None => {
                tracing::warn!("Failed to fetch dapi page {} for tags='{}'", page, tags);
                break;
            }
        };

        let data: serde_json::Value = match serde_json::from_str(&body) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("Failed to parse dapi JSON (page {}): {}", page, e);
                break;
            }
        };

        let posts = match data.get("post") {
            Some(serde_json::Value::Array(arr)) => arr.clone(),
            _ => vec![],
        };

        if posts.is_empty() {
            info!("No posts on page {} for tags='{}' — done", page, tags);
            break;
        }

        let batch_len = posts.len();

        for post in &posts {
            // `id` may be a JSON integer or a string depending on site version
            let post_id = post["id"].as_u64()
                .or_else(|| post["id"].as_str().and_then(|s| s.parse().ok()));
            let post_id = match post_id {
                Some(id) => id,
                None => continue,
            };

            let post_url = clean_url(&format!("{}/index.php?page=post&s=view&id={}", base, post_id));

            let exists: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM links WHERE url = ?")
                .bind(&post_url)
                .fetch_one(pool)
                .await
                .unwrap_or(0);

            if exists > 0 {
                total_skipped += 1;
                continue;
            }

            let new_id = Uuid::new_v4().to_string();
            let created_at = chrono::Utc::now().to_rfc3339();
            match sqlx::query(
                "INSERT INTO links (id, url, created_at, source_id) VALUES (?, ?, ?, ?)"
            )
            .bind(&new_id)
            .bind(&post_url)
            .bind(&created_at)
            .bind(source_id)
            .execute(pool)
            .await
            {
                Ok(_) => total_queued += 1,
                Err(e) => tracing::warn!("Failed to queue post {}: {}", post_url, e),
            }
        }

        info!(
            "Page {}: batch={} queued_so_far={} skipped_so_far={}",
            page, batch_len, total_queued, total_skipped
        );

        if (batch_len as u64) < LIMIT {
            break; // partial page → last page
        }

        page += 1;

        // Polite delay between pages
        let delay = rand::random::<f64>() * 2.0 + 1.0;
        tokio::time::sleep(Duration::from_secs_f64(delay)).await;
    }

    info!(
        "Tag list '{}' expanded: {} posts queued, {} already existed",
        tags, total_queued, total_skipped
    );
    mark_link_processed(pool, link_id).await;
}

fn extract_query_param<'a>(url: &'a str, key: &str) -> Option<String> {
    let query = url.split_once('?')?.1;
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Return `https://domain` from a full URL.
fn extract_domain_base(url: &str) -> Option<String> {
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    let domain = without_scheme.split('/').next()?.split('?').next()?;
    let scheme = if url.starts_with("https") { "https" } else { "http" };
    Some(format!("{}://{}", scheme, domain))
}
