use axum::{
    routing::{get, post},
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
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Serialize, Deserialize, Clone)]
struct Link {
    id: String,
    url: String,
    created_at: String,
    processed_at: Option<String>,
    subscription_id: Option<String>,
}

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
    info!("====================");

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
    tokio::spawn(async move {
        processor_loop(processor_pool, thread_pool_size, processor_state).await;
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
        .route("/subscriptions", get(handler_subscriptions_list).post(handler_create_subscription))
        .route("/links/stats", get(handler_links_stats))
        .route("/backup", post(handler_backup))
        .route("/debug/reset", post(handler_debug_reset))
        .with_state((db_pool, processing_enabled));

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

    info!("Database is at version {}", 4);

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

async fn handler_root() -> Json<serde_json::Value> {
    info!("GET /");
    Json(json!({
        "service": "curator",
        "version": "0.1.0",
        "status": "running"
    }))
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

async fn handler_ingest(
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
    Json(payload): Json<IngestRequest>,
) -> Json<IngestResponse> {
    info!("POST /ingest - Processing {} links", payload.links.len());

    let mut ingested_count: usize = 0;
    let mut ids: Vec<String> = Vec::new();
    let mut duplicate_count: usize = 0;
    let mut duplicates: Vec<String> = Vec::new();

    for url in payload.links {
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

                match sqlx::query(
                    "INSERT INTO links (id, url, created_at, subscription_id) VALUES (?, ?, ?, ?)"
                )
                .bind(&id)
                .bind(&url)
                .bind(&created_at)
                .bind(&payload.subscription_id)
                .execute(&pool)
                .await
                {
                    Ok(_) => {
                        ingested_count += 1;
                        ids.push(id);
                        if let Some(ref sub_id) = payload.subscription_id {
                            info!("Ingested link: {} (subscription: {})", url, sub_id);
                        } else {
                            info!("Ingested link: {}", url);
                        }
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
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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
    axum::extract::State((_, processing_enabled)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
) -> Json<ProcessingStatusResponse> {
    let enabled = *processing_enabled.lock().await;
    info!("GET /processing/status - enabled={}", enabled);
    Json(ProcessingStatusResponse {
        processing_enabled: enabled,
    })
}

async fn handler_toggle_processing(
    axum::extract::State((_, processing_enabled)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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

async fn handler_links_stats(
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
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

async fn handler_backup() -> Json<serde_json::Value> {
    info!("POST /backup - Creating manual backup");

    let config_content = match std::fs::read_to_string("config.toml") {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Failed to read config: {}", e) })),
    };
    let config: Config = match toml::from_str(&config_content) {
        Ok(c) => c,
        Err(e) => return Json(json!({ "error": format!("Failed to parse config: {}", e) })),
    };

    let max_backups = config.database.max_backups.unwrap_or(5);
    match create_backup(&config.database.name, "manual", max_backups).await {
        Ok(path) => {
            info!("Manual backup created: {}", path);
            Json(json!({ "success": true, "path": path }))
        }
        Err(e) => Json(json!({ "error": format!("Backup failed: {}", e) })),
    }
}

async fn handler_debug_reset(
    axum::extract::State((pool, _)): axum::extract::State<(SqlitePool, Arc<Mutex<bool>>)>,
) -> Json<serde_json::Value> {
    tracing::warn!("POST /debug/reset - Deleting all database content");

    let links_deleted: u64 = sqlx::query("DELETE FROM links")
        .execute(&pool)
        .await
        .map(|r| r.rows_affected())
        .unwrap_or(0);

    let subscriptions_deleted: u64 = sqlx::query("DELETE FROM subscriptions")
        .execute(&pool)
        .await
        .map(|r| r.rows_affected())
        .unwrap_or(0);

    tracing::warn!(
        "Debug reset complete: {} links and {} subscriptions deleted",
        links_deleted,
        subscriptions_deleted
    );

    Json(json!({
        "links_deleted": links_deleted,
        "subscriptions_deleted": subscriptions_deleted,
    }))
}

async fn processor_loop(pool: SqlitePool, thread_pool_size: usize, processing_enabled: Arc<Mutex<bool>>) {
    let http_client = reqwest::Client::new();
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
        match sqlx::query("SELECT id, url FROM links WHERE processed_at IS NULL LIMIT ?")
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
                    let client = http_client.clone();
                    let pool_clone = pool.clone();
                    let id_clone = id.clone();

                    let task = tokio::spawn(async move {
                        process_link(&client, &url, &id_clone, &pool_clone).await;
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
    link_id: &str,
    pool: &SqlitePool,
) {
    // Random delay between 1.0 - 5.0 seconds
    let delay = rand::random::<f64>() * 4.0 + 1.0;
    let delay_duration = Duration::from_secs_f64(delay);

    info!("Starting to process link: {} (delay: {:.2}s)", url, delay);

    // Ping the site
    match client.get(url).timeout(Duration::from_secs(10)).send().await {
        Ok(response) => {
            info!("Pinged {} - Status: {}", url, response.status());
        }
        Err(e) => {
            tracing::warn!("Failed to ping {}: {}", url, e);
        }
    }

    // Wait for the specified duration
    tokio::time::sleep(delay_duration).await;

    info!("Completed processing link: {}", url);

    // Update the processed_at timestamp
    let processed_at = chrono::Utc::now().to_rfc3339();
    match sqlx::query("UPDATE links SET processed_at = ? WHERE id = ?")
        .bind(&processed_at)
        .bind(link_id)
        .execute(pool)
        .await
    {
        Ok(_) => info!("Marked link {} as processed", link_id),
        Err(e) => tracing::warn!("Failed to mark link {} as processed: {}", link_id, e),
    }

    // Optionally: remove the link from the database after processing
    // match sqlx::query("DELETE FROM links WHERE id = ?")
    //     .bind(link_id)
    //     .execute(pool)
    //     .await
    // {
    //     Ok(_) => info!("Removed processed link: {}", link_id),
    //     Err(e) => tracing::warn!("Failed to remove link {}: {}", link_id, e),
    // }
}
