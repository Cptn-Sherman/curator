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

#[derive(Serialize, Deserialize, Clone)]
struct Link {
    id: String,
    url: String,
    created_at: String,
}

#[derive(Deserialize)]
struct IngestRequest {
    links: Vec<String>,
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

    // Initialize database
    let db_pool = init_database()
        .await
        .expect("Failed to initialize database");

    info!("Database initialized successfully");

    // Get thread pool size from environment variable
    let thread_pool_size: usize = std::env::var("THREAD_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    info!("Thread pool size: {}", thread_pool_size);

    // Spawn the processor task
    let processor_pool = db_pool.clone();
    tokio::spawn(async move {
        processor_loop(processor_pool, thread_pool_size).await;
    });

    // Build router
    let app = Router::new()
        .route("/", get(handler_root))
        .route("/health", get(handler_health))
        .route("/ingest", post(handler_ingest))
        .route("/check", get(handler_check))
        .with_state(db_pool);

    // Run server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .expect("Failed to bind to port 8080");

    info!("Server listening on http://0.0.0.0:8080");

    axum::serve(listener, app)
        .await
        .expect("Server error");
}

async fn init_database() -> Result<SqlitePool, sqlx::Error> {
    let database_url = "sqlite:curator.db";

    // Create connection string with auto-create enabled
    let connect_options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(connect_options)
        .await?;

    // Run migrations
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS links (
            id TEXT PRIMARY KEY,
            url TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;

    info!("Database tables created/verified");

    Ok(pool)
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

async fn handler_ingest(
    axum::extract::State(pool): axum::extract::State<SqlitePool>,
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
                    "INSERT INTO links (id, url, created_at) VALUES (?, ?, ?)"
                )
                .bind(&id)
                .bind(&url)
                .bind(&created_at)
                .execute(&pool)
                .await
                {
                    Ok(_) => {
                        ingested_count += 1;
                        ids.push(id);
                        info!("Ingested link: {}", url);
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
    axum::extract::State(pool): axum::extract::State<SqlitePool>,
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

async fn processor_loop(pool: SqlitePool, thread_pool_size: usize) {
    let http_client = reqwest::Client::new();
    let mut tasks = Vec::new();

    loop {
        // Fetch links from database
        match sqlx::query("SELECT id, url FROM links LIMIT ?")
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
    _link_id: &str,
    _pool: &SqlitePool,
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

    // Optionally: remove the link from the database after processing
    // match sqlx::query("DELETE FROM links WHERE id = ?")
    //     .bind(_link_id)
    //     .execute(_pool)
    //     .await
    // {
    //     Ok(_) => info!("Removed processed link: {}", _link_id),
    //     Err(e) => tracing::warn!("Failed to remove link {}: {}", _link_id, e),
    // }
}
