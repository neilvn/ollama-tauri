use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use hyper::{Body, Client, Request, Response, Server, Uri};
use hyper::service::{make_service_fn, service_fn};
use futures::TryStreamExt;
use rusqlite::{params, Connection, Result as SqliteResult};
use tokio::sync::Mutex;
use uuid::Uuid;
use serde_json::Value;
use std::sync::atomic::{AtomicI32, Ordering};
use std::io;
use std::path::PathBuf;
use log::{info, error, warn};

struct DbHandler {
    conn: Mutex<Connection>,
}

impl DbHandler {
    // Modified new to take a path potentially
    fn new(db_path: &std::path::Path) -> SqliteResult<Self> {
        info!("Initializing database at: {}", db_path.display());
        let conn = Connection::open(db_path)?;

        match conn.pragma_update(None, "journal_mode", "WAL") {
            Ok(_) => (),
            Err(e) => {
                error!("Failed to enable WAL mode: {}", e);
            }
        }

        conn.execute(
            "CREATE TABLE IF NOT EXISTS responses (
                response_id TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_complete BOOLEAN DEFAULT 0,
                completed_at TIMESTAMP
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS response_chunks (
                chunk_id INTEGER PRIMARY KEY AUTOINCREMENT,
                response_id TEXT,
                sequence_num INTEGER,
                content TEXT,
                created_at TEXT,
                FOREIGN KEY (response_id) REFERENCES responses(response_id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_chunks_response_seq
             ON response_chunks(response_id, sequence_num)",
            [],
        )?;

        Ok(DbHandler { conn: Mutex::new(conn) })
    }

     // Add error logging to initialize_response
    async fn initialize_response(&self, response_id: &str) -> SqliteResult<()> {
        match self.conn.lock().await.execute(
            "INSERT INTO responses (response_id, is_complete) VALUES (?, 0)
             ON CONFLICT(response_id) DO NOTHING",
            params![response_id],
        ) {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("DB Error initializing response {}: {}", response_id, e);
                Err(e)
            }
        }
    }

    // Add error logging to save_response_chunk
    async fn save_response_chunk(
        &self,
        response_id: &str,
        chunk_data: &str,
        sequence_num: i32,
    ) -> SqliteResult<()> {
        let mut conn_guard = self.conn.lock().await;

        // Using a transaction for consistency
        let tx = conn_guard.transaction()?;

        let parsed: Value = match serde_json::from_str(chunk_data) {
             Ok(p) => p,
             Err(e) => {
                 warn!("Failed to parse chunk JSON for response {}: {}", response_id, e);
                 Value::default() // Or handle more gracefully
             }
        };
        let is_done = parsed.get("done").and_then(|v| v.as_bool()).unwrap_or(false);
        let created_at = parsed.get("created_at").and_then(|v| v.as_str()).unwrap_or("");
        let content = parsed.get("response").and_then(|v| v.as_str()).unwrap_or("");

        // Insert chunk
        if let Err(e) = tx.execute(
            "INSERT INTO response_chunks (response_id, sequence_num, content, created_at)
             VALUES (?, ?, ?, ?)",
            params![response_id, sequence_num, content, created_at],
        ) {
             error!("DB Error saving chunk {} for response {}: {}", sequence_num, response_id, e);
             return Err(e);
        }

        // Update response if done
        if is_done {
             if let Err(e) = tx.execute(
                "UPDATE responses SET is_complete = 1, completed_at = CURRENT_TIMESTAMP
                 WHERE response_id = ?",
                params![response_id],
            ) {
                 error!("DB Error marking response {} as complete: {}", response_id, e);
                 return Err(e);
            }
        }

        tx.commit()
    }
}

async fn proxy_request(
    client: Arc<Client<hyper::client::HttpConnector>>,
    mut req: Request<Body>,
    target: &str,
    db: Arc<DbHandler>,
) -> Result<Response<Body>, hyper::Error> {
     // Build the forwarding uri
    let path_and_query = req.uri().path_and_query()
        .map(|x| x.as_str())
        .unwrap_or("");
    let uri_string = format!("{}{}", target, path_and_query);
    let printable_uri = uri_string.clone();

    // Update the request uri
    *req.uri_mut() = Uri::try_from(uri_string).map_err(|e| {
        error!("Failed to create target URI '{}': {}", printable_uri, e);
        io::Error::new(io::ErrorKind::InvalidInput, e) // Convert to io::Error
    }).unwrap();
    req.headers_mut().remove("host");

    if !req.headers().contains_key("content-type") && req.method() == hyper::Method::POST {
        req.headers_mut().insert(
            "content-type",
            hyper::header::HeaderValue::from_static("application/json"),
        );
    }

    info!("Forwarding request to {}", printable_uri);

    let response_id = Uuid::new_v4().to_string();

    // Initialize response record
    let db_clone_init = Arc::clone(&db);
    let response_id_clone_init = response_id.clone();
    if let Err(_e) = db_clone_init.initialize_response(&response_id_clone_init).await {
        // Error already logged in initialize_response
         let error_response = Response::builder()
             .status(500)
             .body::<Body>(Body::from("Database error initializing response")) // Keep body generic
             .unwrap();
         // Need explicit type args if compiler still complains here
         return Ok::<_, hyper::Error>(error_response); // Explicit type args for Ok
    }
    info!("Initialized response record: {}", response_id);

    match client.request(req).await {
        Ok(response) => {
            info!("Received response from target for {}", response_id);
            let (parts, body) = response.into_parts();
            let db_clone_proc = Arc::clone(&db);
            let response_id_clone_proc = response_id.clone();
            // Create a per-stream sequence counter (Original Arc)
            let sequence_counter = Arc::new(AtomicI32::new(0));

            let processed_body = body.map_ok(move |chunk| { // FnMut closure
                // Clone variables needed in the spawned task
                let db_task = Arc::clone(&db_clone_proc);
                let response_id_task = response_id_clone_proc.clone();
                let chunk_data = chunk.clone(); // Clone chunk data itself for the task

                // --- Clone the Arc pointer before the async move block ---
                let sequence_counter_clone = Arc::clone(&sequence_counter);

                // Spawn the database saving task
                tokio::spawn(async move { // Changed from tauri::async_runtime::spawn to tokio::spawn
                    let chunk_str = String::from_utf8_lossy(&chunk_data).to_string();

                    // --- Use the cloned Arc inside the task ---
                    let seq_num = sequence_counter_clone.fetch_add(1, Ordering::SeqCst);

                    // Log/handle error within save_response_chunk or here
                    if let Err(e) = db_task.save_response_chunk(&response_id_task, &chunk_str, seq_num).await {
                         error!("Async DB save task failed for chunk {} response {}: {}", seq_num, response_id_task, e);
                    }
                });
                // Return the original chunk for the stream being sent to the client
                chunk
            });

            Ok(Response::from_parts(parts, Body::wrap_stream(processed_body)))
        },
        Err(e) => {
            error!("Error forwarding request for {}: {}", response_id, e);
            Err(e) // Propagate the hyper::Error
        }
    }
}

// --- This is the function Tauri will spawn ---
pub async fn run_proxy_server(db_path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    // Configure a multi-threaded runtime
    
    // --- Ensure parent directory exists ---
    if let Some(parent_dir) = db_path.parent() {
        if !parent_dir.exists() {
            log::info!("Creating database directory: {}", parent_dir.display());
            std::fs::create_dir_all(parent_dir)?;
        } else {
            log::info!("Database directory already exists");
        }
    }

    // Initialize DB handler using the provided path reference
    let db_handler = match DbHandler::new(&db_path) {
       Ok(handler) => Arc::new(handler),
       Err(e) => {
           error!("Failed to initialize database at {}: {}", db_path.display(), e);
           return Err(Box::new(e) as Box<dyn std::error::Error>);
       }
    };

    // Define the proxy server address
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080)); // Make port configurable?
    let target_uri = "http://localhost:11434".to_string(); // Make target configurable?
    info!("Proxy server binding to http://{}, forwarding to {}", addr, target_uri);

    // Create hyper client with a connection pool
    let _https = hyper::client::HttpConnector::new();
    // Configure the connection pool size for concurrent requests
    let mut http = hyper::client::HttpConnector::new();
    http.set_nodelay(true);
    http.set_keepalive(Some(std::time::Duration::from_secs(30)));
    
    // Create the client with the configured connector
    let client = Client::builder()
        .pool_idle_timeout(std::time::Duration::from_secs(30))
        .pool_max_idle_per_host(32) // Increase max idle connections per host
        .build(http);
    
    let client = Arc::new(client);
    let target_uri_clone = target_uri.clone();

    // Define service
    let make_svc = make_service_fn(move |_conn| {
        let client = Arc::clone(&client);
        let target = target_uri_clone.clone();
        let db = Arc::clone(&db_handler);

        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let client = client.clone();
                let target = target.clone();
                let db = Arc::clone(&db);

                // Each request will be processed in its own spawned task
                async move {
                    // Spawn the request handling as a dedicated task to enable concurrent processing
                    let fut = proxy_request(client, req, &target, db);
                    
                    match fut.await {
                        Ok(response) => Ok(response),
                        Err(e) => {
                            error!("Request processing error: {}", e);
                            // Create an error response if proxy_request itself fails
                            let error_response = Response::builder()
                                .status(500)
                                .body::<Body>(Body::from(format!("Proxy error: {}", e)))
                                .unwrap();

                            Ok::<Response<Body>, Infallible>(error_response)
                        }
                    }
                }
            }))
        }
    });

    // Create and start server with a configured threadpool
    Server::bind(&addr)
        .http1_pipeline_flush(true)
        .http1_keepalive(true)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .tcp_nodelay(true)
        .serve(make_svc)
        .await?;

    Ok(())
}
