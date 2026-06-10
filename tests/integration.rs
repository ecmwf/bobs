use axum::Router;
use base64::Engine;
use bobs::cleanup::start_cleanup_task;
use bobs::config::Config;
use bobs::http::{router, AppState};
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use bobs::io::UringFileIO;
use bobs::io::{DefaultFileIO, TokioFileIO};
use bobs::manager::SpoolManager;
#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
use bobs::metadata::UringSidecarMetadataStore;
use bobs::metadata::{DefaultMetadataStore, SyncSidecarMetadataStore};
use serde_json::{json, Value};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    base_url: String,
    manager: Arc<SpoolManager<DefaultFileIO, DefaultMetadataStore>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
    cleanup_handle: Option<JoinHandle<()>>,
    _tmp: Option<TempDir>,
}

impl TestServer {
    async fn stop(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            let mut handle = handle;
            tokio::select! {
                join_result = &mut handle => {
                    let _ = join_result;
                }
                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                    // Graceful shutdown can wait for idle keep-alive clients;
                    // aborting still simulates a process stop for restart tests.
                    handle.abort();
                    let _ = handle.await;
                }
            }
        }
        if let Some(handle) = self.cleanup_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
    }
}

#[test]
fn test_helpers_can_name_explicit_backend_managers() {
    fn accepts_manager<F, M>()
    where
        F: bobs::io::FileIO,
        M: bobs::metadata::MetadataStore + Clone + Send + Sync + 'static,
    {
    }

    accepts_manager::<TokioFileIO, SyncSidecarMetadataStore>();
    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    accepts_manager::<UringFileIO, UringSidecarMetadataStore>();
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
        if let Some(h) = self.cleanup_handle.take() {
            h.abort();
        }
    }
}

async fn start_server() -> TestServer {
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"), // overridden
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });
    start_server_with_config(config).await
}

/// Start a real TCP HTTP server together with a running cleanup task.
///
/// The `data_dir` in `config` is ignored; a fresh `TempDir` is always used to
/// give each test its own isolated storage.  The cleanup loop is started
/// immediately so that TTL-sensitive tests only need to `sleep` for the
/// expected TTL duration.
async fn start_server_with_config(config: Arc<Config>) -> TestServer {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let mut server = start_server_with_storage_root(Arc::clone(&config), tmp.path()).await;
    server._tmp = Some(tmp);
    server
}

/// Start a real TCP HTTP server using caller-owned storage.
///
/// This helper is used by restart tests: the caller owns `storage_root`, so the
/// first server can be stopped and a second server can recover from the same
/// sidecar metadata and data directory.
async fn start_server_with_storage_root(config: Arc<Config>, storage_root: &Path) -> TestServer {
    let data_dir = storage_root.join("data");

    let config = Arc::new(Config {
        data_dir: data_dir.clone(),
        ..(*config).clone()
    });

    let manager = Arc::new(
        SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
            DefaultMetadataStore::new(&data_dir),
            &data_dir,
            config.page_size,
            config.max_cache_bytes,
        )
        .expect("init manager"),
    );
    manager.recover().await.expect("recover");

    let state = Arc::new(AppState {
        manager: Arc::clone(&manager),
        config: Arc::clone(&config),
        hostname: "bobs-0".into(),
        ordinal: "0".into(),
        internal_base_url: "http://bobs-0:3000/api/v1".into(),
    });
    let app: Router =
        router::<DefaultFileIO, DefaultMetadataStore>().with_state(Arc::clone(&state));

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    let (tx, rx) = oneshot::channel::<()>();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await;
    });

    let cleanup_handle = Some(start_cleanup_task(
        Arc::clone(&manager),
        Arc::clone(&config),
    ));

    TestServer {
        base_url: format!("http://{}", addr),
        manager,
        shutdown_tx: Some(tx),
        handle: Some(handle),
        cleanup_handle,
        _tmp: None,
    }
}

async fn create_key(client: &reqwest::Client, base_url: &str, body: Option<Value>) -> String {
    let req = client.put(format!("{base_url}/api/v1/create"));
    let resp = if let Some(payload) = body {
        req.json(&payload).send().await.expect("create send")
    } else {
        req.send().await.expect("create send")
    };
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: Value = resp.json().await.expect("create json");
    v["key"].as_str().expect("key str").to_string()
}

async fn assert_http_restart_continues_from_acknowledged_offset(
    first_write: Vec<u8>,
    second_write: Vec<u8>,
) {
    let storage_root = tempfile::tempdir().expect("create caller-owned storage root");
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });

    let client = reqwest::Client::new();
    let first_server =
        start_server_with_storage_root(Arc::clone(&config), storage_root.path()).await;
    let key = create_key(&client, &first_server.base_url, None).await;

    let write_resp = client
        .post(format!("{}/api/v1/write/{}/0", first_server.base_url, key))
        .body(first_write.clone())
        .send()
        .await
        .expect("first write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);
    drop(write_resp);

    // Stop without calling /complete. A 200 OK from /write is the producer's
    // acknowledgement; after restart it must be safe to continue at that offset.
    first_server.stop().await;

    let second_server =
        start_server_with_storage_root(Arc::clone(&config), storage_root.path()).await;
    let acknowledged_offset = first_write.len() as u64;
    let continue_resp = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/write/{}/{}",
            second_server.base_url, key, acknowledged_offset
        ))
        .body(second_write.clone())
        .send()
        .await
        .expect("continued write send");
    assert_eq!(continue_resp.status(), reqwest::StatusCode::OK);

    let expected_len = first_write.len() + second_write.len();
    let complete_resp = reqwest::Client::new()
        .post(format!(
            "{}/api/v1/complete/{}",
            second_server.base_url, key
        ))
        .json(&json!({ "expected_size": expected_len }))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_resp = reqwest::Client::new()
        .get(format!("{}/api/v1/read/{}", second_server.base_url, key))
        .header("Range", format!("bytes=0-{}", expected_len - 1))
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let mut expected = first_write;
    expected.extend_from_slice(&second_write);
    let actual = read_resp.bytes().await.expect("read bytes");
    assert_eq!(actual.as_ref(), expected.as_slice());
}

#[tokio::test]
async fn test_http_restart_after_acknowledged_full_pages_write_allows_continue() {
    let first_write: Vec<u8> = (0..(4096 * 2)).map(|i| (i % 251) as u8).collect();
    let second_write: Vec<u8> = (0..4096).map(|i| (255 - (i % 251)) as u8).collect();

    assert_http_restart_continues_from_acknowledged_offset(first_write, second_write).await;
}

#[tokio::test]
async fn test_http_restart_after_acknowledged_trailing_partial_page_write_allows_continue() {
    let first_write: Vec<u8> = (0..(4096 + 123)).map(|i| (i % 251) as u8).collect();
    let second_write: Vec<u8> = (0..5000).map(|i| (255 - (i % 251)) as u8).collect();

    assert_http_restart_continues_from_acknowledged_offset(first_write, second_write).await;
}

#[tokio::test]
async fn test_http_restart_persists_write_locked_and_readable_metadata_sidecar() {
    let storage_root = tempfile::tempdir().expect("create caller-owned storage root");
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });
    let client = reqwest::Client::new();

    let first_server =
        start_server_with_storage_root(Arc::clone(&config), storage_root.path()).await;
    let key = create_key(
        &client,
        &first_server.base_url,
        Some(json!({
            "write_locked": true,
            "content_type": "application/x-bobs-test",
            "content_encoding": "gzip"
        })),
    )
    .await;
    let meta_path = storage_root
        .path()
        .join("data")
        .join(&key)
        .join("meta.json");
    assert!(meta_path.exists(), "create must persist sidecar metadata");

    let locked_read = client
        .get(format!("{}/api/v1/read/{}", first_server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("locked read send");
    assert_eq!(locked_read.status(), reqwest::StatusCode::LOCKED);
    first_server.stop().await;

    let second_server =
        start_server_with_storage_root(Arc::clone(&config), storage_root.path()).await;
    let locked_read_after_restart = client
        .get(format!("{}/api/v1/read/{}", second_server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("locked read after restart send");
    assert_eq!(
        locked_read_after_restart.status(),
        reqwest::StatusCode::LOCKED
    );

    let data = vec![0x5Au8; 5000];
    let write_resp = client
        .post(format!("{}/api/v1/write/{}/0", second_server.base_url, key))
        .body(data.clone())
        .send()
        .await
        .expect("write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);
    let complete_resp = client
        .post(format!(
            "{}/api/v1/complete/{}",
            second_server.base_url, key
        ))
        .json(&json!({ "expected_size": data.len() }))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);
    second_server.stop().await;

    let third_server =
        start_server_with_storage_root(Arc::clone(&config), storage_root.path()).await;
    let read_resp = client
        .get(format!("{}/api/v1/read/{}", third_server.base_url, key))
        .header("Range", format!("bytes=0-{}", data.len() - 1))
        .send()
        .await
        .expect("read after complete restart send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        read_resp
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok()),
        Some("application/x-bobs-test")
    );
    assert_eq!(
        read_resp
            .headers()
            .get("content-encoding")
            .and_then(|h| h.to_str().ok()),
        Some("gzip")
    );
    assert_eq!(
        read_resp.bytes().await.expect("read bytes").as_ref(),
        data.as_slice()
    );
}

#[tokio::test]
async fn test_http_delete_removes_sidecar_metadata_file() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;
    let spool_dir = server.manager.data_dir.join(&key);
    let meta_path = spool_dir.join("meta.json");
    assert!(meta_path.exists(), "create should write sidecar metadata");

    let resp = client
        .delete(format!("{}/api/v1/delete/{}", server.base_url, key))
        .send()
        .await
        .expect("delete send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert!(!meta_path.exists(), "delete should remove sidecar metadata");
    assert!(!spool_dir.exists(), "delete should remove spool directory");
}

#[tokio::test]
async fn test_basic_lifecycle() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let key = create_key(&client, &server.base_url, None).await;
    let bytes: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 251) as u8).collect();

    let write_resp = client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(bytes.clone())
        .send()
        .await
        .expect("write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);

    let complete_resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_resp = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", format!("bytes=0-{}", bytes.len() - 1))
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let read_bytes = read_resp.bytes().await.expect("read bytes");
    assert_eq!(read_bytes.as_ref(), bytes.as_slice());

    let del_resp = client
        .delete(format!("{}/api/v1/delete/{}", server.base_url, key))
        .send()
        .await
        .expect("delete send");
    assert_eq!(del_resp.status(), reqwest::StatusCode::OK);

    let read_after_delete = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("read after delete");
    assert_eq!(read_after_delete.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_follow_mode() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    let read_client = client.clone();
    let read_url = format!("{}/api/v1/read/{}", server.base_url, key);
    let read_task = tokio::spawn(async move {
        let resp = read_client
            .get(read_url)
            .send()
            .await
            .expect("follow read send");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        resp.bytes().await.expect("follow read bytes")
    });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let chunks = vec![
        vec![1u8; 4096],
        vec![2u8; 2048],
        vec![3u8; 1024],
        vec![4u8; 4096],
    ];
    let mut all = Vec::new();
    let mut offset = 0u64;
    for chunk in &chunks {
        all.extend_from_slice(chunk);
        let resp = client
            .post(format!(
                "{}/api/v1/write/{}/{}",
                server.base_url, key, offset
            ))
            .body(chunk.clone())
            .send()
            .await
            .expect("write chunk");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        offset += chunk.len() as u64;
    }

    let complete_resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_bytes = tokio::time::timeout(std::time::Duration::from_secs(5), read_task)
        .await
        .expect("follow read timeout")
        .expect("follow join");
    assert_eq!(read_bytes.as_ref(), all.as_slice());
}

#[tokio::test]
async fn test_write_lock() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(
        &client,
        &server.base_url,
        Some(json!({"write_locked": true, "content_type": "application/octet-stream"})),
    )
    .await;

    let locked_read = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("locked read send");
    assert_eq!(locked_read.status(), reqwest::StatusCode::LOCKED);

    let data = vec![9u8; 5000];
    let write_resp = client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(data.clone())
        .send()
        .await
        .expect("write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);

    let complete_resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_resp = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-4999")
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        read_resp.bytes().await.expect("read bytes").as_ref(),
        data.as_slice()
    );
}

#[tokio::test]
async fn test_error_cases() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let missing_write = client
        .post(format!("{}/api/v1/write/missing/0", server.base_url))
        .body(vec![1u8; 10])
        .send()
        .await
        .expect("missing write");
    assert_eq!(missing_write.status(), reqwest::StatusCode::NOT_FOUND);

    let key = create_key(&client, &server.base_url, None).await;
    let ok_write = client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(vec![1u8; 10])
        .send()
        .await
        .expect("ok write");
    assert_eq!(ok_write.status(), reqwest::StatusCode::OK);

    let bad_offset = client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(vec![2u8; 4])
        .send()
        .await
        .expect("bad offset write");
    assert_eq!(bad_offset.status(), reqwest::StatusCode::BAD_REQUEST);

    let complete_resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let first_reader = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .expect("first reader send");
    assert_eq!(first_reader.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let second_reader = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .expect("second reader send");
    assert_eq!(second_reader.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let write_after_close = client
        .post(format!("{}/api/v1/write/{}/10", server.base_url, key))
        .body(vec![8u8; 1])
        .send()
        .await
        .expect("write after complete");
    assert_eq!(write_after_close.status(), reqwest::StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_checksum_verification() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    let data = vec![0x42u8; 8192];
    let expected_crc = crc32c::crc32c(&data);

    client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(data.clone())
        .send()
        .await
        .expect("write send");

    client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");

    let read_resp = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", format!("bytes=0-{}", data.len() - 1))
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let checksum_header = read_resp
        .headers()
        .get("X-Checksum-CRC32C")
        .expect("checksum header should be present")
        .to_str()
        .expect("valid header string")
        .to_string();

    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&checksum_header)
        .expect("valid base64");
    let actual_crc = u32::from_be_bytes(decoded.try_into().expect("4 bytes"));
    assert_eq!(actual_crc, expected_crc);
}

#[tokio::test]
async fn test_content_encoding_roundtrip() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(
        &client,
        &server.base_url,
        Some(json!({
            "content_type": "application/octet-stream",
            "content_encoding": "gzip"
        })),
    )
    .await;

    let data = vec![0xABu8; 4096];
    client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(data)
        .send()
        .await
        .expect("write send");

    client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");

    let read_resp = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        read_resp
            .headers()
            .get("content-encoding")
            .and_then(|h| h.to_str().ok()),
        Some("gzip")
    );
    assert_eq!(
        read_resp
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok()),
        Some("application/octet-stream")
    );
}

#[tokio::test]
async fn test_complete_with_expected_size_mismatch() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    let data = vec![0x11u8; 1000];
    client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(data)
        .send()
        .await
        .expect("write send");

    let complete_resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .json(&json!({"expected_size": 9999}))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_health_endpoint() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{}/api/v1/health", server.base_url))
        .send()
        .await
        .expect("health send");

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let body: Value = response.json().await.expect("health json");
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn test_invalid_range_returns_bad_request() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(vec![1u8; 32])
        .send()
        .await
        .expect("write send");

    let response = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=abc-5")
        .send()
        .await
        .expect("invalid range send");

    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_open_ended_range_reads_current_eof_without_following() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    let data = vec![0x33u8; 8192];
    client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(data.clone())
        .send()
        .await
        .expect("write send");

    let response = client
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=4096-")
        .send()
        .await
        .expect("read send");

    assert_eq!(response.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok()),
        Some("bytes 4096-8191/*")
    );
    let body = response.bytes().await.expect("read bytes");
    assert_eq!(body.as_ref(), &data[4096..]);
}

#[tokio::test]
async fn test_in_flight_writer_completes_while_parallel_readers_follow_and_read_ranges() {
    let server = start_server().await;
    let client = reqwest::Client::new();
    let key = create_key(&client, &server.base_url, None).await;

    let page_size = 4096usize;
    let total_pages = 8usize;
    let all_data: Vec<u8> = (0..(page_size * total_pages))
        .map(|i| (i % 251) as u8)
        .collect();

    let mut follow_tasks = Vec::new();
    for _ in 0..3 {
        let read_client = client.clone();
        let read_url = format!("{}/api/v1/read/{}", server.base_url, key);
        let expected = all_data.clone();
        follow_tasks.push(tokio::spawn(async move {
            let resp = read_client
                .get(read_url)
                .send()
                .await
                .expect("follow read send");
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
            let body = resp.bytes().await.expect("follow read bytes");
            assert_eq!(body.as_ref(), expected.as_slice());
        }));
    }

    // Let follow readers enter read_page(0) before the writer starts publishing.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for page_idx in 0..2usize {
        let offset = page_idx * page_size;
        let resp = client
            .post(format!(
                "{}/api/v1/write/{}/{}",
                server.base_url, key, offset
            ))
            .body(all_data[offset..offset + page_size].to_vec())
            .send()
            .await
            .expect("initial write send");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
    }

    let mut range_tasks = Vec::new();
    for reader_idx in 0..6usize {
        let read_client = client.clone();
        let read_url = format!("{}/api/v1/read/{}", server.base_url, key);
        let range_start = (reader_idx % 2) * page_size;
        let range_end = range_start + page_size - 1;
        let expected = all_data[range_start..=range_end].to_vec();
        range_tasks.push(tokio::spawn(async move {
            let resp = read_client
                .get(read_url)
                .header("Range", format!("bytes={range_start}-{range_end}"))
                .send()
                .await
                .expect("range read send");
            assert_eq!(resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
            let body = resp.bytes().await.expect("range read bytes");
            assert_eq!(body.as_ref(), expected.as_slice());
        }));
    }

    let write_client = client.clone();
    let write_base_url = server.base_url.clone();
    let write_key = key.clone();
    let write_data = all_data.clone();
    let writer_task = tokio::spawn(async move {
        for page_idx in 2..total_pages {
            let offset = page_idx * page_size;
            let resp = write_client
                .post(format!(
                    "{write_base_url}/api/v1/write/{write_key}/{offset}"
                ))
                .body(write_data[offset..offset + page_size].to_vec())
                .send()
                .await
                .expect("continued write send");
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
        }

        let complete_resp = write_client
            .post(format!("{write_base_url}/api/v1/complete/{write_key}"))
            .json(&json!({ "expected_size": write_data.len() }))
            .send()
            .await
            .expect("complete send");
        assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);
    });

    tokio::time::timeout(std::time::Duration::from_secs(10), writer_task)
        .await
        .expect("writer and complete should not wait behind active readers")
        .expect("writer task join");

    for task in range_tasks {
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("range reader timeout")
            .expect("range reader join");
    }
    for task in follow_tasks {
        tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("follow reader timeout")
            .expect("follow reader join");
    }
}

#[tokio::test]
async fn test_many_active_spools_share_global_cache_cap() {
    let page_size = 1024usize;
    let max_cache_bytes = page_size * 3;
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size,
        max_cache_bytes,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });
    let server = start_server_with_config(config).await;
    let client = reqwest::Client::new();

    let spool_count = 12usize;
    let pages_per_spool = 6usize;
    let mut keys = Vec::new();
    for _ in 0..spool_count {
        keys.push(create_key(&client, &server.base_url, None).await);
    }

    let stop_monitor = Arc::new(AtomicBool::new(false));
    let monitor_done = Arc::clone(&stop_monitor);
    let monitor_manager = Arc::clone(&server.manager);
    let monitor = tokio::spawn(async move {
        while !monitor_done.load(Ordering::Relaxed) {
            let cache = monitor_manager.page_cache.lock().await;
            assert!(
                cache.current_bytes() <= cache.max_bytes(),
                "cache exceeded global cap while writes were active: {} > {}",
                cache.current_bytes(),
                cache.max_bytes()
            );
            drop(cache);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    });

    let barrier = Arc::new(tokio::sync::Barrier::new(spool_count));
    let mut write_tasks = Vec::new();
    for (spool_idx, key) in keys.into_iter().enumerate() {
        let write_client = client.clone();
        let write_base_url = server.base_url.clone();
        let write_barrier = Arc::clone(&barrier);
        write_tasks.push(tokio::spawn(async move {
            let data: Vec<u8> = (0..(page_size * pages_per_spool))
                .map(|i| ((spool_idx + i) % 251) as u8)
                .collect();
            write_barrier.wait().await;
            let resp = write_client
                .post(format!("{write_base_url}/api/v1/write/{key}/0"))
                .body(data)
                .send()
                .await
                .expect("many-spool write send");
            assert_eq!(resp.status(), reqwest::StatusCode::OK);
        }));
    }

    for task in write_tasks {
        tokio::time::timeout(std::time::Duration::from_secs(10), task)
            .await
            .expect("many-spool write timeout")
            .expect("many-spool write join");
    }

    stop_monitor.store(true, Ordering::Relaxed);
    monitor.await.expect("cache monitor join");

    let cache = server.manager.page_cache.lock().await;
    assert_eq!(cache.max_bytes(), max_cache_bytes);
    assert!(
        spool_count * pages_per_spool * page_size > max_cache_bytes,
        "test must write more full pages than the cache cap"
    );
    assert!(
        cache.current_bytes() <= max_cache_bytes,
        "shared page cache must remain within global cap after many active spool writes: {} > {}",
        cache.current_bytes(),
        max_cache_bytes
    );
}

// ---------------------------------------------------------------------------
// Range-coverage cleanup integration tests
//
// These tests use real TCP + reqwest (no tokio::time::pause) and rely on
// wall-clock time.  Short TTLs (1–2 s) with `tokio::time::sleep` provide
// determinism: we wait 3× the TTL to give the cleanup loop several sweep
// cycles.
//
// Each test builds its own Config with the appropriate TTL values and
// passes it to `start_server_with_config`, which starts both the HTTP
// server and the cleanup loop.
//
// NOTE on connection reuse: reqwest's default client pools connections.
// After a write+complete sequence, we call `do_range_read` which creates a
// *fresh* reqwest::Client per call to sidestep any HTTP/1.1 keep-alive
// state left by the write path.  This mirrors what a real-world caller
// (a separate process issuing a GET after a PUT/POST) would do.
// ---------------------------------------------------------------------------

/// Issue a single bounded Range GET and return the HTTP status code.
/// Uses a *fresh* `reqwest::Client` for each call so that connection-pool
/// state from the preceding write+complete sequence never bleeds through.
async fn do_range_read(base_url: &str, key: &str, range: &str) -> reqwest::StatusCode {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base_url}/api/v1/read/{key}"))
        .header("Range", range)
        .send()
        .await
        .expect("range read send");
    let status = resp.status();
    // Drain the body so the server-side stream runs to completion, which
    // ensures last_read_activity_at and full_object_read_at are updated.
    let _ = resp.bytes().await.expect("drain body");
    status
}

/// Write `data` at offset 0 and mark the spool complete, using a fresh
/// `reqwest::Client`.  Returns the spool key.
async fn write_complete_fresh(base_url: &str, data: Vec<u8>) -> String {
    let client = reqwest::Client::new();

    let resp = client
        .put(format!("{base_url}/api/v1/create"))
        .send()
        .await
        .expect("create send");
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: Value = resp.json().await.expect("create json");
    let key = v["key"].as_str().expect("key str").to_string();

    let resp = client
        .post(format!("{base_url}/api/v1/write/{key}/0"))
        .body(data)
        .send()
        .await
        .expect("write send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "write failed");

    let resp = client
        .post(format!("{base_url}/api/v1/complete/{key}"))
        .send()
        .await
        .expect("complete send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "complete failed");

    key
}

/// Config: very short full-read TTL, long idle TTL.
fn config_short_full_read_ttl() -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"), // overridden by start_server_with_config
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        full_read_complete_ttl_secs: 1,
        read_idle_ttl_secs: 60,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 1,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    })
}

/// Config: very short idle TTL, long full-read TTL.
fn config_short_idle_ttl() -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        full_read_complete_ttl_secs: 60,
        read_idle_ttl_secs: 1,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 1,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    })
}

/// Two Range requests covering the full object (0–4095 then 4096–8191) must
/// set `full_object_read_at` and cause the spool to be deleted once
/// `full_read_complete_ttl_secs` (1 s) elapses.
///
/// Each range sub-request uses its own fresh reqwest::Client so connection-pool
/// state from prior write/complete calls cannot interfere.
#[tokio::test]
async fn test_multiple_ranges_covering_full_object_detected() {
    let server = start_server_with_config(config_short_full_read_ttl()).await;

    let data = vec![0xAAu8; 8192];
    let key = write_complete_fresh(&server.base_url, data).await;

    // First half — partial coverage.
    let s1 = do_range_read(&server.base_url, &key, "bytes=0-4095").await;
    assert_eq!(s1, reqwest::StatusCode::PARTIAL_CONTENT, "first half read");

    // Second half — full coverage achieved; full_object_read_at is now set.
    let s2 = do_range_read(&server.base_url, &key, "bytes=4096-8191").await;
    assert_eq!(s2, reqwest::StatusCode::PARTIAL_CONTENT, "second half read");

    // Give the cleanup loop several sweep cycles (sweep_interval = 1 s,
    // full_read_complete_ttl_secs = 1 s → delete fires after ≥ 2 s).
    tokio::time::sleep(std::time::Duration::from_secs(4)).await;

    let after = reqwest::Client::new()
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("post-cleanup read");
    assert_eq!(
        after.status(),
        reqwest::StatusCode::NOT_FOUND,
        "spool must be deleted after full-read TTL expires"
    );
}

/// A single partial Range request (first half only) must NOT trigger the
/// short full-read TTL.  The spool remains accessible after waiting longer
/// than `full_read_complete_ttl_secs` (1 s) because full coverage was never
/// achieved and the idle TTL (60 s) has not expired.
#[tokio::test]
async fn test_partial_range_does_not_trigger_short_ttl() {
    let server = start_server_with_config(config_short_full_read_ttl()).await;

    let data = vec![0xBBu8; 8192];
    let key = write_complete_fresh(&server.base_url, data).await;

    // Read only first half — coverage is incomplete.
    let s = do_range_read(&server.base_url, &key, "bytes=0-4095").await;
    assert_eq!(s, reqwest::StatusCode::PARTIAL_CONTENT, "partial read");

    // Wait longer than full_read_complete_ttl_secs but much less than
    // read_idle_ttl_secs (60 s).
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Spool must still be accessible: partial read did not trigger short TTL,
    // and the idle TTL (60 s) has not expired.
    let after = reqwest::Client::new()
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-4095")
        .send()
        .await
        .expect("post-wait read");
    assert_eq!(
        after.status(),
        reqwest::StatusCode::PARTIAL_CONTENT,
        "partially-read spool must not be deleted by the short full-read TTL"
    );
    let _ = after.bytes().await;
}

/// A spool that is completed but never read must be deleted once
/// `read_idle_ttl_secs` (1 s) elapses from `readable_at` (set at complete
/// time).
#[tokio::test]
async fn test_never_read_spool_cleaned_up_after_read_idle_ttl() {
    let server = start_server_with_config(config_short_idle_ttl()).await;

    let data = vec![0xCCu8; 4096];
    let key = write_complete_fresh(&server.base_url, data).await;

    // No reads at all — idle TTL should fire from readable_at.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let after = reqwest::Client::new()
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("post-idle read");
    assert_eq!(
        after.status(),
        reqwest::StatusCode::NOT_FOUND,
        "never-read spool must be deleted after read_idle_ttl_secs"
    );
}

/// The idle TTL is anchored on `readable_at` (completion time), NOT on
/// `created_at` (first-write time).  A spool whose write phase took longer
/// than the idle TTL must still survive for a full TTL window after
/// completion.
///
/// This guards against the old `unread_ttl_secs` bug where the timer was
/// anchored on `created_at` and could delete a spool before it was ever read
/// if writing was slow.
///
/// Proof: Config has `read_idle_ttl_secs = 2`.  We sleep 3 s during writing
/// (making `created_at` 3 s old at completion time).  Then we complete and
/// immediately wait 1 s (one cleanup sweep).  With correct anchoring on
/// `readable_at`, the spool must still be alive (readable_at is only 1 s old
/// < 2 s TTL).  If cleanup incorrectly used `created_at`, it would have
/// deleted the spool (created_at is 4 s old > 2 s TTL).
#[tokio::test]
async fn test_idle_ttl_not_anchored_on_created_at() {
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        full_read_complete_ttl_secs: 60,
        read_idle_ttl_secs: 2,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 1,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });
    let server = start_server_with_config(config).await;
    let client = reqwest::Client::new();

    let key = create_key(&client, &server.base_url, None).await;

    // Write first chunk — establishes created_at.
    let resp = client
        .post(format!("{}/api/v1/write/{}/0", server.base_url, key))
        .body(vec![0xDDu8; 4096])
        .send()
        .await
        .expect("write send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Simulate a slow write: sleep 3 s so created_at is clearly older than
    // read_idle_ttl_secs (2 s).  With the old (broken) anchor the spool
    // would be deleted on the first cleanup sweep after complete().
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Complete — sets readable_at = now_secs() (fresh, 0 s old).
    let resp = client
        .post(format!("{}/api/v1/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // One cleanup sweep passes (interval = 1 s).  readable_at is ≈ 1 s old
    // < 2 s TTL.  created_at is ≈ 4 s old > 2 s TTL — wrong anchor would
    // delete here.
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    let check = do_range_read(&server.base_url, &key, "bytes=0-4095").await;
    assert_eq!(
        check,
        reqwest::StatusCode::PARTIAL_CONTENT,
        "spool must survive: idle TTL anchors on readable_at (1 s old), not created_at (4 s old)"
    );
}

/// A reader that actively receives bytes keeps the spool alive via the
/// `last_read_activity_at` refresh mechanism.  Each completed Range request
/// updates this timestamp; as long as reads arrive within the idle TTL
/// window the spool must not be deleted.
///
/// This test simulates a "slow reader" by issuing multiple range requests
/// with a sleep between them.  The spool survives each gap because the
/// previous read refreshed the idle anchor.  After the final read, once no
/// further activity occurs for longer than `read_idle_ttl_secs`, the spool
/// is eventually deleted.
///
/// Because integration tests use real TCP, slow reading is simulated as
/// sequential range requests with wall-clock sleeps between them rather than
/// pausing mid-stream chunk delivery. The property tested is identical:
/// activity refreshes the idle timer; absence of activity causes eventual
/// deletion.
#[tokio::test]
async fn test_slow_reader_receiving_bytes_not_cleaned_up() {
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: std::path::PathBuf::from("./data"),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        // Long full-read TTL so it does not fire before we finish simulating
        // the slow reader.
        full_read_complete_ttl_secs: 60,
        // Idle TTL of 2 s: short enough to fire when no reads happen for 3 s,
        // but long enough to survive a 1.5 s inter-chunk gap.
        read_idle_ttl_secs: 2,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 1,
        long_poll_timeout_ms: 25000,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    });
    let server = start_server_with_config(config).await;

    let data = vec![0xEEu8; 8192];
    let key = write_complete_fresh(&server.base_url, data).await;

    // --- Chunk 1: first half ---
    let s1 = do_range_read(&server.base_url, &key, "bytes=0-4095").await;
    assert_eq!(s1, reqwest::StatusCode::PARTIAL_CONTENT, "chunk 1 read");

    // Simulate slow reader: pause between chunks.  The idle anchor is
    // refreshed by chunk 1, so the spool must NOT be deleted during this gap
    // (last_read_activity_at is ≈ 0.5 s old < 2 s idle TTL).
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Spool must still be alive.
    let alive1 = do_range_read(&server.base_url, &key, "bytes=0-0").await;
    assert_eq!(
        alive1,
        reqwest::StatusCode::PARTIAL_CONTENT,
        "spool must survive the first inter-chunk gap (activity < idle TTL)"
    );

    // --- Chunk 2: second half (achieves full coverage) ---
    let s2 = do_range_read(&server.base_url, &key, "bytes=4096-8191").await;
    assert_eq!(s2, reqwest::StatusCode::PARTIAL_CONTENT, "chunk 2 read");

    // Another inter-chunk gap — activity refreshed by chunk 2.
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

    // Spool must still be alive (last_read_activity_at is ≈ 1.5 s old < 2 s
    // idle TTL; full_read_complete_ttl_secs = 60 s won't fire yet).
    let alive2 = do_range_read(&server.base_url, &key, "bytes=0-0").await;
    assert_eq!(
        alive2,
        reqwest::StatusCode::PARTIAL_CONTENT,
        "spool must survive the second inter-chunk gap (activity < idle TTL)"
    );

    // --- No more reads: idle TTL will now fire ---
    // Cleanup sweeps are scheduled independently from the test, so poll until
    // the spool is gone instead of assuming a fixed sleep aligns with a sweep.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(8);
    loop {
        if server.manager.get_spool(&key).is_none() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "spool must be deleted once reads stop for > read_idle_ttl_secs"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let after = reqwest::Client::new()
        .get(format!("{}/api/v1/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("post-idle read");
    assert_eq!(after.status(), reqwest::StatusCode::NOT_FOUND);
}
