use axum::Router;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::TokioFileIO;
use bobs::manager::SpoolManager;
use serde_json::{json, Value};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    base_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    _tmp: TempDir,
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
    }
}

async fn start_server() -> TestServer {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let db_path = tmp.path().join("spools.redb");
    let data_dir = tmp.path().join("data");

    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: data_dir.clone(),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        bob_id: "itest-bob".into(),
    });

    let manager = Arc::new(
        SpoolManager::<TokioFileIO>::new(
            &db_path,
            &data_dir,
            config.bob_id.clone(),
            4096,
            config.max_cache_bytes,
        )
        .expect("init manager"),
    );
    manager.recover().await.expect("recover");

    let state = Arc::new(AppState {
        manager,
        config: Arc::clone(&config),
    });
    let app: Router = router::<TokioFileIO>().with_state(state);

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

    TestServer {
        base_url: format!("http://{}", addr),
        shutdown_tx: Some(tx),
        handle,
        _tmp: tmp,
    }
}

async fn create_key(client: &reqwest::Client, base_url: &str, body: Option<Value>) -> String {
    let req = client.put(format!("{base_url}/create"));
    let resp = if let Some(payload) = body {
        req.json(&payload).send().await.expect("create send")
    } else {
        req.send().await.expect("create send")
    };
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let v: Value = resp.json().await.expect("create json");
    v["key"].as_str().expect("key str").to_string()
}

#[tokio::test]
async fn test_basic_lifecycle() {
    let server = start_server().await;
    let client = reqwest::Client::new();

    let key = create_key(&client, &server.base_url, None).await;
    let bytes: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 251) as u8).collect();

    let write_resp = client
        .post(format!("{}/write/{}/0", server.base_url, key))
        .body(bytes.clone())
        .send()
        .await
        .expect("write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);

    let complete_resp = client
        .post(format!("{}/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_resp = client
        .get(format!("{}/read/{}", server.base_url, key))
        .header("Range", format!("bytes=0-{}", bytes.len() - 1))
        .send()
        .await
        .expect("read send");
    assert_eq!(read_resp.status(), reqwest::StatusCode::PARTIAL_CONTENT);
    let read_bytes = read_resp.bytes().await.expect("read bytes");
    assert_eq!(read_bytes.as_ref(), bytes.as_slice());

    let del_resp = client
        .delete(format!("{}/delete/{}", server.base_url, key))
        .send()
        .await
        .expect("delete send");
    assert_eq!(del_resp.status(), reqwest::StatusCode::OK);

    let read_after_delete = client
        .get(format!("{}/read/{}", server.base_url, key))
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
    let read_url = format!("{}/read/{}", server.base_url, key);
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
            .post(format!("{}/write/{}/{}", server.base_url, key, offset))
            .body(chunk.clone())
            .send()
            .await
            .expect("write chunk");
        assert_eq!(resp.status(), reqwest::StatusCode::OK);
        offset += chunk.len() as u64;
    }

    let complete_resp = client
        .post(format!("{}/complete/{}", server.base_url, key))
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
        .get(format!("{}/read/{}", server.base_url, key))
        .header("Range", "bytes=0-0")
        .send()
        .await
        .expect("locked read send");
    assert_eq!(locked_read.status(), reqwest::StatusCode::LOCKED);

    let data = vec![9u8; 5000];
    let write_resp = client
        .post(format!("{}/write/{}/0", server.base_url, key))
        .body(data.clone())
        .send()
        .await
        .expect("write send");
    assert_eq!(write_resp.status(), reqwest::StatusCode::OK);

    let complete_resp = client
        .post(format!("{}/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let read_resp = client
        .get(format!("{}/read/{}", server.base_url, key))
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
        .post(format!("{}/write/missing/0", server.base_url))
        .body(vec![1u8; 10])
        .send()
        .await
        .expect("missing write");
    assert_eq!(missing_write.status(), reqwest::StatusCode::NOT_FOUND);

    let key = create_key(&client, &server.base_url, None).await;
    let ok_write = client
        .post(format!("{}/write/{}/0", server.base_url, key))
        .body(vec![1u8; 10])
        .send()
        .await
        .expect("ok write");
    assert_eq!(ok_write.status(), reqwest::StatusCode::OK);

    let bad_offset = client
        .post(format!("{}/write/{}/0", server.base_url, key))
        .body(vec![2u8; 4])
        .send()
        .await
        .expect("bad offset write");
    assert_eq!(bad_offset.status(), reqwest::StatusCode::BAD_REQUEST);

    let complete_resp = client
        .post(format!("{}/complete/{}", server.base_url, key))
        .send()
        .await
        .expect("complete send");
    assert_eq!(complete_resp.status(), reqwest::StatusCode::OK);

    let first_reader = client
        .get(format!("{}/read/{}", server.base_url, key))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .expect("first reader send");
    assert_eq!(first_reader.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let second_reader = client
        .get(format!("{}/read/{}", server.base_url, key))
        .header("Range", "bytes=0-9")
        .send()
        .await
        .expect("second reader send");
    assert_eq!(second_reader.status(), reqwest::StatusCode::PARTIAL_CONTENT);

    let write_after_close = client
        .post(format!("{}/write/{}/10", server.base_url, key))
        .body(vec![8u8; 1])
        .send()
        .await
        .expect("write after complete");
    assert_eq!(write_after_close.status(), reqwest::StatusCode::CONFLICT);
}
