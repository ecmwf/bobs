use axum::Router;
use bobs::benchmark::config::{normalize_endpoint, BenchmarkConfig, EndpointSpec};
use bobs::benchmark::run::run_benchmark;
use bobs::cleanup::start_cleanup_task;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::TokioFileIO;
use bobs::manager::SpoolManager;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct TestServer {
    base_url: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    handle: JoinHandle<()>,
    cleanup_handle: JoinHandle<()>,
    _tmp: TempDir,
}
impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.handle.abort();
        self.cleanup_handle.abort();
    }
}

async fn start_server() -> TestServer {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    let db_path = tmp.path().join("spools.redb");
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: data_dir.clone(),
        page_size: 4096,
        max_cache_bytes: 262144,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25000,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "download".into(),
    });
    let manager = Arc::new(
        SpoolManager::<TokioFileIO>::new(
            &db_path,
            &data_dir,
            config.page_size,
            config.max_cache_bytes,
        )
        .expect("manager"),
    );
    manager.recover().await.expect("recover");
    let state = Arc::new(AppState {
        manager: Arc::clone(&manager),
        config: Arc::clone(&config),
        hostname: "bobs-0".into(),
        ordinal: "0".into(),
        internal_base_url: "http://bobs-0:3000/api/v1".into(),
    });
    let app: Router = router::<TokioFileIO>().with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await;
    });
    let cleanup_handle = start_cleanup_task(manager, config);
    TestServer {
        base_url: format!("http://{addr}"),
        shutdown_tx: Some(tx),
        handle,
        cleanup_handle,
        _tmp: tmp,
    }
}

#[tokio::test]
async fn standalone_benchmark_smoke() {
    let server = start_server().await;
    let mut cfg = BenchmarkConfig::default();
    cfg.endpoint = EndpointSpec::Single(normalize_endpoint(&server.base_url).unwrap());
    cfg.objects = 4;
    cfg.object_bytes = 64 * 1024;
    cfg.write_body_chunk_bytes = 16 * 1024;
    cfg.read_body_chunk_bytes = 16 * 1024;
    cfg.start_delay = Duration::from_millis(10);
    let summary = run_benchmark(cfg).await.expect("benchmark run");
    assert_eq!(summary.total_objects, 4);
    assert_eq!(summary.successes, 4);
    assert_eq!(summary.failures, 0);
    assert_eq!(summary.total_bytes_written, 4 * 64 * 1024);
    assert_eq!(summary.total_bytes_read, 4 * 64 * 1024);
    assert!(summary.timings.read.p50_ms.is_some());
    assert!(summary.timings.read.p95_ms.is_some());
    assert!(summary.timings.read.max_ms.is_some());
    assert!(summary.per_ordinal.contains_key("0"));
}
