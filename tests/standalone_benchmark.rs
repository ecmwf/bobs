// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use axum::Router;
use bobs::benchmark::config::{normalize_endpoint, BenchmarkConfig, EndpointSpec};
use bobs::benchmark::run::run_benchmark;
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
use bobs::metrics::BobsMetrics;
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
    start_server_with_default_backend(4096).await
}

async fn start_server_with_default_backend(page_size: usize) -> TestServer {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    start_server_with_manager::<DefaultFileIO, DefaultMetadataStore>(
        tmp,
        data_dir,
        page_size,
        |data_dir| DefaultMetadataStore::new(data_dir),
    )
    .await
}

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
async fn start_server_with_tokio_sidecar_backend(page_size: usize) -> TestServer {
    let tmp = tempfile::tempdir().expect("tempdir");
    let data_dir = tmp.path().join("data");
    start_server_with_manager::<TokioFileIO, SyncSidecarMetadataStore>(
        tmp,
        data_dir,
        page_size,
        |data_dir| SyncSidecarMetadataStore::new(data_dir),
    )
    .await
}

async fn start_server_with_manager<F, M>(
    tmp: TempDir,
    data_dir: std::path::PathBuf,
    page_size: usize,
    metadata_store: impl FnOnce(&std::path::Path) -> M,
) -> TestServer
where
    F: bobs::io::FileIO + 'static,
    M: bobs::metadata::MetadataStore + Clone + Send + Sync + 'static,
{
    let config = Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: data_dir.clone(),
        page_size,
        max_cache_bytes: page_size * 8,
        max_live_spools: 256,
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
        route_name: "download".into(),
        ..Config::default()
    });
    let manager = Arc::new(
        SpoolManager::<F, M>::with_metadata_store(
            metadata_store(&data_dir),
            &data_dir,
            config.page_size,
            config.max_cache_bytes,
            config.max_live_spools,
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
        metrics: Arc::new(BobsMetrics::new(false)),
        async_sync: None,
    });
    let app: Router = router::<F, M>().with_state(state);
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

#[test]
fn benchmark_helpers_can_name_explicit_backend_managers() {
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

#[tokio::test]
async fn standalone_benchmark_smoke() {
    let server = start_server().await;
    let summary = run_benchmark(benchmark_config_for(
        &server.base_url,
        4,
        64 * 1024,
        16 * 1024,
    ))
    .await
    .expect("benchmark run");
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

fn benchmark_config_for(
    base_url: &str,
    objects: usize,
    object_bytes: u64,
    chunk_bytes: usize,
) -> BenchmarkConfig {
    BenchmarkConfig {
        endpoint: EndpointSpec::Single(normalize_endpoint(base_url).unwrap()),
        objects,
        object_bytes,
        write_body_chunk_bytes: chunk_bytes,
        read_body_chunk_bytes: chunk_bytes,
        start_delay: Duration::from_millis(10),
        ..Default::default()
    }
}

#[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
#[tokio::test]
#[ignore = "Linux host benchmark smoke: compares io_uring sidecar/file IO against Tokio fallback with 16 MiB pages"]
async fn io_uring_16m_page_smoke_not_slower_than_tokio_fileio() {
    const PAGE_16_MIB: usize = 16 * 1024 * 1024;
    const OBJECTS: usize = 2;
    const OBJECT_BYTES: u64 = (PAGE_16_MIB as u64) * 2;

    let uring_server = start_server_with_default_backend(PAGE_16_MIB).await;
    let tokio_server = start_server_with_tokio_sidecar_backend(PAGE_16_MIB).await;

    let tokio_summary = run_benchmark(benchmark_config_for(
        &tokio_server.base_url,
        OBJECTS,
        OBJECT_BYTES,
        PAGE_16_MIB,
    ))
    .await
    .expect("tokio fallback benchmark run");
    let uring_summary = run_benchmark(benchmark_config_for(
        &uring_server.base_url,
        OBJECTS,
        OBJECT_BYTES,
        PAGE_16_MIB,
    ))
    .await
    .expect("io_uring benchmark run");

    assert_eq!(tokio_summary.successes, OBJECTS);
    assert_eq!(uring_summary.successes, OBJECTS);
    assert_eq!(tokio_summary.failures, 0);
    assert_eq!(uring_summary.failures, 0);

    let tokio_mib_s = tokio_summary.wall_mib_s;
    let uring_mib_s = uring_summary.wall_mib_s;
    // This is a smoke test, not a lab benchmark. Running both servers in the
    // same test process leaves scheduler and filesystem noise, so allow a 15%
    // band. Anything below 85% of the Tokio fallback suggests a meaningful
    // regression in the default io_uring FileIO/sidecar path for 16 MiB pages.
    let minimum_acceptable = tokio_mib_s * 0.85;
    assert!(
        uring_mib_s >= minimum_acceptable,
        "io_uring throughput ({uring_mib_s:.2} MiB/s) was meaningfully below Tokio fallback ({tokio_mib_s:.2} MiB/s)"
    );
}
