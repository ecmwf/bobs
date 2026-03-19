use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::TokioFileIO;
use bobs::manager::SpoolManager;
use bobs::shutdown;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    let config = Arc::new(match std::env::args().nth(1) {
        Some(path) => Config::from_file(&path).expect("failed to load config file"),
        None => Config::default(),
    });
    tracing::info!(bob_id = %config.bob_id, host = %config.host, port = %config.port, "BOBS starting");

    let manager = Arc::new(
        SpoolManager::<TokioFileIO>::new(
            config.data_dir.join("spools.redb"),
            &config.data_dir,
            config.bob_id.clone(),
            config.page_size,
            config.max_cache_bytes,
        )
        .expect("failed to initialize SpoolManager"),
    );

    manager.recover().await.expect("failed to recover spools");
    let _cleanup = cleanup::start_cleanup_task(manager.clone(), config.clone());

    let state = Arc::new(AppState {
        manager,
        config: config.clone(),
    });
    let app = router::<TokioFileIO>().with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await.expect("failed to bind");
    tracing::info!("listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
        .expect("server error");
}
