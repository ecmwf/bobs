use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{AppState, router};
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
    let config = Arc::new(Config::from_env());
    tracing::info!(bob_id = %config.bob_id, listen = %config.listen_addr, "BOBS starting");

    let manager = Arc::new(
        SpoolManager::<TokioFileIO>::new(
            config.data_dir.join("spools.redb"),
            &config.data_dir,
            config.bob_id.clone(),
            config.page_size,
            config.page_cache_capacity,
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
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .expect("failed to bind");
    tracing::info!("listening on {}", config.listen_addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await
        .expect("server error");
}
