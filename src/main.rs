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
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let config = match std::env::args().nth(1) {
        Some(path) => Config::from_file(&path).expect("failed to load config file"),
        None => Config::default(),
    };

    if config.host_prefix.is_empty() {
        panic!("host_prefix must be set in config");
    }
    if config.domain.is_empty() {
        panic!("domain must be set in config");
    }
    if config.route_name.is_empty() {
        panic!("route_name must be set in config");
    }

    let hostname = std::env::var("HOSTNAME").expect("HOSTNAME environment variable must be set");
    let ordinal = hostname
        .rsplit('-')
        .next()
        .expect("HOSTNAME must contain '-' (e.g. bobs-0)")
        .to_string();

    let config = Arc::new(config);

    tracing::info!(
        hostname = %hostname,
        ordinal = %ordinal,
        host = %config.host,
        port = %config.port,
        data_dir = %config.data_dir.display(),
        page_size = config.page_size,
        max_cache_bytes = config.max_cache_bytes,
        route_name = %config.route_name,
        public_base = %format!("https://{}.{}/{}-{}", config.host_prefix, config.domain, config.route_name, ordinal),
        "BOBS starting",
    );

    let manager = Arc::new(
        SpoolManager::<TokioFileIO>::new(
            config.data_dir.join("spools.redb"),
            &config.data_dir,
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
        hostname: hostname.clone(),
        ordinal: ordinal.clone(),
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
