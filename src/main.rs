use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::TokioFileIO;
use bobs::manager::SpoolManager;
use bobs::shutdown;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

fn parse_ordinal(hostname: &str) -> std::io::Result<String> {
    hostname
        .rsplit('-')
        .next()
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "HOSTNAME must contain '-' (e.g. bobs-0)",
            )
        })
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args();
    let _program = args.next();
    let config = match args.next() {
        Some(path) => Config::from_file(&path)?,
        None => Config::default(),
    };
    config.validate()?;

    let hostname = std::env::var("HOSTNAME").map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "HOSTNAME environment variable must be set",
        )
    })?;
    let ordinal = parse_ordinal(&hostname)?;

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
        public_base = %format!("https://{}.{}/{}-{}/api/v1", config.host_prefix, config.domain, config.route_name, ordinal),
        "BOBS starting",
    );

    let manager = Arc::new(SpoolManager::<TokioFileIO>::new(
        config.data_dir.join("spools.redb"),
        &config.data_dir,
        config.page_size,
        config.max_cache_bytes,
    )?);

    manager.recover().await?;
    let _cleanup = cleanup::start_cleanup_task(manager.clone(), config.clone());

    let state = Arc::new(AppState {
        manager,
        config: config.clone(),
        hostname: hostname.clone(),
        ordinal: ordinal.clone(),
    });
    let app = router::<TokioFileIO>().with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("listening on {}", addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown::shutdown_signal())
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    if let Err(error) = run().await {
        tracing::error!(error = %error, "BOBS failed to start");
        std::process::exit(1);
    }
}
