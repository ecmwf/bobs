use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::TokioFileIO;
use bobs::manager::SpoolManager;
use bobs::shutdown;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tower::ServiceExt;
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

    let internal_base_url_template = std::env::var("BOBS_INTERNAL_BASE_URL_TEMPLATE")
        .ok()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "BOBS_INTERNAL_BASE_URL_TEMPLATE environment variable must be set and non-empty",
            )
        })?;
    let internal_base_url = internal_base_url_template.replace("{ordinal}", &ordinal);

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
        internal_base_url = %internal_base_url,
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
        internal_base_url,
    });
    let app = router::<TokioFileIO>().with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("listening on {}", addr);

    // 16 MiB h2 windows — one body fits in a single window with no flow-control pauses.
    const H2_WINDOW: u32 = 16 * 1024 * 1024;

    let mut shutdown = std::pin::pin!(shutdown::shutdown_signal());
    let mut tasks: JoinSet<()> = JoinSet::new();

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, _) = result?;
                let io = TokioIo::new(stream);
                let app = app.clone();
                tasks.spawn(async move {
                    let mut builder = Builder::new(TokioExecutor::new());
                    builder
                        .http2()
                        .initial_stream_window_size(H2_WINDOW)
                        .initial_connection_window_size(H2_WINDOW);
                    let svc = hyper::service::service_fn(move |req| {
                        let app = app.clone();
                        async move { app.oneshot(req).await }
                    });
                    if let Err(err) = builder.serve_connection_with_upgrades(io, svc).await {
                        tracing::warn!(error = %err, "connection error");
                    }
                });
            }
            _ = &mut shutdown => {
                tracing::info!("shutdown signal received, stopping accept loop");
                break;
            }
            // Reap finished connection tasks to avoid unbounded JoinSet growth.
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }

    // Drain in-flight connections before exiting.
    while tasks.join_next().await.is_some() {}

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
