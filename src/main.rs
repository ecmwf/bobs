use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::DefaultFileIO;
use bobs::manager::SpoolManager;
use bobs::metadata::{legacy_redb, DefaultMetadataStore};
use bobs::metrics::BobsMetrics;
use bobs::shutdown;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tower::ServiceExt;

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
        Some(path) => match Config::from_file(&path) {
            Ok(config) => config,
            Err(error) => {
                tracing::error!("event.name" = "startup.config.failed", outcome = "error", error = %error, "configuration load failed");
                return Err(Box::new(error));
            }
        },
        None => Config::default(),
    };
    if let Err(error) = config.validate() {
        tracing::error!("event.name" = "startup.config.failed", outcome = "error", error = %error, "configuration validation failed");
        return Err(Box::new(error));
    }

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
        "event.name" = "startup.config.loaded",
        outcome = "success",
        hostname = %hostname,
        ordinal = %ordinal,
        host = %config.host,
        port = config.port,
        data_dir = %config.data_dir.display(),
        page_size = config.page_size,
        max_cache_bytes = config.max_cache_bytes,
        route_name = %config.route_name,
        public_base = %format!("https://{}.{}/{}-{}/api/v1", config.host_prefix, config.domain, config.route_name, ordinal),
        internal_base_url = %internal_base_url,
        "configuration loaded",
    );

    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    {
        let ring_pool = bobs::io::initialize_production_ring_pool(config.io_uring_shards)?;
        tracing::debug!(
            configured_shards = ?ring_pool.configured_shards,
            resolved_shards = ring_pool.resolved_shards,
            cpu_pinning_enabled = ring_pool.cpu_pinning_enabled,
            "io_uring production ring pool initialized",
        );
    }

    legacy_redb::migrate_from_redb(config.data_dir.join("spools.redb"), &config.data_dir)?;
    let manager = Arc::new(
        SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
            DefaultMetadataStore::new(&config.data_dir),
            &config.data_dir,
            config.page_size,
            config.max_cache_bytes,
        )?,
    );

    manager.recover().await?;
    let cleanup_task = cleanup::start_cleanup_task(manager.clone(), config.clone());

    let metrics = Arc::new(BobsMetrics::new(
        config.metrics.enabled,
        config.metrics.allowed_labels.clone(),
        config.metrics.max_label_value_length,
    ));

    let state = Arc::new(AppState {
        manager,
        config: config.clone(),
        hostname: hostname.clone(),
        ordinal: ordinal.clone(),
        internal_base_url,
        metrics,
    });
    let app = router::<DefaultFileIO, DefaultMetadataStore>().with_state(state);
    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("event.name" = "startup.server.listening", outcome = "success", addr = %addr, host = %config.host, port = config.port, "server listening");

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
                tracing::info!("event.name" = "startup.shutdown.received", outcome = "success", "shutdown signal received");
                break;
            }
            // Reap finished connection tasks to avoid unbounded JoinSet growth.
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }

    // Drain in-flight connections before exiting.
    while tasks.join_next().await.is_some() {}

    // Drop HTTP entrypoints and background manager users before tearing down
    // the global io_uring pool; otherwise lingering Arc<SpoolManager> handles
    // can keep pool clones alive and prevent driver shutdown from joining.
    drop(listener);
    drop(app);
    cleanup_task.abort();
    match cleanup_task.await {
        Ok(()) => tracing::debug!("cleanup task exited before shutdown"),
        Err(err) if err.is_cancelled() => tracing::debug!("cleanup task aborted for shutdown"),
        Err(err) => tracing::warn!(error = %err, "cleanup task failed during shutdown"),
    }

    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    {
        match bobs::io::uring_fs::shutdown_global_ring_pool_for_exit()? {
            Some(shutdown) => tracing::debug!(
                joined_driver_handles = shutdown.joined_driver_handles,
                in_flight_operations_remaining = shutdown.in_flight_operations_remaining,
                driver_threads_all_stopped = shutdown.driver_threads_all_stopped,
                "io_uring production ring pool shut down"
            ),
            None => tracing::warn!(
                "io_uring production ring pool was still shared during shutdown; dropping global reference"
            ),
        }
    }

    tracing::info!(
        "event.name" = "startup.shutdown.complete",
        outcome = "success",
        "shutdown complete"
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    bobs::observability::init_tracing("bobs");
    if let Err(error) = run().await {
        tracing::error!(error = %error, "BOBS failed to start");
        std::process::exit(1);
    }
}
