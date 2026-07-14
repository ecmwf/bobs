// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use bobs::cleanup;
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::DefaultFileIO;
use bobs::manager::SpoolManager;
use bobs::metadata::DefaultMetadataStore;
use bobs::metrics::BobsMetrics;
use bobs::server::{serve_http, SHUTDOWN_DRAIN_TIMEOUT};
use bobs::shutdown;
use std::sync::Arc;
use tokio::net::TcpListener;

#[cfg(feature = "telemetry")]
use bobs::metrics::{init_meter_provider, serve_metrics};
#[cfg(feature = "telemetry")]
use tokio_util::sync::CancellationToken;

fn parse_ordinal(hostname: &str) -> std::io::Result<String> {
    let (_, ordinal) = hostname.rsplit_once('-').ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "HOSTNAME must end with a numeric StatefulSet ordinal (e.g. bobs-0)",
        )
    })?;

    if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "HOSTNAME must end with a numeric StatefulSet ordinal (e.g. bobs-0)",
        ));
    }

    Ok(ordinal.to_string())
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
        max_live_spools = config.max_live_spools,
        route_name = %config.route_name,
        public_base = %format!("https://{}.{}/{}-{}/api/v1", config.host_prefix, config.domain, config.route_name, ordinal),
        internal_base_url = %internal_base_url,
        "configuration loaded",
    );

    #[cfg(all(target_os = "linux", not(feature = "tokio-fileio-fallback")))]
    {
        let queue_capacity = config.resolved_io_uring_queue_capacity()?;
        let ring_pool =
            bobs::io::initialize_production_ring_pool(config.io_uring_shards, queue_capacity)?;
        tracing::debug!(
            configured_shards = ?ring_pool.configured_shards,
            resolved_shards = ring_pool.resolved_shards,
            cpu_pinning_enabled = ring_pool.cpu_pinning_enabled,
            queue_capacity,
            "io_uring production ring pool initialized",
        );
    }

    let addr = format!("{}:{}", config.host, config.port);
    let listener = TcpListener::bind(&addr).await.map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!("failed to bind main HTTP listener at {addr}: {error}"),
        )
    })?;

    #[cfg(feature = "telemetry")]
    let metrics_listener = if config.metrics.enabled {
        let metrics_addr = format!("{}:{}", config.metrics.bind_address, config.metrics.port);
        let listener = TcpListener::bind(&metrics_addr).await.map_err(|error| {
            std::io::Error::new(
                error.kind(),
                format!("failed to bind metrics HTTP listener at {metrics_addr}: {error}"),
            )
        })?;
        Some((listener, metrics_addr))
    } else {
        None
    };

    #[cfg(feature = "telemetry")]
    let (_meter_provider, metrics_server) = if let Some((listener, metrics_addr)) = metrics_listener
    {
        let (provider, registry) = init_meter_provider(&hostname);
        tracing::info!(
            "event.name" = "startup.metrics.enabled",
            outcome = "success",
            addr = %metrics_addr,
            host = %config.metrics.bind_address,
            port = config.metrics.port,
            "prometheus /metrics scrape endpoint enabled"
        );
        (Some(provider), Some((listener, registry)))
    } else {
        (None, None)
    };

    let metrics = Arc::new(BobsMetrics::new(config.metrics.enabled));

    let mut manager = SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
        DefaultMetadataStore::new(&config.data_dir),
        &config.data_dir,
        config.page_size,
        config.max_cache_bytes,
        config.max_live_spools,
    )?;
    manager.set_metrics(Arc::clone(&metrics));
    let manager = Arc::new(manager);

    manager.recover().await?;
    let cleanup_task = cleanup::start_cleanup_task(manager.clone(), config.clone());

    let state = Arc::new(AppState {
        manager,
        config: config.clone(),
        hostname: hostname.clone(),
        ordinal: ordinal.clone(),
        internal_base_url,
        metrics,
    });
    let app = router::<DefaultFileIO, DefaultMetadataStore>().with_state(state);
    tracing::info!("event.name" = "startup.server.listening", outcome = "success", addr = %addr, host = %config.host, port = config.port, "server listening");

    #[cfg(feature = "telemetry")]
    let (drain, metrics_drain) = if let Some((metrics_listener, registry)) = metrics_server {
        let cancellation = CancellationToken::new();
        let main_cancellation = cancellation.clone();
        let metrics_cancellation = cancellation.clone();
        let main_server = serve_http(
            listener,
            app,
            async move { main_cancellation.cancelled().await },
            SHUTDOWN_DRAIN_TIMEOUT,
        );
        let metrics_server = serve_metrics(
            metrics_listener,
            registry,
            async move { metrics_cancellation.cancelled().await },
            SHUTDOWN_DRAIN_TIMEOUT,
        );
        tokio::pin!(main_server);
        tokio::pin!(metrics_server);

        tokio::select! {
            _ = shutdown::shutdown_signal() => {
                tracing::info!(
                    "event.name" = "startup.shutdown.received",
                    outcome = "success",
                    drain_timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                    "shutdown signal received; stopping accepts and draining connections"
                );
                cancellation.cancel();
                let (main_result, metrics_result) =
                    tokio::join!(main_server.as_mut(), metrics_server.as_mut());
                (main_result?, Some(metrics_result?))
            }
            main_result = main_server.as_mut() => {
                cancellation.cancel();
                let metrics_result = metrics_server.as_mut().await;
                (main_result?, Some(metrics_result?))
            }
            metrics_result = metrics_server.as_mut() => {
                cancellation.cancel();
                let main_result = main_server.as_mut().await;
                (main_result?, Some(metrics_result?))
            }
        }
    } else {
        let drain = serve_http(
            listener,
            app,
            async {
                shutdown::shutdown_signal().await;
                tracing::info!(
                    "event.name" = "startup.shutdown.received",
                    outcome = "success",
                    drain_timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                    "shutdown signal received; stopping accepts and draining connections"
                );
            },
            SHUTDOWN_DRAIN_TIMEOUT,
        )
        .await?;
        (drain, None)
    };

    #[cfg(not(feature = "telemetry"))]
    let drain = serve_http(
        listener,
        app,
        async {
            shutdown::shutdown_signal().await;
            tracing::info!(
                "event.name" = "startup.shutdown.received",
                outcome = "success",
                drain_timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                "shutdown signal received; stopping accepts and draining connections"
            );
        },
        SHUTDOWN_DRAIN_TIMEOUT,
    )
    .await?;

    if drain.timed_out {
        tracing::warn!(
            "event.name" = "startup.shutdown.drain_timeout",
            outcome = "timeout",
            listener = "main",
            drain_timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
            aborted_connections = drain.aborted_connections,
            "HTTP drain deadline reached; aborted remaining connections"
        );
    }

    #[cfg(feature = "telemetry")]
    if let Some(metrics_drain) = metrics_drain {
        if metrics_drain.timed_out {
            tracing::warn!(
                "event.name" = "startup.shutdown.drain_timeout",
                outcome = "timeout",
                listener = "metrics",
                drain_timeout_secs = SHUTDOWN_DRAIN_TIMEOUT.as_secs(),
                aborted_connections = metrics_drain.aborted_connections,
                "HTTP drain deadline reached; aborted remaining connections"
            );
        }
    }

    // Drop HTTP entrypoints and background manager users before tearing down
    // the global io_uring pool; otherwise lingering Arc<SpoolManager> handles
    // can keep pool clones alive and prevent driver shutdown from joining.
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

    #[cfg(feature = "telemetry")]
    if let Some(provider) = _meter_provider {
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down meter provider");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ordinal_accepts_numeric_final_segment() {
        assert_eq!(parse_ordinal("bobs-0").expect("ordinal"), "0");
        assert_eq!(parse_ordinal("release-bobs-12").expect("ordinal"), "12");
    }

    #[test]
    fn parse_ordinal_rejects_malformed_hostnames() {
        for hostname in ["bobs", "bobs-", "bobs-a", "bobs-١"] {
            let error = parse_ordinal(hostname).expect_err("hostname should be rejected");
            assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        }
    }
}
