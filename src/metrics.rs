//! OpenTelemetry metrics instrumentation for bobs.
//!
//! All instruments use the OTel global meter API. When no `SdkMeterProvider` is
//! installed (feature disabled or `metrics.enabled: false`), all operations are
//! no-ops with zero runtime cost.

use std::collections::HashMap;

#[cfg(feature = "telemetry")]
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
#[cfg(feature = "telemetry")]
use opentelemetry::KeyValue;

/// Initialize the OpenTelemetry `SdkMeterProvider` with a Prometheus scrape exporter.
///
/// Returns `(provider, registry)`. The caller must hold the provider, pass the
/// registry to `serve_metrics`, and call `provider.shutdown()` on exit.
#[cfg(feature = "telemetry")]
pub fn init_meter_provider(
    hostname: &str,
) -> (
    opentelemetry_sdk::metrics::SdkMeterProvider,
    prometheus::Registry,
) {
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::Resource;

    let registry = prometheus::Registry::new();

    let exporter = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        .build()
        .expect("prometheus exporter should build");

    let environment = std::env::var("POLYTOPE_ENV").unwrap_or_else(|_| "unknown".to_string());

    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", "bobs"),
            KeyValue::new("service.instance.id", hostname.to_owned()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", environment),
            KeyValue::new(
                "k8s.pod.name",
                std::env::var("K8S_POD_NAME").unwrap_or_default(),
            ),
        ])
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(exporter)
        .build();

    opentelemetry::global::set_meter_provider(provider.clone());
    (provider, registry)
}

/// Spawn a minimal HTTP server exposing `GET /metrics` for Prometheus scraping.
#[cfg(feature = "telemetry")]
pub async fn serve_metrics(registry: prometheus::Registry, port: u16) {
    use axum::extract::State;
    use axum::response::IntoResponse;
    use axum::{routing::get, Router};
    use prometheus::Encoder;

    async fn handler(State(reg): State<prometheus::Registry>) -> impl IntoResponse {
        let encoder = prometheus::TextEncoder::new();
        let families = reg.gather();
        let mut buf = Vec::new();
        encoder.encode(&families, &mut buf).unwrap();
        (
            [(
                axum::http::header::CONTENT_TYPE,
                encoder.format_type().to_owned(),
            )],
            buf,
        )
    }

    let app = Router::new()
        .route("/metrics", get(handler))
        .with_state(registry);

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .unwrap_or_else(|e| {
            tracing::error!(port, error = %e, "failed to bind metrics endpoint");
            std::process::exit(1);
        });

    tracing::info!(port, "prometheus /metrics endpoint listening");
    let _ = axum::serve(listener, app).await;
}

/// Deletion reason label values.
pub mod reason {
    pub const CLIENT: &str = "client";
    pub const IDLE_TTL: &str = "idle_ttl";
    pub const FULL_READ_TTL: &str = "full_read_ttl";
    pub const WRITER_TIMEOUT: &str = "writer_timeout";
}

/// Read mode label values.
pub mod mode {
    pub const FOLLOW: &str = "follow";
    pub const RANGE: &str = "range";
}

/// Read outcome label values.
pub mod outcome {
    pub const SUCCESS: &str = "success";
    pub const TIMEOUT: &str = "timeout";
    pub const ERROR: &str = "error";
    pub const CLIENT_GONE: &str = "client_gone";
}

/// Spool state label values (for active spool gauge).
pub mod state {
    pub const WRITING: &str = "writing";
    pub const WRITE_LOCKED: &str = "write_locked";
    pub const COMPLETE: &str = "complete";
    pub const READABLE: &str = "readable";
}

/// Central metrics handle holding all bobs instruments.
///
/// Constructed once at startup and shared via `AppState`.
#[derive(Clone)]
pub struct BobsMetrics {
    #[cfg(feature = "telemetry")]
    inner: Option<InnerMetrics>,
    #[cfg(feature = "telemetry")]
    allowed_labels: Vec<String>,
    #[cfg(feature = "telemetry")]
    max_label_value_length: usize,
    // Keep fields accessible for tests even without telemetry
    #[cfg(not(feature = "telemetry"))]
    #[allow(dead_code)]
    allowed_labels: Vec<String>,
    #[cfg(not(feature = "telemetry"))]
    #[allow(dead_code)]
    max_label_value_length: usize,
}

#[cfg(feature = "telemetry")]
#[derive(Clone)]
struct InnerMetrics {
    // Spool lifecycle
    spools_created: Counter<u64>,
    spools_completed: Counter<u64>,
    spools_deleted: Counter<u64>,

    // Spool operation durations
    create_duration: Histogram<f64>,
    complete_duration: Histogram<f64>,

    // Write path
    write_bytes: Counter<u64>,
    write_duration: Histogram<f64>,

    // Read path
    read_bytes: Counter<u64>,
    read_duration: Histogram<f64>,
    read_active: UpDownCounter<i64>,

    // System-level
    spools_active: UpDownCounter<i64>,
    disk_usage: Gauge<u64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
}

impl BobsMetrics {
    /// Create metrics instruments. If the `telemetry` feature is disabled or
    /// `enabled` is false, returns a no-op instance.
    pub fn new(enabled: bool, allowed_labels: Vec<String>, max_label_value_length: usize) -> Self {
        #[cfg(feature = "telemetry")]
        {
            let inner = if enabled {
                let meter = opentelemetry::global::meter("bobs");
                Some(Self::build_instruments(&meter))
            } else {
                None
            };
            BobsMetrics {
                inner,
                allowed_labels,
                max_label_value_length,
            }
        }
        #[cfg(not(feature = "telemetry"))]
        {
            let _ = enabled;
            BobsMetrics {
                allowed_labels,
                max_label_value_length,
            }
        }
    }

    #[cfg(feature = "telemetry")]
    fn build_instruments(meter: &Meter) -> InnerMetrics {
        // Naming rules (prevent double-suffixing by the OTel Prometheus exporter):
        //   - Never include the unit word in the instrument name; the exporter
        //     appends it from `.with_unit()` unconditionally.
        //   - Never include "total" in a Counter name; the exporter appends
        //     `_total` for every Counter.
        //   - Prometheus output = underscored name + unit suffix + type suffix.
        //     e.g. `bobs.write.duration` + unit "s" + Histogram
        //          → bobs_write_duration_seconds_{bucket,sum,count}

        // Bucket boundaries (seconds) for write and read duration histograms.
        // Chosen to give useful percentile resolution across the observed range:
        // fast cache-hit reads (~5 ms) through slow fsync-bound completes (~5 s).
        let duration_boundaries = vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

        InnerMetrics {
            // ── Spool lifecycle ───────────────────────────────────────────────
            // Counter → exporter appends `_total`; do not include it in name.
            spools_created: meter
                .u64_counter("bobs.spools.created")
                .with_description("Total spools created")
                .build(),
            spools_completed: meter
                .u64_counter("bobs.spools.completed")
                .with_description("Total spools completed")
                .build(),
            spools_deleted: meter
                .u64_counter("bobs.spools.deleted")
                .with_description("Total spools deleted")
                .build(),
            create_duration: meter
                .f64_histogram("bobs.create.duration")
                .with_description("Duration of spool creation")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0, 5.0,
                ])
                .build(),
            complete_duration: meter
                .f64_histogram("bobs.complete.duration")
                .with_description(
                    "Duration of spool completion (fdatasync + durable metadata commit)",
                )
                .with_unit("s")
                .with_boundaries(vec![
                    0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 10.0,
                ])
                .build(),

            // ── Write path ────────────────────────────────────────────────────
            // Byte counter: keep "bytes" in the name, omit unit annotation so
            // the exporter does not append a second `_bytes` suffix.
            write_bytes: meter
                .u64_counter("bobs.write.bytes")
                .with_description("Total bytes written to spools")
                .build(),
            // Duration histogram: name has no unit word; exporter appends
            // `_seconds` from `.with_unit("s")`.
            write_duration: meter
                .f64_histogram("bobs.write.duration")
                .with_description("Duration of write operations")
                .with_unit("s")
                .with_boundaries(duration_boundaries.clone())
                .build(),

            // ── Read path ─────────────────────────────────────────────────────
            read_bytes: meter
                .u64_counter("bobs.read.bytes")
                .with_description("Total bytes served from spools")
                .build(),
            read_duration: meter
                .f64_histogram("bobs.read.duration")
                .with_description("Duration of read operations")
                .with_unit("s")
                .with_boundaries(duration_boundaries)
                .build(),
            read_active: meter
                .i64_up_down_counter("bobs.read.active")
                .with_description("Currently active readers")
                .build(),

            // ── System-level ──────────────────────────────────────────────────
            spools_active: meter
                .i64_up_down_counter("bobs.spools.active")
                .with_description("Currently active spools by state")
                .build(),
            // Gauge: same rule as byte counter — keep "bytes" in name, omit
            // unit annotation.
            disk_usage: meter
                .u64_gauge("bobs.disk.usage.bytes")
                .with_description("Disk usage of the spool data directory")
                .build(),
            cache_hits: meter
                .u64_counter("bobs.pages.cache.hits")
                .with_description("Page cache hits")
                .build(),
            cache_misses: meter
                .u64_counter("bobs.pages.cache.misses")
                .with_description("Page cache misses")
                .build(),
        }
    }

    /// Convert caller-provided labels into OTel KeyValue attributes,
    /// respecting the allowlist and max value length.
    #[cfg(feature = "telemetry")]
    fn caller_attrs(&self, labels: &HashMap<String, String>) -> Vec<KeyValue> {
        labels
            .iter()
            .filter(|(k, _)| self.allowed_labels.is_empty() || self.allowed_labels.contains(k))
            .map(|(k, v)| {
                let truncated = if v.len() > self.max_label_value_length {
                    &v[..self.max_label_value_length]
                } else {
                    v.as_str()
                };
                KeyValue::new(k.clone(), truncated.to_string())
            })
            .collect()
    }

    // --- Spool Lifecycle ---

    #[allow(unused_variables)]
    pub fn record_spool_created(&self, labels: &HashMap<String, String>) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let attrs = self.caller_attrs(labels);
            inner.spools_created.add(1, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_spool_completed(&self, labels: &HashMap<String, String>) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let attrs = self.caller_attrs(labels);
            inner.spools_completed.add(1, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_spool_deleted(&self, labels: &HashMap<String, String>, reason: &str) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("reason", reason.to_string()));
            inner.spools_deleted.add(1, &attrs);
        }
    }

    // --- Write Path ---

    #[allow(unused_variables)]
    pub fn record_write_bytes(&self, labels: &HashMap<String, String>, bytes: u64) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let attrs = self.caller_attrs(labels);
            inner.write_bytes.add(bytes, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_write_duration(
        &self,
        labels: &HashMap<String, String>,
        outcome: &str,
        seconds: f64,
    ) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("outcome", outcome.to_string()));
            inner.write_duration.record(seconds, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_create_duration(
        &self,
        labels: &HashMap<String, String>,
        outcome: &str,
        seconds: f64,
    ) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("outcome", outcome.to_string()));
            inner.create_duration.record(seconds, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_complete_duration(
        &self,
        labels: &HashMap<String, String>,
        outcome: &str,
        seconds: f64,
    ) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("outcome", outcome.to_string()));
            inner.complete_duration.record(seconds, &attrs);
        }
    }

    // --- Read Path ---

    #[allow(unused_variables)]
    pub fn record_read_bytes(&self, labels: &HashMap<String, String>, mode: &str, bytes: u64) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("mode", mode.to_string()));
            inner.read_bytes.add(bytes, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_read_duration(
        &self,
        labels: &HashMap<String, String>,
        mode: &str,
        outcome: &str,
        seconds: f64,
    ) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let mut attrs = self.caller_attrs(labels);
            attrs.push(KeyValue::new("mode", mode.to_string()));
            attrs.push(KeyValue::new("outcome", outcome.to_string()));
            inner.read_duration.record(seconds, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_reader_acquired(&self, labels: &HashMap<String, String>) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let attrs = self.caller_attrs(labels);
            inner.read_active.add(1, &attrs);
        }
    }

    #[allow(unused_variables)]
    pub fn record_reader_released(&self, labels: &HashMap<String, String>) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            let attrs = self.caller_attrs(labels);
            inner.read_active.add(-1, &attrs);
        }
    }

    // --- System-Level ---

    #[allow(unused_variables)]
    pub fn record_state_transition(&self, old_state: Option<&str>, new_state: &str) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            if let Some(old) = old_state {
                inner
                    .spools_active
                    .add(-1, &[KeyValue::new("state", old.to_string())]);
            }
            inner
                .spools_active
                .add(1, &[KeyValue::new("state", new_state.to_string())]);
        }
    }

    /// Decrement the active spool gauge when a spool is removed (deleted).
    #[allow(unused_variables)]
    pub fn record_spool_removed(&self, old_state: &str) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            inner
                .spools_active
                .add(-1, &[KeyValue::new("state", old_state.to_string())]);
        }
    }

    pub fn record_cache_hit(&self) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            inner.cache_hits.add(1, &[]);
        }
    }

    pub fn record_cache_miss(&self) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            inner.cache_misses.add(1, &[]);
        }
    }

    /// Record current disk usage in bytes (called once per cleanup sweep).
    #[allow(unused_variables)]
    pub fn record_disk_usage(&self, bytes: u64) {
        #[cfg(feature = "telemetry")]
        if let Some(inner) = &self.inner {
            inner.disk_usage.record(bytes, &[]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_metrics_do_not_panic() {
        let metrics = BobsMetrics::new(false, vec![], 128);
        let labels: HashMap<String, String> =
            [("collection".into(), "era5".into())].into_iter().collect();

        metrics.record_spool_created(&labels);
        metrics.record_spool_completed(&labels);
        metrics.record_spool_deleted(&labels, reason::CLIENT);
        metrics.record_write_bytes(&labels, 4096);
        metrics.record_write_duration(&labels, outcome::SUCCESS, 1.5);
        metrics.record_create_duration(&labels, outcome::SUCCESS, 0.05);
        metrics.record_complete_duration(&labels, outcome::SUCCESS, 3.5);
        metrics.record_read_bytes(&labels, mode::FOLLOW, 8192);
        metrics.record_read_duration(&labels, mode::RANGE, outcome::SUCCESS, 0.5);
        metrics.record_reader_acquired(&labels);
        metrics.record_reader_released(&labels);
        metrics.record_state_transition(None, state::WRITING);
        metrics.record_state_transition(Some(state::WRITING), state::COMPLETE);
        metrics.record_cache_hit();
        metrics.record_cache_miss();
    }

    #[test]
    fn allowed_labels_empty_passes_all() {
        let metrics = BobsMetrics::new(false, vec![], 128);
        let labels: HashMap<String, String> = [
            ("collection".into(), "era5".into()),
            ("user".into(), "alice".into()),
        ]
        .into_iter()
        .collect();

        metrics.record_spool_created(&labels);
    }

    #[test]
    fn max_label_value_length_is_stored() {
        let metrics = BobsMetrics::new(false, vec!["collection".into()], 64);
        assert_eq!(metrics.max_label_value_length, 64);
        assert_eq!(metrics.allowed_labels, vec!["collection".to_string()]);
    }
}
