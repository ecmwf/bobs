// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use crate::config::Config;
use crate::error::BobsError;
use crate::io::FileIO;
use crate::manager::SpoolManager;
use crate::metadata::MetadataStore;
use crate::metrics::BobsMetrics;
use crate::time::now_secs;
use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;
use uuid::Uuid;

const JOB_ID_HEADER: &str = "X-Polytope-Job-Id";

fn extract_job_id(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(JOB_ID_HEADER)?.to_str().ok()?;
    let canonical = value.to_ascii_lowercase();
    crate::manager::is_request_id_key(&canonical).then_some(canonical)
}

fn request_span(
    job_id: Option<&str>,
    key: Option<&str>,
    offset: Option<u64>,
    range: Option<&str>,
) -> tracing::Span {
    let span = tracing::info_span!(
        "bobs.request",
        "request.id" = tracing::field::Empty,
        "bobs.spool.key" = tracing::field::Empty,
        offset = tracing::field::Empty,
        range = tracing::field::Empty,
    );
    if let Some(job_id) = job_id {
        span.record("request.id", job_id);
    }
    if let Some(key) = key {
        span.record("bobs.spool.key", key);
    }
    if let Some(offset) = offset {
        span.record("offset", offset);
    }
    if let Some(range) = range {
        span.record("range", range);
    }
    span
}

enum ReadRequestRange {
    Follow,
    Bounded {
        start: u64,
        end_inclusive: Option<u64>,
    },
    Suffix {
        len: u64,
    },
}

pub struct AppState<F: FileIO, M: MetadataStore> {
    pub manager: Arc<SpoolManager<F, M>>,
    pub config: Arc<Config>,
    pub hostname: String,
    pub ordinal: String,
    pub internal_base_url: String,
    pub metrics: Arc<BobsMetrics>,
}

#[derive(Deserialize)]
struct PprofParams {
    seconds: Option<u64>,
}

/// On-demand CPU sampling profiler. `GET /debug/pprof/profile?seconds=N` runs an
/// in-process pprof CPU profile for N seconds (default 30) and returns a
/// flamegraph SVG. Used to find BOBS's per-pod CPU hot path under load.
async fn pprof_profile<F: FileIO, M: MetadataStore>(
    State(state): State<Arc<AppState<F, M>>>,
    Query(params): Query<PprofParams>,
) -> Response {
    if !state.config.enable_pprof {
        return StatusCode::NOT_FOUND.into_response();
    }
    let seconds = params.seconds.unwrap_or(30).clamp(1, 120);
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(199)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("profiler init: {e}"),
            )
                .into_response();
        }
    };
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let report = match guard.report().build() {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::INTERNAL_SERVER_ERROR, format!("report: {e}")).into_response();
        }
    };
    let mut svg = Vec::new();
    if let Err(e) = report.flamegraph(&mut svg) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("flamegraph: {e}"),
        )
            .into_response();
    }
    ([(axum::http::header::CONTENT_TYPE, "image/svg+xml")], svg).into_response()
}

pub fn router<F, M>() -> Router<Arc<AppState<F, M>>>
where
    F: FileIO + 'static,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/v1/health", get(health::<F, M>))
        .route("/api/v1/status", get(health::<F, M>).head(status_head))
        .route("/api/v1/create", put(create_spool::<F, M>))
        .route("/api/v1/write/{key}/{offset}", post(write_spool::<F, M>))
        .route("/api/v1/complete/{key}", post(complete_spool::<F, M>))
        .route("/api/v1/read/{key}", get(read_spool::<F, M>))
        .route("/api/v1/delete/{key}", delete(delete_spool::<F, M>))
        .route("/debug/pprof/profile", get(pprof_profile::<F, M>))
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    hostname: String,
}

async fn health<F: FileIO, M: MetadataStore>(
    State(state): State<Arc<AppState<F, M>>>,
) -> impl IntoResponse {
    Json(StatusResponse {
        status: "ok",
        hostname: state.hostname.clone(),
    })
}

async fn status_head() -> impl IntoResponse {
    StatusCode::OK
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateRequest {
    content_type: Option<String>,
    content_encoding: Option<String>,
    #[serde(default)]
    write_locked: bool,
    /// Caller-provided labels propagated to metrics as OTel attributes.
    #[serde(default)]
    labels: HashMap<String, String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CompleteRequest {
    expected_size: Option<u64>,
}

fn validate_producer_header(
    name: &str,
    value: &Option<String>,
) -> std::result::Result<(), ApiError> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        return Err(ApiError(BobsError::InvalidRequest(format!(
            "{name} must not be empty"
        ))));
    }
    HeaderValue::from_str(value).map_err(|error| {
        ApiError(BobsError::InvalidRequest(format!(
            "invalid {name}: {error}"
        )))
    })?;
    Ok(())
}

fn enforce_content_length(
    headers: &HeaderMap,
    offset: u64,
    max_spool_bytes: u64,
) -> std::result::Result<(), BobsError> {
    let Some(value) = headers.get(axum::http::header::CONTENT_LENGTH) else {
        return Ok(());
    };
    let length = value
        .to_str()
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| BobsError::InvalidRequest("invalid Content-Length".into()))?;
    if offset
        .checked_add(length)
        .is_none_or(|total| total > max_spool_bytes)
    {
        return Err(BobsError::SpoolTooLarge {
            max_bytes: max_spool_bytes,
        });
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct CreateResponse {
    key: String,
    read_url: String,
    write_url: String,
}

async fn create_spool<F, M>(
    State(state): State<Arc<AppState<F, M>>>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Response, ApiError>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let job_id = extract_job_id(&headers);
    let span = request_span(job_id.as_deref(), None, None, None);
    async move {
        let req = if body.is_empty() {
            CreateRequest::default()
        } else {
            serde_json::from_slice::<CreateRequest>(&body)
                .map_err(|e| ApiError(BobsError::InvalidRequest(e.to_string())))?
        };
        validate_producer_header("content_type", &req.content_type)?;
        validate_producer_header("content_encoding", &req.content_encoding)?;
        let labels = state.config.filter_labels(&req.labels);
        // Key the spool by the originating request ID when the caller supplies
        // one (X-Polytope-Job-Id), so spool directories, read URLs and logs all
        // line up with the request ID users quote. Callers without a request ID
        // (e.g. ad-hoc tooling) still get an anonymous UUID.
        let key = job_id
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string());
        let create_start = Instant::now();
        let create_result = state
            .manager
            .create_spool_with_admission_timeout(
                Duration::from_millis(state.config.create_admission_timeout_ms),
                key.clone(),
                req.content_type.clone(),
                req.content_encoding.clone(),
                req.write_locked,
                labels.clone(),
            )
            .await;
        state.metrics.record_create_duration(
            &labels,
            if create_result.is_ok() { crate::metrics::outcome::SUCCESS } else { crate::metrics::outcome::ERROR },
            create_start.elapsed().as_secs_f64(),
        );
        create_result.map_err(ApiError)?;
        state.metrics.record_spool_created(&labels);
        tracing::Span::current().record("bobs.spool.key", key.as_str());
        if let Some(job_id) = &job_id {
            tracing::info!("event.name" = "bobs.spool.created", "request.id" = %job_id, "bobs.spool.key" = %key, content_type = ?req.content_type, content_encoding = ?req.content_encoding, write_locked = req.write_locked, outcome = "success", "spool created");
        } else {
            tracing::info!("event.name" = "bobs.spool.created", "bobs.spool.key" = %key, content_type = ?req.content_type, content_encoding = ?req.content_encoding, write_locked = req.write_locked, outcome = "success", "spool created");
        }
        let read_url = format!(
            "https://{}.{}/{}-{}/{}",
            state.config.host_prefix, state.config.domain, state.config.route_name, state.ordinal, key
        );
        let write_url = state.internal_base_url.clone();
        Ok((
            StatusCode::CREATED,
            Json(CreateResponse {
                key,
                read_url,
                write_url,
            }),
        )
            .into_response())
    }.instrument(span).await
}

async fn write_spool<F, M>(
    State(state): State<Arc<AppState<F, M>>>,
    Path((key, offset)): Path<(String, u64)>,
    headers: HeaderMap,
    mut body: Body,
) -> std::result::Result<Response, ApiError>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let job_id = extract_job_id(&headers);
    let span = request_span(job_id.as_deref(), Some(&key), Some(offset), None);
    async move {
        let spool = state
            .manager
            .get_spool(&key)
            .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;
        let labels = spool.metadata.lock().await.labels.clone();
        let write_start = Instant::now();
        let write_batch_size = state.config.page_size;
        // Start empty so body size hints cannot trigger eager reservation. Full-frame
        // pages remain zero-copy; partial staging grows only with received bytes and
        // is bounded by the validated 64 MiB page-size ceiling.
        let mut pending = bytes::BytesMut::new();
        let mut write_offset = offset;
        let mut received_bytes = 0_u64;
        let write_result: std::result::Result<(), crate::error::BobsError> = async {
            // Check a known size only after resolving the target spool so an
            // oversized request follows the same durable cleanup path as a
            // chunked body that crosses the limit. This still happens before
            // polling the body.
            enforce_content_length(&headers, offset, state.config.max_spool_bytes)?;
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| BobsError::SerializationError(e.to_string()))?;
                if let Ok(data) = frame.into_data() {
                    // Only non-empty payload frames are activity. Empty requests still
                    // validate state and offset below without extending the writer TTL.
                    if data.is_empty() {
                        continue;
                    }
                    received_bytes = received_bytes
                        .checked_add(data.len() as u64)
                        .ok_or(BobsError::SpoolTooLarge {
                            max_bytes: state.config.max_spool_bytes,
                        })?;
                    if offset
                        .checked_add(received_bytes)
                        .is_none_or(|total| total > state.config.max_spool_bytes)
                    {
                        return Err(BobsError::SpoolTooLarge {
                            max_bytes: state.config.max_spool_bytes,
                        });
                    }
                    // Refresh under the lifecycle lock before buffering an accepted
                    // frame so cleanup cannot act on a stale inactivity snapshot.
                    spool.refresh_write_activity(now_secs()).await?;
                    let mut cursor = 0;

                    if !pending.is_empty() {
                        let take = (write_batch_size - pending.len()).min(data.len());
                        pending.extend_from_slice(&data[..take]);
                        cursor = take;
                        if pending.len() == write_batch_size {
                            let batch = pending.split().freeze();
                            let batch_len = batch.len();
                            spool.write(write_offset, batch).await?;
                            write_offset += batch_len as u64;
                        }
                    }

                    // Full pages already owned by the body frame need no staging copy.
                    while data.len() - cursor >= write_batch_size {
                        let end = cursor + write_batch_size;
                        let batch = data.slice(cursor..end);
                        cursor = end;
                        spool.write(write_offset, batch).await?;
                        write_offset += write_batch_size as u64;
                    }

                    if cursor < data.len() {
                        pending.extend_from_slice(&data[cursor..]);
                    }
                }
            }
            if pending.is_empty() {
                if received_bytes == 0 {
                    spool.write(write_offset, Bytes::new()).await?;
                }
            } else {
                let batch_len = pending.len();
                spool.write(write_offset, pending.freeze()).await?;
                write_offset += batch_len as u64;
            }
            Ok(())
        }
        .await;
        let write_elapsed = write_start.elapsed().as_secs_f64();
        let total_written = write_offset.saturating_sub(offset);
        if write_result.is_ok() {
            state.metrics.record_write_bytes(&labels, total_written);
        }
        state.metrics.record_write_duration(
            &labels,
            if write_result.is_ok() { crate::metrics::outcome::SUCCESS } else { crate::metrics::outcome::ERROR },
            write_elapsed,
        );
        if matches!(write_result, Err(BobsError::SpoolTooLarge { .. })) {
            // A known-length request can be rejected before polling its body,
            // while a chunked body can cross the limit after full pages have
            // reached disk. In either case, commit a durable deletion before
            // reporting 413 so the object cannot reappear after restart and its
            // admission slot is reusable. A cleanup failure is a server error,
            // not a safe payload rejection.
            state
                .manager
                .delete_oversize_spool_if_write_head(&key, write_offset, job_id.as_deref())
                .await
                .map_err(ApiError)?;
        }
        write_result.map_err(ApiError)?;
        if let Some(job_id) = &job_id {
            tracing::debug!("event.name" = "bobs.spool.write.completed", "request.id" = %job_id, "bobs.spool.key" = %key, offset = offset, bytes = total_written, outcome = "success", "spool write completed");
        } else {
            tracing::debug!("event.name" = "bobs.spool.write.completed", "bobs.spool.key" = %key, offset = offset, bytes = total_written, outcome = "success", "spool write completed");
        }
        Ok(StatusCode::OK.into_response())
    }.instrument(span).await
}

async fn complete_spool<F, M>(
    State(state): State<Arc<AppState<F, M>>>,
    Path(key): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> std::result::Result<Response, ApiError>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let job_id = extract_job_id(&headers);
    let span = request_span(job_id.as_deref(), Some(&key), None, None);
    async move {
        let req = if body.is_empty() {
            CompleteRequest::default()
        } else {
            serde_json::from_slice::<CompleteRequest>(&body)
                .map_err(|e| ApiError(BobsError::InvalidRequest(e.to_string())))?
        };
        let spool = state
            .manager
            .get_spool(&key)
            .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;
        let labels = spool.metadata.lock().await.labels.clone();
        let complete_start = Instant::now();
        let complete_result = spool.complete(req.expected_size).await;
        state.metrics.record_complete_duration(
            &labels,
            if complete_result.is_ok() { crate::metrics::outcome::SUCCESS } else { crate::metrics::outcome::ERROR },
            complete_start.elapsed().as_secs_f64(),
        );
        complete_result.map_err(ApiError)?;
        state.metrics.record_spool_completed(&labels);
        let meta = spool.metadata.lock().await;
        if let Some(job_id) = &job_id {
            tracing::info!("event.name" = "bobs.spool.completed", "request.id" = %job_id, "bobs.spool.key" = %key, expected_size = ?req.expected_size, bytes = meta.total_bytes_written, outcome = "success", "spool completed");
        } else {
            tracing::info!("event.name" = "bobs.spool.completed", "bobs.spool.key" = %key, expected_size = ?req.expected_size, bytes = meta.total_bytes_written, outcome = "success", "spool completed");
        }
        Ok(StatusCode::OK.into_response())
    }.instrument(span).await
}

/// RAII guard that decrements the spool's reader count on drop, ensuring cleanup
/// sees the correct active reader count even if the stream is cancelled mid-flight.
struct ReaderLease<F, M>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    spool: Arc<crate::spool::Spool<F, M>>,
    metrics: Arc<BobsMetrics>,
    labels: HashMap<String, String>,
    mode: &'static str,
    started_at: Instant,
    duration_recorded: bool,
    bytes_served: u64,
}

fn read_page_chunk(page: &Bytes, slice_start: usize, slice_end: usize) -> Bytes {
    page.slice(slice_start..slice_end)
}

struct ReadMetadata {
    content_type: Option<String>,
    content_encoding: Option<String>,
    complete_size: Option<u64>,
    total_bytes_written: u64,
    servable_bytes: u64,
}

struct ResolvedReadRange {
    start: u64,
    end: Option<u64>,
    follow: bool,
}

fn resolve_read_range(
    request_range: ReadRequestRange,
    metadata: &ReadMetadata,
) -> crate::error::Result<ResolvedReadRange> {
    match request_range {
        // A no-Range request follows an in-progress object. Once the object is
        // complete, the final size is known and the same request becomes a
        // bounded full-object read so clients can receive Content-Length.
        ReadRequestRange::Follow => Ok(ResolvedReadRange {
            start: 0,
            end: metadata.complete_size,
            follow: true,
        }),
        ReadRequestRange::Bounded {
            start,
            end_inclusive,
        } => {
            let requested_end = match end_inclusive {
                Some(end_inclusive) => end_inclusive
                    .checked_add(1)
                    .ok_or_else(|| BobsError::InvalidRange("range end overflow".into()))?,
                None => metadata.total_bytes_written,
            };
            // Bounded range responses must only advertise bytes that this
            // response can actually serve. For in-progress spools, recovered or
            // freshly written trailing partial bytes contribute to offset
            // validation (`total_bytes_written`) but are not servable until the
            // page is completed or the spool is completed.
            Ok(ResolvedReadRange {
                start,
                end: Some(requested_end.min(metadata.servable_bytes)),
                follow: false,
            })
        }
        ReadRequestRange::Suffix { len } => {
            let Some(total) = metadata.complete_size else {
                return Err(BobsError::RangeNotSatisfiable {
                    total: None,
                    reason: "suffix range requires complete spool".into(),
                });
            };
            if len == 0 {
                return Err(BobsError::RangeNotSatisfiable {
                    total: Some(total),
                    reason: "suffix range length is zero".into(),
                });
            }
            Ok(ResolvedReadRange {
                start: total.saturating_sub(len),
                end: Some(total),
                follow: false,
            })
        }
    }
}

fn validate_resolved_range(
    range: &ResolvedReadRange,
    metadata: &ReadMetadata,
) -> crate::error::Result<()> {
    if let Some(end) = range.end {
        if range.start > end {
            return Err(BobsError::RangeNotSatisfiable {
                total: metadata.complete_size,
                reason: "range start exceeds end".into(),
            });
        }
    }

    if !range.follow && range.start >= metadata.servable_bytes {
        return Err(BobsError::RangeNotSatisfiable {
            total: metadata.complete_size,
            reason: "range start exceeds servable bytes".into(),
        });
    }

    Ok(())
}

fn apply_read_response_headers(
    response: &mut Response,
    metadata: &ReadMetadata,
    range: &ResolvedReadRange,
) -> std::result::Result<(), ApiError> {
    let content_type = metadata
        .content_type
        .as_deref()
        .unwrap_or("application/octet-stream");
    let content_type_header = HeaderValue::from_str(content_type)
        .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, content_type_header);

    response.headers_mut().insert(
        "X-Content-Type-Options",
        HeaderValue::from_static("nosniff"),
    );
    // The payload and media type are producer-controlled and BOBS serves every
    // spool from one deployment origin. Force download as the browser-level
    // containment boundary; nosniff/CSP remain defence in depth for user agents
    // that render despite Content-Disposition.
    response.headers_mut().insert(
        axum::http::header::CONTENT_DISPOSITION,
        HeaderValue::from_static("attachment"),
    );
    let base_content_type = content_type.split(';').next().unwrap_or_default().trim();
    if ["text/html", "application/xhtml+xml", "image/svg+xml"]
        .iter()
        .any(|active| base_content_type.eq_ignore_ascii_case(active))
    {
        // Spools share a deployment origin. Sandboxing producer-controlled active
        // documents prevents them from inheriting that origin or running script.
        response.headers_mut().insert(
            "Content-Security-Policy",
            HeaderValue::from_static("sandbox; default-src 'none'; frame-ancestors 'none'"),
        );
    }
    if let Some(enc) = &metadata.content_encoding {
        let encoding_header = HeaderValue::from_str(enc)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_ENCODING, encoding_header);
    }

    // Content-Length must be the number of bytes *this response will deliver*,
    // not the total object size. Bounded Range reads know the exact byte count.
    // No-Range reads know it too once the spool is complete; in-progress follow
    // reads keep `end = None` and therefore use chunked encoding.
    if let Some(range_end) = range.end {
        let response_bytes = range_end.saturating_sub(range.start);
        response.headers_mut().insert(
            axum::http::header::CONTENT_LENGTH,
            HeaderValue::from(response_bytes),
        );
    }
    response
        .headers_mut()
        .insert("X-Accel-Buffering", HeaderValue::from_static("no"));
    response.headers_mut().insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );

    if !range.follow {
        let total = metadata
            .complete_size
            .map(|v| v.to_string())
            .unwrap_or_else(|| "*".to_string());
        let last_byte = match range.end {
            Some(e) => e.saturating_sub(1),
            None => range.start,
        };
        let content_range = format!("bytes {}-{}/{}", range.start, last_byte, total);
        let content_range_header = HeaderValue::from_str(&content_range)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_RANGE, content_range_header);
    }

    Ok(())
}

impl<F, M> Drop for ReaderLease<F, M>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.spool.release_reader();
        self.metrics.record_reader_released(&self.labels);
        if !self.duration_recorded {
            self.metrics
                .record_read_bytes(&self.labels, self.mode, self.bytes_served);
            self.metrics.record_read_duration(
                &self.labels,
                self.mode,
                crate::metrics::outcome::CLIENT_GONE,
                self.started_at.elapsed().as_secs_f64(),
            );
        }
    }
}

async fn read_spool<F, M>(
    State(state): State<Arc<AppState<F, M>>>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> std::result::Result<Response, ApiError>
where
    F: FileIO + 'static,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let job_id = extract_job_id(&headers);
    let raw_range = headers
        .get(axum::http::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("follow")
        .to_string();
    let request_range = parse_range(headers.get(axum::http::header::RANGE)).map_err(ApiError)?;
    let span = request_span(job_id.as_deref(), Some(&key), None, Some(&raw_range));

    async move {
        let spool = state
            .manager
            .get_spool(&key)
            .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;

        {
            let _lifecycle_guard = spool.lifecycle_lock.lock().await;
            let meta = spool.metadata.lock().await;
            if meta.state == crate::spool::SpoolState::Deleting {
                return Err(ApiError(BobsError::SpoolNotFound { key: key.clone() }));
            }
            if !meta.state.is_readable() || (meta.state == crate::spool::SpoolState::Writing && meta.write_locked) {
                return Err(ApiError(BobsError::SpoolLocked));
            }
            spool.acquire_reader();
        }

        let read_labels = spool.metadata.lock().await.labels.clone();
        let read_mode = if matches!(request_range, ReadRequestRange::Follow) {
            crate::metrics::mode::FOLLOW
        } else {
            crate::metrics::mode::RANGE
        };
        state.metrics.record_reader_acquired(&read_labels);
        let read_start = Instant::now();

        let mut lease = ReaderLease {
            spool: Arc::clone(&spool),
            metrics: Arc::clone(&state.metrics),
            labels: read_labels.clone(),
            mode: read_mode,
            started_at: read_start,
            duration_recorded: false,
            bytes_served: 0,
        };
        let long_poll_timeout = Duration::from_millis(state.config.long_poll_timeout_ms);
        let page_size = spool.page_size as u64;
        let metadata = {
            let meta = spool.metadata.lock().await;
            let is_complete = meta.state == crate::spool::SpoolState::Complete;
            let complete_size = if is_complete {
                Some(meta.total_bytes_written)
            } else {
                None
            };
            let servable_bytes = if is_complete {
                meta.total_bytes_written
            } else {
                meta.total_pages * page_size
            };
            ReadMetadata {
                content_type: meta.content_type.clone(),
                content_encoding: meta.content_encoding.clone(),
                complete_size,
                total_bytes_written: meta.total_bytes_written,
                servable_bytes,
            }
        };
        let range = resolve_read_range(request_range, &metadata).map_err(ApiError)?;
        validate_resolved_range(&range, &metadata).map_err(ApiError)?;
        let ResolvedReadRange { start, end, follow } = range;
        let response_range = ResolvedReadRange { start, end, follow };
        tracing::info!("event.name" = "bobs.spool.read.started", "bobs.spool.key" = %key, range = %raw_range, start = start, end = ?end, follow = follow, outcome = "success", "spool read started");

    // Pre-fetch the first page before committing to a streaming response.
    // If the timeout fires before any data arrives, return a 307 redirect
    // so standard clients (curl -L, browsers) retry automatically.
    let first_page_idx = start / page_size;
    let first_page = match tokio::time::timeout(long_poll_timeout, spool.read_page(first_page_idx))
        .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => {
            lease.duration_recorded = true;
            state.metrics.record_read_duration(&read_labels, read_mode, crate::metrics::outcome::ERROR, read_start.elapsed().as_secs_f64());
            return Err(ApiError(e));
        }
        Err(_) => {
            tracing::warn!("event.name" = "bobs.spool.read.timeout", "bobs.spool.key" = %key, range = %raw_range, start = start, end = ?end, follow = follow, outcome = "error", "spool read timed out");
            lease.duration_recorded = true;
            state.metrics.record_read_duration(&read_labels, read_mode, crate::metrics::outcome::TIMEOUT, read_start.elapsed().as_secs_f64());
            return Ok(long_poll_redirect(&key, &headers));
        }
    };

    let stream_job_id = job_id.clone();
    let stream_key = key.clone();
    let stream_range = raw_range.clone();
    let stream_metrics = Arc::clone(&state.metrics);
    let stream_labels = read_labels.clone();
    let stream_mode = read_mode;
    let stream = stream! {
        let mut lease = lease;
        let mut offset = start;
        let mut bytes_served = 0_u64;
        let mut outcome = crate::metrics::outcome::SUCCESS;
        let mut prefetched = first_page;

        loop {
            if let Some(end) = end {
                if offset >= end {
                    break;
                }
            }

            // Map the current byte offset to a page-aligned read.
            let page_idx = offset / page_size;
            let page_start = page_idx * page_size;
            let page_end = page_start + page_size;

            // First iteration uses the pre-fetched page; subsequent follow-mode
            // reads long-poll with a timeout. Once response bytes have been sent,
            // a timeout must abort the chunked body rather than look like clean EOF.
            let maybe_page = if let Some(page) = prefetched.take() {
                Some(page)
            } else if follow {
                match tokio::time::timeout(
                    long_poll_timeout,
                    spool.read_page(page_idx),
                ).await {
                    Ok(Ok(v)) => v,
                    Ok(Err(e)) => {
                        outcome = crate::metrics::outcome::ERROR;
                        yield Err::<Bytes, BobsError>(e);
                        break;
                    }
                    Err(_) => {
                        outcome = crate::metrics::outcome::TIMEOUT;
                        tracing::warn!("event.name" = "bobs.spool.read.timeout", "bobs.spool.key" = %stream_key, range = %stream_range, start = start, end = ?end, follow = follow, bytes = bytes_served, outcome = "error", "spool read timed out mid-stream");
                        lease.duration_recorded = true;
                        stream_metrics.record_read_bytes(&stream_labels, stream_mode, bytes_served);
                        stream_metrics.record_read_duration(&stream_labels, stream_mode, outcome, read_start.elapsed().as_secs_f64());
                        yield Err::<Bytes, BobsError>(BobsError::IoError(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "follow read long-poll timed out",
                        )));
                        break;
                    }
                }
            } else {
                match spool.read_page(page_idx).await {
                    Ok(v) => v,
                    Err(e) => {
                        outcome = crate::metrics::outcome::ERROR;
                        yield Err::<Bytes, BobsError>(e);
                        break;
                    }
                }
            };
            let Some(page) = maybe_page else {
                break;
            };

            // Slice the page to the requested byte range. The offset may not be
            // page-aligned (partial first page) and the end may land mid-page.
            let slice_start = (offset - page_start) as usize;
            let logical_end = end.unwrap_or(page_end).min(page_end);
            let slice_end = ((logical_end - page_start) as usize).min(page.len());

            if slice_start < slice_end {
                let chunk = read_page_chunk(&page, slice_start, slice_end);
                let chunk_start = offset;
                let chunk_len = chunk.len() as u64;
                offset += chunk_len;
                bytes_served += chunk_len;
                lease.bytes_served = bytes_served;
                let chunk_end = offset;

                // Refresh monotonic activity and coverage under the lifecycle lock so
                // cleanup cannot act on a stale eligibility snapshot.
                spool
                    .mark_served_and_maybe_fully_read(chunk_start, chunk_end)
                    .await;

                // 4. If this chunk completes a bounded read, record duration
                //    NOW before yielding. hyper drops the response body
                //    without a final poll once Content-Length is satisfied,
                //    so the post-loop cleanup would never execute.
                if end.is_some_and(|e| offset >= e) && !lease.duration_recorded {
                    let completion_span = request_span(
                        stream_job_id.as_deref(),
                        Some(&stream_key),
                        None,
                        Some(&stream_range),
                    );
                    let _guard = completion_span.enter();
                    tracing::info!("event.name" = "bobs.spool.read.completed", bytes = bytes_served, outcome = outcome, "spool read completed");
                    lease.duration_recorded = true;
                    stream_metrics.record_read_bytes(&stream_labels, stream_mode, bytes_served);
                    stream_metrics.record_read_duration(&stream_labels, stream_mode, outcome, read_start.elapsed().as_secs_f64());
                }

                yield Ok::<Bytes, BobsError>(chunk);
            } else if follow {
                // Page exists but has no data at our offset yet. Check if the
                // writer is done; if so, we've consumed everything. Otherwise
                // loop back and long-poll for more data.
                let done = {
                    let meta = spool.metadata.lock().await;
                    meta.state == crate::spool::SpoolState::Complete
                        && offset >= meta.total_bytes_written
                };
                if done {
                    break;
                }
                continue;
            } else {
                break;
            }
        }
        // Post-loop fallback for chunked responses (follow mode on
        // in-progress spools) where Content-Length is not set and hyper
        // polls the stream to completion normally.
        if !lease.duration_recorded {
            let completion_span = request_span(
                stream_job_id.as_deref(),
                Some(&stream_key),
                None,
                Some(&stream_range),
            );
            let _completion_span_guard = completion_span.enter();
            tracing::info!("event.name" = "bobs.spool.read.completed", bytes = bytes_served, outcome = outcome, "spool read completed");
            lease.duration_recorded = true;
            stream_metrics.record_read_bytes(&stream_labels, stream_mode, bytes_served);
            stream_metrics.record_read_duration(&stream_labels, stream_mode, outcome, read_start.elapsed().as_secs_f64());
        }
    };

    let mut response = Body::from_stream(stream).into_response();
    *response.status_mut() = if follow {
        StatusCode::OK
    } else {
        StatusCode::PARTIAL_CONTENT
    };

    apply_read_response_headers(&mut response, &metadata, &response_range)?;

    Ok(response)
    }
    .instrument(span)
    .await
}

fn long_poll_redirect(key: &str, headers: &HeaderMap) -> Response {
    let prefix = headers
        .get("X-Forwarded-Prefix")
        .and_then(|value| validated_forwarded_prefix(value.as_bytes()));
    let location = match prefix {
        Some(prefix) => format!("{prefix}/api/v1/read/{key}"),
        None => format!("/api/v1/read/{key}"),
    };
    let mut response = (
        StatusCode::TEMPORARY_REDIRECT,
        [(axum::http::header::LOCATION, location)],
    )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    response.headers_mut().insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    response
}

fn validated_forwarded_prefix(raw: &[u8]) -> Option<&str> {
    if raw.is_empty() || raw.len() > 64 || raw[0] != b'/' {
        return None;
    }

    let mut segment_start = 1;
    for (idx, &byte) in raw.iter().enumerate() {
        if matches!(byte, 0x00..=0x1f | 0x7f | b'\\' | b'%' | b'@' | b'?' | b'#') {
            return None;
        }
        if byte == b'/' {
            if idx != 0 {
                if idx == segment_start || &raw[segment_start..idx] == b".." {
                    return None;
                }
                segment_start = idx + 1;
            }
            continue;
        }
        if !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'~' | b'-') {
            return None;
        }
    }
    if segment_start == raw.len() || &raw[segment_start..] == b".." {
        return None;
    }

    // Trust assumption: in-cluster callers can set X-Forwarded-Prefix directly.
    // BOBS' in-cluster write/read surface is trusted-by-design; this strict
    // allowlist bounds redirects to short relative path prefixes.
    std::str::from_utf8(raw).ok()
}

fn parse_range(header: Option<&HeaderValue>) -> crate::error::Result<ReadRequestRange> {
    let Some(val) = header else {
        return Ok(ReadRequestRange::Follow);
    };
    let raw = val
        .to_str()
        .map_err(|_| BobsError::InvalidRange("range header is not valid ASCII".into()))?;
    let body = raw
        .strip_prefix("bytes=")
        .ok_or_else(|| BobsError::InvalidRange(format!("unsupported range unit: {raw}")))?;
    let (start_s, end_s) = body
        .split_once('-')
        .ok_or_else(|| BobsError::InvalidRange(format!("malformed range: {raw}")))?;
    if start_s.is_empty() {
        let len = end_s.parse::<u64>().map_err(|_| {
            BobsError::InvalidRange(format!("suffix range length is invalid: {raw}"))
        })?;
        return Ok(ReadRequestRange::Suffix { len });
    }
    let start = start_s
        .parse::<u64>()
        .map_err(|_| BobsError::InvalidRange(format!("range start is invalid: {raw}")))?;

    if end_s.is_empty() {
        return Ok(ReadRequestRange::Bounded {
            start,
            end_inclusive: None,
        });
    }

    let end_inclusive = end_s
        .parse::<u64>()
        .map_err(|_| BobsError::InvalidRange(format!("range end is invalid: {raw}")))?;
    if start > end_inclusive {
        return Err(BobsError::InvalidRange(format!(
            "range start exceeds end: {raw}"
        )));
    }

    Ok(ReadRequestRange::Bounded {
        start,
        end_inclusive: Some(end_inclusive),
    })
}

async fn delete_spool<F, M>(
    State(state): State<Arc<AppState<F, M>>>,
    Path(key): Path<String>,
    headers: HeaderMap,
) -> std::result::Result<Response, ApiError>
where
    F: FileIO,
    M: MetadataStore + Clone + Send + Sync + 'static,
{
    let job_id = extract_job_id(&headers);
    let span = request_span(job_id.as_deref(), Some(&key), None, None);
    async move {
        // Capture labels before deletion removes the spool from memory.
        let delete_labels = state.manager.get_spool(&key);
        let labels = if let Some(spool) = &delete_labels {
            spool.metadata.lock().await.labels.clone()
        } else {
            std::collections::HashMap::new()
        };
        drop(delete_labels);
        state
            .manager
            .delete_spool_with_reason(
                &key,
                crate::manager::DeleteReason::Explicit,
                job_id.as_deref(),
            )
            .await
            .map_err(ApiError)?;
        state
            .metrics
            .record_spool_deleted(&labels, crate::metrics::reason::CLIENT);
        Ok(StatusCode::OK.into_response())
    }
    .instrument(span)
    .await
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

struct ApiError(BobsError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            BobsError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
            BobsError::SpoolTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            BobsError::AdmissionTimeout => StatusCode::SERVICE_UNAVAILABLE,
            BobsError::SpoolNotFound { .. } => StatusCode::NOT_FOUND,
            BobsError::SpoolAlreadyExists { .. } => StatusCode::CONFLICT,
            BobsError::OffsetMismatch { .. } => StatusCode::BAD_REQUEST,
            BobsError::SizeMismatch { .. } => StatusCode::BAD_REQUEST,
            BobsError::InvalidRange(_) => StatusCode::BAD_REQUEST,
            BobsError::RangeNotSatisfiable { .. } => StatusCode::RANGE_NOT_SATISFIABLE,
            BobsError::SpoolLocked => StatusCode::LOCKED,
            BobsError::SpoolClosed => StatusCode::CONFLICT,
            BobsError::InvalidState { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let mut response = (
            status,
            Json(ErrorResponse {
                error: self.0.to_string(),
            }),
        )
            .into_response();
        if let BobsError::RangeNotSatisfiable { total, .. } = &self.0 {
            let value = total
                .map(|total| format!("bytes */{total}"))
                .unwrap_or_else(|| "bytes */*".to_string());
            if let Ok(header) = HeaderValue::from_str(&value) {
                response
                    .headers_mut()
                    .insert(axum::http::header::CONTENT_RANGE, header);
            }
        }
        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cleanup::start_cleanup_task;
    use crate::config::MetricsConfig;
    use crate::error::BobsError;
    use crate::io::{DefaultFileIO, TokioFileIO};
    use crate::metadata::{DefaultMetadataStore, SyncSidecarMetadataStore};
    use axum::http::Request;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use std::sync::atomic::Ordering;
    use tower::ServiceExt;

    fn test_config(dir: &std::path::Path) -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: dir.to_path_buf(),
            page_size: 4096,
            max_cache_bytes: 65536,
            max_live_spools: 256,
            max_spool_bytes: 1024 * 1024,
            create_admission_timeout_ms: 100,
            enable_pprof: false,
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
            route_name: "bobs".into(),
            metrics: MetricsConfig::default(),
        })
    }

    /// Short-TTL config for cleanup integration tests.
    /// sweep=1s so a single `tokio::time::advance(3s)` triggers several sweeps.
    /// read_idle_ttl_secs=2 / full_read_complete_ttl_secs=2 are short but non-zero.
    fn test_config_ttl(dir: &std::path::Path) -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: dir.to_path_buf(),
            page_size: 4096,
            max_cache_bytes: 65536,
            max_live_spools: 256,
            max_spool_bytes: 1024 * 1024,
            create_admission_timeout_ms: 100,
            enable_pprof: false,
            writer_inactivity_timeout_secs: 300,
            read_idle_ttl_secs: 2,
            full_read_complete_ttl_secs: 2,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 1,
            long_poll_timeout_ms: 25000,
            io_uring_shards: None,
            io_uring_queue_capacity: 1024,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
            metrics: MetricsConfig::default(),
        })
    }

    async fn app_with_config_and_metrics(
        configure: impl FnOnce(&mut Config),
        metrics: Arc<BobsMetrics>,
    ) -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let data_dir = root.join("data");
        let mut config = (*test_config(&data_dir)).clone();
        configure(&mut config);
        let config = Arc::new(config);
        let manager = Arc::new(
            SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
                DefaultMetadataStore::new(&data_dir),
                &data_dir,
                config.page_size,
                config.max_cache_bytes,
                config.max_live_spools,
            )
            .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config,
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
            internal_base_url: "http://bobs-0:3000/api/v1".into(),
            metrics,
        });
        let app = router::<DefaultFileIO, DefaultMetadataStore>().with_state(Arc::clone(&state));
        (app, state)
    }

    async fn app_with_config(
        configure: impl FnOnce(&mut Config),
    ) -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        app_with_config_and_metrics(configure, Arc::new(BobsMetrics::new(false))).await
    }

    async fn app_with_options(
        long_poll_timeout_ms: u64,
        metrics: Arc<BobsMetrics>,
    ) -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        app_with_config_and_metrics(
            |config| config.long_poll_timeout_ms = long_poll_timeout_ms,
            metrics,
        )
        .await
    }

    /// Returns both the `Router` and the shared `AppState` so tests can inspect
    /// spool fields after HTTP round-trips.
    async fn app_with_state() -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        app_with_config(|_| {}).await
    }

    /// Like `app_with_state` but uses short TTLs and the synchronous sidecar
    /// backend so paused-time tests do not depend on io_uring completion timing.
    async fn app_with_ttl_config() -> (Router, Arc<AppState<TokioFileIO, SyncSidecarMetadataStore>>)
    {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let data_dir = root.join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO, SyncSidecarMetadataStore>::with_metadata_store(
                SyncSidecarMetadataStore::new(&data_dir),
                &data_dir,
                4096,
                65536,
                256,
            )
            .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config_ttl(&data_dir),
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
            internal_base_url: "http://bobs-0:3000/api/v1".into(),
            metrics: Arc::new(BobsMetrics::new(false)),
        });
        let app = router::<TokioFileIO, SyncSidecarMetadataStore>().with_state(Arc::clone(&state));
        (app, state)
    }

    async fn app() -> Router {
        app_with_state().await.0
    }

    // -----------------------------------------------------------------------
    // Shared test helpers
    // -----------------------------------------------------------------------

    async fn wait_for_spool_removal<F, M>(manager: &SpoolManager<F, M>, key: &str)
    where
        F: FileIO,
        M: MetadataStore + Clone + Send + Sync + 'static,
    {
        for _ in 0..2_000 {
            if manager.get_spool(key).is_none() {
                return;
            }
            tokio::task::spawn_blocking(|| std::thread::sleep(Duration::from_millis(1)))
                .await
                .expect("removal wait task panicked");
        }
        panic!("spool {key} was not removed after cleanup was scheduled");
    }

    #[test]
    fn read_page_chunk_uses_zero_copy_slice() {
        let page = Bytes::from((0u8..64).collect::<Vec<_>>());
        let slice_start = 7;
        let slice_end = 23;

        let chunk = read_page_chunk(&page, slice_start, slice_end);

        assert_eq!(&chunk[..], &page[slice_start..slice_end]);
        assert_eq!(
            chunk.as_ptr(),
            unsafe { page.as_ptr().add(slice_start) },
            "chunk should point into the cached page allocation"
        );
    }

    #[tokio::test]
    async fn read_spool_range_yields_zero_copy_cached_page_slice() {
        let (app, state) = app_with_state().await;
        let data = (0..4096).map(|v| (v % 251) as u8).collect::<Vec<_>>();
        let key = write_and_complete(&app, data).await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        let page = spool
            .read_page(0)
            .await
            .expect("page read should succeed")
            .expect("page should exist");
        let slice_start = 7usize;
        let slice_end = 23usize;

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", format!("bytes={}-{}", slice_start, slice_end - 1))
            .body(Body::empty())
            .expect("build range read request");
        let resp = app.clone().oneshot(req).await.expect("range read oneshot");
        assert_eq!(resp.status(), StatusCode::PARTIAL_CONTENT);

        let mut body = resp.into_body();
        let frame = body
            .frame()
            .await
            .expect("body should yield a frame")
            .expect("frame should be ok");
        let chunk = frame.into_data().expect("frame should contain data");

        assert_eq!(&chunk[..], &page[slice_start..slice_end]);
        assert_eq!(
            chunk.as_ptr(),
            unsafe { page.as_ptr().add(slice_start) },
            "yielded chunk should point into the cached page allocation"
        );
        assert!(body.frame().await.is_none(), "range should yield one chunk");
    }

    #[tokio::test]
    async fn read_range_hides_trailing_partial_until_complete_then_serves_it() {
        let app = app().await;
        let key = create_key(&app).await;
        let data = vec![0xA5u8; 777];

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(data.clone()))
            .expect("build write request");
        let write_resp = app.clone().oneshot(write_req).await.expect("write oneshot");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let hidden_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-776")
            .body(Body::empty())
            .expect("build pre-complete read request");
        let hidden_resp = app
            .clone()
            .oneshot(hidden_req)
            .await
            .expect("pre-complete read oneshot");
        assert_eq!(
            hidden_resp.status(),
            StatusCode::RANGE_NOT_SATISFIABLE,
            "bounded reads must not expose a trailing partial page before /complete"
        );
        assert_eq!(
            hidden_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes */*")
        );

        let complete_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("build complete request");
        let complete_resp = app
            .clone()
            .oneshot(complete_req)
            .await
            .expect("complete oneshot");
        assert_eq!(complete_resp.status(), StatusCode::OK);

        let visible_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-776")
            .body(Body::empty())
            .expect("build post-complete read request");
        let visible_resp = app
            .clone()
            .oneshot(visible_req)
            .await
            .expect("post-complete read oneshot");
        assert_eq!(visible_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            visible_resp
                .headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .expect("content-length")
                .to_str()
                .expect("content-length string"),
            data.len().to_string()
        );
        let body = visible_resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        assert_eq!(&body[..], data.as_slice());
    }

    /// Write `data` at offset 0 and mark the spool complete. Returns the key.
    async fn write_and_complete(app: &Router, data: Vec<u8>) -> String {
        let key = create_key(app).await;

        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(data))
            .expect("build write request");
        let resp = app.clone().oneshot(req).await.expect("write oneshot");
        assert_eq!(resp.status(), StatusCode::OK, "write failed");

        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("build complete request");
        let resp = app.clone().oneshot(req).await.expect("complete oneshot");
        assert_eq!(resp.status(), StatusCode::OK, "complete failed");

        key
    }

    /// Issue a Range request, drain the entire response body (forcing the
    /// stream to run to completion), and return the HTTP status code.
    async fn range_read_drain(app: &Router, key: &str, range: &str) -> StatusCode {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", range)
            .body(Body::empty())
            .expect("build range read request");
        let resp = app.clone().oneshot(req).await.expect("range read oneshot");
        let status = resp.status();
        resp.into_body()
            .collect()
            .await
            .expect("drain response body");
        status
    }

    /// Issue a follow-GET (no Range header), drain the full body, return status.
    async fn follow_read_drain(app: &Router, key: &str) -> StatusCode {
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("build follow read request");
        let resp = app.clone().oneshot(req).await.expect("follow read oneshot");
        let status = resp.status();
        resp.into_body()
            .collect()
            .await
            .expect("drain response body");
        status
    }

    async fn create_key(app: &Router) -> String {
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .body(Body::empty())
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let v: Value = serde_json::from_slice(&body).expect("json parse");
        v["key"].as_str().expect("key string").to_string()
    }

    async fn write_in_progress_page(app: &Router, byte: u8) -> String {
        let key = create_key(app).await;
        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(vec![byte; 4096]))
            .expect("build write request");
        let resp = app.clone().oneshot(req).await.expect("write oneshot");
        assert_eq!(resp.status(), StatusCode::OK);
        key
    }

    #[tokio::test]
    async fn create_uses_request_id_header_as_spool_key() {
        let app = app().await;
        let request_id = "0123456789abcdefghjkmnpqrs"; // 26-char Crockford base32
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, request_id)
            .body(Body::empty())
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["key"].as_str().unwrap(), request_id);
        // The read URL is keyed by the request ID too.
        assert!(v["read_url"].as_str().unwrap().ends_with(request_id));
    }

    #[tokio::test]
    async fn create_falls_back_to_uuid_without_valid_request_id() {
        let app = app().await;
        // Wrong length -> not a valid request ID -> anonymous UUID key.
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, "too-short")
            .body(Body::empty())
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(uuid::Uuid::parse_str(v["key"].as_str().unwrap()).is_ok());
    }

    #[tokio::test]
    async fn create_canonicalizes_uppercase_request_id() {
        let app = app().await;
        let request_id = "0123456789ABCDEFGHJKMNPQRS";
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, request_id)
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value["key"].as_str().unwrap(),
            request_id.to_ascii_lowercase()
        );
    }

    #[tokio::test]
    async fn repeated_request_id_create_returns_conflict_without_truncation_or_permit_leak() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_live_spools = 2;
        })
        .await;
        let request_id = "0123456789abcdefghjkmnpqrs";

        let first = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, request_id)
            .body(Body::empty())
            .expect("first create request");
        assert_eq!(
            app.clone().oneshot(first).await.unwrap().status(),
            StatusCode::CREATED
        );
        let write = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{request_id}/0"))
            .body(Body::from("safe"))
            .expect("write request");
        assert_eq!(
            app.clone().oneshot(write).await.unwrap().status(),
            StatusCode::OK
        );

        let duplicate = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, request_id)
            .body(Body::from(r#"{"write_locked":true}"#))
            .expect("duplicate create request");
        assert_eq!(
            app.oneshot(duplicate).await.unwrap().status(),
            StatusCode::CONFLICT
        );

        assert_eq!(
            tokio::fs::read(state.config.data_dir.join(request_id).join("spool.dat"))
                .await
                .expect("read original spool bytes"),
            b"safe"
        );
        assert_eq!(state.manager.spools.len(), 1);
        assert_eq!(state.manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn concurrent_request_id_creates_have_one_winner_and_one_conflict() {
        let (app, state) = app_with_config(|config| config.max_live_spools = 2).await;
        let request_id = "0123456789abcdefghjkmnpqrs";
        let start = Arc::new(tokio::sync::Barrier::new(3));
        let mut tasks = Vec::new();

        for _ in 0..2 {
            let task_app = app.clone();
            let task_start = Arc::clone(&start);
            tasks.push(tokio::spawn(async move {
                let request = Request::builder()
                    .method("PUT")
                    .uri("/api/v1/create")
                    .header(JOB_ID_HEADER, request_id)
                    .body(Body::empty())
                    .expect("create request");
                task_start.wait().await;
                task_app
                    .oneshot(request)
                    .await
                    .expect("create response")
                    .status()
            }));
        }
        start.wait().await;
        let first = tasks.remove(0).await.expect("first task join");
        let second = tasks.remove(0).await.expect("second task join");
        assert!(
            matches!(
                (first, second),
                (StatusCode::CREATED, StatusCode::CONFLICT)
                    | (StatusCode::CONFLICT, StatusCode::CREATED)
            ),
            "unexpected statuses: {first}, {second}"
        );
        assert_eq!(state.manager.spools.len(), 1);
        assert_eq!(state.manager.admission.available_permits(), 1);
        assert_eq!(
            tokio::fs::metadata(state.config.data_dir.join(request_id).join("spool.dat"))
                .await
                .expect("created data file")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn pprof_is_not_exposed_by_default() {
        let app = app().await;
        let req = Request::builder()
            .uri("/debug/pprof/profile?seconds=1")
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_rejects_invalid_producer_headers() {
        for field in ["content_type", "content_encoding"] {
            let app = app().await;
            let body = format!(r#"{{"{field}":"invalid\nvalue"}}"#);
            let req = Request::builder()
                .method("PUT")
                .uri("/api/v1/create")
                .body(Body::from(body))
                .expect("request build");
            let resp = app.oneshot(req).await.expect("oneshot");
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "field={field}");
        }
    }

    #[tokio::test]
    async fn create_and_complete_deny_unknown_fields() {
        let app = app().await;
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .body(Body::from(r#"{"surprise":true}"#))
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let key = create_key(&app).await;
        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::from(r#"{"unexpected":1}"#))
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn chunked_oversize_write_deletes_partial_spool_and_releases_admission() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let spool_dir = state.config.data_dir.join(&key);
        let (first_page_written_tx, first_page_written_rx) = tokio::sync::oneshot::channel();
        let (send_overflow_tx, send_overflow_rx) = tokio::sync::oneshot::channel();

        let body_stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"1234"));
            let _ = first_page_written_tx.send(());
            let _ = send_overflow_rx.await;
            yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"567"));
        };
        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from_stream(body_stream))
            .expect("request build");
        let write_app = app.clone();
        let write_task =
            tokio::spawn(async move { write_app.oneshot(req).await.expect("oneshot") });

        first_page_written_rx
            .await
            .expect("body polled for the overflow frame");
        let spool = state.manager.get_spool(&key).expect("spool exists");
        assert_eq!(spool.metadata.lock().await.total_bytes_written, 4);
        assert!(spool_dir.join("spool.dat").exists());

        send_overflow_tx.send(()).expect("send overflow frame");
        let resp = write_task.await.expect("write task");
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(state.manager.get_spool(&key).is_none());
        assert!(
            !spool_dir.exists(),
            "partial spool directory must be removed"
        );

        // max_live_spools=1: a successful create proves the rejected spool's
        // admission permit was released before the 413 response.
        let replacement_key = create_key(&app).await;
        assert_ne!(replacement_key, key);
    }

    #[tokio::test]
    async fn known_length_oversize_write_deletes_spool_without_reading_body() {
        let (app, state) = app_with_config(|config| {
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let spool_dir = state.config.data_dir.join(&key);
        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body_stream = async_stream::stream! {
            body_polled_in_stream.store(true, Ordering::SeqCst);
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234567"));
        };
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .header(axum::http::header::CONTENT_LENGTH, "7")
            .body(Body::from_stream(body_stream))
            .expect("oversize request");

        let response = app
            .clone()
            .oneshot(request)
            .await
            .expect("oversize response");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!body_polled.load(Ordering::SeqCst));
        assert!(state.manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
        assert!(
            state
                .manager
                .metadata_store
                .read(&key)
                .await
                .unwrap()
                .is_none(),
            "oversize cleanup must not leave recoverable metadata"
        );

        // max_live_spools=1: successful replacement proves the rejected
        // spool's admission permit was released before the 413 response.
        let replacement_key = create_key(&app).await;
        assert_ne!(replacement_key, key);
    }

    #[tokio::test]
    async fn overflow_cleanup_serializes_with_concurrent_complete() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let spool_dir = state.config.data_dir.join(&key);
        let (first_page_tx, first_page_rx) = tokio::sync::oneshot::channel();
        let (overflow_tx, overflow_rx) = tokio::sync::oneshot::channel();

        let body_stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
            let _ = first_page_tx.send(());
            let _ = overflow_rx.await;
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"567"));
        };
        let write_request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from_stream(body_stream))
            .expect("write request");
        let write_app = app.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(write_request)
                .await
                .expect("write response")
        });

        first_page_rx.await.expect("first page consumed");
        let spool = state.manager.get_spool(&key).expect("spool exists");
        assert_eq!(spool.metadata.lock().await.total_bytes_written, 4);
        let lifecycle_guard = spool.lifecycle_lock.lock().await;
        overflow_tx.send(()).expect("send overflow frame");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        let complete_request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("complete request");
        let complete_app = app.clone();
        let complete_task = tokio::spawn(async move {
            complete_app
                .oneshot(complete_request)
                .await
                .expect("complete response")
        });
        drop(lifecycle_guard);

        let write_response = write_task.await.expect("write task");
        assert_eq!(write_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let complete_status = complete_task.await.expect("complete task").status();
        assert!(
            matches!(complete_status, StatusCode::OK | StatusCode::NOT_FOUND),
            "unexpected completion status: {complete_status}"
        );
        assert!(state.manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
        assert!(
            state
                .manager
                .metadata_store
                .read(&key)
                .await
                .unwrap()
                .is_none(),
            "overflow cleanup must not leave recoverable metadata"
        );
        assert_eq!(state.manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn known_length_oversize_cleanup_serializes_with_concurrent_complete() {
        let (app, state) = app_with_config(|config| {
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let spool_dir = state.config.data_dir.join(&key);
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;
        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body_stream = async_stream::stream! {
            body_polled_in_stream.store(true, Ordering::SeqCst);
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234567"));
        };
        let write_request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .header(axum::http::header::CONTENT_LENGTH, "7")
            .body(Body::from_stream(body_stream))
            .expect("oversize request");
        let write_app = app.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(write_request)
                .await
                .expect("write response")
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        let complete_request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("complete request");
        let complete_app = app.clone();
        let complete_task = tokio::spawn(async move {
            complete_app
                .oneshot(complete_request)
                .await
                .expect("complete response")
        });
        drop(lifecycle_guard);

        let write_response = write_task.await.expect("write task");
        assert_eq!(write_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(!body_polled.load(Ordering::SeqCst));
        let complete_status = complete_task.await.expect("complete task").status();
        assert!(
            matches!(complete_status, StatusCode::OK | StatusCode::NOT_FOUND),
            "unexpected completion status: {complete_status}"
        );
        assert!(state.manager.get_spool(&key).is_none());
        assert!(!spool_dir.exists());
        assert!(
            state
                .manager
                .metadata_store
                .read(&key)
                .await
                .unwrap()
                .is_none(),
            "oversize cleanup must not leave recoverable metadata"
        );
        assert_eq!(state.manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn known_length_oversize_does_not_delete_when_complete_wins_race() {
        let (app, state) = app_with_config(|config| config.max_spool_bytes = 6).await;
        let key = create_key(&app).await;
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let complete_app = app.clone();
        let complete_key = key.clone();
        let complete_task = tokio::spawn(async move {
            complete_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/complete/{complete_key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let write_app = app.clone();
        let write_key = key.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/write/{write_key}/0"))
                        .header(axum::http::header::CONTENT_LENGTH, "7")
                        .body(Body::from_stream(async_stream::stream! {
                            body_polled_in_stream.store(true, Ordering::SeqCst);
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234567"));
                        }))
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        drop(lifecycle_guard);

        assert_eq!(complete_task.await.unwrap().status(), StatusCode::OK);
        assert_eq!(write_task.await.unwrap().status(), StatusCode::CONFLICT);
        assert!(!body_polled.load(Ordering::SeqCst));
        let surviving = state
            .manager
            .get_spool(&key)
            .expect("completed spool survives");
        assert_eq!(
            surviving.metadata.lock().await.state,
            crate::spool::SpoolState::Complete
        );
        assert!(state.config.data_dir.join(&key).join("spool.dat").exists());
    }

    #[tokio::test]
    async fn chunked_oversize_does_not_delete_when_complete_wins_race() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_spool_bytes = 6;
        })
        .await;
        let key = create_key(&app).await;
        let (first_page_tx, first_page_rx) = tokio::sync::oneshot::channel();
        let (overflow_tx, overflow_rx) = tokio::sync::oneshot::channel();
        let write_app = app.clone();
        let write_key = key.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/write/{write_key}/0"))
                        .body(Body::from_stream(async_stream::stream! {
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
                            let _ = first_page_tx.send(());
                            let _ = overflow_rx.await;
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"567"));
                        }))
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        first_page_rx.await.expect("first page written");
        let spool = state.manager.get_spool(&key).expect("spool exists");
        assert_eq!(spool.metadata.lock().await.total_bytes_written, 4);
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let complete_app = app.clone();
        let complete_key = key.clone();
        let complete_task = tokio::spawn(async move {
            complete_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/complete/{complete_key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        overflow_tx.send(()).expect("send overflow");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        drop(lifecycle_guard);

        assert_eq!(complete_task.await.unwrap().status(), StatusCode::OK);
        assert_eq!(write_task.await.unwrap().status(), StatusCode::CONFLICT);
        let surviving = state
            .manager
            .get_spool(&key)
            .expect("completed spool survives");
        assert_eq!(
            surviving.metadata.lock().await.state,
            crate::spool::SpoolState::Complete
        );
        assert_eq!(
            tokio::fs::read(state.config.data_dir.join(&key).join("spool.dat"))
                .await
                .unwrap(),
            b"1234"
        );
    }

    #[tokio::test]
    async fn known_length_oversize_returns_not_found_when_delete_wins_race() {
        let (app, state) = app_with_config(|config| {
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let delete_app = app.clone();
        let delete_key = key.clone();
        let delete_task = tokio::spawn(async move {
            delete_app
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(format!("/api/v1/delete/{delete_key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let write_app = app.clone();
        let write_key = key.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/write/{write_key}/0"))
                        .header(axum::http::header::CONTENT_LENGTH, "7")
                        .body(Body::from_stream(async_stream::stream! {
                            body_polled_in_stream.store(true, Ordering::SeqCst);
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234567"));
                        }))
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        drop(lifecycle_guard);

        assert_eq!(delete_task.await.unwrap().status(), StatusCode::OK);
        assert_eq!(write_task.await.unwrap().status(), StatusCode::NOT_FOUND);
        assert!(!body_polled.load(Ordering::SeqCst));
        assert!(state.manager.get_spool(&key).is_none());
        assert_eq!(state.manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn chunked_oversize_returns_not_found_when_delete_wins_race() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let (first_page_tx, first_page_rx) = tokio::sync::oneshot::channel();
        let (overflow_tx, overflow_rx) = tokio::sync::oneshot::channel();
        let write_app = app.clone();
        let write_key = key.clone();
        let write_task = tokio::spawn(async move {
            write_app
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/v1/write/{write_key}/0"))
                        .body(Body::from_stream(async_stream::stream! {
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234"));
                            let _ = first_page_tx.send(());
                            let _ = overflow_rx.await;
                            yield Ok::<_, std::io::Error>(Bytes::from_static(b"567"));
                        }))
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        first_page_rx.await.expect("first page written");
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let lifecycle_guard = spool.lifecycle_lock.lock().await;

        let delete_app = app.clone();
        let delete_key = key.clone();
        let delete_task = tokio::spawn(async move {
            delete_app
                .oneshot(
                    Request::builder()
                        .method("DELETE")
                        .uri(format!("/api/v1/delete/{delete_key}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap()
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        overflow_tx.send(()).expect("send overflow");
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        drop(lifecycle_guard);

        assert_eq!(delete_task.await.unwrap().status(), StatusCode::OK);
        assert_eq!(write_task.await.unwrap().status(), StatusCode::NOT_FOUND);
        assert!(state.manager.get_spool(&key).is_none());
        assert!(!state.config.data_dir.join(&key).exists());
        assert_eq!(state.manager.admission.available_permits(), 1);
    }

    #[tokio::test]
    async fn known_length_oversize_cleanup_failure_keeps_duplicate_create_conflicting() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 4;
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = "0123456789abcdefghjkmnpqrs".to_string();
        let create = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, &key)
            .body(Body::empty())
            .expect("create request");
        assert_eq!(
            app.clone().oneshot(create).await.unwrap().status(),
            StatusCode::CREATED
        );
        let initial_write = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from("1234"))
            .expect("initial write request");
        assert_eq!(
            app.clone().oneshot(initial_write).await.unwrap().status(),
            StatusCode::OK
        );

        let meta_path = state.config.data_dir.join(&key).join("meta.json");
        tokio::fs::remove_file(&meta_path)
            .await
            .expect("remove metadata file");
        tokio::fs::create_dir(&meta_path)
            .await
            .expect("replace metadata with directory");

        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let overflow_body = async_stream::stream! {
            body_polled_in_stream.store(true, Ordering::SeqCst);
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"567"));
        };
        let overflow_request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/4"))
            .header(axum::http::header::CONTENT_LENGTH, "3")
            .body(Body::from_stream(overflow_body))
            .expect("overflow request");
        let response = app
            .clone()
            .oneshot(overflow_request)
            .await
            .expect("overflow response");

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(!body_polled.load(Ordering::SeqCst));
        let duplicate = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .header(JOB_ID_HEADER, &key)
            .body(Body::empty())
            .expect("duplicate create request");
        let duplicate_response = app
            .oneshot(duplicate)
            .await
            .expect("duplicate create response");
        assert_eq!(duplicate_response.status(), StatusCode::CONFLICT);

        let spool = state
            .manager
            .get_spool(&key)
            .expect("failed cleanup must remain tracked");
        assert_eq!(
            spool.metadata.lock().await.state,
            crate::spool::SpoolState::Deleting
        );
        assert!(state.config.data_dir.join(&key).exists());
        assert_eq!(state.manager.admission.available_permits(), 0);
    }

    #[tokio::test]
    async fn empty_body_write_validates_offset_and_state_without_keepalive() {
        let (app, state) = app_with_state().await;
        let key = create_key(&app).await;
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let activity_before = spool.cleanup_anchors().last_write_at;

        let wrong_offset = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/1"))
            .body(Body::empty())
            .expect("wrong-offset request");
        assert_eq!(
            app.clone().oneshot(wrong_offset).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(spool.cleanup_anchors().last_write_at, activity_before);

        let valid_empty = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::empty())
            .expect("empty request");
        assert_eq!(
            app.clone().oneshot(valid_empty).await.unwrap().status(),
            StatusCode::OK
        );
        assert_eq!(spool.cleanup_anchors().last_write_at, activity_before);

        let complete = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("complete request");
        assert_eq!(
            app.clone().oneshot(complete).await.unwrap().status(),
            StatusCode::OK
        );
        let activity_after_complete = spool.cleanup_anchors().last_write_at;
        let closed_empty = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::empty())
            .expect("closed write request");
        assert_eq!(
            app.oneshot(closed_empty).await.unwrap().status(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            spool.cleanup_anchors().last_write_at,
            activity_after_complete
        );
    }

    #[tokio::test]
    async fn known_length_oversize_wrong_offset_returns_400_without_deleting_or_polling() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 8;
            config.max_spool_bytes = 8;
        })
        .await;
        let key = create_key(&app).await;
        let body_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let body_polled_in_stream = Arc::clone(&body_polled);
        let body_stream = async_stream::stream! {
            body_polled_in_stream.store(true, Ordering::SeqCst);
            yield Ok::<_, std::io::Error>(Bytes::from_static(b"oversize"));
        };

        let hinted_oversize = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/1"))
            .header(axum::http::header::CONTENT_LENGTH, u64::MAX.to_string())
            .body(Body::from_stream(body_stream))
            .expect("hinted request");
        assert_eq!(
            app.clone().oneshot(hinted_oversize).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert!(!body_polled.load(Ordering::SeqCst));
        assert!(state.manager.get_spool(&key).is_some());

        let small_write = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from("tiny"))
            .expect("small write request");
        assert_eq!(
            app.oneshot(small_write).await.unwrap().status(),
            StatusCode::OK
        );
        let spool = state
            .manager
            .get_spool(&key)
            .expect("original spool remains");
        assert_eq!(spool.write_buffer.lock().await.as_ref(), b"tiny");
        assert_eq!(spool.metadata.lock().await.total_bytes_written, 4);
    }

    #[tokio::test]
    async fn chunked_oversize_wrong_offset_returns_400_without_deleting() {
        let (app, state) = app_with_config(|config| {
            config.page_size = 8;
            config.max_spool_bytes = 6;
            config.max_live_spools = 1;
        })
        .await;
        let key = create_key(&app).await;
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/1"))
            .body(Body::from_stream(async_stream::stream! {
                yield Ok::<_, std::io::Error>(Bytes::from_static(b"1234567"));
            }))
            .expect("chunked oversize request");

        assert_eq!(
            app.oneshot(request).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );
        assert!(state.manager.get_spool(&key).is_some());
        assert_eq!(
            tokio::fs::metadata(state.config.data_dir.join(&key).join("spool.dat"))
                .await
                .expect("original data file")
                .len(),
            0
        );
        assert_eq!(state.manager.admission.available_permits(), 0);
    }

    #[tokio::test]
    async fn create_admission_timeout_returns_service_unavailable() {
        let (app, _) = app_with_config(|config| {
            config.max_live_spools = 1;
            config.create_admission_timeout_ms = 10;
        })
        .await;
        create_key(&app).await;
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn active_content_reads_are_forced_downloads_nosniff_and_sandboxed() {
        let app = app().await;
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .body(Body::from(r#"{"content_type":"text/html; charset=utf-8"}"#))
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let value: Value = serde_json::from_slice(&body).unwrap();
        let key = value["key"].as_str().unwrap();

        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from("<script>alert(1)</script>"))
            .expect("request build");
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );
        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("request build");
        assert_eq!(
            app.clone().oneshot(req).await.unwrap().status(),
            StatusCode::OK
        );

        let req = Request::builder()
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(
            resp.headers()[axum::http::header::CONTENT_DISPOSITION],
            HeaderValue::from_static("attachment")
        );
        assert_eq!(
            resp.headers()["X-Content-Type-Options"],
            HeaderValue::from_static("nosniff")
        );
        assert_eq!(
            resp.headers()["Content-Security-Policy"],
            HeaderValue::from_static("sandbox; default-src 'none'; frame-ancestors 'none'")
        );
    }

    #[test]
    fn test_api_error_spool_not_found() {
        let err = ApiError(BobsError::SpoolNotFound {
            key: "k".to_string(),
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_api_error_offset_mismatch() {
        let err = ApiError(BobsError::OffsetMismatch {
            expected: 0,
            got: 10,
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_api_error_size_mismatch() {
        let err = ApiError(BobsError::SizeMismatch {
            expected: 100,
            actual: 50,
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_api_error_spool_locked() {
        let err = ApiError(BobsError::SpoolLocked);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::LOCKED);
    }

    #[test]
    fn test_api_error_spool_closed() {
        let err = ApiError(BobsError::SpoolClosed);
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn test_api_error_invalid_state() {
        let err = ApiError(BobsError::InvalidState {
            current: "Complete".to_string(),
            attempted_action: "write".to_string(),
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
    }

    #[test]
    fn test_api_error_io_error() {
        let err = ApiError(BobsError::IoError(std::io::Error::other("disk full")));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_api_error_serialization_error() {
        let err = ApiError(BobsError::SerializationError("bad json".to_string()));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn test_api_error_invalid_range() {
        let err = ApiError(BobsError::InvalidRange("bad range".to_string()));
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_api_error_range_not_satisfiable_sets_content_range() {
        let err = ApiError(BobsError::RangeNotSatisfiable {
            total: Some(123),
            reason: "past end".to_string(),
        });
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes */123")
        );
    }

    #[tokio::test]
    async fn test_health() {
        let app = app().await;
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/health")
            .body(Body::empty())
            .expect("request build");

        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let v: Value = serde_json::from_slice(&body).expect("json parse");
        assert_eq!(v["status"], "ok");
    }

    #[tokio::test]
    async fn test_status() {
        let app = app().await;
        let req = Request::builder()
            .method("GET")
            .uri("/api/v1/status")
            .body(Body::empty())
            .expect("request build");

        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_create_and_delete() {
        let app = app().await;
        let key = create_key(&app).await;

        let req = Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/delete/{key}"))
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_read_deleting_spool_returns_not_found() {
        let (app, state) = app_with_state().await;
        let key = create_key(&app).await;
        let spool = state.manager.get_spool(&key).expect("spool exists");
        spool.metadata.lock().await.state = crate::spool::SpoolState::Deleting;

        let request = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("request build");
        let response = app.oneshot(request).await.expect("oneshot");
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let app = app().await;
        let req = Request::builder()
            .method("DELETE")
            .uri("/api/v1/delete/missing")
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_parse_range_standard() {
        let val = HeaderValue::from_static("bytes=0-1023");
        assert!(matches!(
            parse_range(Some(&val)).expect("range should parse"),
            ReadRequestRange::Bounded {
                start: 0,
                end_inclusive: Some(1023)
            }
        ));
    }

    #[test]
    fn test_parse_range_open_ended() {
        let val = HeaderValue::from_static("bytes=500-");
        assert!(matches!(
            parse_range(Some(&val)).expect("range should parse"),
            ReadRequestRange::Bounded {
                start: 500,
                end_inclusive: None
            }
        ));
    }

    #[test]
    fn test_parse_range_no_header() {
        assert!(matches!(
            parse_range(None).expect("range should parse"),
            ReadRequestRange::Follow
        ));
    }

    #[test]
    fn test_parse_range_suffix() {
        let val = HeaderValue::from_static("bytes=-500");
        assert!(matches!(
            parse_range(Some(&val)).expect("range should parse"),
            ReadRequestRange::Suffix { len: 500 }
        ));
    }

    #[test]
    fn test_parse_range_rejects_missing_bytes_prefix() {
        let val = HeaderValue::from_static("0-999");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_parse_range_rejects_malformed_start() {
        let val = HeaderValue::from_static("bytes=abc-100");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_parse_range_rejects_malformed_end() {
        let val = HeaderValue::from_static("bytes=10-xyz");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_parse_range_rejects_both_malformed() {
        let val = HeaderValue::from_static("bytes=abc-def");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_parse_range_rejects_empty_value() {
        let val = HeaderValue::from_static("");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_parse_range_rejects_descending_range() {
        let val = HeaderValue::from_static("bytes=10-1");
        assert!(matches!(
            parse_range(Some(&val)),
            Err(BobsError::InvalidRange(_))
        ));
    }

    #[test]
    fn test_long_poll_redirect_uses_api_path_without_prefix() {
        let headers = HeaderMap::new();
        let response = long_poll_redirect("abc123", &headers);
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("/api/v1/read/abc123")
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CACHE_CONTROL)
                .and_then(|value| value.to_str().ok()),
            Some("no-store")
        );
    }

    #[test]
    fn test_long_poll_redirect_uses_valid_forwarded_prefix() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "X-Forwarded-Prefix",
            HeaderValue::from_static("/download-3"),
        );
        let response = long_poll_redirect("abc123", &headers);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("/download-3/api/v1/read/abc123")
        );
    }

    #[test]
    fn test_forwarded_prefix_validation_rejects_unsafe_values() {
        let long = format!("/{}", "a".repeat(64));
        for raw in [
            b"//evil".as_slice(),
            b"/\\evil".as_slice(),
            b"/x/../evil".as_slice(),
            b"/foo%2Fevil".as_slice(),
            b"/x@evil.com".as_slice(),
            b"/x\r\nevil".as_slice(),
            b"/x\0evil".as_slice(),
            long.as_bytes(),
            b"https://evil".as_slice(),
            b"?x".as_slice(),
            b"/x#frag".as_slice(),
        ] {
            assert_eq!(validated_forwarded_prefix(raw), None, "{raw:?}");
        }
    }

    #[test]
    fn test_long_poll_redirect_ignores_invalid_forwarded_prefix() {
        for raw in [
            "//evil",
            "/\\evil",
            "/x/../evil",
            "/foo%2Fevil",
            "/x@evil.com",
            "https://evil",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                "X-Forwarded-Prefix",
                HeaderValue::from_str(raw).expect("header value"),
            );
            let response = long_poll_redirect("abc123", &headers);
            assert_eq!(
                response
                    .headers()
                    .get(axum::http::header::LOCATION)
                    .and_then(|value| value.to_str().ok()),
                Some("/api/v1/read/abc123"),
                "{raw}"
            );
        }
    }

    #[tokio::test]
    async fn first_page_long_poll_timeout_still_redirects() {
        let (app, _state) = app_with_options(10, Arc::new(BobsMetrics::new(false))).await;
        let key = create_key(&app).await;
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("build follow request");

        let response = app.oneshot(req).await.expect("follow read oneshot");

        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some(format!("/api/v1/read/{key}").as_str())
        );
    }

    #[tokio::test]
    async fn mid_stream_follow_timeout_aborts_chunked_body() {
        let (app, state) = app_with_options(10, Arc::new(BobsMetrics::new(false))).await;
        let key = write_in_progress_page(&app, 0x5a).await;
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("build follow request");
        let response = app.oneshot(req).await.expect("follow read oneshot");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response
                .headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .is_none(),
            "in-progress follow response must be chunked"
        );

        let mut body = response.into_body();
        let first = body
            .frame()
            .await
            .expect("first body frame")
            .expect("first frame succeeds")
            .into_data()
            .expect("first frame contains data");
        assert_eq!(first, Bytes::from(vec![0x5a; 4096]));
        let timeout_frame = body.frame().await.expect("timeout error frame");
        assert!(
            timeout_frame.is_err(),
            "mid-stream timeout must abort the transfer, not return clean EOF"
        );
        drop(body);

        let spool = state.manager.get_spool(&key).expect("spool remains");
        assert_eq!(spool.reader_count.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "telemetry")]
    #[tokio::test]
    async fn mid_stream_follow_timeout_records_timeout_metrics_once() {
        use prometheus::Encoder;

        let (_provider, registry) = crate::metrics::init_meter_provider("timeout-test");
        let metrics = Arc::new(BobsMetrics::new(true));
        let (app, _state) = app_with_options(10, metrics).await;
        let key = write_in_progress_page(&app, 0x33).await;
        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("build follow request");
        let response = app.oneshot(req).await.expect("follow read oneshot");
        assert!(response.into_body().collect().await.is_err());

        let mut encoded = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut encoded)
            .expect("encode metrics");
        let scrape = String::from_utf8(encoded).expect("metrics are UTF-8");
        let count_lines: Vec<_> = scrape
            .lines()
            .filter(|line| line.starts_with("bobs_read_duration_seconds_count"))
            .collect();
        assert!(
            count_lines
                .iter()
                .any(|line| line.contains("mode=\"follow\"")
                    && line.contains("outcome=\"timeout\"")
                    && line.ends_with(" 1")),
            "missing timeout metric: {scrape}"
        );
        assert!(
            count_lines
                .iter()
                .all(|line| !line.contains("outcome=\"success\"")
                    && !line.contains("outcome=\"client_gone\"")),
            "timeout must not also record success/client_gone: {scrape}"
        );
    }

    #[tokio::test]
    async fn test_invalid_range_returns_bad_request() {
        let app = app().await;
        let key = create_key(&app).await;

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(vec![7u8; 8]))
            .expect("request build");
        let write_resp = app.clone().oneshot(write_req).await.expect("oneshot");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=abc-1")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_open_ended_range_is_bounded_not_follow_mode() {
        let app = app().await;
        let key = create_key(&app).await;
        let data = vec![9u8; 8192];

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(data.clone()))
            .expect("request build");
        let write_resp = app.clone().oneshot(write_req).await.expect("oneshot");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=4096-")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes 4096-8191/*")
        );
    }

    #[tokio::test]
    async fn test_bounded_range_does_not_advertise_unservable_trailing_partial() {
        let app = app().await;
        let key = create_key(&app).await;
        let data = vec![9u8; 5000];

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(data))
            .expect("request build");
        let write_resp = app.clone().oneshot(write_req).await.expect("oneshot");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-4999")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.clone().oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .and_then(|h| h.to_str().ok()),
            Some("4096")
        );
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes 0-4095/*")
        );
        let body = read_resp
            .into_body()
            .collect()
            .await
            .expect("drain response body")
            .to_bytes();
        assert_eq!(body.len(), 4096);
    }

    #[tokio::test]
    async fn test_bounded_range_rejects_only_unservable_trailing_partial() {
        let app = app().await;
        let key = create_key(&app).await;

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(vec![7u8; 1000]))
            .expect("request build");
        let write_resp = app.clone().oneshot(write_req).await.expect("oneshot");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-999")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes */*")
        );
    }

    #[tokio::test]
    async fn test_suffix_range_on_complete_spool() {
        let app = app().await;
        let data = (0..1000).map(|v| (v % 251) as u8).collect::<Vec<_>>();
        let key = write_and_complete(&app, data.clone()).await;

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=-500")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes 500-999/1000")
        );
        let body = read_resp
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(&body[..], &data[500..]);
    }

    #[tokio::test]
    async fn test_suffix_range_larger_than_total_returns_whole_object() {
        let app = app().await;
        let data = vec![1u8; 777];
        let key = write_and_complete(&app, data.clone()).await;

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=-5000")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes 0-776/777")
        );
        let body = read_resp
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(&body[..], data.as_slice());
    }

    #[tokio::test]
    async fn test_unsatisfiable_ranges_return_416_content_range() {
        let app = app().await;
        let key = write_and_complete(&app, vec![1u8; 1000]).await;

        for range in ["bytes=-0", "bytes=999999-"] {
            let read_req = Request::builder()
                .method("GET")
                .uri(format!("/api/v1/read/{key}"))
                .header("Range", range)
                .body(Body::empty())
                .expect("request build");
            let read_resp = app.clone().oneshot(read_req).await.expect("oneshot");
            assert_eq!(
                read_resp.status(),
                StatusCode::RANGE_NOT_SATISFIABLE,
                "{range}"
            );
            assert_eq!(
                read_resp
                    .headers()
                    .get(axum::http::header::CONTENT_RANGE)
                    .and_then(|h| h.to_str().ok()),
                Some("bytes */1000"),
                "{range}"
            );
        }
    }

    #[tokio::test]
    async fn test_in_progress_suffix_range_returns_416_unknown_total() {
        let app = app().await;
        let key = create_key(&app).await;
        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(vec![7u8; 4096]))
            .expect("request build");
        assert_eq!(
            app.clone()
                .oneshot(write_req)
                .await
                .expect("oneshot")
                .status(),
            StatusCode::OK
        );

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=-500")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::RANGE_NOT_SATISFIABLE);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes */*")
        );
    }

    #[tokio::test]
    async fn test_multi_range_remains_bad_request() {
        let app = app().await;
        let key = write_and_complete(&app, vec![1u8; 1000]).await;
        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-1,3-4")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_create_returns_write_url() {
        let app = app().await;
        let req = Request::builder()
            .method("PUT")
            .uri("/api/v1/create")
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let v: Value = serde_json::from_slice(&body).expect("json parse");
        let key = v["key"].as_str().expect("key present");
        let read_url = v["read_url"].as_str().expect("read_url present");
        assert_eq!(read_url, format!("https://test.example.com/bobs-0/{key}"));
        let write_url = v["write_url"].as_str().expect("write_url present");
        assert!(!write_url.is_empty(), "write_url must be non-empty");
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let app = app().await;
        let key = create_key(&app).await;

        let data = vec![7u8; 8192];
        let req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from(data.clone()))
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK);

        let complete_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/complete/{key}"))
            .body(Body::empty())
            .expect("request build");
        let complete_resp = app.clone().oneshot(complete_req).await.expect("oneshot");
        assert_eq!(complete_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .header("Range", "bytes=0-8191")
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::PARTIAL_CONTENT);
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::ACCEPT_RANGES)
                .and_then(|h| h.to_str().ok()),
            Some("bytes")
        );
        assert_eq!(
            read_resp
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|h| h.to_str().ok()),
            Some("bytes 0-8191/8192")
        );
        assert_eq!(
            read_resp
                .headers()
                .get("X-Accel-Buffering")
                .and_then(|h| h.to_str().ok()),
            Some("no")
        );
    }

    #[tokio::test]
    async fn test_write_stream_flushes_page_before_body_end() {
        let (app, state) = app_with_state().await;
        let key = create_key(&app).await;
        let (first_page_consumed_tx, first_page_consumed_rx) = tokio::sync::oneshot::channel();
        let (allow_second_page_tx, allow_second_page_rx) = tokio::sync::oneshot::channel();

        let body_stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(bytes::Bytes::from(vec![1u8; 4096]));
            let _ = first_page_consumed_tx.send(());
            let _ = allow_second_page_rx.await;
            yield Ok::<_, std::io::Error>(bytes::Bytes::from(vec![2u8; 4096]));
        };

        let write_req = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from_stream(body_stream))
            .expect("request build");
        let write_task = tokio::spawn({
            let app = app.clone();
            async move { app.oneshot(write_req).await.expect("write oneshot") }
        });

        first_page_consumed_rx
            .await
            .expect("writer should consume the first page before waiting");
        let spool = state.manager.get_spool(&key).expect("spool exists");
        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 1);
        assert_eq!(meta.total_bytes_written, 4096);
        drop(meta);

        allow_second_page_tx
            .send(())
            .expect("write task should still be waiting for the second page");
        let write_resp = write_task.await.expect("write task join");
        assert_eq!(write_resp.status(), StatusCode::OK);

        let meta = spool.metadata.lock().await;
        assert_eq!(meta.total_pages, 2);
        assert_eq!(meta.total_bytes_written, 8192);
    }

    #[tokio::test]
    async fn test_slow_body_frame_refreshes_monotonic_writer_activity_before_batch_flush() {
        let (app, state) = app_with_state().await;
        let key = create_key(&app).await;
        let spool = state.manager.get_spool(&key).expect("spool exists");
        spool.metadata.lock().await.last_write_at = 1;
        let stale_anchor = Instant::now() - Duration::from_secs(60);
        spool
            .cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_write_at = stale_anchor;

        let (frame_processed_tx, frame_processed_rx) = tokio::sync::oneshot::channel();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let body_stream = async_stream::stream! {
            yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"x"));
            let _ = frame_processed_tx.send(());
            let _ = finish_rx.await;
        };
        let request = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/write/{key}/0"))
            .body(Body::from_stream(body_stream))
            .expect("request build");
        let write_task = tokio::spawn({
            let app = app.clone();
            async move { app.oneshot(request).await.expect("write oneshot") }
        });

        frame_processed_rx
            .await
            .expect("handler should process the first frame before waiting");
        let metadata = spool.metadata.lock().await;
        assert!(
            metadata.last_write_at > 1,
            "every received frame must refresh wall-clock activity before a batch is flushed"
        );
        assert_eq!(
            metadata.total_bytes_written, 0,
            "sub-page frame remains pending"
        );
        drop(metadata);
        assert!(
            spool.cleanup_anchors().last_write_at > stale_anchor,
            "every received frame must refresh the monotonic cleanup anchor before a batch is flushed"
        );

        finish_tx.send(()).expect("write task still waiting");
        assert_eq!(
            write_task.await.expect("write task join").status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn frame_refresh_rejection_surfaces_completion_and_delete_states() {
        for delete_before_frame in [false, true] {
            let (app, state) = app_with_state().await;
            let key = create_key(&app).await;
            let spool = state.manager.get_spool(&key).expect("spool exists");
            let (body_polled_tx, body_polled_rx) = tokio::sync::oneshot::channel();
            let (release_frame_tx, release_frame_rx) = tokio::sync::oneshot::channel();
            let body_stream = async_stream::stream! {
                let _ = body_polled_tx.send(());
                let _ = release_frame_rx.await;
                yield Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"x"));
            };
            let request = Request::builder()
                .method("POST")
                .uri(format!("/api/v1/write/{key}/0"))
                .body(Body::from_stream(body_stream))
                .expect("request build");
            let task = tokio::spawn({
                let app = app.clone();
                async move { app.oneshot(request).await.expect("write oneshot") }
            });
            body_polled_rx
                .await
                .expect("handler captured the spool before lifecycle transition");

            let expected = if delete_before_frame {
                state
                    .manager
                    .delete_spool(&key)
                    .await
                    .expect("delete before frame");
                StatusCode::NOT_FOUND
            } else {
                spool.complete(None).await.expect("complete before frame");
                StatusCode::CONFLICT
            };
            release_frame_tx
                .send(())
                .expect("handler still polling body");
            assert_eq!(
                task.await.expect("write task join").status(),
                expected,
                "HTTP must return the lifecycle refresh rejection"
            );
            assert_eq!(
                spool.metadata.lock().await.total_bytes_written,
                0,
                "rejected frame must never reach the pending buffer or data file"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Read-coverage tracking — full_object_read_at and last_read_activity_at
    // -----------------------------------------------------------------------

    /// A single Range request that covers the entire 8 KiB object must set
    /// the full-object cleanup anchor after the response body is fully drained.
    #[tokio::test]
    async fn test_full_range_sets_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        let status = range_read_drain(&app, &key, "bytes=0-8191").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "full-range read must set full_object_read_at"
        );
        let cache = spool.page_cache.lock().await;
        assert!(
            !cache.contains(&key, 0) && !cache.contains(&key, 1),
            "full-read transition must free this spool's page cache entries"
        );
    }

    /// A partial Range request leaves bytes un-served; the full-object cleanup
    /// anchor must remain unset.
    #[tokio::test]
    async fn test_partial_range_does_not_set_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        let status = range_read_drain(&app, &key, "bytes=0-4095").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_none(),
            "partial range must not set the full-object cleanup anchor"
        );
        let cache = spool.page_cache.lock().await;
        assert!(
            cache.contains(&key, 0) || cache.contains(&key, 1),
            "partial reads must not free this spool's page cache entries"
        );
    }

    /// Two non-overlapping ranges that together cover the full 8 KiB object
    /// must set the full-object cleanup anchor after the second request.
    #[tokio::test]
    async fn test_two_ranges_covering_full_object_sets_flag() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        // First half — coverage incomplete.
        range_read_drain(&app, &key, "bytes=0-4095").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_none(),
            "after first half: full-object cleanup anchor must still be unset"
        );

        // Second half — now fully covered.
        range_read_drain(&app, &key, "bytes=4096-8191").await;
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "after second half: full_object_read_at must be set"
        );
    }

    /// Out-of-order ranges set the full-object cleanup anchor only once all bytes
    /// have been covered.
    #[tokio::test]
    async fn test_out_of_order_ranges_set_flag_on_completion() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![3u8; 8192]).await;

        // Second half first.
        range_read_drain(&app, &key, "bytes=4096-8191").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_none(),
            "only second half served: full-object cleanup anchor must be unset"
        );

        // First half — completes coverage.
        range_read_drain(&app, &key, "bytes=0-4095").await;
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "after first half served: full_object_read_at must be set"
        );
    }

    /// Overlapping ranges must not double-count bytes. Two overlapping requests
    /// that together cover a 4 KiB object must set the full-object cleanup anchor.
    #[tokio::test]
    async fn test_overlapping_ranges_do_not_double_count() {
        let (app, state) = app_with_state().await;
        // 4 KiB = exactly one page.
        let key = write_and_complete(&app, vec![9u8; 4096]).await;

        // bytes 0-3000 (first request).
        range_read_drain(&app, &key, "bytes=0-3000").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_none(),
            "bytes 3001-4095 still missing: cleanup anchor must be unset"
        );

        // bytes 2000-4095 — overlaps [0,3001) and covers [3001,4096).
        range_read_drain(&app, &key, "bytes=2000-4095").await;
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "overlapping second range completes coverage: flag must be set"
        );
    }

    /// A follow-GET (no Range header) that consumes the entire body must set
    /// the full-object cleanup anchor.
    #[tokio::test]
    async fn test_follow_read_sets_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![5u8; 4096]).await;

        let status = follow_read_drain(&app, &key).await;
        assert_eq!(status, StatusCode::OK);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "follow-GET must set full_object_read_at after all bytes are served"
        );
    }

    /// A follow-GET against a completed spool knows the final size and must set
    /// Content-Length so redirect-following clients can download it safely.
    #[tokio::test]
    async fn test_completed_follow_read_sets_content_length() {
        let (app, _state) = app_with_state().await;
        let data = vec![5u8; 4096];
        let key = write_and_complete(&app, data.clone()).await;

        let req = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/read/{key}"))
            .body(Body::empty())
            .expect("build follow read request");
        let resp = app.clone().oneshot(req).await.expect("follow read oneshot");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .and_then(|h| h.to_str().ok()),
            Some("4096")
        );
        let body = resp
            .into_body()
            .collect()
            .await
            .expect("drain response body")
            .to_bytes();
        assert_eq!(body, data);
    }

    /// Any read that yields at least one chunk must update the read-activity anchor.
    #[tokio::test]
    async fn test_last_read_activity_updated_on_chunk_yield() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![1u8; 4096]).await;

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().last_read_activity_at.is_none(),
            "no reads yet: read-activity anchor must be unset"
        );

        range_read_drain(&app, &key, "bytes=0-4095").await;

        assert!(
            spool.cleanup_anchors().last_read_activity_at.is_some(),
            "after range read: last_read_activity_at must be updated"
        );
    }

    // -----------------------------------------------------------------------
    // Cleanup TTL integration — HTTP read → monotonic cleanup deletion
    // -----------------------------------------------------------------------

    /// Full-range HTTP read sets `full_object_read_at`; cleanup deletes the
    /// spool once `full_read_complete_ttl_secs` has elapsed.
    #[tokio::test]
    async fn test_short_ttl_cleanup_fires_after_full_read() {
        tokio::time::pause();
        let (app, state) = app_with_ttl_config().await;
        let key = write_and_complete(&app, vec![7u8; 4096]).await;

        // Full range read — HTTP path must set full_object_read_at.
        let status = range_read_drain(&app, &key, "bytes=0-4095").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.cleanup_anchors().full_object_read_at.is_some(),
            "HTTP read path must set full_object_read_at after full coverage"
        );

        // Age both monotonic anchors beyond the short TTL.
        {
            let mut anchors = spool
                .cleanup_anchors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let old = Instant::now() - Duration::from_secs(10);
            anchors.full_object_read_at = Some(old);
            anchors.last_read_activity_at = Some(old);
        }

        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        wait_for_spool_removal(&state.manager, &key).await;

        assert!(
            state.manager.get_spool(&key).is_none(),
            "spool must be deleted after full-read TTL expires"
        );
        task.abort();
    }

    /// A spool that is completed but never read must be deleted once
    /// `read_idle_ttl_secs` elapses from `readable_at`.
    #[tokio::test]
    async fn test_idle_ttl_fires_without_reads() {
        tokio::time::pause();
        let (app, state) = app_with_ttl_config().await;
        let key = write_and_complete(&app, vec![5u8; 4096]).await;

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(spool.cleanup_anchors().last_read_activity_at.is_none());

        {
            let mut anchors = spool
                .cleanup_anchors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anchors.readable_at = Some(Instant::now() - Duration::from_secs(10));
        }

        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        wait_for_spool_removal(&state.manager, &key).await;

        assert!(
            state.manager.get_spool(&key).is_none(),
            "unread spool must be deleted after idle TTL expires"
        );
        task.abort();
    }

    /// A range read refreshes `last_read_activity_at`, which prevents idle
    /// cleanup. Once activity stops the idle TTL fires.
    #[tokio::test]
    async fn test_idle_ttl_refreshed_prevents_deletion() {
        tokio::time::pause();
        let (app, state) = app_with_ttl_config().await;
        let key = write_and_complete(&app, vec![3u8; 4096]).await;

        let spool = state.manager.get_spool(&key).expect("spool must exist");

        // Make the readable anchor old so that, without activity, idle TTL would fire.
        {
            let mut anchors = spool
                .cleanup_anchors
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            anchors.readable_at = Some(Instant::now() - Duration::from_secs(10));
        }

        // Perform a range read, which refreshes the monotonic activity anchor.
        let status = range_read_drain(&app, &key, "bytes=0-4095").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert!(
            spool.cleanup_anchors().last_read_activity_at.is_some(),
            "range read must update last_read_activity_at"
        );

        // Trigger several cleanup sweeps; recent monotonic activity protects the spool.
        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;

        assert!(
            state.manager.get_spool(&key).is_some(),
            "spool with recent read activity must NOT be deleted"
        );

        // Age the activity anchor beyond the idle TTL.
        spool
            .cleanup_anchors
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .last_read_activity_at = Some(Instant::now() - Duration::from_secs(10));

        tokio::time::advance(Duration::from_secs(3)).await;
        // Deletion stays visible until metadata and directory removal finish.
        tokio::task::yield_now().await;
        wait_for_spool_removal(&state.manager, &key).await;

        assert!(
            state.manager.get_spool(&key).is_none(),
            "spool must be deleted once activity stops and idle TTL expires"
        );
        task.abort();
    }
}
