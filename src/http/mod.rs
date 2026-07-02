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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::Instrument;
use uuid::Uuid;

const JOB_ID_HEADER: &str = "X-Polytope-Job-Id";

fn extract_job_id(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(JOB_ID_HEADER)?.to_str().ok()?;
    if value.len() != 26 {
        return None;
    }
    if value.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b't' | b'v'..=b'z')) {
        Some(value.to_string())
    } else {
        None
    }
}

fn request_span(
    job_id: Option<&str>,
    key: Option<&str>,
    offset: Option<u64>,
    range: Option<&str>,
) -> tracing::Span {
    match (job_id, key, offset, range) {
        (None, None, None, None) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = tracing::field::Empty,
            offset = tracing::field::Empty,
            range = tracing::field::Empty,
        ),
        (Some(job_id), None, None, None) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = tracing::field::Empty,
            offset = tracing::field::Empty,
            range = tracing::field::Empty,
        ),
        (None, Some(key), None, None) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = key,
            offset = tracing::field::Empty,
            range = tracing::field::Empty,
        ),
        (Some(job_id), Some(key), None, None) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = key,
            offset = tracing::field::Empty,
            range = tracing::field::Empty,
        ),
        (None, None, Some(offset), None) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = tracing::field::Empty,
            offset = offset,
            range = tracing::field::Empty,
        ),
        (Some(job_id), None, Some(offset), None) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = tracing::field::Empty,
            offset = offset,
            range = tracing::field::Empty,
        ),
        (None, Some(key), Some(offset), None) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = key,
            offset = offset,
            range = tracing::field::Empty,
        ),
        (Some(job_id), Some(key), Some(offset), None) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = key,
            offset = offset,
            range = tracing::field::Empty,
        ),
        (None, None, None, Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = tracing::field::Empty,
            offset = tracing::field::Empty,
            range = range,
        ),
        (Some(job_id), None, None, Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = tracing::field::Empty,
            offset = tracing::field::Empty,
            range = range,
        ),
        (None, Some(key), None, Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = key,
            offset = tracing::field::Empty,
            range = range,
        ),
        (Some(job_id), Some(key), None, Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = key,
            offset = tracing::field::Empty,
            range = range,
        ),
        (None, None, Some(offset), Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = tracing::field::Empty,
            offset = offset,
            range = range,
        ),
        (Some(job_id), None, Some(offset), Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = tracing::field::Empty,
            offset = offset,
            range = range,
        ),
        (None, Some(key), Some(offset), Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = tracing::field::Empty,
            "bobs.spool.key" = key,
            offset = offset,
            range = range,
        ),
        (Some(job_id), Some(key), Some(offset), Some(range)) => tracing::info_span!(
            "bobs.request",
            "job.id" = job_id,
            "bobs.spool.key" = key,
            offset = offset,
            range = range,
        ),
    }
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
async fn pprof_profile(Query(params): Query<PprofParams>) -> Response {
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
        .route("/api/v1/status", get(status::<F, M>).head(status_head))
        .route("/api/v1/create", put(create_spool::<F, M>))
        .route("/api/v1/write/{key}/{offset}", post(write_spool::<F, M>))
        .route("/api/v1/complete/{key}", post(complete_spool::<F, M>))
        .route("/api/v1/read/{key}", get(read_spool::<F, M>))
        .route("/api/v1/delete/{key}", delete(delete_spool::<F, M>))
        .route("/debug/pprof/profile", get(pprof_profile))
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

async fn status<F: FileIO, M: MetadataStore>(
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
struct CompleteRequest {
    expected_size: Option<u64>,
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
                .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?
        };
        let labels = state.config.filter_labels(&req.labels);
        let key = Uuid::new_v4().to_string();
        let create_start = Instant::now();
        let create_result = state
            .manager
            .create_spool(
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
            tracing::info!("event.name" = "bobs.spool.created", "job.id" = %job_id, "bobs.spool.key" = %key, content_type = ?req.content_type, content_encoding = ?req.content_encoding, write_locked = req.write_locked, outcome = "success", "spool created");
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
        let mut pending = bytes::BytesMut::with_capacity(write_batch_size);
        let mut write_offset = offset;
        let write_result: std::result::Result<(), crate::error::BobsError> = async {
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|e| BobsError::SerializationError(e.to_string()))?;
                if let Ok(data) = frame.into_data() {
                    let mut cursor = 0;
                    while cursor < data.len() {
                        let remaining_batch_space = write_batch_size - pending.len();
                        let take = remaining_batch_space.min(data.len() - cursor);
                        pending.extend_from_slice(&data[cursor..cursor + take]);
                        cursor += take;
                        if pending.len() == write_batch_size {
                            let batch = std::mem::replace(&mut pending, bytes::BytesMut::with_capacity(write_batch_size)).freeze();
                            let batch_len = batch.len();
                            spool.write(write_offset, batch).await?;
                            write_offset += batch_len as u64;
                        }
                    }
                }
            }
            if !pending.is_empty() {
                let batch_len = pending.len();
                spool.write(write_offset, std::mem::take(&mut pending).freeze()).await?;
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
        write_result.map_err(ApiError)?;
        if let Some(job_id) = &job_id {
            tracing::debug!("event.name" = "bobs.spool.write.completed", "job.id" = %job_id, "bobs.spool.key" = %key, offset = offset, bytes = total_written, outcome = "success", "spool write completed");
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
                .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?
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
            tracing::info!("event.name" = "bobs.spool.completed", "job.id" = %job_id, "bobs.spool.key" = %key, expected_size = ?req.expected_size, bytes = meta.total_bytes_written, outcome = "success", "spool completed");
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
    let content_type_header = HeaderValue::from_str(
        metadata
            .content_type
            .as_deref()
            .unwrap_or("application/octet-stream"),
    )
    .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, content_type_header);

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

        if !spool.is_readable().await {
            return Err(ApiError(BobsError::SpoolLocked));
        }
        spool.acquire_reader();

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
        };
        let long_poll_timeout = Duration::from_millis(state.config.long_poll_timeout_ms);
        let page_size = spool.page_size as u64;
        let metadata = {
            let meta = spool.metadata.lock().await;
            let is_complete = matches!(
                meta.state,
                crate::spool::SpoolState::Complete | crate::spool::SpoolState::Deleting
            );
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

            // First iteration uses the pre-fetched page; subsequent iterations
            // long-poll via read_page with a timeout. Mid-stream timeouts just
            // end the stream (the connection was recently active, not idle).
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
                stream_metrics.record_read_bytes(&stream_labels, stream_mode, chunk_len);
                let chunk_end = offset;

                // 1. Refresh activity timestamp (atomic, lock-free).
                let now = now_secs();
                spool.last_read_activity_at.store(now, Ordering::Relaxed);

                // 2. Record coverage and run the full-read transition when this
                // chunk completes first coverage of the whole object.
                spool
                    .mark_served_and_maybe_fully_read(chunk_start, chunk_end, now)
                    .await;

                // 3. Keep legacy last_read_at for observability (not used in new cleanup).
                {
                    let mut meta = spool.metadata.lock().await;
                    meta.last_read_at = Some(now);
                }

                yield Ok::<Bytes, BobsError>(chunk);
            } else if follow {
                // Page exists but has no data at our offset yet. Check if the
                // writer is done; if so, we've consumed everything. Otherwise
                // loop back and long-poll for more data.
                let done = {
                    let meta = spool.metadata.lock().await;
                    matches!(meta.state, crate::spool::SpoolState::Complete | crate::spool::SpoolState::Deleting)
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
        let completion_span = request_span(
            stream_job_id.as_deref(),
            Some(&stream_key),
            None,
            Some(&stream_range),
        );
        let _completion_span_guard = completion_span.enter();
        tracing::info!("event.name" = "bobs.spool.read.completed", bytes = bytes_served, outcome = outcome, "spool read completed");
        lease.duration_recorded = true;
        stream_metrics.record_read_duration(&stream_labels, stream_mode, outcome, read_start.elapsed().as_secs_f64());
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
        let delete_labels = state.manager.get_spool(&key).map(|s| {
            // We can't async-lock inside a sync map ref, so clone the Arc.
            s
        });
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
            BobsError::SpoolNotFound { .. } => StatusCode::NOT_FOUND,
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
    use crate::io::DefaultFileIO;
    use crate::metadata::DefaultMetadataStore;
    use axum::http::Request;
    use axum::response::IntoResponse;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    fn test_config(dir: &std::path::Path) -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: dir.to_path_buf(),
            page_size: 4096,
            max_cache_bytes: 65536,
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

    /// Returns both the `Router` and the shared `AppState` so tests can inspect
    /// spool fields (e.g. `full_object_read_at`) after HTTP round-trips.
    async fn app_with_state() -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let data_dir = root.join("data");
        let manager = Arc::new(
            SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
                DefaultMetadataStore::new(&data_dir),
                &data_dir,
                4096,
                65536,
                256,
            )
            .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config(&data_dir),
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
            internal_base_url: "http://bobs-0:3000/api/v1".into(),
            metrics: Arc::new(BobsMetrics::new(false, vec![], 128)),
        });
        let app = router::<DefaultFileIO, DefaultMetadataStore>().with_state(Arc::clone(&state));
        (app, state)
    }

    /// Like `app_with_state` but uses `test_config_ttl` (short sweep + TTL values).
    async fn app_with_ttl_config() -> (Router, Arc<AppState<DefaultFileIO, DefaultMetadataStore>>) {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let data_dir = root.join("data");
        let manager = Arc::new(
            SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
                DefaultMetadataStore::new(&data_dir),
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
            metrics: Arc::new(BobsMetrics::new(false, vec![], 128)),
        });
        let app = router::<DefaultFileIO, DefaultMetadataStore>().with_state(Arc::clone(&state));
        (app, state)
    }

    async fn app() -> Router {
        app_with_state().await.0
    }

    // -----------------------------------------------------------------------
    // Shared test helpers
    // -----------------------------------------------------------------------

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
    fn test_api_error_writer_inactive() {
        let err = ApiError(BobsError::WriterInactive);
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

    // -----------------------------------------------------------------------
    // Read-coverage tracking — full_object_read_at and last_read_activity_at
    // -----------------------------------------------------------------------

    /// A single Range request that covers the entire 8 KiB object must set
    /// `full_object_read_at` after the response body is fully drained.
    #[tokio::test]
    async fn test_full_range_sets_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        let status = range_read_drain(&app, &key, "bytes=0-8191").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
            "full-range read must set full_object_read_at"
        );
        let cache = spool.page_cache.lock().await;
        assert!(
            !cache.contains(&key, 0) && !cache.contains(&key, 1),
            "full-read transition must free this spool's page cache entries"
        );
    }

    /// A partial Range request leaves bytes un-served; `full_object_read_at`
    /// must remain 0.
    #[tokio::test]
    async fn test_partial_range_does_not_set_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        let status = range_read_drain(&app, &key, "bytes=0-4095").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert_eq!(
            spool.full_object_read_at.load(Ordering::Relaxed),
            0,
            "partial range must not set full_object_read_at"
        );
        let cache = spool.page_cache.lock().await;
        assert!(
            cache.contains(&key, 0) || cache.contains(&key, 1),
            "partial reads must not free this spool's page cache entries"
        );
    }

    /// Two non-overlapping ranges that together cover the full 8 KiB object
    /// must set `full_object_read_at` after the second request completes.
    #[tokio::test]
    async fn test_two_ranges_covering_full_object_sets_flag() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![7u8; 8192]).await;

        // First half — coverage incomplete.
        range_read_drain(&app, &key, "bytes=0-4095").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert_eq!(
            spool.full_object_read_at.load(Ordering::Relaxed),
            0,
            "after first half: full_object_read_at must still be 0"
        );

        // Second half — now fully covered.
        range_read_drain(&app, &key, "bytes=4096-8191").await;
        assert!(
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
            "after second half: full_object_read_at must be set"
        );
    }

    /// Out-of-order ranges: serve the second half first, then the first half.
    /// `full_object_read_at` must be 0 after the first request and > 0 only
    /// after the second.
    #[tokio::test]
    async fn test_out_of_order_ranges_set_flag_on_completion() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![3u8; 8192]).await;

        // Second half first.
        range_read_drain(&app, &key, "bytes=4096-8191").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert_eq!(
            spool.full_object_read_at.load(Ordering::Relaxed),
            0,
            "only second half served: full_object_read_at must be 0"
        );

        // First half — completes coverage.
        range_read_drain(&app, &key, "bytes=0-4095").await;
        assert!(
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
            "after first half served: full_object_read_at must be set"
        );
    }

    /// Overlapping ranges must not double-count bytes. Two overlapping requests
    /// that together cover a 4 KiB object must set `full_object_read_at`.
    #[tokio::test]
    async fn test_overlapping_ranges_do_not_double_count() {
        let (app, state) = app_with_state().await;
        // 4 KiB = exactly one page.
        let key = write_and_complete(&app, vec![9u8; 4096]).await;

        // bytes 0-3000 (first request).
        range_read_drain(&app, &key, "bytes=0-3000").await;
        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert_eq!(
            spool.full_object_read_at.load(Ordering::Relaxed),
            0,
            "bytes 3001-4095 still missing: flag must be 0"
        );

        // bytes 2000-4095 — overlaps [0,3001) and covers [3001,4096).
        range_read_drain(&app, &key, "bytes=2000-4095").await;
        assert!(
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
            "overlapping second range completes coverage: flag must be set"
        );
    }

    /// A follow-GET (no Range header) that consumes the entire body must set
    /// `full_object_read_at`.
    #[tokio::test]
    async fn test_follow_read_sets_full_object_read_at() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![5u8; 4096]).await;

        let status = follow_read_drain(&app, &key).await;
        assert_eq!(status, StatusCode::OK);

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert!(
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
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

    /// Any read that yields at least one chunk must update `last_read_activity_at`.
    #[tokio::test]
    async fn test_last_read_activity_updated_on_chunk_yield() {
        let (app, state) = app_with_state().await;
        let key = write_and_complete(&app, vec![1u8; 4096]).await;

        let spool = state.manager.get_spool(&key).expect("spool must exist");
        assert_eq!(
            spool.last_read_activity_at.load(Ordering::Relaxed),
            0,
            "no reads yet: last_read_activity_at must be 0"
        );

        range_read_drain(&app, &key, "bytes=0-4095").await;

        assert!(
            spool.last_read_activity_at.load(Ordering::Relaxed) > 0,
            "after range read: last_read_activity_at must be updated"
        );
    }

    // -----------------------------------------------------------------------
    // Cleanup TTL integration — HTTP read → cleanup deletion
    //
    // NOTE on wall-clock vs tokio time:
    // `crate::time::now_secs()` uses `SystemTime::now()` (wall clock).
    // `tokio::time::advance()` only advances
    // the tokio virtual clock, which controls `tokio::time::interval` sweeps.
    // To make a TTL condition fire we set the stored timestamp to `1` (a value
    // in 1970 that is always >> any TTL seconds behind the current wall clock).
    // `tokio::time::advance()` is used solely to trigger the cleanup sweep.
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
            spool.full_object_read_at.load(Ordering::Relaxed) > 0,
            "HTTP read path must set full_object_read_at after full coverage"
        );

        // Simulate that full-read and the latest byte-serving activity happened
        // long ago so the short-TTL comparison fires. Wall clock can't be
        // advanced by tokio::time, so use an old epoch value.
        spool.full_object_read_at.store(1, Ordering::SeqCst);
        spool.last_read_activity_at.store(1, Ordering::SeqCst);

        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;

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
        // No reads: last_read_activity_at must be 0.
        assert_eq!(spool.last_read_activity_at.load(Ordering::Relaxed), 0);

        // Set readable_at to an old epoch value so the idle TTL fires.
        // (complete() sets readable_at = now_secs(); we override to simulate an
        // old spool whose idle TTL has clearly expired.)
        {
            let mut meta = spool.metadata.lock().await;
            meta.readable_at = Some(1);
        }

        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;

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

        // Make readable_at old so that, without activity, idle TTL would fire.
        {
            let mut meta = spool.metadata.lock().await;
            meta.readable_at = Some(1);
        }

        // Perform a range read — this stores last_read_activity_at = now_secs().
        let status = range_read_drain(&app, &key, "bytes=0-4095").await;
        assert_eq!(status, StatusCode::PARTIAL_CONTENT);
        assert!(
            spool.last_read_activity_at.load(Ordering::Relaxed) > 0,
            "range read must update last_read_activity_at"
        );

        // Advance tokio time to trigger several cleanup sweeps. Because
        // last_read_activity_at ≈ now_secs(), the idle check
        // (now - last_activity ≈ 0 < 2) must NOT delete the spool.
        let task = start_cleanup_task(state.manager.clone(), state.config.clone());
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::task::yield_now().await;

        assert!(
            state.manager.get_spool(&key).is_some(),
            "spool with recent read activity must NOT be deleted"
        );

        // Simulate that activity has now stopped (reset to old epoch value).
        // idle_anchor = last_read_activity_at = 1 → now - 1 >> 2 → idle fires.
        spool.last_read_activity_at.store(1, Ordering::SeqCst);

        tokio::time::advance(Duration::from_secs(3)).await;
        // measure_disk_usage is now spawned fire-and-forget, so the cleanup
        // loop returns to interval.tick() without blocking on spawn_blocking
        // I/O.  A single yield is enough for the deletion sweep to run.
        tokio::task::yield_now().await;

        assert!(
            state.manager.get_spool(&key).is_none(),
            "spool must be deleted once activity stops and idle TTL expires"
        );
        task.abort();
    }
}
