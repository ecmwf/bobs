use crate::config::Config;
use crate::error::BobsError;
use crate::io::FileIO;
use crate::manager::SpoolManager;
use async_stream::stream;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

enum ReadRequestRange {
    Follow,
    Bounded {
        start: u64,
        end_inclusive: Option<u64>,
    },
}

pub struct AppState<F: FileIO> {
    pub manager: Arc<SpoolManager<F>>,
    pub config: Arc<Config>,
    pub hostname: String,
    pub ordinal: String,
    pub internal_base_url: String,
}

pub fn router<F: FileIO + 'static>() -> Router<Arc<AppState<F>>> {
    Router::new()
        .route("/api/v1/health", get(health::<F>))
        .route("/api/v1/status", get(status).head(status_head))
        .route("/api/v1/create", put(create_spool::<F>))
        .route("/api/v1/write/{key}/{offset}", post(write_spool::<F>))
        .route("/api/v1/complete/{key}", post(complete_spool::<F>))
        .route("/api/v1/read/{key}", get(read_spool::<F>))
        .route("/api/v1/delete/{key}", delete(delete_spool::<F>))
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    hostname: String,
}

async fn health<F: FileIO>(State(state): State<Arc<AppState<F>>>) -> impl IntoResponse {
    Json(StatusResponse {
        status: "ok",
        hostname: state.hostname.clone(),
    })
}

async fn status<F: FileIO>(State(state): State<Arc<AppState<F>>>) -> impl IntoResponse {
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

async fn create_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    body: Bytes,
) -> std::result::Result<Response, ApiError> {
    let req = if body.is_empty() {
        CreateRequest::default()
    } else {
        serde_json::from_slice::<CreateRequest>(&body)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?
    };
    tracing::info!(
        content_type = ?req.content_type,
        content_encoding = ?req.content_encoding,
        write_locked = req.write_locked,
        "create spool request"
    );
    let key = Uuid::new_v4().to_string();
    state
        .manager
        .create_spool(
            key.clone(),
            req.content_type,
            req.content_encoding,
            req.write_locked,
        )
        .await
        .map_err(ApiError)?;
    tracing::info!(key = %key, "spool created");
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
}

async fn write_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path((key, offset)): Path<(String, u64)>,
    mut body: Body,
) -> std::result::Result<Response, ApiError> {
    tracing::info!(key = %key, offset = offset, "write spool request");
    let spool = state
        .manager
        .get_spool(&key)
        .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;

    // Batch incoming body frames into page-sized writes. `spool.write` does a
    // redb metadata commit per call, so writing each HTTP/2 DATA frame directly
    // would be expensive; buffering the whole request body would make memory
    // scale with the client chunk size. Page-sized batches keep the write path
    // bounded while preserving BOBS's page-at-a-time persistence model.
    let write_batch_size = state.config.page_size;
    let mut pending = bytes::BytesMut::with_capacity(write_batch_size);
    let mut write_offset = offset;

    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        if let Ok(data) = frame.into_data() {
            let mut cursor = 0;
            while cursor < data.len() {
                let remaining_batch_space = write_batch_size - pending.len();
                let take = remaining_batch_space.min(data.len() - cursor);
                pending.extend_from_slice(&data[cursor..cursor + take]);
                cursor += take;

                if pending.len() == write_batch_size {
                    spool
                        .write(write_offset, &pending)
                        .await
                        .map_err(ApiError)?;
                    write_offset += pending.len() as u64;
                    pending.clear();
                }
            }
        }
    }

    if !pending.is_empty() {
        spool
            .write(write_offset, &pending)
            .await
            .map_err(ApiError)?;
    }

    Ok(StatusCode::OK.into_response())
}

async fn complete_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path(key): Path<String>,
    body: Bytes,
) -> std::result::Result<Response, ApiError> {
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
    spool.complete(req.expected_size).await.map_err(ApiError)?;
    tracing::info!(key = %key, expected_size = ?req.expected_size, "spool completed");
    Ok(StatusCode::OK.into_response())
}

/// RAII guard that decrements the spool's reader count on drop, ensuring cleanup
/// sees the correct active reader count even if the stream is cancelled mid-flight.
struct ReaderLease<F: FileIO> {
    spool: Arc<crate::spool::Spool<F>>,
}

impl<F: FileIO> Drop for ReaderLease<F> {
    fn drop(&mut self) {
        self.spool.release_reader();
    }
}

async fn read_spool<F: FileIO + 'static>(
    State(state): State<Arc<AppState<F>>>,
    Path(key): Path<String>,
    headers: axum::http::HeaderMap,
) -> std::result::Result<Response, ApiError> {
    let request_range = parse_range(headers.get(axum::http::header::RANGE)).map_err(ApiError)?;

    let spool = state
        .manager
        .get_spool(&key)
        .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;

    if !spool.is_readable().await {
        return Err(ApiError(BobsError::SpoolLocked));
    }
    spool.acquire_reader();

    let lease = ReaderLease {
        spool: Arc::clone(&spool),
    };
    let long_poll_timeout = Duration::from_millis(state.config.long_poll_timeout_ms);
    let page_size = spool.page_size as u64;
    let (content_type, content_encoding, complete_size, total_bytes_written) = {
        let meta = spool.metadata.lock().await;
        let complete_size = if matches!(
            meta.state,
            crate::spool::SpoolState::Complete | crate::spool::SpoolState::Deleting
        ) {
            Some(meta.total_bytes_written)
        } else {
            None
        };
        (
            meta.content_type.clone(),
            meta.content_encoding.clone(),
            complete_size,
            meta.total_bytes_written,
        )
    };
    let (start, end, follow) = match request_range {
        // A no-Range request follows an in-progress object. Once the object is
        // complete, the final size is known and the same request becomes a
        // bounded full-object read so clients can receive Content-Length.
        ReadRequestRange::Follow => (0, complete_size, true),
        ReadRequestRange::Bounded {
            start,
            end_inclusive,
        } => {
            let end = match end_inclusive {
                Some(end_inclusive) => Some(end_inclusive.checked_add(1).ok_or_else(|| {
                    ApiError(BobsError::InvalidRange("range end overflow".into()))
                })?),
                None => Some(total_bytes_written),
            };
            (start, end, false)
        }
    };
    tracing::info!(key = %key, start = start, end = ?end, follow = follow, "read spool request");

    if let Some(end) = end {
        if start > end {
            return Err(ApiError(BobsError::InvalidRange(
                "range start exceeds end".into(),
            )));
        }
    }

    if !follow && start >= total_bytes_written {
        return Err(ApiError(BobsError::InvalidRange(
            "range start exceeds available bytes".into(),
        )));
    }

    // Pre-fetch the first page before committing to a streaming response.
    // If the timeout fires before any data arrives, return a 307 redirect
    // so standard clients (curl -L, browsers) retry automatically.
    let first_page_idx = start / page_size;
    let first_page =
        match tokio::time::timeout(long_poll_timeout, spool.read_page(first_page_idx)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => return Err(ApiError(e)),
            Err(_) => {
                // Lease drops here, releasing the reader.
                return Ok(long_poll_redirect(&key));
            }
        };

    let stream = stream! {
        let _lease = lease;
        let mut offset = start;
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
                        yield Err::<Bytes, BobsError>(e);
                        break;
                    }
                    Err(_) => break,
                }
            } else {
                match spool.read_page(page_idx).await {
                    Ok(v) => v,
                    Err(e) => {
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
                let chunk = Bytes::copy_from_slice(&page[slice_start..slice_end]);
                let chunk_start = offset;
                offset += chunk.len() as u64;
                let chunk_end = offset;

                // 1. Refresh activity timestamp (atomic, lock-free).
                let now = now_secs();
                spool.last_read_activity_at.store(now, Ordering::Relaxed);

                // 2. Record coverage and detect full-read completion.
                // Both locks are uncontested in the common single-reader case;
                // can be batched in a future optimisation if profiling shows contention.
                let became_fully_read = {
                    let mut mr = spool.missing_ranges.lock().await;
                    mr.mark_served(chunk_start, chunk_end);
                    mr.is_complete()
                        && spool.full_object_read_at.load(Ordering::Relaxed) == 0
                        && spool
                            .full_object_read_at
                            .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst)
                            .is_ok()
                };
                // Once every byte has been served at least once, the in-memory
                // page cache is redundant (further reads come from disk), so free
                // it now. Done outside the missing_ranges lock to avoid nesting.
                if became_fully_read {
                    spool.page_cache.lock().await.clear();
                    spool.release_admission();
                }

                // 3. Keep legacy last_read_at for observability (not used in new cleanup).
                {
                    let mut meta = spool.metadata.lock().await;
                    meta.last_read_at = Some(now);
                }

                yield Ok::<Bytes, BobsError>(chunk);
            } else if follow {
                // Page exists but has no data at our offset yet. If the writer is
                // done, no further data will ever arrive, so stop -- never
                // busy-spin on a completed spool. (Previously this required
                // offset >= total_bytes_written; a short/truncated page left
                // offset stuck below total and turned the `continue` into an
                // unbounded CPU spin. Breaking on writer-done makes that
                // impossible: by here we have served all available data.)
                // Otherwise loop back and long-poll for more data.
                let writer_done = {
                    let meta = spool.metadata.lock().await;
                    matches!(
                        meta.state,
                        crate::spool::SpoolState::Complete | crate::spool::SpoolState::Deleting
                    )
                };
                if writer_done {
                    break;
                }
                continue;
            } else {
                break;
            }
        }
    };

    let mut response = Body::from_stream(stream).into_response();
    *response.status_mut() = if follow {
        StatusCode::OK
    } else {
        StatusCode::PARTIAL_CONTENT
    };

    let content_type_header = HeaderValue::from_str(
        content_type
            .as_deref()
            .unwrap_or("application/octet-stream"),
    )
    .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
    response
        .headers_mut()
        .insert(axum::http::header::CONTENT_TYPE, content_type_header);
    if let Some(enc) = &content_encoding {
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
    if let Some(range_end) = end {
        let response_bytes = range_end.saturating_sub(start);
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
    if !follow {
        let total = complete_size
            .map(|v| v.to_string())
            .unwrap_or_else(|| "*".to_string());
        let last_byte = match end {
            Some(e) => e.saturating_sub(1),
            None => start,
        };
        let content_range = format!("bytes {}-{}/{}", start, last_byte, total);
        let content_range_header = HeaderValue::from_str(&content_range)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response
            .headers_mut()
            .insert(axum::http::header::CONTENT_RANGE, content_range_header);
    }

    Ok(response)
}

fn long_poll_redirect(key: &str) -> Response {
    let location = format!("/api/v1/read/{key}");
    let mut response = (
        StatusCode::TEMPORARY_REDIRECT,
        [(axum::http::header::LOCATION, location)],
    )
        .into_response();
    response.headers_mut().insert(
        axum::http::header::ACCEPT_RANGES,
        HeaderValue::from_static("bytes"),
    );
    response
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
        return Err(BobsError::InvalidRange(format!(
            "range start missing: {raw}"
        )));
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

async fn delete_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path(key): Path<String>,
) -> std::result::Result<Response, ApiError> {
    tracing::info!(key = %key, "delete spool request");
    state.manager.delete_spool(&key).await.map_err(ApiError)?;
    Ok(StatusCode::OK.into_response())
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

struct ApiError(BobsError);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0 {
            BobsError::SpoolNotFound { .. } => StatusCode::NOT_FOUND,
            BobsError::OffsetMismatch { .. } => StatusCode::BAD_REQUEST,
            BobsError::SizeMismatch { .. } => StatusCode::BAD_REQUEST,
            BobsError::InvalidRange(_) => StatusCode::BAD_REQUEST,
            BobsError::SpoolLocked => StatusCode::LOCKED,
            BobsError::SpoolClosed => StatusCode::CONFLICT,
            BobsError::InvalidState { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(ErrorResponse {
                error: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cleanup::start_cleanup_task;
    use crate::error::BobsError;
    use crate::io::TokioFileIO;
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
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
        })
    }

    /// Short-TTL config for cleanup integration tests.
    /// sweep=1s so a single `tokio::time::advance(3s)` triggers several sweeps.
    /// read_idle_ttl_secs=2 / full_read_complete_ttl_secs=2 are short but non-zero.
    fn test_config_ttl() -> Arc<Config> {
        Arc::new(Config {
            host: "127.0.0.1".into(),
            port: 0,
            data_dir: std::path::PathBuf::from("./data"),
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
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
        })
    }

    /// Returns both the `Router` and the shared `AppState` so tests can inspect
    /// spool fields (e.g. `full_object_read_at`) after HTTP round-trips.
    async fn app_with_state() -> (Router, Arc<AppState<TokioFileIO>>) {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let db_path = root.join("spools.redb");
        let data_dir = root.join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 65536, 256)
                .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config(&data_dir),
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
            internal_base_url: "http://bobs-0:3000/api/v1".into(),
        });
        let app = router::<TokioFileIO>().with_state(Arc::clone(&state));
        (app, state)
    }

    /// Like `app_with_state` but uses `test_config_ttl` (short sweep + TTL values).
    async fn app_with_ttl_config() -> (Router, Arc<AppState<TokioFileIO>>) {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let db_path = root.join("spools.redb");
        let data_dir = root.join("data");
        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 65536, 256)
                .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config_ttl(),
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
            internal_base_url: "http://bobs-0:3000/api/v1".into(),
        });
        let app = router::<TokioFileIO>().with_state(Arc::clone(&state));
        (app, state)
    }

    async fn app() -> Router {
        app_with_state().await.0
    }

    // -----------------------------------------------------------------------
    // Shared test helpers
    // -----------------------------------------------------------------------

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
    fn test_long_poll_redirect_uses_api_path() {
        let response = long_poll_redirect("abc123");
        assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|value| value.to_str().ok()),
            Some("/api/v1/read/abc123")
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
    // `now_secs()` in both read_spool and run_cleanup_loop uses
    // `SystemTime::now()` (wall clock). `tokio::time::advance()` only advances
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
        tokio::task::yield_now().await;

        assert!(
            state.manager.get_spool(&key).is_none(),
            "spool must be deleted once activity stops and idle TTL expires"
        );
        task.abort();
    }
}
