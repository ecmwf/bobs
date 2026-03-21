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
use std::sync::Arc;
use std::time::Duration;

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
    let key = state
        .manager
        .create_spool(req.content_type, req.content_encoding, req.write_locked)
        .await
        .map_err(ApiError)?;
    tracing::info!(key = %key, "spool created");
    let read_url = format!(
        "https://{}.{}/{}-{}/api/v1/read/{}",
        state.config.host_prefix, state.config.domain, state.config.route_name, state.ordinal, key
    );
    Ok((StatusCode::CREATED, Json(CreateResponse { key, read_url })).into_response())
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

    let mut current_offset = offset;
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        if let Ok(data) = frame.into_data() {
            spool.write(current_offset, &data).await.map_err(ApiError)?;
            current_offset += data.len() as u64;
        }
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
    let (content_type, content_encoding, checksum_crc32c, complete_size, total_bytes_written) = {
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
            meta.checksum_crc32c,
            complete_size,
            meta.total_bytes_written,
        )
    };
    let (start, end, follow) = match request_range {
        ReadRequestRange::Follow => (0, None, true),
        ReadRequestRange::Bounded {
            start,
            end_inclusive,
        } => {
            let end = match end_inclusive {
                Some(end_inclusive) => Some(
                    end_inclusive
                        .checked_add(1)
                        .ok_or_else(|| ApiError(BobsError::InvalidRange("range end overflow".into())))?,
                ),
                None => Some(total_bytes_written),
            };
            (start, end, false)
        }
    };
    tracing::info!(key = %key, start = start, end = ?end, follow = follow, "read spool request");

    if let Some(end) = end {
        if start > end {
            return Err(ApiError(BobsError::InvalidRange("range start exceeds end".into())));
        }
    }

    if !follow && start >= total_bytes_written {
        return Err(ApiError(BobsError::InvalidRange("range start exceeds available bytes".into())));
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
                {
                    let mut meta = spool.metadata.lock().await;
                    meta.last_read_at = Some(now_secs());
                }
                offset += chunk.len() as u64;
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
    if let Some(crc) = checksum_crc32c {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let checksum_header = HeaderValue::from_str(&encoded)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response
            .headers_mut()
            .insert("X-Checksum-CRC32C", checksum_header);
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
        return Err(BobsError::InvalidRange(format!("range start missing: {raw}")));
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
        return Err(BobsError::InvalidRange(format!("range start exceeds end: {raw}")));
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
            writer_inactivity_timeout_secs: 300,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 30,
            long_poll_timeout_ms: 25000,
            host_prefix: "test".into(),
            domain: "example.com".into(),
            route_name: "bobs".into(),
        })
    }

    async fn app() -> Router {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let db_path = root.join("spools.redb");
        let data_dir = root.join("data");

        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, 4096, 65536)
                .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config(&data_dir),
            hostname: "bobs-0".into(),
            ordinal: "0".into(),
        });

        router::<TokioFileIO>().with_state(state)
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
        let err = ApiError(BobsError::IoError(std::io::Error::new(
            std::io::ErrorKind::Other,
            "disk full",
        )));
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
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
    }

    #[test]
    fn test_parse_range_rejects_malformed_start() {
        let val = HeaderValue::from_static("bytes=abc-100");
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
    }

    #[test]
    fn test_parse_range_rejects_malformed_end() {
        let val = HeaderValue::from_static("bytes=10-xyz");
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
    }

    #[test]
    fn test_parse_range_rejects_both_malformed() {
        let val = HeaderValue::from_static("bytes=abc-def");
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
    }

    #[test]
    fn test_parse_range_rejects_empty_value() {
        let val = HeaderValue::from_static("");
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
    }

    #[test]
    fn test_parse_range_rejects_descending_range() {
        let val = HeaderValue::from_static("bytes=10-1");
        assert!(matches!(parse_range(Some(&val)), Err(BobsError::InvalidRange(_))));
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
}
