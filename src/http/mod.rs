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

pub struct AppState<F: FileIO> {
    pub manager: Arc<SpoolManager<F>>,
    pub config: Arc<Config>,
}

pub fn router<F: FileIO + 'static>() -> Router<Arc<AppState<F>>> {
    Router::new()
        .route("/status", get(status).head(status_head))
        .route("/create", put(create_spool::<F>))
        .route("/write/{key}/{offset}", post(write_spool::<F>))
        .route("/complete/{key}", post(complete_spool::<F>))
        .route("/read/{key}/{start}/{end}", get(read_spool::<F>))
        .route("/delete/{key}", delete(delete_spool::<F>))
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    status: &'static str,
    bob_id: String,
}

async fn status<F: FileIO>(State(state): State<Arc<AppState<F>>>) -> impl IntoResponse {
    Json(StatusResponse {
        status: "ok",
        bob_id: state.config.bob_id.clone(),
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
    let key = state
        .manager
        .create_spool(req.content_type, req.content_encoding, req.write_locked)
        .await
        .map_err(ApiError)?;
    Ok((StatusCode::CREATED, Json(CreateResponse { key })).into_response())
}

async fn write_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path((key, offset)): Path<(String, u64)>,
    mut body: Body,
) -> std::result::Result<Response, ApiError> {
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
    Path((key, start, end)): Path<(String, u64, u64)>,
) -> std::result::Result<Response, ApiError> {
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
    let page_size = spool.page_size as u64;
    // end=0 means "follow" mode: stream until the writer closes the spool.
    // end>0 means bounded read: return bytes in [start, end) and stop.
    let follow = end == 0;
    let long_poll_timeout = Duration::from_millis(state.config.long_poll_timeout_ms);
    let (content_type, content_encoding, checksum_crc32c) = {
        let meta = spool.metadata.lock().await;
        (
            meta.content_type.clone(),
            meta.content_encoding.clone(),
            meta.checksum_crc32c,
        )
    };

    // Pre-fetch the first page before committing to a streaming response.
    // If the timeout fires before any data arrives, return a 307 redirect
    // so standard clients (curl -L, browsers) retry automatically.
    let first_page_idx = start / page_size;
    let first_page = match tokio::time::timeout(
        long_poll_timeout,
        spool.read_page(first_page_idx),
    )
    .await
    {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return Err(ApiError(e)),
        Err(_) => {
            // Lease drops here, releasing the reader.
            return Ok(long_poll_redirect(&key, start, end));
        }
    };

    let stream = stream! {
        let _lease = lease;
        let mut offset = start;
        let mut prefetched = first_page;

        loop {
            if !follow && offset >= end {
                break;
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
            } else {
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
            };
            let Some(page) = maybe_page else {
                break;
            };

            // Slice the page to the requested byte range. The offset may not be
            // page-aligned (partial first page) and the end may land mid-page.
            let slice_start = (offset - page_start) as usize;
            let logical_end = if follow { page_end } else { end.min(page_end) };
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
    let content_type_header = HeaderValue::from_str(
        content_type.as_deref().unwrap_or("application/octet-stream"),
    )
    .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        content_type_header,
    );
    if let Some(enc) = &content_encoding {
        let encoding_header = HeaderValue::from_str(enc)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response.headers_mut().insert(
            axum::http::header::CONTENT_ENCODING,
            encoding_header,
        );
    }
    if let Some(crc) = checksum_crc32c {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(crc.to_be_bytes());
        let checksum_header = HeaderValue::from_str(&encoded)
            .map_err(|e| ApiError(BobsError::SerializationError(e.to_string())))?;
        response.headers_mut().insert(
            "X-Checksum-CRC32C",
            checksum_header,
        );
    }
    response.headers_mut().insert(
        "X-Accel-Buffering",
        HeaderValue::from_static("no"),
    );

    Ok(response)
}

fn long_poll_redirect(key: &str, start: u64, end: u64) -> Response {
    let location = format!("/read/{key}/{start}/{end}");
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(axum::http::header::LOCATION, location)],
    )
        .into_response()
}

async fn delete_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path(key): Path<String>,
) -> std::result::Result<Response, ApiError> {
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
            BobsError::SpoolLocked => StatusCode::LOCKED,
            BobsError::SpoolClosed => StatusCode::CONFLICT,
            BobsError::InvalidState { .. } => StatusCode::CONFLICT,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, Json(ErrorResponse { error: self.0.to_string() })).into_response()
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
    use crate::io::TokioFileIO;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    fn test_config(dir: &std::path::Path) -> Arc<Config> {
        Arc::new(Config {
            listen_addr: "127.0.0.1:0".into(),
            data_dir: dir.to_path_buf(),
            page_size: 4096,
            page_cache_capacity: 16,
            writer_inactivity_timeout_secs: 300,
            reader_done_ttl_secs: 60,
            unread_ttl_secs: 3600,
            cleanup_sweep_interval_secs: 30,
            long_poll_timeout_ms: 25000,
            bob_id: "http-bob".into(),
        })
    }

    async fn app() -> Router {
        let root = std::env::temp_dir().join(format!("bobs-http-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).expect("create test root");
        let db_path = root.join("spools.redb");
        let data_dir = root.join("data");

        let manager = Arc::new(
            SpoolManager::<TokioFileIO>::new(&db_path, &data_dir, "http-bob".into(), 4096, 16)
                .expect("manager init"),
        );
        let state = Arc::new(AppState {
            manager,
            config: test_config(&data_dir),
        });

        router::<TokioFileIO>().with_state(state)
    }

    async fn create_key(app: &Router) -> String {
        let req = Request::builder()
            .method("PUT")
            .uri("/create")
            .body(Body::empty())
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::CREATED);
        let body = resp.into_body().collect().await.expect("collect body").to_bytes();
        let v: Value = serde_json::from_slice(&body).expect("json parse");
        v["key"].as_str().expect("key string").to_string()
    }

    #[tokio::test]
    async fn test_status() {
        let app = app().await;
        let req = Request::builder()
            .method("GET")
            .uri("/status")
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
            .uri(format!("/delete/{key}"))
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
            .uri("/delete/missing")
            .body(Body::empty())
            .expect("request build");
        let resp = app.oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let app = app().await;
        let key = create_key(&app).await;

        let data = vec![7u8; 8192];
        let req = Request::builder()
            .method("POST")
            .uri(format!("/write/{key}/0"))
            .body(Body::from(data.clone()))
            .expect("request build");
        let resp = app.clone().oneshot(req).await.expect("oneshot");
        assert_eq!(resp.status(), StatusCode::OK);

        let complete_req = Request::builder()
            .method("POST")
            .uri(format!("/complete/{key}"))
            .body(Body::empty())
            .expect("request build");
        let complete_resp = app.clone().oneshot(complete_req).await.expect("oneshot");
        assert_eq!(complete_resp.status(), StatusCode::OK);

        let read_req = Request::builder()
            .method("GET")
            .uri(format!("/read/{key}/0/8192"))
            .body(Body::empty())
            .expect("request build");
        let read_resp = app.oneshot(read_req).await.expect("oneshot");
        assert_eq!(read_resp.status(), StatusCode::OK);
        assert_eq!(
            read_resp.headers().get("X-Accel-Buffering").and_then(|h| h.to_str().ok()),
            Some("no")
        );
    }
}
