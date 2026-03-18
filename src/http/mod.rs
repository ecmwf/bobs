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

pub struct AppState<F: FileIO> {
    pub manager: Arc<SpoolManager<F>>,
    pub config: Arc<Config>,
}

pub fn router<F: FileIO + 'static>() -> Router<Arc<AppState<F>>> {
    Router::new()
        .route("/status", get(status).head(status_head))
        .route("/create", put(create_spool::<F>))
        .route("/write/{key}/{offset}", post(write_spool::<F>))
        .route("/close/{key}", post(close_spool::<F>))
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
    #[serde(default)]
    write_locked: bool,
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
        .create_spool(req.content_type, req.write_locked)
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

async fn close_spool<F: FileIO>(
    State(state): State<Arc<AppState<F>>>,
    Path(key): Path<String>,
) -> std::result::Result<Response, ApiError> {
    let spool = state
        .manager
        .get_spool(&key)
        .ok_or_else(|| ApiError(BobsError::SpoolNotFound { key: key.clone() }))?;
    spool.close().await.map_err(ApiError)?;
    Ok(StatusCode::OK.into_response())
}

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
    spool.acquire_reader().map_err(ApiError)?;

    let lease = ReaderLease {
        spool: Arc::clone(&spool),
    };
    let page_size = spool.page_size as u64;
    let follow = end == 0;

    let stream = stream! {
        let _lease = lease;
        let mut offset = start;

        loop {
            if !follow && offset >= end {
                break;
            }

            let page_idx = offset / page_size;
            let page_start = page_idx * page_size;
            let page_end = page_start + page_size;

            let maybe_page = match spool.read_page(page_idx).await {
                Ok(v) => v,
                Err(e) => {
                    yield Err::<Bytes, BobsError>(e);
                    break;
                }
            };
            let Some(page) = maybe_page else {
                break;
            };

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
                let done = {
                    let meta = spool.metadata.lock().await;
                    matches!(meta.state, crate::spool::SpoolState::Closed | crate::spool::SpoolState::Deleting)
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
    response.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    response.headers_mut().insert(
        "X-Accel-Buffering",
        HeaderValue::from_static("no"),
    );

    Ok(response)
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
            BobsError::SpoolLocked => StatusCode::LOCKED,
            BobsError::SpoolClosed | BobsError::ReaderAlreadyActive => StatusCode::CONFLICT,
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

        let close_req = Request::builder()
            .method("POST")
            .uri(format!("/close/{key}"))
            .body(Body::empty())
            .expect("request build");
        let close_resp = app.clone().oneshot(close_req).await.expect("oneshot");
        assert_eq!(close_resp.status(), StatusCode::OK);

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
