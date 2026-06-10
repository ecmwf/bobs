use axum::body::{Body, Bytes};
use axum::http::{Method, Request, StatusCode};
use bobs::config::Config;
use bobs::http::{router, AppState};
use bobs::io::DefaultFileIO;
use bobs::manager::SpoolManager;
use bobs::metadata::DefaultMetadataStore;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tower::ServiceExt;

const VALID_JOB_ID: &str = "0123456789abcdefghjkmnpqrs";

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn with_rust_log<T>(rust_log: Option<&str>, f: impl FnOnce() -> T) -> T {
    let _guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let previous = std::env::var("RUST_LOG").ok();
    match rust_log {
        Some(value) => std::env::set_var("RUST_LOG", value),
        None => std::env::remove_var("RUST_LOG"),
    }
    let result = f();
    match previous {
        Some(value) => std::env::set_var("RUST_LOG", value),
        None => std::env::remove_var("RUST_LOG"),
    }
    result
}

fn test_config(dir: &std::path::Path) -> Arc<Config> {
    Arc::new(Config {
        host: "127.0.0.1".into(),
        port: 0,
        data_dir: dir.to_path_buf(),
        page_size: 4096,
        max_cache_bytes: 65536,
        writer_inactivity_timeout_secs: 300,
        read_idle_ttl_secs: 600,
        full_read_complete_ttl_secs: 30,
        reader_done_ttl_secs: 60,
        unread_ttl_secs: 3600,
        cleanup_sweep_interval_secs: 30,
        long_poll_timeout_ms: 25,
        io_uring_shards: None,
        io_uring_queue_capacity: 1024,
        host_prefix: "test".into(),
        domain: "example.com".into(),
        route_name: "bobs".into(),
    })
}

async fn app() -> axum::Router {
    let dir = tempdir().expect("tempdir").keep();
    let data_dir = dir.join("data");
    let manager = Arc::new(
        SpoolManager::<DefaultFileIO, DefaultMetadataStore>::with_metadata_store(
            DefaultMetadataStore::new(&data_dir),
            &data_dir,
            4096,
            65536,
        )
        .expect("manager"),
    );
    let state = Arc::new(AppState {
        manager,
        config: test_config(&data_dir),
        hostname: "bobs-0".into(),
        ordinal: "0".into(),
        internal_base_url: "http://bobs-0:3000/api/v1".into(),
    });
    router::<DefaultFileIO, DefaultMetadataStore>().with_state(state)
}

fn req(method: Method, uri: String, body: Body, job_id: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(job_id) = job_id {
        builder = builder.header("X-Polytope-Job-Id", job_id);
    }
    builder.body(body).unwrap()
}

async fn create(
    app: axum::Router,
    job_id: Option<&str>,
    content_type: &str,
) -> (axum::Router, String) {
    let body = json!({"content_type": content_type, "content_encoding": "identity", "write_locked": false}).to_string();
    let response = app
        .clone()
        .oneshot(req(
            Method::PUT,
            "/api/v1/create".into(),
            Body::from(body),
            job_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (app, value["key"].as_str().unwrap().to_string())
}

async fn write_complete_read_delete(
    app: axum::Router,
    key: &str,
    job_id: Option<&str>,
) -> axum::Router {
    let response = app
        .clone()
        .oneshot(req(
            Method::POST,
            format!("/api/v1/write/{key}/0"),
            Body::from("hello"),
            job_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(req(
            Method::POST,
            format!("/api/v1/complete/{key}"),
            Body::from(r#"{"expected_size":5}"#),
            job_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(req(
            Method::GET,
            format!("/api/v1/read/{key}"),
            Body::empty(),
            job_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(bytes, Bytes::from_static(b"hello"));
    let response = app
        .clone()
        .oneshot(req(
            Method::DELETE,
            format!("/api/v1/delete/{key}"),
            Body::empty(),
            job_id,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    app
}

#[tokio::test(flavor = "current_thread")]
async fn observability_valid_job_flow_emits_spool_events() {
    let (subscriber, logs) = with_rust_log(Some("debug"), || {
        bobs::observability::capturing_subscriber("bobs")
    });
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = app().await;
    let (app, key) = create(app, Some(VALID_JOB_ID), "application/octet-stream").await;
    let _app = write_complete_read_delete(app, &key, Some(VALID_JOB_ID)).await;
    logs.assert_required_fields();
    for name in [
        "bobs.spool.created",
        "bobs.spool.write.completed",
        "bobs.spool.completed",
        "bobs.spool.read.started",
        "bobs.spool.read.completed",
        "bobs.spool.deleted",
    ] {
        logs.assert_event_emitted(name);
        assert!(
            logs.events_named(name)
                .iter()
                .any(|event| event["attributes"]["job.id"] == VALID_JOB_ID),
            "missing job.id on {name}: {}",
            logs.raw()
        );
    }
    assert!(logs
        .events_named("bobs.spool.deleted")
        .iter()
        .any(|event| event["attributes"]["reason"] == "explicit"));
    assert!(logs
        .events_named("bobs.spool.created")
        .iter()
        .any(|event| event["attributes"]["bobs.spool.key"] == key));
}

#[tokio::test(flavor = "current_thread")]
async fn observability_no_header_flow_succeeds_without_job_id() {
    let (subscriber, logs) = with_rust_log(Some("debug"), || {
        bobs::observability::capturing_subscriber("bobs")
    });
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = app().await;
    let (app, key) = create(app, None, "application/octet-stream").await;
    let _app = write_complete_read_delete(app, &key, None).await;
    for name in [
        "bobs.spool.created",
        "bobs.spool.write.completed",
        "bobs.spool.completed",
        "bobs.spool.read.started",
        "bobs.spool.read.completed",
        "bobs.spool.deleted",
    ] {
        assert!(logs
            .events_named(name)
            .iter()
            .all(|event| event["attributes"].get("job.id").is_none()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn observability_header_validation_and_redaction() {
    let (subscriber, logs) =
        with_rust_log(None, || bobs::observability::capturing_subscriber("bobs"));
    let _guard = tracing::subscriber::set_default(subscriber);
    let app = app().await;
    let (app, key) = create(
        app,
        None,
        "Bearer FAKETOKEN_OBSERVABILITY_PROBE; password=secret",
    )
    .await;
    let response = app
        .clone()
        .oneshot(req(
            Method::POST,
            format!("/api/v1/write/{key}/0"),
            Body::from("hello"),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(req(
            Method::POST,
            format!("/api/v1/complete/{key}"),
            Body::empty(),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    for probe in [
        "x".repeat(10 * 1024),
        "0123456789abcdefghjkmnpqrst".to_string(),
        "0123456789abcdefgihjkmnpqr".to_string(),
    ] {
        let response = app
            .clone()
            .oneshot(req(
                Method::GET,
                format!("/api/v1/read/{key}"),
                Body::empty(),
                Some(&probe),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.into_body().collect().await.unwrap();
    }
    let response = app
        .clone()
        .oneshot(req(
            Method::GET,
            format!("/api/v1/read/{key}"),
            Body::empty(),
            Some(VALID_JOB_ID),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.into_body().collect().await.unwrap();

    logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
    logs.assert_no_substring("password=secret");
    assert!(logs.raw().contains("[REDACTED]"));
    let read_events = logs.events_named("bobs.spool.read.started");
    assert!(read_events
        .iter()
        .any(|event| event["attributes"]["job.id"] == VALID_JOB_ID));
    assert!(
        read_events
            .iter()
            .filter(|event| event["attributes"].get("job.id").is_none())
            .count()
            >= 3
    );
}
