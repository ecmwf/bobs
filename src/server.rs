// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use axum::Router;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::server::graceful::GracefulShutdown;
use std::future::Future;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tower::ServiceExt;

/// Production deadline for draining HTTP connections after SIGTERM or Ctrl+C.
///
/// Hyper is first asked to shut every connection down gracefully: idle HTTP/1.1
/// keep-alive sockets close immediately, HTTP/2 stops accepting new streams, and
/// active requests keep running. Connections that have not finished after this
/// deadline are aborted so shutdown cannot wait forever on a stalled peer.
pub const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

// One 16 MiB body fits in a single h2 window without flow-control pauses.
const H2_WINDOW: u32 = 16 * 1024 * 1024;
// The spec ceiling is 2^24-1. This keeps a 16 MiB write to roughly 1-2 DATA
// frames instead of the roughly 1024 frames produced by Hyper's 16 KiB default.
const H2_MAX_FRAME: u32 = 16 * 1024 * 1024 - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DrainReport {
    pub timed_out: bool,
    pub aborted_connections: usize,
}

/// Serve HTTP/1.1 and h2c until `shutdown` resolves, then gracefully drain.
///
/// The listener is dropped before the connection shutdown broadcast. Every
/// accepted connection is registered with Hyper's graceful-shutdown watcher,
/// including connections still deciding whether they are HTTP/1.1 or HTTP/2.
/// This ordering prevents new accepts while giving request bodies and handlers
/// that own spool mutations up to `drain_timeout` to finish.
pub async fn serve_http<F>(
    listener: TcpListener,
    app: Router,
    shutdown: F,
    drain_timeout: Duration,
) -> std::io::Result<DrainReport>
where
    F: Future<Output = ()>,
{
    let graceful = GracefulShutdown::new();
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => {
                break;
            }
            result = listener.accept() => {
                let (stream, _) = result?;
                let io = TokioIo::new(stream);
                let app = app.clone();
                let watcher = graceful.watcher();
                tasks.spawn(async move {
                    let mut builder = Builder::new(TokioExecutor::new());
                    builder
                        .http2()
                        .initial_stream_window_size(H2_WINDOW)
                        .initial_connection_window_size(H2_WINDOW)
                        .max_frame_size(H2_MAX_FRAME);
                    let service = hyper::service::service_fn(move |request| {
                        let app = app.clone();
                        async move { app.oneshot(request).await }
                    });
                    let connection = builder.serve_connection_with_upgrades(io, service);
                    if let Err(error) = watcher.watch(connection).await {
                        tracing::warn!(error = %error, "connection error");
                    }
                });
            }
            // Reap finished connection tasks to keep the JoinSet bounded.
            Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                if let Err(error) = result {
                    tracing::warn!(error = %error, "connection task failed");
                }
            }
        }
    }

    // Stop accepting before signalling all registered connections. The
    // GracefulShutdown future waits until every watcher has finished.
    drop(listener);
    let timed_out = tokio::time::timeout(drain_timeout, graceful.shutdown())
        .await
        .is_err();

    if timed_out {
        tasks.abort_all();
    }

    let mut aborted_connections = 0;
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            if error.is_cancelled() {
                aborted_connections += 1;
            } else {
                tracing::warn!(error = %error, "connection task failed during shutdown");
            }
        }
    }

    Ok(DrainReport {
        timed_out,
        aborted_connections,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::{get, post};
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::{oneshot, Notify};

    async fn read_http1_response(stream: &mut TcpStream) -> Vec<u8> {
        let mut response = Vec::new();
        let mut content_length = None;
        let mut header_end = None;

        loop {
            let mut chunk = [0_u8; 1024];
            let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut chunk))
                .await
                .expect("HTTP/1.1 response read timed out")
                .expect("HTTP/1.1 response read failed");
            assert!(read > 0, "connection closed before the response completed");
            response.extend_from_slice(&chunk[..read]);

            if header_end.is_none() {
                header_end = response
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4);
                if let Some(end) = header_end {
                    let headers = std::str::from_utf8(&response[..end]).expect("ASCII headers");
                    content_length = headers.lines().find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length").then(|| {
                            value
                                .trim()
                                .parse::<usize>()
                                .expect("content-length integer")
                        })
                    });
                }
            }

            if let (Some(end), Some(length)) = (header_end, content_length) {
                if response.len() >= end + length {
                    return response;
                }
            }
        }
    }

    #[tokio::test]
    async fn in_flight_http1_body_finishes_inside_graceful_drain() {
        let first_frame_read = Arc::new(Notify::new());
        let app = Router::new().route(
            "/body",
            post({
                let first_frame_read = Arc::clone(&first_frame_read);
                move |request: axum::extract::Request| {
                    let first_frame_read = Arc::clone(&first_frame_read);
                    async move {
                        let mut body = request.into_body();
                        let first = body
                            .frame()
                            .await
                            .expect("first request body frame")
                            .expect("valid first request body frame");
                        let mut received = first.into_data().expect("first data frame").len();
                        first_frame_read.notify_one();
                        while let Some(frame) = body.frame().await {
                            if let Ok(data) = frame.expect("valid request body frame").into_data() {
                                received += data.len();
                            }
                        }
                        received.to_string()
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http(
            listener,
            app,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_secs(1),
        ));

        let mut stream = TcpStream::connect(address).await.expect("connect HTTP/1.1");
        stream
            .write_all(b"POST /body HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nab")
            .await
            .expect("write partial request body");
        first_frame_read.notified().await;
        shutdown_tx.send(()).expect("signal shutdown");
        tokio::task::yield_now().await;
        stream
            .write_all(b"cd")
            .await
            .expect("finish request body during drain");

        let response = read_http1_response(&mut stream).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(response.ends_with(b"4"));
        let report = server.await.expect("server task").expect("server result");
        assert_eq!(
            report,
            DrainReport {
                timed_out: false,
                aborted_connections: 0,
            }
        );
    }

    #[tokio::test]
    async fn active_request_finishes_inside_graceful_drain() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let app = Router::new().route(
            "/active",
            get({
                let started = Arc::clone(&started);
                let release = Arc::clone(&release);
                move || {
                    let started = Arc::clone(&started);
                    let release = Arc::clone(&release);
                    async move {
                        started.notify_one();
                        release.notified().await;
                        "completed"
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http(
            listener,
            app,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_secs(1),
        ));

        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{address}/active"))
                .await
                .expect("active request")
                .text()
                .await
                .expect("active response body")
        });
        started.notified().await;
        shutdown_tx.send(()).expect("signal shutdown");
        tokio::task::yield_now().await;
        assert!(
            !request.is_finished(),
            "active handler was cancelled at shutdown"
        );
        release.notify_one();

        assert_eq!(request.await.expect("request task"), "completed");
        let report = server.await.expect("server task").expect("server result");
        assert_eq!(
            report,
            DrainReport {
                timed_out: false,
                aborted_connections: 0,
            }
        );
    }

    #[tokio::test]
    async fn drain_deadline_aborts_stalled_active_request() {
        let started = Arc::new(Notify::new());
        let app = Router::new().route(
            "/stalled",
            get({
                let started = Arc::clone(&started);
                move || {
                    let started = Arc::clone(&started);
                    async move {
                        started.notify_one();
                        std::future::pending::<()>().await;
                        "unreachable"
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("listener address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_http(
            listener,
            app,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_millis(50),
        ));

        let request = tokio::spawn(reqwest::get(format!("http://{address}/stalled")));
        started.notified().await;
        shutdown_tx.send(()).expect("signal shutdown");

        let report = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server exceeded bounded drain")
            .expect("server task")
            .expect("server result");
        assert!(report.timed_out);
        assert_eq!(report.aborted_connections, 1);
        assert!(request.await.expect("request task").is_err());
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn sigterm_server_helper() {
        let Ok(port) = std::env::var("BOBS_SIGTERM_TEST_PORT") else {
            return;
        };
        let listener = TcpListener::bind(("127.0.0.1", port.parse::<u16>().expect("test port")))
            .await
            .expect("bind SIGTERM helper");
        let app = Router::new().route("/", get(|| async { "idle" }));
        let report = serve_http(
            listener,
            app,
            crate::shutdown::shutdown_signal(),
            Duration::from_secs(1),
        )
        .await
        .expect("SIGTERM helper server");
        assert!(!report.timed_out);
        assert_eq!(report.aborted_connections, 0);
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn idle_http1_keepalive_closes_promptly_on_real_sigterm() {
        use std::process::Stdio;

        let reservation = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
        let port = reservation.local_addr().expect("reserved address").port();
        drop(reservation);

        let mut child = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "server::tests::sigterm_server_helper",
                "--nocapture",
            ])
            .env("BOBS_SIGTERM_TEST_PORT", port.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn SIGTERM helper");

        let mut stream = loop {
            match TcpStream::connect(("127.0.0.1", port)).await {
                Ok(stream) => break stream,
                Err(error) => {
                    if let Some(status) = child.try_wait().expect("inspect helper") {
                        panic!("SIGTERM helper exited before listening: {status}");
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    if child.id().is_none() {
                        panic!("SIGTERM helper disappeared while connecting: {error}");
                    }
                }
            }
        };

        stream
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
            .await
            .expect("write keep-alive request");
        let response = read_http1_response(&mut stream).await;
        assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
        assert!(response.ends_with(b"idle"));

        let pid = child.id().expect("helper pid") as libc::pid_t;
        // SAFETY: `pid` belongs to the live child process spawned above and SIGTERM
        // carries no pointer or memory-safety preconditions.
        assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);

        let mut byte = [0_u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("idle keep-alive socket stayed open after SIGTERM")
            .expect("read idle keep-alive EOF");
        assert_eq!(read, 0, "graceful shutdown must close an idle socket");

        let status = tokio::time::timeout(Duration::from_secs(2), child.wait())
            .await
            .expect("SIGTERM helper did not exit")
            .expect("wait for SIGTERM helper");
        assert!(status.success(), "SIGTERM helper failed: {status}");
    }
}
