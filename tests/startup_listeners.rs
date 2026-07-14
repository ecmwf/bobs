// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

#![cfg(all(feature = "telemetry", unix))]

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const HTTP_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(100);

struct BobsProcess {
    child: Option<Child>,
    _dir: TempDir,
}

impl BobsProcess {
    fn spawn(main_port: u16, metrics_port: u16) -> Self {
        let dir = tempdir().expect("create startup test directory");
        let config_path = dir.path().join("config.yaml");
        let data_dir = dir.path().join("data");
        std::fs::write(
            &config_path,
            format!(
                "host: 127.0.0.1\nport: {main_port}\ndata_dir: {}\npage_size: 4096\nmax_cache_bytes: 16384\nmax_live_spools: 4\nio_uring_shards: 1\nio_uring_queue_capacity: 8\nhost_prefix: test\ndomain: example.com\nroute_name: bobs\nmetrics:\n  enabled: true\n  bind_address: 127.0.0.1\n  port: {metrics_port}\n",
                data_dir.display()
            ),
        )
        .expect("write startup test config");

        let child = Command::new(env!("CARGO_BIN_EXE_bobs"))
            .arg(config_path)
            .env("HOSTNAME", "bobs-0")
            .env(
                "BOBS_INTERNAL_BASE_URL_TEMPLATE",
                "http://bobs-{ordinal}.example.test",
            )
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn telemetry-enabled bobs");

        Self {
            child: Some(child),
            _dir: dir,
        }
    }

    fn has_exited(&mut self) -> bool {
        self.child
            .as_mut()
            .expect("child must be present")
            .try_wait()
            .expect("query bobs process")
            .is_some()
    }

    fn terminate(&mut self) {
        let pid = self.child.as_ref().expect("child must be present").id() as i32;
        let result = unsafe { libc::kill(pid, libc::SIGTERM) };
        assert_eq!(result, 0, "send SIGTERM to bobs process");
    }

    fn wait_for_exit(mut self, timeout: Duration) -> Output {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if self.has_exited() {
                let child = self.child.take().expect("child must be present");
                return child.wait_with_output().expect("collect bobs output");
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut child = self.child.take().expect("child must be present");
        let _ = child.kill();
        let output = child
            .wait_with_output()
            .expect("collect timed-out bobs output");
        panic!(
            "bobs did not exit within {timeout:?}: {}",
            combined_output(&output)
        );
    }
}

impl Drop for BobsProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn reserve_two_ports() -> (TcpListener, TcpListener, u16, u16) {
    let main = TcpListener::bind("127.0.0.1:0").expect("reserve main port");
    let metrics = TcpListener::bind("127.0.0.1:0").expect("reserve metrics port");
    let main_port = main.local_addr().expect("main address").port();
    let metrics_port = metrics.local_addr().expect("metrics address").port();
    assert_ne!(main_port, metrics_port);
    (main, metrics, main_port, metrics_port)
}

fn http_response(port: u16, path: &str) -> Option<Vec<u8>> {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let mut stream = TcpStream::connect_timeout(&address, HTTP_ATTEMPT_TIMEOUT).ok()?;
    stream
        .set_read_timeout(Some(HTTP_ATTEMPT_TIMEOUT))
        .expect("set HTTP read timeout");
    stream
        .set_write_timeout(Some(HTTP_ATTEMPT_TIMEOUT))
        .expect("set HTTP write timeout");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;

    let mut response = Vec::new();
    match stream.read_to_end(&mut response) {
        Ok(_) => Some(response),
        Err(error) if matches!(error.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => None,
        Err(_) => None,
    }
}

fn wait_for_http(process: &mut BobsProcess, port: u16, path: &str) -> Vec<u8> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    while Instant::now() < deadline {
        assert!(
            !process.has_exited(),
            "bobs exited before {path} became available"
        );
        if let Some(response) = http_response(port, path) {
            if response.starts_with(b"HTTP/1.1 200") {
                return response;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("{path} on port {port} did not become available");
}

fn assert_never_ready(port: u16, path: &str) {
    let deadline = Instant::now() + Duration::from_millis(500);
    while Instant::now() < deadline {
        if let Some(response) = http_response(port, path) {
            assert!(
                !response.starts_with(b"HTTP/1.1 200"),
                "unexpected readiness response on failed startup: {}",
                String::from_utf8_lossy(&response)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn open_keepalive(port: u16, path: &str) -> TcpStream {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect keep-alive client");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .expect("set keep-alive read timeout");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
                .as_bytes(),
        )
        .expect("send keep-alive request");

    let mut response_headers = Vec::new();
    let mut chunk = [0_u8; 1024];
    while !response_headers
        .windows(4)
        .any(|window| window == b"\r\n\r\n")
    {
        let read = stream.read(&mut chunk).expect("read keep-alive response");
        assert_ne!(read, 0, "server closed before sending response headers");
        response_headers.extend_from_slice(&chunk[..read]);
    }
    assert!(
        response_headers.starts_with(b"HTTP/1.1 200"),
        "unexpected keep-alive response: {}",
        String::from_utf8_lossy(&response_headers)
    );
    stream
}

fn assert_peer_closed(mut stream: TcpStream) {
    let mut remainder = Vec::new();
    match stream.read_to_end(&mut remainder) {
        Ok(_) => {}
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::ConnectionReset | ErrorKind::BrokenPipe
            ) => {}
        Err(error) => panic!("listener connection remained open after shutdown: {error}"),
    }
}

#[test]
fn occupied_metrics_port_fails_before_main_readiness() {
    let (main_reservation, metrics_reservation, main_port, metrics_port) = reserve_two_ports();
    drop(main_reservation);

    let process = BobsProcess::spawn(main_port, metrics_port);
    assert_never_ready(main_port, "/api/v1/health");
    let output = process.wait_for_exit(EXIT_TIMEOUT);
    let logs = combined_output(&output);

    assert!(
        !output.status.success(),
        "occupied metrics port must fail startup"
    );
    assert!(
        logs.contains("failed to bind metrics HTTP listener"),
        "{logs}"
    );
    assert!(!logs.contains("startup.server.listening"), "{logs}");
    assert!(!logs.contains("startup.metrics.enabled"), "{logs}");
    assert!(
        !logs.contains("prometheus /metrics endpoint listening"),
        "{logs}"
    );
    TcpListener::bind(("127.0.0.1", main_port)).expect("failed startup must release main port");
    drop(metrics_reservation);
}

#[test]
fn occupied_main_port_fails_without_starting_metrics() {
    let (main_reservation, metrics_reservation, main_port, metrics_port) = reserve_two_ports();
    drop(metrics_reservation);

    let process = BobsProcess::spawn(main_port, metrics_port);
    assert_never_ready(metrics_port, "/metrics");
    let output = process.wait_for_exit(EXIT_TIMEOUT);
    let logs = combined_output(&output);

    assert!(
        !output.status.success(),
        "occupied main port must fail startup"
    );
    assert!(logs.contains("failed to bind main HTTP listener"), "{logs}");
    assert!(!logs.contains("startup.metrics.enabled"), "{logs}");
    assert!(
        !logs.contains("prometheus /metrics endpoint listening"),
        "{logs}"
    );
    TcpListener::bind(("127.0.0.1", metrics_port))
        .expect("main bind failure must not retain metrics port");
    drop(main_reservation);
}

#[test]
fn free_main_and_metrics_ports_both_serve() {
    let (main_reservation, metrics_reservation, main_port, metrics_port) = reserve_two_ports();
    drop(main_reservation);
    drop(metrics_reservation);

    let mut process = BobsProcess::spawn(main_port, metrics_port);
    let health = wait_for_http(&mut process, main_port, "/api/v1/health");
    let metrics = wait_for_http(&mut process, metrics_port, "/metrics");
    assert!(health
        .windows(13)
        .any(|window| window == b"\"status\":\"ok\""));
    assert!(metrics.starts_with(b"HTTP/1.1 200"));

    process.terminate();
    let output = process.wait_for_exit(EXIT_TIMEOUT);
    assert!(output.status.success(), "{}", combined_output(&output));
}

#[test]
fn sigterm_drains_main_and_metrics_listeners() {
    let (main_reservation, metrics_reservation, main_port, metrics_port) = reserve_two_ports();
    drop(main_reservation);
    drop(metrics_reservation);

    let mut process = BobsProcess::spawn(main_port, metrics_port);
    wait_for_http(&mut process, main_port, "/api/v1/health");
    wait_for_http(&mut process, metrics_port, "/metrics");
    let main_connection = open_keepalive(main_port, "/api/v1/health");
    let metrics_connection = open_keepalive(metrics_port, "/metrics");

    process.terminate();
    let output = process.wait_for_exit(EXIT_TIMEOUT);
    let logs = combined_output(&output);
    assert!(output.status.success(), "{logs}");
    assert!(logs.contains("startup.shutdown.received"), "{logs}");
    assert!(logs.contains("startup.shutdown.complete"), "{logs}");
    assert!(!logs.contains("startup.shutdown.drain_timeout"), "{logs}");
    assert_peer_closed(main_connection);
    assert_peer_closed(metrics_connection);
    TcpListener::bind(("127.0.0.1", main_port)).expect("main listener released after drain");
    TcpListener::bind(("127.0.0.1", metrics_port)).expect("metrics listener released after drain");
}
