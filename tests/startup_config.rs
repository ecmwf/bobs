// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use bobs::config::MAX_IO_URING_QUEUE_CAPACITY;
use std::process::{Command, Output};
use tempfile::tempdir;

fn start_with_queue_capacity(queue_capacity: u64) -> Output {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!(
            "io_uring_queue_capacity: {queue_capacity}\nhost_prefix: test\ndomain: example.com\nroute_name: bobs\n"
        ),
    )
    .expect("write startup config");

    Command::new(env!("CARGO_BIN_EXE_bobs"))
        .arg(config_path)
        .env_remove("HOSTNAME")
        .env_remove("BOBS_INTERNAL_BASE_URL_TEMPLATE")
        .output()
        .expect("run bobs")
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn startup_rejects_u64_max_queue_capacity_as_configuration_error_without_panicking() {
    let output = start_with_queue_capacity(u64::MAX);
    let logs = combined_output(&output);

    assert!(!output.status.success());
    assert!(logs.contains("configuration error: io_uring_queue_capacity must not exceed"));
    assert!(logs.contains(&MAX_IO_URING_QUEUE_CAPACITY.to_string()));
    assert!(
        !logs.contains("panicked"),
        "unexpected panic output: {logs}"
    );
}

#[test]
fn startup_accepts_tokio_queue_capacity_boundary_during_configuration_validation() {
    let output = start_with_queue_capacity(MAX_IO_URING_QUEUE_CAPACITY as u64);
    let logs = combined_output(&output);

    assert!(!output.status.success());
    assert!(
        logs.contains("HOSTNAME environment variable must be set"),
        "startup did not advance beyond configuration validation: {logs}"
    );
    assert!(!logs.contains("io_uring_queue_capacity must not exceed"));
    assert!(
        !logs.contains("panicked"),
        "unexpected panic output: {logs}"
    );
}
