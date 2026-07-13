// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use bobs::config::{MAX_IO_URING_QUEUE_CAPACITY, MAX_PAGE_SIZE_BYTES};
use std::process::{Command, Output};
use tempfile::tempdir;

fn start_with_config(extra_config: &str) -> Output {
    let dir = tempdir().expect("tempdir");
    let config_path = dir.path().join("config.yaml");
    std::fs::write(
        &config_path,
        format!("{extra_config}host_prefix: test\ndomain: example.com\nroute_name: bobs\n"),
    )
    .expect("write startup config");

    Command::new(env!("CARGO_BIN_EXE_bobs"))
        .arg(config_path)
        .env_remove("HOSTNAME")
        .env_remove("BOBS_INTERNAL_BASE_URL_TEMPLATE")
        .output()
        .expect("run bobs")
}

fn start_with_queue_capacity(queue_capacity: u64) -> Output {
    start_with_config(&format!("io_uring_queue_capacity: {queue_capacity}\n"))
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_configuration_error(extra_config: &str, expected: &str) {
    let output = start_with_config(extra_config);
    let logs = combined_output(&output);

    assert!(!output.status.success());
    assert!(
        logs.contains(&format!("configuration error: {expected}")),
        "expected configuration error in output: {logs}"
    );
    assert!(
        !logs.contains("panicked"),
        "unexpected panic output: {logs}"
    );
}

fn assert_configuration_validation_passes(extra_config: &str) {
    let output = start_with_config(extra_config);
    let logs = combined_output(&output);

    assert!(!output.status.success());
    assert!(
        logs.contains("HOSTNAME environment variable must be set"),
        "startup did not advance beyond configuration validation: {logs}"
    );
    assert!(!logs.contains("configuration error:"));
    assert!(
        !logs.contains("panicked"),
        "unexpected panic output: {logs}"
    );
}

#[test]
fn startup_rejects_zero_page_size_as_configuration_error() {
    assert_configuration_error("page_size: 0\n", "page_size must be greater than 0");
}

#[test]
fn startup_accepts_default_page_size_during_configuration_validation() {
    assert_configuration_validation_passes("");
}

#[test]
fn startup_accepts_maximum_page_size_during_configuration_validation() {
    assert_configuration_validation_passes(&format!("page_size: {MAX_PAGE_SIZE_BYTES}\n"));
}

#[test]
fn startup_rejects_page_size_above_maximum_as_configuration_error() {
    assert_configuration_error(
        &format!("page_size: {}\n", MAX_PAGE_SIZE_BYTES + 1),
        "page_size must not exceed",
    );
}

#[cfg(target_pointer_width = "64")]
#[test]
fn startup_rejects_u64_max_page_size_as_configuration_error_without_panicking() {
    assert_configuration_error(
        &format!("page_size: {}\n", u64::MAX),
        "page_size must not exceed",
    );
}

#[test]
fn startup_rejects_page_size_larger_than_spool_limit() {
    assert_configuration_error(
        "page_size: 4096\nmax_spool_bytes: 4095\n",
        "page_size must not exceed max_spool_bytes",
    );
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
