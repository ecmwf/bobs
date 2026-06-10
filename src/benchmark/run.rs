use super::config::BenchmarkConfig;
use super::http_client::BobsHttpClient;
use super::schedule::{build_schedule, extract_ordinal_from_read_url, ObjectPlan};
use super::summary::{log_line, ms, summarize, BenchmarkSummary, ObjectResult};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

pub async fn run_benchmark(cfg: BenchmarkConfig) -> Result<BenchmarkSummary, String> {
    let schedule = build_schedule(&cfg)?;
    let client = BobsHttpClient::new(cfg.connect_timeout, cfg.request_timeout);
    let process_start = Instant::now();
    let barrier = process_start + schedule.barrier_delay;
    let mut handles = Vec::with_capacity(schedule.plans.len());
    for plan in schedule.plans {
        let c = cfg.clone();
        let cl = client.clone();
        handles.push(tokio::spawn(async move {
            run_object(plan, c, cl, barrier, process_start).await
        }));
    }
    let mut results = Vec::with_capacity(handles.len());
    for h in handles {
        results.push(h.await.map_err(|e| e.to_string())?);
    }
    Ok(summarize(results, process_start.elapsed()))
}

async fn wait_until_barrier(barrier: Instant) {
    tokio::time::sleep_until(barrier).await;
}

async fn run_object(
    plan: ObjectPlan,
    cfg: BenchmarkConfig,
    client: BobsHttpClient,
    barrier: Instant,
    process_start: Instant,
) -> ObjectResult {
    wait_until_barrier(barrier).await;
    let active_start = Instant::now();
    let endpoint_url = plan.endpoint.root_url.clone();
    let mut key: Option<String> = None;
    let mut response_ordinal = None;
    let mut create_ms = None;
    let mut write_ms = None;
    let mut complete_ms = None;
    let mut wait_to_read_ms = None;
    let mut read_ms = None;
    let mut bytes_written = 0u64;
    let mut bytes_read = 0u64;
    let mut write_start_rel = None;
    let mut write_end_rel = None;
    let mut read_start_rel = None;
    let mut read_end_rel = None;

    macro_rules! fail_now {
        ($error:expr) => {
            return FailedObject {
                plan,
                endpoint_url,
                key,
                response_ordinal,
                create_ms,
                write_ms,
                complete_ms,
                wait_to_read_ms,
                read_ms,
                bytes_written,
                bytes_read,
                write_start_ms: write_start_rel,
                write_end_ms: write_end_rel,
                read_start_ms: read_start_rel,
                read_end_ms: read_end_rel,
                lifecycle: active_start.elapsed(),
                error: $error,
            }
            .into_result()
        };
    }

    println!(
        "{}",
        log_line(
            &plan.object_label,
            None,
            plan.configured_ordinal,
            "create_start",
            ""
        )
    );
    let t = Instant::now();
    let created = match client.create(&plan.endpoint).await {
        Ok((out, status)) => {
            create_ms = Some(ms(t.elapsed()));
            println!(
                "{}",
                log_line(
                    &plan.object_label,
                    Some(&out.key),
                    plan.configured_ordinal,
                    "create_end",
                    &format!("status={status} duration_ms={}", create_ms.unwrap() as u64)
                )
            );
            out
        }
        Err(e) => {
            fail_now!(e)
        }
    };
    key = Some(created.key.clone());
    response_ordinal = extract_ordinal_from_read_url(&created.read_url);
    let ordinal = response_ordinal.or(plan.configured_ordinal);
    if plan.configured_ordinal.is_some()
        && response_ordinal.is_some()
        && plan.configured_ordinal != response_ordinal
    {
        println!(
            "{}",
            log_line(
                &plan.object_label,
                key.as_deref(),
                ordinal,
                "object_error",
                &format!(
                    "error=ordinal_mismatch configured={:?} response={:?}",
                    plan.configured_ordinal, response_ordinal
                )
            )
        );
    }

    println!(
        "{}",
        log_line(
            &plan.object_label,
            key.as_deref(),
            ordinal,
            "write_start",
            &format!("bytes={}", plan.object_bytes)
        )
    );
    let write_start = Instant::now();
    write_start_rel = Some(ms(write_start.duration_since(process_start)));
    let buf = vec![(plan.object_index % 251) as u8; cfg.write_body_chunk_bytes];
    let mut offset = 0u64;
    while offset < plan.object_bytes {
        let n = (plan.object_bytes - offset).min(plan.write_request_bytes);
        if cfg.log_chunks || plan.write_request_bytes < plan.object_bytes {
            println!(
                "{}",
                log_line(
                    &plan.object_label,
                    key.as_deref(),
                    ordinal,
                    "write_start",
                    &format!("offset={offset} bytes={n}")
                )
            );
        }
        let chunk_t = Instant::now();
        match client
            .write_repeated(&plan.endpoint, &created.key, offset, n, &buf)
            .await
        {
            Ok(status) => {
                if cfg.log_chunks || plan.write_request_bytes < plan.object_bytes {
                    println!(
                        "{}",
                        log_line(
                            &plan.object_label,
                            key.as_deref(),
                            ordinal,
                            "write_end",
                            &format!(
                                "offset={offset} bytes={n} status={status} duration_ms={}",
                                ms(chunk_t.elapsed()) as u64
                            )
                        )
                    );
                }
            }
            Err(e) => {
                fail_now!(e)
            }
        }
        bytes_written += n;
        offset += n;
    }
    write_ms = Some(ms(write_start.elapsed()));
    write_end_rel = Some(ms(Instant::now().duration_since(process_start)));
    println!(
        "{}",
        log_line(
            &plan.object_label,
            key.as_deref(),
            ordinal,
            "write_end",
            &format!(
                "status=200 duration_ms={} bytes={}",
                write_ms.unwrap() as u64,
                bytes_written
            )
        )
    );

    println!(
        "{}",
        log_line(
            &plan.object_label,
            key.as_deref(),
            ordinal,
            "complete_start",
            ""
        )
    );
    let complete_start = Instant::now();
    match client
        .complete(&plan.endpoint, &created.key, plan.object_bytes)
        .await
    {
        Ok(status) => {
            complete_ms = Some(ms(complete_start.elapsed()));
            println!(
                "{}",
                log_line(
                    &plan.object_label,
                    key.as_deref(),
                    ordinal,
                    "complete_end",
                    &format!(
                        "status={status} duration_ms={}",
                        complete_ms.unwrap() as u64
                    )
                )
            );
        }
        Err(e) => {
            fail_now!(e)
        }
    }

    let (tx, rx) = oneshot::channel::<Instant>();
    let read_client = client.clone();
    let read_endpoint = plan.endpoint.clone();
    let read_key = created.key.clone();
    let read_label = plan.object_label.clone();
    let read_ordinal = ordinal;
    let read_bytes_target = plan.object_bytes;
    let read_chunk = cfg.read_body_chunk_bytes;
    let ps = process_start;
    let reader = tokio::spawn(async move {
        let complete_end = rx
            .await
            .map_err(|_| "reader completion signal dropped".to_string())?;
        let rs = Instant::now();
        let wait_ms = ms(rs.duration_since(complete_end));
        println!(
            "{}",
            log_line(
                &read_label,
                Some(&read_key),
                read_ordinal,
                "read_start",
                &format!("bytes={read_bytes_target}")
            )
        );
        let out = read_client
            .read_discard(&read_endpoint, &read_key, read_bytes_target, read_chunk)
            .await;
        let re = Instant::now();
        match out {
            Ok((status, got)) => {
                println!(
                    "{}",
                    log_line(
                        &read_label,
                        Some(&read_key),
                        read_ordinal,
                        "read_end",
                        &format!(
                            "status={status} duration_ms={} bytes={got}",
                            ms(re.duration_since(rs)) as u64
                        )
                    )
                );
                Ok((
                    wait_ms,
                    ms(re.duration_since(rs)),
                    got,
                    ms(rs.duration_since(ps)),
                    ms(re.duration_since(ps)),
                ))
            }
            Err(e) => Err(e),
        }
    });
    let _ = tx.send(Instant::now());
    match reader.await.map_err(|e| e.to_string()).and_then(|r| r) {
        Ok((wtr, rm, got, rs, re)) => {
            wait_to_read_ms = Some(wtr);
            read_ms = Some(rm);
            bytes_read = got;
            read_start_rel = Some(rs);
            read_end_rel = Some(re);
        }
        Err(e) => {
            fail_now!(e)
        }
    }

    if cfg.delete_after_read {
        println!(
            "{}",
            log_line(
                &plan.object_label,
                key.as_deref(),
                ordinal,
                "delete_start",
                ""
            )
        );
        let dt = Instant::now();
        match client.delete(&plan.endpoint, &created.key).await {
            Ok(status) => println!(
                "{}",
                log_line(
                    &plan.object_label,
                    key.as_deref(),
                    ordinal,
                    "delete_end",
                    &format!("status={status} duration_ms={}", ms(dt.elapsed()) as u64)
                )
            ),
            Err(e) => println!(
                "{}",
                log_line(
                    &plan.object_label,
                    key.as_deref(),
                    ordinal,
                    "object_error",
                    &format!("error=delete_failed:{e}")
                )
            ),
        }
    }

    ObjectResult {
        object_index: plan.object_index,
        object_label: plan.object_label,
        endpoint_url,
        configured_ordinal: plan.configured_ordinal,
        response_ordinal,
        key,
        outcome: "success".into(),
        error: None,
        create_ms,
        write_ms,
        complete_ms,
        wait_to_read_ms,
        read_ms,
        lifecycle_ms: ms(active_start.elapsed()),
        bytes_written,
        bytes_read,
        write_start_ms: write_start_rel,
        write_end_ms: write_end_rel,
        read_start_ms: read_start_rel,
        read_end_ms: read_end_rel,
    }
}

struct FailedObject {
    plan: ObjectPlan,
    endpoint_url: String,
    key: Option<String>,
    response_ordinal: Option<u32>,
    create_ms: Option<f64>,
    write_ms: Option<f64>,
    complete_ms: Option<f64>,
    wait_to_read_ms: Option<f64>,
    read_ms: Option<f64>,
    bytes_written: u64,
    bytes_read: u64,
    write_start_ms: Option<f64>,
    write_end_ms: Option<f64>,
    read_start_ms: Option<f64>,
    read_end_ms: Option<f64>,
    lifecycle: Duration,
    error: String,
}

impl FailedObject {
    fn into_result(self) -> ObjectResult {
        let ordinal = self.response_ordinal.or(self.plan.configured_ordinal);
        println!(
            "{}",
            log_line(
                &self.plan.object_label,
                self.key.as_deref(),
                ordinal,
                "object_error",
                &format!("error={}", self.error.replace(' ', "_"))
            )
        );
        ObjectResult {
            object_index: self.plan.object_index,
            object_label: self.plan.object_label,
            endpoint_url: self.endpoint_url,
            configured_ordinal: self.plan.configured_ordinal,
            response_ordinal: self.response_ordinal,
            key: self.key,
            outcome: "failure".into(),
            error: Some(self.error),
            create_ms: self.create_ms,
            write_ms: self.write_ms,
            complete_ms: self.complete_ms,
            wait_to_read_ms: self.wait_to_read_ms,
            read_ms: self.read_ms,
            lifecycle_ms: ms(self.lifecycle),
            bytes_written: self.bytes_written,
            bytes_read: self.bytes_read,
            write_start_ms: self.write_start_ms,
            write_end_ms: self.write_end_ms,
            read_start_ms: self.read_start_ms,
            read_end_ms: self.read_end_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::benchmark::config::{normalize_endpoint, EndpointSpec};
    #[test]
    fn schedule_has_shared_barrier_model() {
        let c = BenchmarkConfig {
            objects: 3,
            endpoint: EndpointSpec::Single(normalize_endpoint("http://localhost:3000").unwrap()),
            ..Default::default()
        };
        let s = build_schedule(&c).unwrap();
        assert_eq!(s.plans.len(), 3);
        assert_eq!(s.plans[0].object_bytes, c.object_bytes);
    }
}
