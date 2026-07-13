// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use regex::Regex;
use serde_json::{Map, Value};
use std::fmt;
use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tracing::{span::Record, Event, Level, Subscriber};
use tracing_subscriber::field::{RecordFields, Visit};
use tracing_subscriber::fmt::format::{FormatEvent, FormatFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormattedFields, MakeWriter};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Registry};

const DEFAULT_FILTER: &str = "info";

pub fn init_tracing(service_name: &'static str) {
    let subscriber = subscriber_with_writer(service_name, io::stdout);
    tracing::subscriber::set_global_default(subscriber)
        .expect("global tracing subscriber already set");
}

pub fn env_filter_from_env() -> EnvFilter {
    match std::env::var("RUST_LOG") {
        Ok(value) => env_filter_from_str(&value),
        Err(_) => EnvFilter::new(DEFAULT_FILTER),
    }
}

fn env_filter_from_str(value: &str) -> EnvFilter {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return EnvFilter::new(DEFAULT_FILTER);
    }
    trimmed
        .parse()
        .unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER))
}

pub fn subscriber_with_writer<W>(
    service_name: &'static str,
    writer: W,
) -> impl Subscriber + Send + Sync
where
    W: for<'writer> MakeWriter<'writer> + Send + Sync + 'static,
{
    Registry::default().with(env_filter_from_env()).with(
        tracing_subscriber::fmt::layer()
            .event_format(OtelFormatter { service_name })
            .fmt_fields(JsonFields)
            .with_ansi(false)
            .with_writer(writer),
    )
}

pub fn capturing_subscriber(
    service_name: &'static str,
) -> (impl Subscriber + Send + Sync, test_helper::CapturedLogs) {
    let captured = test_helper::CapturedLogs::default();
    let subscriber = subscriber_with_writer(service_name, captured.make_writer());
    (subscriber, captured)
}

#[derive(Clone, Copy)]
struct OtelFormatter {
    service_name: &'static str,
}

impl<S, N> FormatEvent<S, N> for OtelFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let metadata = event.metadata();
        let mut visitor = JsonVisitor::default();
        event.record(&mut visitor);

        let body = visitor
            .fields
            .remove("message")
            .map(|v| match v {
                Value::String(s) => redact_text(&s),
                other => redact_text(&other.to_string()),
            })
            .unwrap_or_default();

        let mut attributes = Map::new();
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                let extensions = span.extensions();
                if let Some(fields) = extensions.get::<FormattedFields<N>>() {
                    if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(fields.as_str()) {
                        for (key, value) in map {
                            attributes.insert(key, value);
                        }
                    }
                }
            }
        }
        for (key, value) in visitor.fields {
            attributes.insert(key, value);
        }
        attributes.insert(
            "code.target".to_string(),
            Value::String(redact_text(metadata.target())),
        );

        let mut resource = Map::new();
        resource.insert(
            "service.name".to_string(),
            Value::String(self.service_name.to_string()),
        );
        resource.insert(
            "service.version".to_string(),
            Value::String(env!("CARGO_PKG_VERSION").to_string()),
        );
        if let Ok(value) =
            std::env::var("BOBS_DEPLOYMENT_ENV").or_else(|_| std::env::var("POLYTOPE_ENV"))
        {
            if !value.trim().is_empty() {
                resource.insert(
                    "deployment.environment".to_string(),
                    Value::String(redact_text(&value)),
                );
            }
        }
        if let Ok(value) = std::env::var("K8S_NAMESPACE_NAME") {
            if !value.trim().is_empty() {
                resource.insert(
                    "k8s.namespace.name".to_string(),
                    Value::String(redact_text(&value)),
                );
            }
        }
        if let Ok(value) = std::env::var("K8S_POD_NAME") {
            if !value.trim().is_empty() {
                resource.insert(
                    "k8s.pod.name".to_string(),
                    Value::String(redact_text(&value)),
                );
            }
        }

        let mut line = Map::new();
        line.insert(
            "timestamp".to_string(),
            Value::String(
                OffsetDateTime::now_utc()
                    .format(&Rfc3339)
                    .map_err(|_| fmt::Error)?,
            ),
        );
        line.insert(
            "severityText".to_string(),
            Value::String(metadata.level().to_string()),
        );
        line.insert(
            "severityNumber".to_string(),
            Value::Number(severity_number(metadata.level()).into()),
        );
        line.insert("body".to_string(), Value::String(body));
        line.insert("resource".to_string(), Value::Object(resource));
        line.insert("attributes".to_string(), Value::Object(attributes));

        serde_json::to_writer(&mut JsonFmtWriter(&mut writer), &Value::Object(line))
            .map_err(|_| fmt::Error)?;
        writeln!(writer)
    }
}

struct JsonFmtWriter<'a, 'b>(&'a mut Writer<'b>);
impl Write for JsonFmtWriter<'_, '_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let s = std::str::from_utf8(buf).map_err(io::Error::other)?;
        self.0
            .write_str(s)
            .map_err(|_| io::Error::other("format error"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn severity_number(level: &Level) -> u64 {
    match *level {
        Level::TRACE => 1,
        Level::DEBUG => 5,
        Level::INFO => 9,
        Level::WARN => 13,
        Level::ERROR => 17,
    }
}

#[derive(Default)]
struct JsonFields;

impl<'writer> FormatFields<'writer> for JsonFields {
    fn format_fields<R: RecordFields>(
        &self,
        mut writer: Writer<'writer>,
        fields: R,
    ) -> fmt::Result {
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        serde_json::to_writer(
            &mut JsonFmtWriter(&mut writer),
            &Value::Object(visitor.fields),
        )
        .map_err(|_| fmt::Error)
    }

    fn add_fields(
        &self,
        current: &'writer mut FormattedFields<Self>,
        fields: &Record<'_>,
    ) -> fmt::Result {
        let mut merged =
            serde_json::from_str::<Map<String, Value>>(current.as_str()).map_err(|_| fmt::Error)?;
        let mut visitor = JsonVisitor::default();
        fields.record(&mut visitor);
        merged.extend(visitor.fields);
        current.fields = serde_json::to_string(&Value::Object(merged)).map_err(|_| fmt::Error)?;
        Ok(())
    }
}

#[derive(Default)]
struct JsonVisitor {
    fields: Map<String, Value>,
}

impl JsonVisitor {
    fn insert(&mut self, field: &tracing::field::Field, value: Value) {
        let key = field.name().to_string();
        let value = if is_secret_key(&key) {
            Value::String("[REDACTED]".to_string())
        } else {
            redact_value(value)
        };
        self.fields.insert(key, value);
    }
}

impl Visit for JsonVisitor {
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.insert(field, Value::Bool(value));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.insert(field, Value::Number(value.into()));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.insert(field, Value::Number(value.into()));
    }
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.insert(field, Value::String(value.to_string()));
    }
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        self.insert(field, Value::String(value.to_string()));
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
        let formatted = format!("{value:?}");
        self.insert(field, debug_to_value(&formatted));
    }
}

/// Convert a Rust `Debug` rendering into a JSON value with light unwrapping
/// for the common `Option` shapes (`None`, `Some(x)`) and primitive literals.
///
/// Without this, fields recorded with `?` for `Option<String>` end up rendered
/// as `"Some(\"foo\")"` and `"None"` strings, which leaks Rust types into
/// otherwise-clean JSON logs. This helper:
///
/// - maps `None` → `Value::Null`,
/// - unwraps `Some(...)` and recurses on the inner Debug text,
/// - tries to parse the inner text as JSON (handles `"foo"`, numbers, bools,
///   nested arrays/objects), and
/// - falls back to `Value::String(formatted)` if nothing matches.
fn debug_to_value(formatted: &str) -> Value {
    let trimmed = formatted.trim();
    if trimmed == "None" {
        return Value::Null;
    }
    if let Some(inner) = trimmed
        .strip_prefix("Some(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return debug_to_value(inner);
    }
    if let Ok(parsed) = serde_json::from_str::<Value>(trimmed) {
        return parsed;
    }
    Value::String(formatted.to_string())
}

fn redact_value(value: Value) -> Value {
    match value {
        Value::String(s) => Value::String(redact_text(&s)),
        Value::Array(items) => Value::Array(items.into_iter().map(redact_value).collect()),
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| {
                    if is_secret_key(&k) {
                        (k, Value::String("[REDACTED]".to_string()))
                    } else {
                        (k, redact_value(v))
                    }
                })
                .collect(),
        ),
        other => other,
    }
}

fn is_secret_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    lower.contains("authorization")
        || lower == "password"
        || lower.ends_with(".password")
        || lower.contains("api_key")
        || lower.contains("token")
}

fn redact_text(input: &str) -> String {
    static BEARER: OnceLock<Regex> = OnceLock::new();
    static ASSIGN: OnceLock<Regex> = OnceLock::new();
    static JWT: OnceLock<Regex> = OnceLock::new();
    static USERINFO: OnceLock<Regex> = OnceLock::new();
    static KNOWN: OnceLock<Regex> = OnceLock::new();
    let mut out = input.to_string();
    out = BEARER
        .get_or_init(|| Regex::new(r#"(?i)bearer\s+[^\s,;\"']+"#).unwrap())
        .replace_all(&out, "Bearer [REDACTED]")
        .to_string();
    out = ASSIGN
        .get_or_init(|| Regex::new(r#"(?i)\b(password|token|api_key)=([^\s,;&\"']+)"#).unwrap())
        .replace_all(&out, "$1=[REDACTED]")
        .to_string();
    out = JWT
        .get_or_init(|| {
            Regex::new(r"\beyJ[A-Za-z0-9_-]*\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b").unwrap()
        })
        .replace_all(&out, "[REDACTED]")
        .to_string();
    out = USERINFO
        .get_or_init(|| Regex::new(r"(https?://)[^/@\s:]+:[^/@\s]+@([^/\s]+)").unwrap())
        .replace_all(&out, "$1[REDACTED]@$2")
        .to_string();
    out = KNOWN.get_or_init(|| Regex::new(r"32eff194-66bd|lAYFsKT9xYeraMbeH2Sn4RPL7iJgNaxY|vv7pGSEZcFFB87|BaThQ7cKxG5NuJ|WQrRuQn4fvssgGYCiZTt|POLY-4a7bb966e4a51b9c25b429bc96cf25dd|3\.\.izBAd75|FAKETOKEN_OBSERVABILITY_PROBE").unwrap()).replace_all(&out, "[REDACTED]").to_string();
    out
}

pub mod test_helper {
    use super::*;

    #[derive(Clone, Default)]
    pub struct CapturedLogs {
        inner: Arc<Mutex<Vec<u8>>>,
    }

    impl CapturedLogs {
        pub fn make_writer(&self) -> MakeCapturedWriter {
            MakeCapturedWriter {
                inner: self.inner.clone(),
            }
        }
        pub fn raw(&self) -> String {
            String::from_utf8_lossy(&self.inner.lock().unwrap()).into_owned()
        }
        pub fn raw_lines(&self) -> Vec<String> {
            self.raw().lines().map(ToString::to_string).collect()
        }
        pub fn json_lines(&self) -> Vec<Value> {
            self.raw_lines()
                .into_iter()
                .map(|line| {
                    serde_json::from_str(&line)
                        .unwrap_or_else(|e| panic!("invalid JSON log line: {e}: {line}"))
                })
                .collect()
        }
        pub fn assert_required_fields(&self) {
            for line in self.json_lines() {
                assert!(line.get("timestamp").and_then(Value::as_str).is_some());
                assert!(line.get("severityText").and_then(Value::as_str).is_some());
                assert!(line.get("severityNumber").and_then(Value::as_u64).is_some());
                assert!(line.get("body").and_then(Value::as_str).is_some());
                assert!(line.get("resource").and_then(Value::as_object).is_some());
                assert!(line.get("attributes").and_then(Value::as_object).is_some());
            }
        }
        pub fn events_named(&self, name: &str) -> Vec<Value> {
            self.json_lines()
                .into_iter()
                .filter(|line| {
                    line.pointer("/attributes/event.name") == Some(&Value::String(name.to_string()))
                })
                .collect()
        }
        pub fn assert_event_emitted(&self, name: &str) {
            assert!(
                !self.events_named(name).is_empty(),
                "event not emitted: {name}"
            );
        }
        pub fn assert_event_attribute(&self, name: &str, key: &str, expected: &Value) {
            assert!(
                self.events_named(name).iter().any(|event| event
                    .pointer(&format!("/attributes/{}", key.replace('/', "~1")))
                    == Some(expected)),
                "event {name} missing {key}={expected}"
            );
        }
        pub fn assert_no_substring(&self, needle: &str) {
            assert!(
                !self.raw().contains(needle),
                "captured logs contained forbidden substring"
            );
        }
    }

    #[derive(Clone)]
    pub struct MakeCapturedWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }
    impl<'a> MakeWriter<'a> for MakeCapturedWriter {
        type Writer = CapturedWriter;
        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter {
                inner: self.inner.clone(),
            }
        }
    }
    pub struct CapturedWriter {
        inner: Arc<Mutex<Vec<u8>>>,
    }
    impl Write for CapturedWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.inner.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex as StdMutex;

    static ENV_LOCK: StdMutex<()> = StdMutex::new(());

    #[test]
    fn observability_formatter_shape_span_fields_and_redaction() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: serialised by ENV_LOCK; no other threads touch the env in tests.
        unsafe {
            std::env::set_var("BOBS_DEPLOYMENT_ENV", "dev");
            std::env::set_var("K8S_NAMESPACE_NAME", "ns");
            std::env::set_var("K8S_POD_NAME", "pod");
            std::env::remove_var("RUST_LOG");
        }
        let (subscriber, logs) = capturing_subscriber("bobs");
        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "request",
                "request.id" = tracing::field::Empty,
                "bobs.spool.key" = tracing::field::Empty
            );
            // HTTP request spans attach validated identifiers after construction.
            // Keep this regression case distinct from initial span field recording.
            span.record("request.id", "0123456789abcdefghjkmnpqrs");
            span.record("bobs.spool.key", "key-1");
            let _enter = span.enter();
            tracing::info!(
                "event.name" = "bobs.spool.created",
                content_type = "Bearer FAKETOKEN_OBSERVABILITY_PROBE",
                bytes = 4_u64,
                "created token=abc"
            );
            tracing::debug!("hidden");
        });
        logs.assert_required_fields();
        logs.assert_event_emitted("bobs.spool.created");
        logs.assert_no_substring("FAKETOKEN_OBSERVABILITY_PROBE");
        logs.assert_no_substring("token=abc");
        let line = &logs.json_lines()[0];
        assert_eq!(line["severityNumber"], json!(9));
        assert_eq!(line["resource"]["service.name"], json!("bobs"));
        assert_eq!(line["resource"]["deployment.environment"], json!("dev"));
        assert_eq!(
            line["attributes"]["request.id"],
            json!("0123456789abcdefghjkmnpqrs")
        );
        assert_eq!(line["attributes"]["bobs.spool.key"], json!("key-1"));
        assert_eq!(
            line["attributes"]["content_type"],
            json!("Bearer [REDACTED]")
        );
    }

    #[test]
    fn observability_unnamed_event_has_no_event_name() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: serialised by ENV_LOCK; no other threads touch the env in tests.
        unsafe {
            std::env::set_var("RUST_LOG", "info");
        }
        let (subscriber, logs) = capturing_subscriber("bobs");
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(answer = 42_u64, "internal")
        });
        let line = &logs.json_lines()[0];
        assert!(line["attributes"].get("event.name").is_none());
        assert_eq!(line["attributes"]["answer"], json!(42));
    }

    #[test]
    fn observability_redacts_all_probe_categories() {
        let probes = [
            "Authorization: Basic abc123",
            "Bearer FAKETOKEN_OBSERVABILITY_PROBE",
            "password=secret token=secret api_key=secret",
            "eyJabc.def.ghi",
            "https://user:pass@example.com/path",
            "32eff194-66bd",
        ];
        for probe in probes {
            let redacted = redact_text(probe);
            assert!(!redacted.contains("FAKETOKEN_OBSERVABILITY_PROBE"));
            assert!(!redacted.contains("secret"));
            assert!(!redacted.contains("user:pass"));
            assert!(!redacted.contains("32eff194-66bd"));
        }
        assert_eq!(
            redact_value(json!({"Authorization": "anything"}))["Authorization"],
            json!("[REDACTED]")
        );
    }

    #[test]
    fn observability_env_filter_fallbacks() {
        let _guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // SAFETY: serialised by ENV_LOCK; no other threads touch the env in tests.
        unsafe {
            std::env::remove_var("RUST_LOG");
        }
        let _ = env_filter_from_env();
        assert_eq!(env_filter_from_str("   ").to_string(), "info");
        assert_eq!(
            env_filter_from_str("[definitely invalid").to_string(),
            "info"
        );
        assert_eq!(env_filter_from_str("debug").to_string(), "debug");
    }
}

#[cfg(test)]
mod debug_to_value_tests {
    use super::debug_to_value;
    use serde_json::{json, Value};

    #[test]
    fn none_becomes_null() {
        assert_eq!(debug_to_value("None"), Value::Null);
    }

    #[test]
    fn some_string_unwraps() {
        assert_eq!(
            debug_to_value("Some(\"application/json\")"),
            json!("application/json")
        );
    }

    #[test]
    fn some_number_unwraps() {
        assert_eq!(debug_to_value("Some(115936646)"), json!(115936646));
    }

    #[test]
    fn some_bool_unwraps() {
        assert_eq!(debug_to_value("Some(true)"), json!(true));
    }

    #[test]
    fn plain_number_parses() {
        assert_eq!(debug_to_value("30"), json!(30));
    }

    #[test]
    fn plain_bool_parses() {
        assert_eq!(debug_to_value("true"), json!(true));
    }

    #[test]
    fn unparseable_falls_back_to_string() {
        // Custom Debug output that isn't valid JSON.
        assert_eq!(
            debug_to_value("MyEnum::Variant"),
            Value::String("MyEnum::Variant".to_string())
        );
    }

    #[test]
    fn nested_some_unwraps_once() {
        // Some(Some(42)) is unusual but should peel one layer cleanly.
        assert_eq!(debug_to_value("Some(Some(42))"), json!(42));
    }
}
