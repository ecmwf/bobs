use std::fmt;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BenchmarkConfig {
    pub endpoint: EndpointSpec,
    pub objects: usize,
    pub object_bytes: u64,
    pub write_body_chunk_bytes: usize,
    pub read_body_chunk_bytes: usize,
    pub write_request_bytes: Option<u64>,
    pub start_delay: Duration,
    pub delete_after_read: bool,
    pub summary_json: Option<String>,
    pub results_jsonl: Option<String>,
    pub log_chunks: bool,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointSpec {
    Single(Endpoint),
    Template {
        template: String,
        ordinals: Vec<u32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct Endpoint {
    pub root_url: String,
    pub host_header: String,
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseOutcome {
    Run(BenchmarkConfig),
    Help(String),
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            endpoint: EndpointSpec::Single(normalize_endpoint("http://127.0.0.1:3000").unwrap()),
            objects: 1,
            object_bytes: 1024 * 1024,
            write_body_chunk_bytes: 1024 * 1024,
            read_body_chunk_bytes: 1024 * 1024,
            write_request_bytes: None,
            start_delay: Duration::from_millis(1000),
            delete_after_read: false,
            summary_json: None,
            results_jsonl: None,
            log_chunks: false,
            connect_timeout: Duration::from_millis(10000),
            request_timeout: Duration::from_millis(300000),
        }
    }
}

impl BenchmarkConfig {
    pub fn parse_args_from<I, S>(args: I) -> Result<ParseOutcome, String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut cfg = BenchmarkConfig::default();
        let mut base_url: Option<String> = None;
        let mut template: Option<String> = None;
        let mut ordinals: Option<Vec<u32>> = None;
        let mut it = args.into_iter().map(Into::into).peekable();
        let _program = it.next();
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--help" | "-h" => return Ok(ParseOutcome::Help(usage())),
                "--base-url" => base_url = Some(take_value(&mut it, &arg)?),
                "--base-url-template" => template = Some(take_value(&mut it, &arg)?),
                "--ordinals" => ordinals = Some(parse_ordinals(&take_value(&mut it, &arg)?)?),
                "--objects" => cfg.objects = parse_num(&take_value(&mut it, &arg)?, &arg)?,
                "--object-bytes" => {
                    cfg.object_bytes = parse_num(&take_value(&mut it, &arg)?, &arg)?
                }
                "--write-body-chunk-bytes" => {
                    cfg.write_body_chunk_bytes = parse_num(&take_value(&mut it, &arg)?, &arg)?
                }
                "--read-body-chunk-bytes" => {
                    cfg.read_body_chunk_bytes = parse_num(&take_value(&mut it, &arg)?, &arg)?
                }
                "--write-request-bytes" => {
                    cfg.write_request_bytes = Some(parse_num(&take_value(&mut it, &arg)?, &arg)?)
                }
                "--start-delay-ms" => {
                    let ms: u64 = parse_num(&take_value(&mut it, &arg)?, &arg)?;
                    cfg.start_delay = Duration::from_millis(ms);
                }
                "--delete-after-read" => cfg.delete_after_read = true,
                "--summary-json" => cfg.summary_json = Some(take_value(&mut it, &arg)?),
                "--results-jsonl" => cfg.results_jsonl = Some(take_value(&mut it, &arg)?),
                "--log-chunks" => cfg.log_chunks = true,
                "--connect-timeout-ms" => {
                    let ms: u64 = parse_num(&take_value(&mut it, &arg)?, &arg)?;
                    cfg.connect_timeout = Duration::from_millis(ms);
                }
                "--request-timeout-ms" => {
                    let ms: u64 = parse_num(&take_value(&mut it, &arg)?, &arg)?;
                    cfg.request_timeout = Duration::from_millis(ms);
                }
                _ if arg.starts_with('-') => return Err(format!("unknown option {arg}")),
                _ => return Err(format!("unexpected argument {arg}")),
            }
        }

        cfg.endpoint = match (base_url, template) {
            (Some(_), Some(_)) | (None, None) => {
                return Err("exactly one of --base-url or --base-url-template is required".into())
            }
            (Some(url), None) => EndpointSpec::Single(normalize_endpoint(&url)?),
            (None, Some(t)) => {
                if !t.contains("{ordinal}") {
                    return Err("--base-url-template must contain {ordinal}".into());
                }
                validate_http_url(&t.replace("{ordinal}", "0"))?;
                let ords = ordinals
                    .ok_or_else(|| "--ordinals is required with --base-url-template".to_string())?;
                if ords.is_empty() {
                    return Err("--ordinals must not be empty".into());
                }
                EndpointSpec::Template {
                    template: t,
                    ordinals: ords,
                }
            }
        };
        cfg.validate()?;
        Ok(ParseOutcome::Run(cfg))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.objects == 0 {
            return Err("--objects must be greater than zero".into());
        }
        if self.object_bytes == 0 {
            return Err("--object-bytes must be greater than zero".into());
        }
        if self.write_body_chunk_bytes == 0 {
            return Err("--write-body-chunk-bytes must be greater than zero".into());
        }
        if self.read_body_chunk_bytes == 0 {
            return Err("--read-body-chunk-bytes must be greater than zero".into());
        }
        if matches!(self.write_request_bytes, Some(0)) {
            return Err("--write-request-bytes must be greater than zero".into());
        }
        Ok(())
    }
}

fn take_value<I>(it: &mut std::iter::Peekable<I>, opt: &str) -> Result<String, String>
where
    I: Iterator<Item = String>,
{
    it.next().ok_or_else(|| format!("{opt} requires a value"))
}

fn parse_num<T>(s: &str, opt: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: fmt::Display,
{
    s.parse::<T>()
        .map_err(|e| format!("invalid value for {opt}: {e}"))
}

pub fn parse_ordinals(s: &str) -> Result<Vec<u32>, String> {
    if s.trim().is_empty() {
        return Err("--ordinals must not be empty".into());
    }
    s.split(',')
        .map(|p| {
            if p.trim().is_empty() {
                return Err("malformed --ordinals list".into());
            }
            p.trim()
                .parse::<u32>()
                .map_err(|e| format!("invalid ordinal '{p}': {e}"))
        })
        .collect()
}

pub fn normalize_endpoint(input: &str) -> Result<Endpoint, String> {
    validate_http_url(input)?;
    let mut rest = input.strip_prefix("http://").unwrap();
    let slash = rest.find('/').unwrap_or(rest.len());
    let authority = &rest[..slash];
    let path = &rest[slash..];
    if authority.is_empty() {
        return Err("URL host must not be empty".into());
    }
    let (host, port, host_header) = parse_authority(authority)?;
    let clean_path = path.trim_end_matches('/');
    let prefix = if clean_path.is_empty() {
        ""
    } else if clean_path == "/api/v1" {
        ""
    } else {
        return Err("endpoint URL path must be empty or /api/v1".into());
    };
    let root_url = format!("http://{host_header}{prefix}");
    rest = "";
    let _ = rest;
    Ok(Endpoint {
        root_url,
        host_header,
        host,
        port,
    })
}

fn parse_authority(authority: &str) -> Result<(String, u16, String), String> {
    let (host, port) = if let Some((h, p)) = authority.rsplit_once(':') {
        if h.is_empty() {
            return Err("URL host must not be empty".into());
        }
        let port = p
            .parse::<u16>()
            .map_err(|e| format!("invalid URL port: {e}"))?;
        (h.to_string(), port)
    } else {
        (authority.to_string(), 80)
    };
    Ok((host, port, authority.to_string()))
}

pub fn endpoint_api_path(path: &str) -> String {
    format!("/api/v1{path}")
}

pub fn validate_http_url(url: &str) -> Result<(), String> {
    if url.starts_with("https://") {
        return Err(
            "HTTPS endpoints require an approved dependency/feature change; use http://".into(),
        );
    }
    if !url.starts_with("http://") {
        return Err("only http:// endpoints are supported".into());
    }
    Ok(())
}

pub fn usage() -> String {
    "Usage: bobs-benchmark --base-url http://host:port [options]\n       bobs-benchmark --base-url-template http://host-{ordinal}:3000 --ordinals 0,1 [options]\n\nOptions:\n  --base-url URL\n  --base-url-template URL_WITH_{ordinal}\n  --ordinals LIST\n  --objects N\n  --object-bytes N\n  --write-body-chunk-bytes N\n  --read-body-chunk-bytes N\n  --write-request-bytes N\n  --start-delay-ms N\n  --delete-after-read\n  --summary-json PATH\n  --results-jsonl PATH\n  --log-chunks\n  --connect-timeout-ms N\n  --request-timeout-ms N\n  --help\n".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(args: &[&str]) -> Result<ParseOutcome, String> {
        BenchmarkConfig::parse_args_from(args.iter().copied())
    }
    #[test]
    fn valid_single_endpoint_args() {
        let c = match run(&["b", "--base-url", "http://localhost:3000", "--objects", "2"]).unwrap()
        {
            ParseOutcome::Run(c) => c,
            _ => panic!(),
        };
        assert_eq!(c.objects, 2);
    }
    #[test]
    fn valid_template_args() {
        let c = match run(&[
            "b",
            "--base-url-template",
            "http://bobs-{ordinal}:3000",
            "--ordinals",
            "0,1",
        ])
        .unwrap()
        {
            ParseOutcome::Run(c) => c,
            _ => panic!(),
        };
        assert!(matches!(c.endpoint, EndpointSpec::Template { .. }));
    }
    #[test]
    fn help() {
        assert!(matches!(
            run(&["b", "--help"]).unwrap(),
            ParseOutcome::Help(_)
        ));
    }
    #[test]
    fn missing_endpoint() {
        assert!(run(&["b"]).unwrap_err().contains("exactly one"));
    }
    #[test]
    fn invalid_scheme() {
        assert!(run(&["b", "--base-url", "https://x"])
            .unwrap_err()
            .contains("HTTPS"));
    }
    #[test]
    fn zero_sizes() {
        assert!(run(&["b", "--base-url", "http://x", "--objects", "0"])
            .unwrap_err()
            .contains("objects"));
    }
    #[test]
    fn missing_ordinals() {
        assert!(run(&["b", "--base-url-template", "http://x-{ordinal}"])
            .unwrap_err()
            .contains("ordinals"));
    }
    #[test]
    fn malformed_ordinal_lists() {
        assert!(run(&[
            "b",
            "--base-url-template",
            "http://x-{ordinal}",
            "--ordinals",
            "0,"
        ])
        .is_err());
    }
    #[test]
    fn template_without_placeholder() {
        assert!(
            run(&["b", "--base-url-template", "http://x", "--ordinals", "0"])
                .unwrap_err()
                .contains("{ordinal}")
        );
    }
    #[test]
    fn normalize_urls() {
        assert_eq!(
            normalize_endpoint("http://localhost:3000/api/v1/")
                .unwrap()
                .root_url,
            "http://localhost:3000"
        );
    }
}
