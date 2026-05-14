use super::config::{endpoint_api_path, Endpoint};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Clone)]
pub struct BobsHttpClient {
    connect_timeout: Duration,
    request_timeout: Duration,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateOutput {
    pub key: String,
    pub read_url: String,
    pub write_url: String,
}

#[derive(Debug, Clone)]
pub struct HttpResponseHead {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body_prefix: Vec<u8>,
}

impl BobsHttpClient {
    pub fn new(connect_timeout: Duration, request_timeout: Duration) -> Self {
        Self {
            connect_timeout,
            request_timeout,
        }
    }

    pub async fn create(&self, endpoint: &Endpoint) -> Result<(CreateOutput, u16), String> {
        let body = b"{}";
        let req = format!("PUT {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", endpoint_api_path("/create"), endpoint.host_header, body.len());
        let resp = self
            .request_small(endpoint, req.into_bytes(), Some(body), 64 * 1024)
            .await?;
        validate_status(&resp, &[201])?;
        let out: CreateOutput = serde_json::from_slice(&resp.body_prefix)
            .map_err(|e| format!("create JSON parse failed: {e}"))?;
        Ok((out, resp.status))
    }

    pub async fn write_repeated(
        &self,
        endpoint: &Endpoint,
        key: &str,
        offset: u64,
        bytes: u64,
        buf: &[u8],
    ) -> Result<u16, String> {
        let req = format!(
            "POST {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
            endpoint_api_path(&format!("/write/{key}/{offset}")),
            endpoint.host_header,
            bytes
        );
        let fut = async {
            let mut stream = self.connect(endpoint).await?;
            stream.write_all(req.as_bytes()).await.map_err(ioerr)?;
            let mut remaining = bytes;
            while remaining > 0 {
                let n = remaining.min(buf.len() as u64) as usize;
                stream.write_all(&buf[..n]).await.map_err(ioerr)?;
                remaining -= n as u64;
            }
            let resp = read_response(stream, 64 * 1024, true).await?;
            validate_status(&resp, &[200])?;
            Ok(resp.status)
        };
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| "write request timed out".to_string())?
    }

    pub async fn complete(
        &self,
        endpoint: &Endpoint,
        key: &str,
        expected_size: u64,
    ) -> Result<u16, String> {
        let body = format!("{{\"expected_size\":{expected_size}}}");
        let req = format!("POST {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n", endpoint_api_path(&format!("/complete/{key}")), endpoint.host_header, body.len());
        let resp = self
            .request_small(endpoint, req.into_bytes(), Some(body.as_bytes()), 64 * 1024)
            .await?;
        validate_status(&resp, &[200])?;
        Ok(resp.status)
    }

    pub async fn read_discard(
        &self,
        endpoint: &Endpoint,
        key: &str,
        bytes: u64,
        read_chunk_bytes: usize,
    ) -> Result<(u16, u64), String> {
        let end = bytes.saturating_sub(1);
        let req = format!(
            "GET {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nRange: bytes=0-{}\r\n\r\n",
            endpoint_api_path(&format!("/read/{key}")),
            endpoint.host_header,
            end
        );
        let fut = async {
            let mut stream = self.connect(endpoint).await?;
            stream.write_all(req.as_bytes()).await.map_err(ioerr)?;
            let (head, rest) = read_head(&mut stream, 64 * 1024).await?;
            let mut resp = parse_head(&head, Vec::new())?;
            validate_status(&resp, &[200, 206])?;
            let got = if header_lookup(&resp.headers, "transfer-encoding")
                .map(|v| v.to_ascii_lowercase().contains("chunked"))
                .unwrap_or(false)
            {
                discard_chunked_body(&mut stream, rest, read_chunk_bytes.max(1)).await?
            } else {
                discard_to_eof(&mut stream, rest, read_chunk_bytes.max(1)).await?
            };
            if got != bytes {
                return Err(format!(
                    "read byte count mismatch: got {got}, expected {bytes}"
                ));
            }
            resp.body_prefix.clear();
            Ok((resp.status, got))
        };
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| "read request timed out".to_string())?
    }

    pub async fn delete(&self, endpoint: &Endpoint, key: &str) -> Result<u16, String> {
        let req = format!(
            "DELETE {} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n",
            endpoint_api_path(&format!("/delete/{key}")),
            endpoint.host_header
        );
        let resp = self
            .request_small(endpoint, req.into_bytes(), None, 64 * 1024)
            .await?;
        validate_status(&resp, &[200])?;
        Ok(resp.status)
    }

    async fn connect(&self, endpoint: &Endpoint) -> Result<TcpStream, String> {
        tokio::time::timeout(
            self.connect_timeout,
            TcpStream::connect((endpoint.host.as_str(), endpoint.port)),
        )
        .await
        .map_err(|_| "connect timed out".to_string())?
        .map_err(ioerr)
    }

    async fn request_small(
        &self,
        endpoint: &Endpoint,
        mut request: Vec<u8>,
        body: Option<&[u8]>,
        limit: usize,
    ) -> Result<HttpResponseHead, String> {
        if let Some(b) = body {
            request.extend_from_slice(b);
        }
        let fut = async {
            let mut stream = self.connect(endpoint).await?;
            stream.write_all(&request).await.map_err(ioerr)?;
            read_response(stream, limit, false).await
        };
        tokio::time::timeout(self.request_timeout, fut)
            .await
            .map_err(|_| "request timed out".to_string())?
    }
}

fn ioerr(e: std::io::Error) -> String {
    e.to_string()
}

async fn read_response(
    mut stream: TcpStream,
    limit: usize,
    body_prefix_only: bool,
) -> Result<HttpResponseHead, String> {
    let (head, mut rest) = read_head(&mut stream, limit).await?;
    let headers = parse_headers_only(&head)?;
    let content_len =
        header_lookup(&headers, "content-length").and_then(|s| s.parse::<usize>().ok());
    if !body_prefix_only {
        let target = content_len.unwrap_or(limit).min(limit);
        while rest.len() < target {
            let mut buf = vec![0u8; (target - rest.len()).min(8192)];
            let n = stream.read(&mut buf).await.map_err(ioerr)?;
            if n == 0 {
                break;
            }
            rest.extend_from_slice(&buf[..n]);
            if rest.len() >= limit {
                break;
            }
        }
    } else {
        rest.truncate(limit.min(rest.len()));
    }
    parse_head(&head, rest)
}

async fn read_head(stream: &mut TcpStream, limit: usize) -> Result<(Vec<u8>, Vec<u8>), String> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
    loop {
        let n = stream.read(&mut tmp).await.map_err(ioerr)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = find_header_end(&buf) {
            let rest = buf[pos + 4..].to_vec();
            let head = buf[..pos + 4].to_vec();
            return Ok((head, rest));
        }
        if buf.len() > limit {
            return Err("response headers exceeded limit".into());
        }
    }
    Err("connection closed before response headers".into())
}

async fn discard_to_eof(
    stream: &mut TcpStream,
    rest: Vec<u8>,
    read_chunk_bytes: usize,
) -> Result<u64, String> {
    let mut got = rest.len() as u64;
    let mut buf = vec![0u8; read_chunk_bytes];
    loop {
        let n = stream.read(&mut buf).await.map_err(ioerr)?;
        if n == 0 {
            break;
        }
        got += n as u64;
    }
    Ok(got)
}

async fn discard_chunked_body(
    stream: &mut TcpStream,
    mut pending: Vec<u8>,
    read_chunk_bytes: usize,
) -> Result<u64, String> {
    let mut got = 0u64;
    loop {
        let line = read_line_crlf(stream, &mut pending).await?;
        let size_text = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|e| format!("invalid chunk size '{size_text}': {e}"))?;
        if size == 0 {
            // Consume trailers up to the terminating empty line.
            loop {
                if read_line_crlf(stream, &mut pending).await?.is_empty() {
                    return Ok(got);
                }
            }
        }
        discard_exact(stream, &mut pending, size, read_chunk_bytes).await?;
        got += size as u64;
        discard_exact(stream, &mut pending, 2, 2).await?;
    }
}

async fn read_line_crlf(stream: &mut TcpStream, pending: &mut Vec<u8>) -> Result<String, String> {
    loop {
        if let Some(pos) = pending.windows(2).position(|w| w == b"\r\n") {
            let line = pending[..pos].to_vec();
            pending.drain(..pos + 2);
            return String::from_utf8(line).map_err(|e| format!("invalid chunk line: {e}"));
        }
        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.map_err(ioerr)?;
        if n == 0 {
            return Err("connection closed while reading chunk line".into());
        }
        pending.extend_from_slice(&buf[..n]);
        if pending.len() > 64 * 1024 {
            return Err("chunk line exceeded limit".into());
        }
    }
}

async fn discard_exact(
    stream: &mut TcpStream,
    pending: &mut Vec<u8>,
    mut len: usize,
    read_chunk_bytes: usize,
) -> Result<(), String> {
    let take = pending.len().min(len);
    pending.drain(..take);
    len -= take;
    let mut buf = vec![0u8; read_chunk_bytes.max(1).min(len.max(1))];
    while len > 0 {
        let want = buf.len().min(len);
        let n = stream.read(&mut buf[..want]).await.map_err(ioerr)?;
        if n == 0 {
            return Err("connection closed while reading body".into());
        }
        len -= n;
    }
    Ok(())
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

pub fn parse_head(head: &[u8], body_prefix: Vec<u8>) -> Result<HttpResponseHead, String> {
    let text = std::str::from_utf8(head).map_err(|e| format!("invalid response headers: {e}"))?;
    let mut lines = text.split("\r\n");
    let status_line = lines.next().ok_or("missing status line")?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or("missing status code")?
        .parse::<u16>()
        .map_err(|e| format!("invalid status code: {e}"))?;
    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }
    Ok(HttpResponseHead {
        status,
        headers,
        body_prefix,
    })
}

fn parse_headers_only(head: &[u8]) -> Result<HashMap<String, String>, String> {
    Ok(parse_head(head, Vec::new())?.headers)
}

pub fn header_lookup<'a>(headers: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    headers.get(&name.to_ascii_lowercase()).map(String::as_str)
}

pub fn validate_status(resp: &HttpResponseHead, expected: &[u16]) -> Result<(), String> {
    if expected.contains(&resp.status) {
        return Ok(());
    }
    let snippet = String::from_utf8_lossy(&resp.body_prefix[..resp.body_prefix.len().min(512)]);
    Err(format!(
        "unexpected HTTP status {}, expected {:?}: {}",
        resp.status, expected, snippet
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn response_parsing_and_header_lookup() {
        let h = b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\nX-Test: yes\r\n\r\n";
        let r = parse_head(h, b"{}".to_vec()).unwrap();
        assert_eq!(r.status, 201);
        assert_eq!(header_lookup(&r.headers, "content-length"), Some("2"));
    }
    #[test]
    fn status_validation_captures_body() {
        let r = HttpResponseHead {
            status: 500,
            headers: HashMap::new(),
            body_prefix: b"failure details".to_vec(),
        };
        assert!(validate_status(&r, &[200])
            .unwrap_err()
            .contains("failure details"));
    }
    #[test]
    fn content_length_handling() {
        let h = b"HTTP/1.1 200 OK\r\ncontent-length: 10\r\n\r\n";
        let r = parse_head(h, Vec::new()).unwrap();
        assert_eq!(header_lookup(&r.headers, "Content-Length"), Some("10"));
    }
}
