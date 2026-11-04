// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::fs;
use std::io;
use std::io::Write as _;
use std::net::SocketAddr;
use std::ops::Deref;
use std::ops::DerefMut;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::response::sse::Event;
use axum::response::sse::Sse;
use axum::routing::get;
use futures_util::stream;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;

type ServingTask = JoinHandle<Result<qubit_web::ShutdownReport, qubit_web::WebServerError>>;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
static SSE_PROGRESS: AtomicU64 = AtomicU64::new(0);
static SERVICE_HEARTBEAT: AtomicU64 = AtomicU64::new(0);

async fn start_server() -> (SocketAddr, oneshot::Sender<()>, ServingTask) {
    let app = Router::new()
        .route(
            "/events",
            get(|| async {
                let events = stream::unfold(0_u64, |index| async move {
                    time::sleep(Duration::from_millis(80)).await;
                    Some((
                        Ok::<_, std::convert::Infallible>(Event::default().data(format!("tick-{index}"))),
                        index + 1,
                    ))
                });
                Sse::new(events)
            }),
        )
        .route("/health", get(|| async { "ok" }));
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap())
            .with_max_transport_connections(1)
            .unwrap(),
    )
    .await
    .expect("bind test server");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    (address, shutdown_tx, serving)
}

fn frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len();
    let mut bytes = vec![
        ((len >> 16) & 0xff) as u8,
        ((len >> 8) & 0xff) as u8,
        (len & 0xff) as u8,
        kind,
        flags,
    ];
    bytes.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes
}

async fn read_frame(reader: &mut OwnedReadHalf) -> io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    reader.read_exact(&mut header).await?;
    let len = (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let stream_id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok((header[3], header[4], stream_id, payload))
}

async fn send_headers(writer: &mut OwnedWriteHalf, stream_id: u32, end_headers: bool, block: &[u8]) {
    let flags = (if end_headers { 0x04 } else { 0 }) | 0x01; // END_STREAM
    writer.write_all(&frame(0x01, flags, stream_id, block)).await.unwrap();
}

// Valid HPACK block: GET, http, /events, authority=localhost.
const EVENTS_HEADERS: &[u8] = b"\x82\x86\x04\x07/events\x01\x09localhost";
const HEALTH_HEADERS: &[u8] = b"\x82\x86\x04\x07/health\x01\x09localhost";

async fn begin_h2(address: SocketAddr) -> (OwnedReadHalf, OwnedWriteHalf) {
    let tcp = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()
        .expect("connect");
    let (mut reader, mut writer) = tcp.into_split();
    writer.write_all(PREFACE).await.unwrap();
    writer.write_all(&frame(0x04, 0, 0, &[])).await.unwrap(); // SETTINGS
    let settings = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
        .await
        .unwrap()
        .expect("read SETTINGS frame");
    assert_eq!(settings.0, 0x04, "expected server SETTINGS before requests");
    writer.write_all(&frame(0x04, 0x01, 0, &[])).await.unwrap(); // ACK
    (reader, writer)
}

async fn read_until_stream_data(reader: &mut OwnedReadHalf, stream_id: u32) -> bool {
    loop {
        let Ok(Ok((kind, _, id, payload))) = time::timeout(Duration::from_secs(3), read_frame(reader)).await else {
            return false;
        };
        if kind == 0x00 && id == stream_id && !payload.is_empty() {
            return true;
        }
        if kind == 0x03 || kind == 0x07 {
            return false;
        }
    }
}

async fn finish_server(shutdown: oneshot::Sender<()>, serving: ServingTask) {
    let _ = shutdown.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .unwrap()
        .expect("server task")
        .expect("server shutdown");
    assert!(report.graceful);
}

#[derive(Debug)]
enum HeldClient {
    Http2 {
        _reader: OwnedReadHalf,
        _writer: OwnedWriteHalf,
    },
}

struct ChildGuard(Child);

impl Deref for ChildGuard {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ChildGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

fn child_path(dir: &Path, name: &str) -> PathBuf {
    dir.join(name)
}

fn launch_resource_service(dir: &Path, small_budget: bool) -> io::Result<ChildGuard> {
    Command::new(std::env::current_exe()?)
        .arg("--ignored")
        .arg("--exact")
        .arg("resource_service_child")
        .arg("--nocapture")
        .env("RS_WEB_RESOURCE_CHILD_DIR", dir)
        .env("RS_WEB_RESOURCE_SMALL_BUDGET", if small_budget { "1" } else { "0" })
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map(ChildGuard)
}

async fn wait_for_child_address(dir: &Path, child: &mut Child) -> SocketAddr {
    let file = child_path(dir, "address");
    time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(text) = fs::read_to_string(&file)
                && let Ok(address) = text.trim().parse()
            {
                return address;
            }
            assert!(
                child.try_wait().unwrap().is_none(),
                "resource service exited before readiness"
            );
            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("resource service readiness timeout")
}

fn linux_rss_kib(pid: u32) -> io::Result<u64> {
    let status = fs::read_to_string(format!("/proc/{pid}/status"))?;
    let line = status
        .lines()
        .find(|line| line.starts_with("VmRSS:"))
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "VmRSS missing from /proc status"))?;
    line.split_whitespace()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid VmRSS value"))
}

fn linux_server_connection_count(port: u16) -> io::Result<usize> {
    let expected_port = format!("{port:04X}");
    let mut count = 0;
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let contents = fs::read_to_string(table)?;
        for line in contents.lines().skip(1) {
            let fields: Vec<_> = line.split_whitespace().collect();
            if fields.get(1).and_then(|local| local.rsplit(':').next()) == Some(expected_port.as_str())
                && fields.get(3) == Some(&"01")
            {
                count += 1;
            }
        }
    }
    Ok(count)
}

async fn make_clients(address: SocketAddr, state: &str, count: usize) -> Vec<HeldClient> {
    let mut clients = Vec::with_capacity(count);
    for index in 0..count {
        let (mut reader, mut writer) = begin_h2(address).await;
        match state {
            "idle" => clients.push(HeldClient::Http2 {
                _reader: reader,
                _writer: writer,
            }),
            "sse" => {
                send_headers(&mut writer, 1, true, EVENTS_HEADERS).await;
                assert!(
                    read_until_stream_data(&mut reader, 1).await,
                    "HTTP/2 SSE did not produce initial data"
                );
                clients.push(HeldClient::Http2 {
                    _reader: reader,
                    _writer: writer,
                });
            }
            "short" => {
                send_headers(&mut writer, 1, true, HEALTH_HEADERS).await;
                loop {
                    let (kind, flags, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
                        .await
                        .unwrap()
                        .unwrap();
                    if id == 1 && (kind == 0x00 || kind == 0x01) && flags & 0x01 != 0 {
                        break;
                    }
                }
                clients.push(HeldClient::Http2 {
                    _reader: reader,
                    _writer: writer,
                });
            }
            "incomplete" => {
                // Complete valid HPACK fields, but deliberately leave END_HEADERS clear.
                send_headers(&mut writer, (index as u32 * 2) + 1, false, HEALTH_HEADERS).await;
                clients.push(HeldClient::Http2 {
                    _reader: reader,
                    _writer: writer,
                });
            }
            _ => unreachable!(),
        }
    }
    clients
}

async fn probe_server(address: SocketAddr) -> bool {
    let Ok(Ok(mut stream)) = time::timeout(Duration::from_secs(2), TcpStream::connect(address)).await else {
        return false;
    };
    if stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut status_line = Vec::new();
    loop {
        let mut byte = [0; 1];
        match time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte)).await {
            Ok(Ok(_)) => status_line.push(byte[0]),
            _ => return false,
        }
        if status_line.ends_with(b"\r\n") {
            break;
        }
        if status_line.len() > 256 {
            return false;
        }
    }
    status_line.starts_with(b"HTTP/1.1 200")
}

async fn all_transport_permits_serve_health(address: SocketAddr, count: usize) -> bool {
    let mut streams = Vec::with_capacity(count);
    for _ in 0..count {
        let Ok(Ok(mut stream)) = time::timeout(Duration::from_secs(2), TcpStream::connect(address)).await else {
            return false;
        };
        if stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .is_err()
        {
            return false;
        }
        let mut status_line = Vec::new();
        loop {
            let mut byte = [0; 1];
            match time::timeout(Duration::from_secs(2), stream.read_exact(&mut byte)).await {
                Ok(Ok(_)) => status_line.push(byte[0]),
                _ => return false,
            }
            if status_line.ends_with(b"\r\n") {
                break;
            }
            if status_line.len() > 256 {
                return false;
            }
        }
        if !status_line.starts_with(b"HTTP/1.1 200") {
            return false;
        }
        streams.push(stream);
    }
    drop(streams);
    true
}

fn append_hpack_string(output: &mut Vec<u8>, value: &[u8]) {
    let mut length = value.len();
    if length < 127 {
        output.push(length as u8);
    } else {
        output.push(127);
        length -= 127;
        while length >= 128 {
            output.push((length as u8 & 0x7f) | 0x80);
            length >>= 7;
        }
        output.push(length as u8);
    }
    output.extend_from_slice(value);
}

#[tokio::test]
async fn cleartext_http2_header_list_budget_rejects_oversized_request_and_recovers() {
    let handler_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = handler_calls.clone();
    let app = Router::new().route(
        "/health",
        get(move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                "ok"
            }
        }),
    );
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(1)
        .unwrap()
        .with_http2_max_header_list_bytes(16 * 1024)
        .unwrap();
    let server = WebServer::bind_http(options).await.expect("bind h2 budget test server");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let (mut reader, mut writer) = begin_h2(address).await;
    let mut block = HEALTH_HEADERS.to_vec();
    block.push(0x00); // Literal header without indexing; new name follows.
    append_hpack_string(&mut block, b"x-pad");
    append_hpack_string(&mut block, &vec![b'a'; 32 * 1024]);
    let split = 16 * 1024;
    writer.write_all(&frame(0x01, 0x01, 1, &block[..split])).await.unwrap();
    for (index, fragment) in block[split..].chunks(16 * 1024).enumerate() {
        let last = split + (index + 1) * 16 * 1024 >= block.len();
        writer
            .write_all(&frame(0x09, if last { 0x04 } else { 0 }, 1, fragment))
            .await
            .unwrap();
    }
    let rejected = time::timeout(Duration::from_secs(3), async {
        loop {
            match read_frame(&mut reader).await {
                Ok((0x03 | 0x07, _, _, _)) | Err(_) => return true,
                Ok((0x01, _, 1, _)) => return true,
                Ok(_) => {}
            }
        }
    })
    .await
    .expect("oversized request rejection deadline");
    assert!(rejected, "oversized HTTP/2 request was not rejected before timeout");
    assert_eq!(
        handler_calls.load(Ordering::Relaxed),
        0,
        "oversized request reached the route handler"
    );
    drop(writer);
    drop(reader);

    let mut followup = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()
        .unwrap();
    followup
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(3), followup.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "server did not recover after oversized HTTP/2 headers"
    );
    finish_server(shutdown_tx, serving).await;
}

/// Child process entrypoint for the explicitly invoked resource experiment.
#[tokio::test]
#[ignore = "child process entrypoint for resource experiment"]
async fn resource_service_child() {
    let Ok(dir) = std::env::var("RS_WEB_RESOURCE_CHILD_DIR") else {
        return;
    };
    let dir = PathBuf::from(dir);
    let small_budget = matches!(std::env::var("RS_WEB_RESOURCE_SMALL_BUDGET").as_deref(), Ok("1"));
    let mut options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(33)
        .unwrap()
        .with_transport_idle_timeout(Duration::from_secs(75));
    if small_budget {
        options = options.with_http2_max_header_list_bytes(16 * 1024).unwrap();
    }
    let app = Router::new().route("/health", get(|| async { "ok" })).route(
        "/events",
        get(|| async {
            let events = stream::unfold(0_u64, |index| async move {
                time::sleep(Duration::from_millis(100)).await;
                SSE_PROGRESS.fetch_add(1, Ordering::Relaxed);
                Some((
                    Ok::<_, std::convert::Infallible>(Event::default().data(index.to_string())),
                    index + 1,
                ))
            });
            Sse::new(events)
        }),
    );
    let server = WebServer::bind_http(options).await.expect("bind isolated server");
    let address = server.local_addr();
    fs::write(child_path(&dir, "address"), address.to_string()).unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let progress = child_path(&dir, "sse-progress");
    let heartbeat_file = child_path(&dir, "heartbeat");
    let progress_task = tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let _ = fs::write(&progress, SSE_PROGRESS.load(Ordering::Relaxed).to_string());
            let _ = fs::write(
                &heartbeat_file,
                SERVICE_HEARTBEAT.fetch_add(1, Ordering::Relaxed).to_string(),
            );
        }
    });
    let verify = child_path(&dir, "verify-recovery");
    let recovery_result = child_path(&dir, "recovery-result");
    while !verify.exists() {
        time::sleep(Duration::from_millis(50)).await;
    }
    let recovered = all_transport_permits_serve_health(address, 33).await;
    fs::write(&recovery_result, if recovered { "ok" } else { "failed" }).unwrap();
    let stop = child_path(&dir, "stop");
    while !stop.exists() {
        time::sleep(Duration::from_millis(50)).await;
    }
    progress_task.abort();
    let _ = shutdown_tx.send(());
    let report = time::timeout(Duration::from_secs(8), serving)
        .await
        .expect("child server shutdown deadline")
        .expect("server task join")
        .expect("server shutdown");
    assert!(report.graceful, "child server did not shut down gracefully");
}

/// Runs the 2 × 3 × 4 matrix in isolated server processes. Linux /proc is
/// required for RSS; every case is capped at 60 seconds and samples at 5-second
/// intervals.
#[tokio::test]
#[ignore = "bounded, explicit HTTP/2 resource experiment; not a CI threshold"]
async fn bounded_http2_resource_matrix() {
    assert!(
        PathBuf::from("/proc/self/status").exists(),
        "RSS measurement requires Linux /proc"
    );
    let root = std::env::temp_dir().join(format!(
        "rs-web-http2-resource-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&root).expect("create unique experiment directory exclusively");
    let csv_path = PathBuf::from(
        std::env::var("RS_WEB_RESOURCE_CSV")
            .unwrap_or_else(|_| "doc/http2-resource-experiment-20261104.csv".to_owned()),
    );
    let mut csv_file = fs::File::create(&csv_path).expect("create permanent raw experiment data");
    let mut csv = String::from(
        "budget,state,client_connections,server_connections,elapsed_seconds,service_rss_kib,sse_events,header_bytes,service_pid,responsive,stop_reason\n",
    );
    csv_file.write_all(csv.as_bytes()).unwrap();
    csv_file.flush().unwrap();
    csv.clear();
    for small_budget in [false, true] {
        for count in [1, 8, 32] {
            for state in ["idle", "short", "sse", "incomplete"] {
                let case = format!("{}-{state}-{count}", if small_budget { "small" } else { "default" });
                let dir = root.join(&case);
                fs::create_dir_all(&dir).unwrap();
                let mut child = launch_resource_service(&dir, small_budget).unwrap();
                let address = wait_for_child_address(&dir, &mut child).await;
                let clients = make_clients(address, state, count).await;
                let header_bytes = match state {
                    "short" | "idle" => {
                        if state == "short" {
                            count * (9 + HEALTH_HEADERS.len())
                        } else {
                            0
                        }
                    }
                    "sse" => count * (9 + EVENTS_HEADERS.len()),
                    "incomplete" => count * (9 + HEALTH_HEADERS.len()),
                    _ => unreachable!(),
                };
                assert!(
                    header_bytes <= 8 * 1024 * 1024,
                    "per-group inbound header-data cap exceeded"
                );
                let started = std::time::Instant::now();
                let mut elapsed = 0;
                loop {
                    let rss = linux_rss_kib(child.id()).expect("sample service RSS");
                    let server_connections =
                        linux_server_connection_count(address.port()).expect("sample established server connections");
                    let events = fs::read_to_string(child_path(&dir, "sse-progress"))
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    let heartbeat = fs::read_to_string(child_path(&dir, "heartbeat"))
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0);
                    let responsive = probe_server(address).await;
                    let reason = if rss >= 512 * 1024 {
                        "rss_limit"
                    } else if !responsive {
                        "service_unresponsive"
                    } else if elapsed >= 60 {
                        "duration"
                    } else {
                        "sample"
                    };
                    csv.push_str(&format!(
                        "{},{},{},{},{},{},{},{},{},{},{}\n",
                        if small_budget { "16KiB" } else { "default" },
                        state,
                        clients.len(),
                        server_connections,
                        elapsed,
                        rss,
                        events,
                        header_bytes,
                        child.id(),
                        responsive,
                        reason
                    ));
                    csv_file.write_all(csv.as_bytes()).unwrap();
                    csv_file.flush().unwrap();
                    csv.clear();
                    if rss >= 512 * 1024 {
                        break;
                    }
                    if !responsive {
                        break;
                    }
                    if elapsed >= 60 {
                        break;
                    }
                    time::sleep(Duration::from_secs(5)).await;
                    elapsed = started.elapsed().as_secs();
                    let next_heartbeat = fs::read_to_string(child_path(&dir, "heartbeat"))
                        .ok()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(heartbeat);
                    if next_heartbeat == heartbeat {
                        let final_rss = linux_rss_kib(child.id()).unwrap_or(rss);
                        let final_connections =
                            linux_server_connection_count(address.port()).unwrap_or(server_connections);
                        csv.push_str(&format!(
                            "{},{},{},{},{},{},{},{},{},false,heartbeat_lost\n",
                            if small_budget { "16KiB" } else { "default" },
                            state,
                            clients.len(),
                            final_connections,
                            elapsed,
                            final_rss,
                            events,
                            header_bytes,
                            child.id()
                        ));
                        csv_file.write_all(csv.as_bytes()).unwrap();
                        csv_file.flush().unwrap();
                        csv.clear();
                        break;
                    }
                }
                drop(clients);
                fs::write(child_path(&dir, "verify-recovery"), "verify").unwrap();
                let recovery_result = child_path(&dir, "recovery-result");
                let recovered = time::timeout(Duration::from_secs(4), async {
                    loop {
                        if let Ok(result) = fs::read_to_string(&recovery_result) {
                            return result == "ok";
                        }
                        time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .unwrap_or(false);
                fs::write(child_path(&dir, "stop"), "stop").unwrap();
                let exit = time::timeout(Duration::from_secs(10), async {
                    loop {
                        if let Some(status) = child.try_wait().unwrap() {
                            return status;
                        }
                        time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await;
                if exit.is_err() {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                assert!(exit.is_ok(), "resource service did not stop promptly for {case}");
                assert!(
                    recovered,
                    "server did not accept a follow-up request after clients closed for {case}"
                );
            }
        }
    }
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn investigates_incomplete_h2_headers_with_active_sse() {
    let (address, shutdown, serving) = start_server().await;
    let (mut reader, mut writer) = begin_h2(address).await;

    // Baseline: complete SSE request headers, then confirm its response advances.
    send_headers(&mut writer, 1, true, EVENTS_HEADERS).await;
    assert!(
        read_until_stream_data(&mut reader, 1).await,
        "SSE should produce data before the next stream"
    );

    // A second complete request is a protocol control and must be served on the
    // same connection.
    send_headers(&mut writer, 3, true, HEALTH_HEADERS).await;
    let mut health_headers_seen = false;
    while !health_headers_seen {
        let (kind, _, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read frame");
        health_headers_seen = kind == 0x01 && id == 3;
    }
    assert!(
        read_until_stream_data(&mut reader, 1).await,
        "the first SSE remains active during a complete request"
    );

    // A legal HEADERS frame whose block is explicitly unfinished. Do not send
    // CONTINUATION. The fragment begins with a valid HPACK indexed :method
    // field.
    send_headers(&mut writer, 5, false, b"\x82").await;
    let advanced_while_incomplete = read_until_stream_data(&mut reader, 1).await;
    eprintln!("SSE_DATA_WHILE_SECOND_HEADER_BLOCK_INCOMPLETE={advanced_while_incomplete}");

    // Completing the header block allows the connection parser to resume.
    writer
        .write_all(&frame(0x09, 0x04, 5, b"\x86\x84\x01\x09localhost"))
        .await
        .unwrap();
    let _ = time::timeout(Duration::from_secs(1), read_frame(&mut reader)).await;
    drop(writer);
    drop(reader);

    // Closing the only transport releases its server-side connection permit.
    let mut followup = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .unwrap()
        .expect("transport capacity should be available");
    followup
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(3), followup.read_to_end(&mut response))
        .await
        .unwrap()
        .expect("follow-up response should finish");
    assert!(
        response.starts_with(b"HTTP/1.1 200"),
        "capacity follow-up failed: {:?}",
        String::from_utf8_lossy(&response)
    );

    finish_server(shutdown, serving).await;
}

#[tokio::test]
async fn incomplete_h2_header_block_on_idle_connection_waits_for_continuation() {
    let (address, shutdown, serving) = start_server().await;
    let (mut reader, mut writer) = begin_h2(address).await;

    // Complete and drain one finite response so no stream remains active.
    send_headers(&mut writer, 1, true, HEALTH_HEADERS).await;
    let mut response_ended = false;
    while !response_ended {
        let (_, flags, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read idle-control response frame");
        if id == 1 && flags & 0x01 != 0 {
            response_ended = true;
        }
    }

    // With the connection otherwise idle, an incomplete stream produces no response
    // frame.
    send_headers(&mut writer, 3, false, b"\x82").await;
    let idle_observation = time::timeout(Duration::from_millis(300), read_frame(&mut reader)).await;
    assert!(
        idle_observation.is_err(),
        "an unfinished header block should wait for CONTINUATION"
    );

    // Completing the same valid HPACK header block lets the server produce its
    // response.
    writer
        .write_all(&frame(0x09, 0x04, 3, b"\x86\x84\x01\x09localhost"))
        .await
        .unwrap();
    let mut response_ended = false;
    while !response_ended {
        let (_, flags, id, _) = time::timeout(Duration::from_secs(3), read_frame(&mut reader))
            .await
            .unwrap()
            .expect("read completed idle-control response frame");
        if id == 3 && flags & 0x01 != 0 {
            response_ended = true;
        }
    }

    drop(writer);
    drop(reader);
    finish_server(shutdown, serving).await;
}
