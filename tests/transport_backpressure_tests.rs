// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Explicit, bounded HTTP/2 response backpressure diagnostic.
//!
//! This is ignored because it depends on network scheduling and Hyper's
//! transport polling. Run with `cargo test --locked --test
//! transport_backpressure_tests -- --ignored --nocapture`.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::response::sse::Event;
use axum::response::sse::Sse;
use axum::routing::get;
use futures_util::stream;
use hyper::body::Bytes;
use qubit_web::HttpLimits;
use qubit_web::RequestLimitLayer;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;
use tokio::time::timeout;

const PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const HEALTH_HEADERS: &[u8] = b"\x82\x86\x04\x07/health\x01\x09localhost";
const STREAM_HEADERS: &[u8] = b"\x82\x86\x04\x06/large\x01\x09localhost";
const SMALL_HEADERS: &[u8] = b"\x82\x86\x04\x06/small\x01\x09localhost";
const EVENTS_HEADERS: &[u8] = b"\x82\x86\x04\x07/events\x01\x09localhost";
const MAX_DATA: usize = 1024 * 1024;
const MAX_WAIT: Duration = Duration::from_secs(3);
const MAX_TOTAL: Duration = Duration::from_secs(10);

type Serving = JoinHandle<Result<qubit_web::ShutdownReport, qubit_web::WebServerError>>;

struct RunningServer {
    stop: Option<oneshot::Sender<()>>,
    serving: Option<Serving>,
}

impl RunningServer {
    async fn shutdown(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let serving = self.serving.take().expect("server task is owned");
        let report = timeout(MAX_WAIT, serving)
            .await
            .expect("server shutdown deadline")
            .expect("server task join")
            .expect("server shutdown");
        assert!(report.graceful);
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        // If a bounded experiment assertion or outer timeout exits early, do
        // not leave detached server work behind.
        self.stop.take();
        if let Some(serving) = &self.serving {
            serving.abort();
        }
    }
}

struct BodySignals {
    created: Notify,
    dropped: Notify,
    polls: AtomicUsize,
    bytes: AtomicUsize,
    sse_events: AtomicUsize,
}

impl BodySignals {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            created: Notify::new(),
            dropped: Notify::new(),
            polls: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            sse_events: AtomicUsize::new(0),
        })
    }
}

struct BodyDrop(Arc<BodySignals>);

impl Drop for BodyDrop {
    fn drop(&mut self) {
        self.0.dropped.notify_one();
    }
}

async fn start_server(signals: Arc<BodySignals>) -> (SocketAddr, RunningServer) {
    let limits = HttpLimits::default()
        .with_max_concurrent_requests(1)
        .unwrap()
        .with_request_timeout(Duration::from_millis(200))
        .unwrap();
    let limited = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/small", get(|| async { "small" }))
        .route(
            "/large",
            get({
                let signals = signals.clone();
                move || {
                    let signals = signals.clone();
                    async move {
                        signals.created.notify_one();
                        let guard = BodyDrop(signals.clone());
                        let source = stream::unfold((0usize, guard), move |(index, guard)| {
                            let signals = signals.clone();
                            async move {
                                if index >= MAX_DATA {
                                    return None;
                                }
                                signals.polls.fetch_add(1, Ordering::SeqCst);
                                let count = (MAX_DATA - index).min(1024);
                                signals.bytes.fetch_add(count, Ordering::SeqCst);
                                Some((
                                    Ok::<_, io::Error>(Bytes::from(vec![b'x'; count])),
                                    (index + count, guard),
                                ))
                            }
                        });
                        Body::from_stream(source)
                    }
                }
            }),
        )
        .layer(RequestLimitLayer::new(limits));
    let events = Router::new().route(
        "/events",
        get({
            let signals = signals.clone();
            move || {
                let signals = signals.clone();
                async move {
                    let events = stream::unfold(0usize, move |index| {
                        let signals = signals.clone();
                        async move {
                            time::sleep(Duration::from_millis(30)).await;
                            signals.sse_events.fetch_add(1, Ordering::SeqCst);
                            Some((
                                Ok::<_, std::convert::Infallible>(Event::default().data(format!("tick-{index}"))),
                                index + 1,
                            ))
                        }
                    });
                    Sse::new(events)
                }
            }
        }),
    );
    let app = Router::new().merge(limited).merge(events);
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap())
            .with_max_transport_connections(1)
            .unwrap()
            .with_transport_idle_timeout(Duration::from_millis(300)),
    )
    .await
    .expect("bind experiment server");
    let address = server.local_addr();
    let (stop, stopped) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = stopped.await;
    }));
    (
        address,
        RunningServer {
            stop: Some(stop),
            serving: Some(serving),
        },
    )
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
    let id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let mut payload = vec![0; len];
    reader.read_exact(&mut payload).await?;
    Ok((header[3], header[4], id, payload))
}

async fn connect_h2(address: SocketAddr, zero_window: bool) -> (OwnedReadHalf, OwnedWriteHalf) {
    let tcp = timeout(MAX_WAIT, TcpStream::connect(address)).await.unwrap().unwrap();
    let (mut reader, mut writer) = tcp.into_split();
    writer.write_all(PREFACE).await.unwrap();
    let mut settings = Vec::new();
    if zero_window {
        settings.extend_from_slice(&4_u16.to_be_bytes());
        settings.extend_from_slice(&0_u32.to_be_bytes());
    }
    writer.write_all(&frame(0x04, 0, 0, &settings)).await.unwrap();
    let deadline = Instant::now() + MAX_WAIT;
    loop {
        let (kind, flags, _stream, _payload) = timeout(
            deadline.saturating_duration_since(Instant::now()),
            read_frame(&mut reader),
        )
        .await
        .expect("HTTP/2 settings deadline")
        .expect("read HTTP/2 settings");
        if kind == 0x04 && flags & 0x01 == 0 {
            writer.write_all(&frame(0x04, 0x01, 0, &[])).await.unwrap();
            break;
        }
    }
    let ack_deadline = Instant::now() + MAX_WAIT;
    loop {
        let (kind, flags, _, _) = timeout(
            ack_deadline.saturating_duration_since(Instant::now()),
            read_frame(&mut reader),
        )
        .await
        .expect("client SETTINGS acknowledgement deadline")
        .expect("read client SETTINGS acknowledgement");
        if kind == 0x04 && flags & 0x01 != 0 {
            break;
        }
    }
    (reader, writer)
}

async fn await_response_headers(
    reader: &mut OwnedReadHalf,
    writer: &mut OwnedWriteHalf,
    stream_id: u32,
    deadline: Instant,
) -> bool {
    loop {
        let Ok(Ok((kind, flags, id, _))) =
            timeout(deadline.saturating_duration_since(Instant::now()), read_frame(reader)).await
        else {
            return false;
        };
        if kind == 0x04 && flags & 0x01 == 0 && writer.write_all(&frame(0x04, 0x01, 0, &[])).await.is_err() {
            return false;
        }
        if kind == 0x01 && id == stream_id {
            return true;
        }
        if kind == 0x03 || kind == 0x07 {
            return false;
        }
    }
}

async fn start_health_probe(address: SocketAddr) -> io::Result<(OwnedReadHalf, OwnedWriteHalf)> {
    let tcp = timeout(MAX_WAIT, TcpStream::connect(address))
        .await
        .map_err(io::Error::other)??;
    let (mut reader, mut writer) = tcp.into_split();
    writer.write_all(PREFACE).await?;
    writer.write_all(&frame(0x04, 0, 0, &[])).await?;
    send_request(&mut writer, 1, HEALTH_HEADERS).await;
    // Do not wait for server SETTINGS here: with transport capacity 1, this
    // TCP connection may be accepted but not admitted until the first one ends.
    let _ = &mut reader;
    Ok((reader, writer))
}

async fn send_request(writer: &mut OwnedWriteHalf, stream_id: u32, headers: &[u8]) {
    writer.write_all(&frame(0x01, 0x05, stream_id, headers)).await.unwrap(); // END_HEADERS | END_STREAM
}

async fn read_to_end_stream(reader: &mut OwnedReadHalf, stream_id: u32, deadline: Instant) -> Option<usize> {
    let mut data_bytes = 0;
    loop {
        let Ok(Ok((kind, flags, id, payload))) =
            timeout(deadline.saturating_duration_since(Instant::now()), read_frame(reader)).await
        else {
            return None;
        };
        if kind == 0x00 && id == stream_id {
            data_bytes += payload.len();
            if flags & 0x01 != 0 {
                return Some(data_bytes);
            }
        }
        if kind == 0x03 && id == stream_id {
            return None;
        }
    }
}

#[tokio::test]
#[ignore = "bounded transport diagnostic; explicitly run to investigate response backpressure"]
async fn test_zero_window_timeout_and_reset_recover_transport_capacity() {
    timeout(MAX_TOTAL, zero_window_timeout_and_reset_case())
        .await
        .expect("whole zero-window diagnostic must finish within 10 seconds");
}

async fn zero_window_timeout_and_reset_case() {
    let started = Instant::now();
    let total_deadline = started + MAX_TOTAL;
    let signals = BodySignals::new();
    let (address, server) = start_server(signals.clone()).await;
    let mut first = Some(connect_h2(address, true).await);
    let (reader, writer) = first.as_mut().unwrap();
    eprintln!(
        "t+{:?}: server acknowledged zero-window SETTINGS; settings handshake complete",
        started.elapsed()
    );
    send_request(writer, 1, STREAM_HEADERS).await;
    timeout(MAX_WAIT, signals.created.notified())
        .await
        .expect("body creation signal");
    eprintln!("t+{:?}: application response body created", started.elapsed());
    assert!(
        await_response_headers(reader, writer, 1, Instant::now() + MAX_WAIT).await,
        "response headers arrive before body data"
    );
    eprintln!("t+{:?}: response headers reached client", started.elapsed());

    // No stream credit has been granted. A transport DATA frame is forbidden
    // until the later WINDOW_UPDATE. Observe the socket briefly after headers.
    let mut data_before_resume = false;
    let no_data_deadline = Instant::now() + Duration::from_millis(100);
    while Instant::now() < no_data_deadline {
        let left = no_data_deadline.saturating_duration_since(Instant::now());
        match timeout(left, read_frame(reader)).await {
            Ok(Ok((kind, _, id, _))) if kind == 0x00 && id == 1 => data_before_resume = true,
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(!data_before_resume, "zero stream window must withhold response DATA");
    eprintln!(
        "t+{:?}: no DATA received during 100ms zero-window observation",
        started.elapsed()
    );

    // Require evidence that production was polled while sending zero stream
    // credit, and cap bytes to the declared 1 MiB maximum.
    timeout(MAX_WAIT, async {
        while signals.polls.load(Ordering::SeqCst) == 0 {
            time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("body was polled under zero-window backpressure");
    eprintln!(
        "t+{:?}: body polled; produced bytes={}",
        started.elapsed(),
        signals.bytes.load(Ordering::SeqCst)
    );
    assert!(signals.bytes.load(Ordering::SeqCst) <= MAX_DATA);
    timeout(MAX_WAIT, signals.dropped.notified())
        .await
        .expect("request deadline drops producer");
    eprintln!(
        "t+{:?}: request deadline dropped application producer",
        started.elapsed()
    );

    // A successful TCP connect is not admission evidence. Only a health
    // response proves that the single transport permit has become available.
    let (mut health_reader, mut health_writer) = start_health_probe(address).await.unwrap();
    let health_before_reset = await_response_headers(
        &mut health_reader,
        &mut health_writer,
        1,
        Instant::now() + Duration::from_millis(300),
    )
    .await;
    eprintln!(
        "t+{:?}: second-connection health response before reset={health_before_reset}",
        started.elapsed()
    );
    send_request(writer, 3, HEALTH_HEADERS).await;
    let same_connection_health = await_response_headers(reader, writer, 3, Instant::now() + MAX_WAIT).await;
    eprintln!(
        "t+{:?}: same-connection health handler response={same_connection_health}",
        started.elapsed()
    );
    assert!(
        same_connection_health,
        "short-request permit should recover after the application deadline"
    );
    writer
        .write_all(&frame(0x08, 0, 3, &65_535_u32.to_be_bytes()))
        .await
        .unwrap();
    let same_connection_bytes = read_to_end_stream(reader, 3, Instant::now() + MAX_WAIT)
        .await
        .expect("same-connection health body should complete after WINDOW_UPDATE");
    assert_eq!(same_connection_bytes, 2);

    // Keep the TCP connection open after RST_STREAM. The second connection's
    // health response proves reset releases stream activity and idle cleanup
    // returns the transport permit; TCP connect alone is not admission proof.
    if let Some((_, writer)) = first.as_mut() {
        writer
            .write_all(&frame(0x03, 0, 1, &0_u32.to_be_bytes()))
            .await
            .unwrap();
    }
    eprintln!(
        "t+{:?}: client sent RST_STREAM while original connection stays open",
        started.elapsed()
    );
    let recovered = await_response_headers(&mut health_reader, &mut health_writer, 1, Instant::now() + MAX_WAIT).await;
    eprintln!("t+{:?}: health response after reset={recovered}", started.elapsed());
    first.take();

    server.shutdown().await;
    assert!(Instant::now() <= total_deadline, "whole experiment exceeded 10 seconds");
    eprintln!(
        "body polls={} produced_bytes={} health_before_reset={} health_after_reset={recovered}",
        signals.polls.load(Ordering::SeqCst),
        signals.bytes.load(Ordering::SeqCst),
        health_before_reset,
    );
    assert!(
        recovered,
        "transport capacity should recover after stream reset/connection drop"
    );
}

#[tokio::test]
#[ignore = "bounded transport diagnostic; normal consumption and WINDOW_UPDATE control"]
async fn test_consuming_or_resuming_window_releases_short_and_transport_capacity() {
    timeout(MAX_TOTAL, consuming_or_resuming_case())
        .await
        .expect("whole consumption/resume diagnostic must finish within 10 seconds");
}

async fn consuming_or_resuming_case() {
    let started = Instant::now();
    let signals = BodySignals::new();
    let (address, server) = start_server(signals.clone()).await;
    let (mut reader, mut writer) = connect_h2(address, false).await;
    send_request(&mut writer, 1, SMALL_HEADERS).await;
    assert!(await_response_headers(&mut reader, &mut writer, 1, Instant::now() + MAX_WAIT).await);
    let normal_bytes = read_to_end_stream(&mut reader, 1, Instant::now() + MAX_WAIT)
        .await
        .expect("normal response body reaches END_STREAM");
    assert_eq!(normal_bytes, 5, "normal-consumption control should deliver `small`");

    // A second stream advertises zero credit, then explicitly resumes it.
    drop((reader, writer));
    let (mut reader, mut writer) = connect_h2(address, true).await;
    send_request(&mut writer, 1, STREAM_HEADERS).await;
    timeout(MAX_WAIT, signals.created.notified())
        .await
        .expect("large body created");
    assert!(await_response_headers(&mut reader, &mut writer, 1, Instant::now() + MAX_WAIT).await);
    assert!(
        signals.polls.load(Ordering::SeqCst) > 0,
        "server body must be polled before resume"
    );
    let mut window_update = Vec::with_capacity(4);
    window_update.extend_from_slice(&(MAX_DATA as u32).to_be_bytes());
    writer.write_all(&frame(0x08, 0, 0, &window_update)).await.unwrap();
    writer.write_all(&frame(0x08, 0, 1, &window_update)).await.unwrap();
    let resumed_bytes = read_to_end_stream(&mut reader, 1, Instant::now() + MAX_WAIT)
        .await
        .expect("resumed response reaches END_STREAM");
    assert_eq!(resumed_bytes, MAX_DATA);
    timeout(MAX_WAIT, signals.dropped.notified())
        .await
        .expect("resumed body is destroyed");
    assert!(
        Instant::now().duration_since(started) < MAX_TOTAL,
        "combined controls exceed 10 seconds"
    );
    drop((reader, writer));
    server.shutdown().await;
}

#[tokio::test]
#[ignore = "bounded transport diagnostic; active SSE multiplexing and peer-disconnect control"]
async fn test_active_sse_progresses_with_short_request_and_disconnect_releases_capacity() {
    timeout(MAX_TOTAL, active_sse_and_disconnect_case())
        .await
        .expect("whole SSE/disconnect diagnostic must finish within 10 seconds");
}

async fn active_sse_and_disconnect_case() {
    let started = Instant::now();
    let signals = BodySignals::new();
    let (address, server) = start_server(signals.clone()).await;
    let (mut reader, mut writer) = connect_h2(address, false).await;
    send_request(&mut writer, 1, EVENTS_HEADERS).await;
    assert!(await_response_headers(&mut reader, &mut writer, 1, Instant::now() + MAX_WAIT).await);
    send_request(&mut writer, 3, HEALTH_HEADERS).await;
    assert!(await_response_headers(&mut reader, &mut writer, 3, Instant::now() + MAX_WAIT).await);

    let mut sse_data = 0;
    let mut short_data = 0;
    let mut short_ended = false;
    let mut sse_advanced_after_short = false;
    let data_deadline = Instant::now() + MAX_WAIT;
    while (!short_ended || !sse_advanced_after_short) && Instant::now() < data_deadline {
        let Ok(Ok((kind, flags, id, payload))) = timeout(
            data_deadline.saturating_duration_since(Instant::now()),
            read_frame(&mut reader),
        )
        .await
        else {
            break;
        };
        if kind == 0x00 && !payload.is_empty() {
            if id == 1 {
                sse_data += payload.len();
                if short_ended {
                    sse_advanced_after_short = true;
                }
            } else if id == 3 {
                short_data += payload.len();
                if flags & 0x01 != 0 {
                    short_ended = true;
                }
            }
        }
    }
    assert!(sse_data > 0, "active SSE must advance on the multiplexed connection");
    assert!(
        short_data > 0 && short_ended,
        "short response must complete alongside SSE"
    );
    assert!(
        sse_advanced_after_short,
        "SSE should progress after the short response completes"
    );
    time::sleep(Duration::from_millis(250)).await;
    let sse_after_short_deadline = timeout(MAX_WAIT, async {
        loop {
            let (kind, _, id, payload) = read_frame(&mut reader).await?;
            if kind == 0x00 && id == 1 && !payload.is_empty() {
                return io::Result::Ok(payload.len());
            }
        }
    })
    .await
    .expect("SSE should remain active beyond the short-request deadline")
    .expect("read SSE data after short-request deadline");
    assert!(sse_after_short_deadline > 0);
    assert!(signals.sse_events.load(Ordering::SeqCst) > 0);

    // Closing the connection, without a stream reset, must release its
    // transport permit so a health handler on the next connection can enter.
    drop((reader, writer));
    let (mut next_reader, mut next_writer) = start_health_probe(address).await.unwrap();
    let admitted = await_response_headers(&mut next_reader, &mut next_writer, 1, Instant::now() + MAX_WAIT).await;
    assert!(admitted, "health handler should enter after peer disconnect");
    assert!(
        Instant::now().duration_since(started) < MAX_TOTAL,
        "SSE/disconnect control exceeds 10 seconds"
    );

    drop((next_reader, next_writer));
    server.shutdown().await;
}
