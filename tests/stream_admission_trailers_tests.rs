// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::HeaderMap;
use axum::http::Request;
use axum::http::StatusCode;
use axum::routing::post;
use futures_util::FutureExt;
use http_body_util::BodyExt;
use hyper::body::Body as HttpBody;
use qubit_web::ServerOptions;
use qubit_web::ShutdownReport;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
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
use tower::ServiceExt;

type ServingTask = JoinHandle<Result<ShutdownReport, qubit_web::WebServerError>>;

const CASE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_TRAILERS: usize = 1024 * 1024;

fn assert_within_trailer_experiment_limit(bytes: usize) {
    assert!(bytes < MAX_TRAILERS);
}

struct TrailerFrames {
    frames: std::vec::IntoIter<hyper::body::Frame<Bytes>>,
}

impl TrailerFrames {
    fn new(frames: Vec<hyper::body::Frame<Bytes>>) -> Self {
        Self {
            frames: frames.into_iter(),
        }
    }
}

impl HttpBody for TrailerFrames {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(self.frames.next().map(Ok))
    }
}

fn trailer(name: &'static str, value: String) -> hyper::body::Frame<Bytes> {
    let mut headers = HeaderMap::new();
    headers.append(name, value.parse().expect("valid header value"));
    hyper::body::Frame::trailers(headers)
}

fn valid_trailer_body() -> Body {
    let mut duplicates = HeaderMap::new();
    duplicates.append("x-repeat", "one".parse().unwrap());
    duplicates.append("x-repeat", "two".parse().unwrap());
    Body::new(TrailerFrames::new(vec![
        hyper::body::Frame::trailers(duplicates),
        trailer("x-final", "kept".to_owned()),
    ]))
}

async fn admission_router(calls: Arc<AtomicUsize>, limits: StreamAdmissionLimits) -> Router {
    Router::new()
        .route(
            "/",
            post(move |request: Request<Body>| {
                let calls = Arc::clone(&calls);
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let collected = request.into_body().collect().await.expect("replayed body");
                    let trailers = collected.trailers().cloned().unwrap_or_default();
                    let duplicates = trailers.get_all("x-repeat");
                    assert_eq!(duplicates.iter().count(), 2);
                    assert_eq!(trailers["x-final"], "kept");
                    assert!(collected.to_bytes().is_empty());
                    StatusCode::OK
                }
            }),
        )
        .layer(StreamAdmissionLayer::new(limits))
}

#[tokio::test]
async fn test_trailers_without_data_accumulate_and_do_not_consume_data_budget() {
    let limits = StreamAdmissionLimits::default()
        .with_max_body_bytes(1)
        .unwrap()
        .with_max_concurrent_admissions(8)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let router = admission_router(calls.clone(), limits).await;
    let mut first = HeaderMap::new();
    first.append("x-repeat", "one".parse().unwrap());
    first.append("x-repeat", "two".parse().unwrap());
    let first = hyper::body::Frame::trailers(first);
    let second = trailer("x-final", "kept".to_owned());

    let body = Body::new(TrailerFrames::new(vec![first, second]));
    let response = time::timeout(CASE_TIMEOUT, router.oneshot(Request::post("/").body(body).unwrap()))
        .await
        .expect("trailers-only case deadline")
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_eight_bounded_trailer_bodies_are_admitted_concurrently() {
    let limits = StreamAdmissionLimits::default()
        .with_max_body_bytes(1)
        .unwrap()
        .with_max_concurrent_admissions(8)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let router = admission_router(calls.clone(), limits).await;
    let each = 64 * 1024;
    assert_within_trailer_experiment_limit(each * 8);
    let mut requests = Vec::new();
    for _ in 0..8 {
        let mut repeated = HeaderMap::new();
        repeated.append("x-repeat", "a".repeat(each / 2).parse().unwrap());
        repeated.append("x-repeat", "b".repeat(each / 2).parse().unwrap());
        let frames = vec![
            hyper::body::Frame::trailers(repeated),
            trailer("x-final", "kept".to_owned()),
        ];
        requests.push(
            router
                .clone()
                .oneshot(Request::post("/").body(Body::new(TrailerFrames::new(frames))).unwrap()),
        );
    }
    let responses = time::timeout(CASE_TIMEOUT, futures_util::future::join_all(requests))
        .await
        .expect("eight bounded cases deadline");
    for response in responses {
        assert_eq!(response.unwrap().status(), StatusCode::OK);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 8);
}

struct ErrorBody;

impl HttpBody for ErrorBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        Poll::Ready(Some(Err(std::io::Error::other("controlled body error"))))
    }
}

struct PendingBody(Arc<Notify>);

impl HttpBody for PendingBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
        self.0.notify_one();
        Poll::Pending
    }
}

#[tokio::test]
async fn test_body_error_and_cancellation_release_admission_permits() {
    let limits = StreamAdmissionLimits::default()
        .with_max_concurrent_admissions(1)
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let router = admission_router(calls, limits).await;
    let errored = router
        .clone()
        .oneshot(Request::post("/").body(Body::new(ErrorBody)).unwrap());
    let response = time::timeout(CASE_TIMEOUT, errored)
        .await
        .expect("error cleanup deadline")
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let polled = Arc::new(Notify::new());
    let pending = router.clone();
    let body_polled = Arc::clone(&polled);
    let request = tokio::spawn(async move {
        pending
            .oneshot(Request::post("/").body(Body::new(PendingBody(body_polled))).unwrap())
            .await
    });
    time::timeout(CASE_TIMEOUT, polled.notified())
        .await
        .expect("pending body should be polled");
    request.abort();
    let _ = request.await;

    let recovered = time::timeout(
        CASE_TIMEOUT,
        router.oneshot(Request::post("/").body(valid_trailer_body()).unwrap()),
    )
    .await
    .expect("permit recovery after cancellation")
    .unwrap();
    assert_eq!(recovered.status(), StatusCode::OK);
}

async fn start_server(
    options: ServerOptions,
    calls: Arc<AtomicUsize>,
    observed_trailer_bytes: Arc<AtomicUsize>,
) -> (SocketAddr, ServerGuard) {
    let app = Router::new()
        .route(
            "/",
            post(move |request: Request<Body>| {
                let calls = Arc::clone(&calls);
                let observed_trailer_bytes = Arc::clone(&observed_trailer_bytes);
                async move {
                    let collected = request.into_body().collect().await.expect("body should be valid");
                    let trailer_bytes = collected
                        .trailers()
                        .and_then(|trailers| trailers.get("x-pad"))
                        .map_or(0, |value| value.as_bytes().len());
                    observed_trailer_bytes.store(trailer_bytes, Ordering::SeqCst);
                    calls.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        )
        .layer(StreamAdmissionLayer::new(StreamAdmissionLimits::default()));
    let server = WebServer::bind_http(options).await.expect("bind server");
    let address = server.local_addr();
    let (shutdown, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    (
        address,
        ServerGuard {
            shutdown: Some(shutdown),
            serving: Some(serving),
        },
    )
}

struct ServerGuard {
    shutdown: Option<oneshot::Sender<()>>,
    serving: Option<ServingTask>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(serving) = self.serving.take() {
            serving.abort();
        }
    }
}

const H2_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

fn h2_frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let length = payload.len();
    let mut frame = vec![
        ((length >> 16) & 0xff) as u8,
        ((length >> 8) & 0xff) as u8,
        (length & 0xff) as u8,
        kind,
        flags,
    ];
    frame.extend_from_slice(&(stream_id & 0x7fff_ffff).to_be_bytes());
    frame.extend_from_slice(payload);
    frame
}

async fn read_h2_frame(reader: &mut OwnedReadHalf) -> std::io::Result<(u8, u8, u32, Vec<u8>)> {
    let mut header = [0; 9];
    reader.read_exact(&mut header).await?;
    let length = (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let stream_id = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    Ok((header[3], header[4], stream_id, payload))
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

async fn connect_h2(address: SocketAddr) -> (OwnedReadHalf, OwnedWriteHalf) {
    let tcp = time::timeout(CASE_TIMEOUT, TcpStream::connect(address))
        .await
        .expect("TCP connection deadline")
        .expect("TCP connect");
    let (mut reader, mut writer) = tcp.into_split();
    writer.write_all(H2_PREFACE).await.expect("write HTTP/2 preface");
    writer
        .write_all(&h2_frame(0x04, 0, 0, &[]))
        .await
        .expect("write client SETTINGS");
    let settings = time::timeout(CASE_TIMEOUT, read_h2_frame(&mut reader))
        .await
        .expect("server SETTINGS deadline")
        .expect("read server SETTINGS");
    assert_eq!(settings.0, 0x04, "expected server SETTINGS");
    writer
        .write_all(&h2_frame(0x04, 0x01, 0, &[]))
        .await
        .expect("acknowledge server SETTINGS");
    (reader, writer)
}

async fn send_trailer_request(writer: &mut OwnedWriteHalf, value_bytes: usize) {
    // HPACK: :method POST, :scheme http, :path /, :authority localhost,
    // trailer: x-pad.
    let mut request_headers = b"\x83\x86\x84\x01\x09localhost\x00".to_vec();
    append_hpack_string(&mut request_headers, b"trailer");
    append_hpack_string(&mut request_headers, b"x-pad");
    writer
        .write_all(&h2_frame(0x01, 0x04, 1, &request_headers))
        .await
        .expect("write request headers");
    writer
        .write_all(&h2_frame(0x00, 0, 1, b"x"))
        .await
        .expect("write request DATA");
    let mut trailer_block = vec![0x00]; // Literal header without indexing.
    append_hpack_string(&mut trailer_block, b"x-pad");
    append_hpack_string(&mut trailer_block, &vec![b'z'; value_bytes]);
    if trailer_block.len() <= 16_384 {
        writer
            .write_all(&h2_frame(0x01, 0x05, 1, &trailer_block))
            .await
            .expect("write END_STREAM trailers");
    } else {
        writer
            .write_all(&h2_frame(0x01, 0x01, 1, &trailer_block[..16_384]))
            .await
            .expect("write initial trailer HEADERS fragment");
        for (index, fragment) in trailer_block[16_384..].chunks(16_384).enumerate() {
            let is_last = 16_384 + (index + 1) * 16_384 >= trailer_block.len();
            writer
                .write_all(&h2_frame(0x09, if is_last { 0x04 } else { 0 }, 1, fragment))
                .await
                .expect("write CONTINUATION trailer fragment");
        }
    }
}

async fn read_response_body(reader: &mut OwnedReadHalf) -> Vec<u8> {
    let mut body = Vec::new();
    time::timeout(CASE_TIMEOUT, async {
        loop {
            let (kind, flags, stream_id, payload) = read_h2_frame(reader).await.expect("read response frame");
            if stream_id != 1 {
                continue;
            }
            if kind == 0x00 {
                body.extend_from_slice(&payload);
                if flags & 0x01 != 0 {
                    return;
                }
            } else if kind == 0x03 {
                return;
            }
        }
    })
    .await
    .expect("HTTP/2 response deadline");
    body
}

async fn send_http1_chunked_trailers(address: SocketAddr, value_bytes: usize) -> Vec<u8> {
    let mut stream = time::timeout(CASE_TIMEOUT, TcpStream::connect(address))
        .await
        .expect("HTTP/1 connect deadline")
        .expect("HTTP/1 connect");
    let request = format!(
        "POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nTrailer: x-pad\r\nConnection: close\r\n\r\n1\r\nx\r\n0\r\nx-pad: {}\r\n\r\n",
        "h".repeat(value_bytes),
    );
    time::timeout(CASE_TIMEOUT, async {
        stream
            .write_all(request.as_bytes())
            .await
            .expect("write chunked request");
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.expect("read HTTP/1 response");
        response
    })
    .await
    .expect("HTTP/1 request/response deadline")
}

async fn finish_server(mut guard: ServerGuard) {
    if let Some(shutdown) = guard.shutdown.take() {
        let _ = shutdown.send(());
    }
    let mut serving = guard.serving.take().expect("server task is present");
    let joined = time::timeout(CASE_TIMEOUT, &mut serving).await;
    let result = match joined {
        Ok(joined) => joined.expect("server task").expect("server shutdown"),
        Err(_) => {
            serving.abort();
            let _ = serving.await;
            panic!("server stop deadline");
        }
    };
    assert!(result.graceful);
}

async fn finish_after<T>(server: ServerGuard, outcome: Result<T, Box<dyn std::any::Any + Send>>) -> T {
    finish_server(server).await;
    match outcome {
        Ok(value) => value,
        Err(panic) => resume_unwind(panic),
    }
}

#[tokio::test]
async fn test_h2_default_and_explicit_16k_trailer_header_budgets() {
    const TRAILER_VALUE_BYTES: usize = 8 * 1024;
    assert_within_trailer_experiment_limit(TRAILER_VALUE_BYTES);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let (address, server) = start_server(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()),
        calls.clone(),
        observed.clone(),
    )
    .await;
    let outcome = AssertUnwindSafe(async {
        let (mut reader, mut writer) = connect_h2(address).await;
        send_trailer_request(&mut writer, TRAILER_VALUE_BYTES).await;
        let response_body = read_response_body(&mut reader).await;
        assert_eq!(response_body, b"ok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), TRAILER_VALUE_BYTES);
        drop(writer);
        drop(reader);
    })
    .catch_unwind()
    .await;
    finish_after(server, outcome).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http2_max_header_list_bytes(16 * 1024)
        .unwrap();
    let (address, server) = start_server(options.clone(), calls.clone(), observed.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let (mut legal_reader, mut legal_writer) = connect_h2(address).await;
        send_trailer_request(&mut legal_writer, TRAILER_VALUE_BYTES).await;
        assert_eq!(read_response_body(&mut legal_reader).await, b"ok");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), TRAILER_VALUE_BYTES);
        drop(legal_writer);
        drop(legal_reader);

        let (mut reader, mut writer) = connect_h2(address).await;
        observed.store(0, Ordering::SeqCst);
        send_trailer_request(&mut writer, 20 * 1024).await;
        let response_body = read_response_body(&mut reader).await;
        let calls_after_over_budget = calls.load(Ordering::SeqCst);
        let trailer_bytes_after_over_budget = observed.load(Ordering::SeqCst);

        drop(writer);
        drop(reader);
        let (mut recovered_reader, mut recovered_writer) = connect_h2(address).await;
        send_trailer_request(&mut recovered_writer, 32).await;
        let recovered_body = read_response_body(&mut recovered_reader).await;
        assert_eq!(recovered_body, b"ok");
        assert_eq!(calls.load(Ordering::SeqCst), calls_after_over_budget + 1);
        assert_eq!(observed.load(Ordering::SeqCst), 32);
        drop(recovered_writer);
        drop(recovered_reader);
        assert_eq!(
            calls_after_over_budget, 2,
            "h2 passed the over-limit trailer frame to the handler"
        );
        assert_eq!(
            response_body, b"ok",
            "the over-limit request returned application success"
        );
        assert_eq!(
            trailer_bytes_after_over_budget, 0,
            "over-limit trailer fields are absent from decoded trailers"
        );
    })
    .catch_unwind()
    .await;
    finish_after(server, outcome).await;
}

#[tokio::test]
async fn test_http1_default_and_explicit_16k_chunked_trailers() {
    const LEGAL_TRAILER_BYTES: usize = 8 * 1024;
    assert_within_trailer_experiment_limit(LEGAL_TRAILER_BYTES);
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let (address, server) = start_server(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()),
        calls.clone(),
        observed.clone(),
    )
    .await;
    let outcome = AssertUnwindSafe(async {
        let response = send_http1_chunked_trailers(address, LEGAL_TRAILER_BYTES).await;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "legal default trailers reached the app: {response:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), LEGAL_TRAILER_BYTES);
    })
    .catch_unwind()
    .await;
    finish_after(server, outcome).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http1_max_buffer_bytes(16 * 1024)
        .unwrap();
    let (address, server) = start_server(options.clone(), calls.clone(), observed.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let response = send_http1_chunked_trailers(address, LEGAL_TRAILER_BYTES).await;
        assert!(
            response.starts_with(b"HTTP/1.1 200"),
            "legal explicit-buffer trailers reached the app: {response:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), LEGAL_TRAILER_BYTES);
    })
    .catch_unwind()
    .await;
    finish_after(server, outcome).await;

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let (address, server) = start_server(options, calls.clone(), observed.clone()).await;
    let outcome = AssertUnwindSafe(async {
        let response = send_http1_chunked_trailers(address, 20 * 1024).await;
        let handler_entered = calls.load(Ordering::SeqCst);
        assert_eq!(handler_entered, 0, "oversized HTTP/1 trailer block reached the handler");
        assert!(
            response.starts_with(b"HTTP/1.1 400 Bad Request"),
            "expected the observed body-read rejection: {response:?}"
        );
        assert!(
            response
                .windows(b"body_read_failed".len())
                .any(|window| window == b"body_read_failed")
        );
    })
    .catch_unwind()
    .await;
    finish_after(server, outcome).await;
}
