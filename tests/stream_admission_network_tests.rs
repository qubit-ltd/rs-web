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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware;
use axum::middleware::Next;
use axum::response::Response;
use axum::routing::post;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::stream;
use http_body_util::BodyExt;
use hyper::Request as HyperRequest;
use hyper::body::Bytes;
use hyper::client::conn::http2;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use qubit_web::ServerOptions;
use qubit_web::StreamAdmissionLayer;
use qubit_web::StreamAdmissionLimits;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;

const DEADLINE: Duration = Duration::from_secs(5);
const PAYLOAD_SIZE: usize = 64 * 1024;
type Serving = JoinHandle<Result<qubit_web::ShutdownReport, qubit_web::WebServerError>>;

async fn start_server(app: Router) -> (SocketAddr, oneshot::Sender<()>, Serving) {
    let server = time::timeout(
        DEADLINE,
        WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap())),
    )
    .await
    .unwrap()
    .expect("bind server");
    let address = server.local_addr();
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve(app, async move {
        let _ = stopped.await;
    }));
    (address, stop, task)
}

async fn stop_server(stop: oneshot::Sender<()>, mut task: Serving) {
    let _ = stop.send(());
    let report = match time::timeout(DEADLINE, &mut task).await {
        Ok(Ok(Ok(report))) => report,
        Ok(Ok(Err(error))) => panic!("server shutdown failed: {error}"),
        Ok(Err(error)) => panic!("server task failed: {error}"),
        Err(_) => {
            task.abort();
            panic!("server stop deadline");
        }
    };
    assert!(report.graceful);
}

async fn finish_server<T>(
    stop: oneshot::Sender<()>,
    task: Serving,
    outcome: Result<T, Box<dyn std::any::Any + Send>>,
) -> T {
    stop_server(stop, task).await;
    match outcome {
        Ok(value) => value,
        Err(panic) => resume_unwind(panic),
    }
}

struct ClientDriver(Option<JoinHandle<Result<(), hyper::Error>>>);

impl ClientDriver {
    async fn join(mut self) {
        let task = self.0.as_mut().expect("client driver handle");
        let result = time::timeout(DEADLINE, task)
            .await
            .expect("client driver deadline")
            .expect("client driver task");
        self.0.take();
        assert!(result.is_ok());
    }
}

impl Drop for ClientDriver {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
        }
    }
}

async fn echo(State(calls): State<Arc<AtomicUsize>>, body: Bytes) -> String {
    calls.fetch_add(1, Ordering::SeqCst);
    format!("{}:{}", body.len(), checksum(&body))
}

fn checksum(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)))
}

fn app(capacity: usize, calls: Arc<AtomicUsize>, observed_data: Arc<Semaphore>) -> Router {
    Router::new()
        .route("/", post(echo))
        .with_state(calls)
        .layer(StreamAdmissionLayer::new(
            StreamAdmissionLimits::default()
                .with_max_concurrent_admissions(capacity)
                .unwrap(),
        ))
        .layer(middleware::from_fn_with_state(observed_data, observe_first_data))
}

async fn observe_first_data(
    State(observed_data): State<Arc<Semaphore>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let body = std::mem::replace(request.body_mut(), Body::empty());
    let mut signaled = false;
    let observed = body.into_data_stream().inspect(move |frame| {
        if !signaled && frame.is_ok() {
            signaled = true;
            observed_data.add_permits(1);
        }
    });
    *request.body_mut() = Body::from_stream(observed);
    next.run(request).await
}

async fn wait_for_data(semaphore: &Semaphore, count: u32) {
    let _permit = time::timeout(DEADLINE, semaphore.acquire_many(count))
        .await
        .expect("server DATA barrier deadline")
        .expect("server DATA barrier closed");
}

async fn tcp(address: SocketAddr) -> TcpStream {
    time::timeout(DEADLINE, TcpStream::connect(address))
        .await
        .unwrap()
        .expect("connect")
}

async fn write(stream: &mut TcpStream, bytes: &[u8]) {
    time::timeout(DEADLINE, stream.write_all(bytes))
        .await
        .unwrap()
        .expect("write request");
}

async fn read_response(reader: &mut TcpStream) -> (u16, Vec<u8>) {
    let mut headers = Vec::new();
    time::timeout(DEADLINE, async {
        while !headers.windows(4).any(|window| window == b"\r\n\r\n") {
            let mut buffer = [0; 1024];
            let count = reader.read(&mut buffer).await?;
            if count == 0 {
                break;
            }
            headers.extend_from_slice(&buffer[..count]);
        }
        Ok::<_, std::io::Error>(())
    })
    .await
    .unwrap()
    .expect("read response headers");
    let split = headers.windows(4).position(|window| window == b"\r\n\r\n");
    let (head, initial_body) = split
        .map(|index| headers.split_at(index + 4))
        .unwrap_or((&headers[..], &[][..]));
    let text = String::from_utf8_lossy(head);
    let status = text
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("missing HTTP status in response: {text:?}"));
    let length = text
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(initial_body.len());
    let mut body = initial_body.to_vec();
    if body.len() < length {
        let old = body.len();
        body.resize(length, 0);
        time::timeout(DEADLINE, reader.read_exact(&mut body[old..]))
            .await
            .unwrap()
            .expect("read response body");
    }
    body.truncate(length);
    (status, body)
}

async fn http1_request(address: SocketAddr, body: &[u8]) -> (u16, Vec<u8>) {
    let mut stream = tcp(address).await;
    write(
        &mut stream,
        b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
    )
    .await;
    for chunk in body.chunks(4096) {
        write(&mut stream, format!("{:x}\r\n", chunk.len()).as_bytes()).await;
        write(&mut stream, chunk).await;
        write(&mut stream, b"\r\n").await;
    }
    write(&mut stream, b"0\r\n\r\n").await;
    read_response(&mut stream).await
}

#[tokio::test]
async fn test_http1_chunked_payload_integrity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(2, calls.clone(), observed_data)).await;
    let result = AssertUnwindSafe(async {
        let payload = vec![b'x'; PAYLOAD_SIZE];
        let mut stream = tcp(address).await;
        write(
            &mut stream,
            b"POST / HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
        )
        .await;
        for chunk in payload.chunks(4096) {
            write(&mut stream, format!("{:x}\r\n", chunk.len()).as_bytes()).await;
            write(&mut stream, chunk).await;
            write(&mut stream, b"\r\n").await;
        }
        write(&mut stream, b"0\r\n\r\n").await;
        let (status, body) = read_response(&mut stream).await;
        assert_eq!(status, 200);
        assert_eq!(
            String::from_utf8(body).unwrap(),
            format!("{}:{}", payload.len(), checksum(&payload))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

async fn h2_client(address: SocketAddr) -> (http2::SendRequest<Body>, ClientDriver) {
    let socket = tcp(address).await;
    let (sender, driver) = time::timeout(DEADLINE, http2::handshake(TokioExecutor::new(), TokioIo::new(socket)))
        .await
        .unwrap()
        .expect("HTTP/2 handshake");
    (sender, ClientDriver(Some(tokio::spawn(driver))))
}

#[tokio::test]
async fn test_http2_fragmented_payload_integrity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(2, calls.clone(), observed_data)).await;
    let result = AssertUnwindSafe(async {
        let (mut sender, driver) = h2_client(address).await;
        let payload = Bytes::from(vec![b'z'; PAYLOAD_SIZE]);
        let chunks = payload.chunks(4096).map(Bytes::copy_from_slice).collect::<Vec<_>>();
        let chunks = chunks.into_iter().map(Ok::<_, Infallible>);
        let request = HyperRequest::builder()
            .method("POST")
            .uri(format!("http://{address}/"))
            .body(Body::from_stream(stream::iter(chunks)))
            .unwrap();
        let response = time::timeout(DEADLINE, sender.send_request(request))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = time::timeout(DEADLINE, response.into_body().collect())
            .await
            .unwrap()
            .unwrap()
            .to_bytes();
        assert_eq!(body, format!("{}:{}", payload.len(), checksum(&payload)).as_bytes());
        drop(sender);
        driver.join().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

async fn partial_http1(address: SocketAddr, length: usize) -> TcpStream {
    let mut stream = tcp(address).await;
    let header = format!("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {length}\r\n\r\n");
    write(&mut stream, header.as_bytes()).await;
    write(&mut stream, b"partial").await;
    stream
}

#[tokio::test]
async fn test_http1_admission_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(2, calls.clone(), observed_data.clone())).await;
    let result = AssertUnwindSafe(async {
        let first = partial_http1(address, 20).await;
        let second = partial_http1(address, 20).await;
        wait_for_data(&observed_data, 2).await;
        let mut third = tcp(address).await;
        write(
            &mut third,
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await;
        let (third_status, _) = read_response(&mut third).await;
        assert_eq!(third_status, 503);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut first = first;
        write(&mut first, b"1234567890123").await;
        let mut second = second;
        write(&mut second, b"1234567890123").await;
        assert_eq!(read_response(&mut first).await.0, 200);
        assert_eq!(read_response(&mut second).await.0, 200);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

#[tokio::test]
async fn test_http2_same_connection_admission_capacity() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(2, calls.clone(), observed_data.clone())).await;
    let result = AssertUnwindSafe(async {
        let (mut sender, driver) = h2_client(address).await;
        fn controlled_body() -> (Body, oneshot::Sender<()>) {
            let (release, wait) = oneshot::channel();
            let chunks = stream::unfold((0_u8, Some(wait)), |(stage, wait)| async move {
                match stage {
                    0 => Some((Ok::<_, Infallible>(Bytes::from_static(b"part")), (1, wait))),
                    1 => {
                        if let Some(wait) = wait {
                            let _ = wait.await;
                        }
                        Some((Ok(Bytes::from_static(b"done")), (2, None)))
                    }
                    _ => None,
                }
            });
            let body = Body::from_stream(chunks);
            (body, release)
        }
        let (first_body, release_first) = controlled_body();
        let (second_body, release_second) = controlled_body();
        let first = sender.send_request(
            HyperRequest::builder()
                .method("POST")
                .uri(format!("http://{address}/"))
                .body(first_body)
                .unwrap(),
        );
        let second = sender.send_request(
            HyperRequest::builder()
                .method("POST")
                .uri(format!("http://{address}/"))
                .body(second_body)
                .unwrap(),
        );
        wait_for_data(&observed_data, 2).await;
        let third = sender.send_request(
            HyperRequest::builder()
                .method("POST")
                .uri(format!("http://{address}/"))
                .body(Body::from("third"))
                .unwrap(),
        );
        let rejected = time::timeout(DEADLINE, third).await.unwrap().unwrap();
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let _ = release_first.send(());
        let _ = release_second.send(());
        let (a, b) = tokio::join!(first, second);
        assert_eq!(a.unwrap().status(), 200);
        assert_eq!(b.unwrap().status(), 200);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(sender);
        driver.join().await;
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

#[tokio::test]
async fn test_http1_disconnect_releases_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(1, calls.clone(), observed_data.clone())).await;
    let result = AssertUnwindSafe(async {
        let stream = partial_http1(address, PAYLOAD_SIZE).await;
        wait_for_data(&observed_data, 1).await;
        drop(stream);
        let (status, _) = time::timeout(DEADLINE, http1_request(address, b"recovered"))
            .await
            .unwrap();
        assert_eq!(status, 200);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

#[tokio::test]
async fn test_http2_reset_releases_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed_data = Arc::new(Semaphore::new(0));
    let (address, stop, server) = start_server(app(1, calls.clone(), observed_data.clone())).await;
    let result = AssertUnwindSafe(async {
        let (mut sender, driver) = h2_client(address).await;
        let pending = stream::unfold(0, |index| async move {
            if index > 0 {
                return std::future::pending().await;
            }
            Some((Ok::<_, Infallible>(Bytes::from_static(b"partial")), 1))
        });
        let body = Body::from_stream(pending);
        let request = HyperRequest::builder()
            .method("POST")
            .uri(format!("http://{address}/"))
            .body(body)
            .unwrap();
        let pending_response = Box::pin(sender.send_request(request));
        wait_for_data(&observed_data, 1).await;
        let occupied = time::timeout(
            DEADLINE,
            sender.send_request(
                HyperRequest::builder()
                    .method("POST")
                    .uri(format!("http://{address}/"))
                    .body(Body::from("probe"))
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(occupied.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        drop(pending_response); // Hyper sends RST_STREAM(CANCEL), preserving the connection.
        let response = time::timeout(
            DEADLINE,
            sender.send_request(
                HyperRequest::builder()
                    .method("POST")
                    .uri(format!("http://{address}/"))
                    .body(Body::from("ok"))
                    .unwrap(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(sender);
        driver.join().await;
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}

#[tokio::test]
async fn test_response_headers_release_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let observed_data = Arc::new(Semaphore::new(0));
    let app = Router::new()
        .route(
            "/hold",
            post({
                let release = release.clone();
                move || {
                    let release = release.clone();
                    async move {
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/plain")],
                            Body::from_stream(stream::once(async move {
                                release.notified().await;
                                Ok::<_, Infallible>(Bytes::from_static(b"done"))
                            })),
                        )
                    }
                }
            }),
        )
        .route("/", post(echo))
        .with_state(calls.clone())
        .layer(StreamAdmissionLayer::new(
            StreamAdmissionLimits::default()
                .with_max_concurrent_admissions(1)
                .unwrap(),
        ))
        .layer(middleware::from_fn_with_state(observed_data, observe_first_data));
    let (address, stop, server) = start_server(app).await;
    let result = AssertUnwindSafe(async {
        let mut held = tcp(address).await;
        write(
            &mut held,
            b"POST /hold HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        let mut held_reader = held;
        let mut head = [0; 12];
        time::timeout(DEADLINE, held_reader.read_exact(&mut head))
            .await
            .unwrap()
            .unwrap();
        let (status, _) = http1_request(address, b"second").await;
        assert_eq!(status, 200);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        release.notify_one();
        drop(held_reader);
    })
    .catch_unwind()
    .await;
    finish_server(stop, server, result).await;
}
