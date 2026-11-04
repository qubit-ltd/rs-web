// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::Request;
use axum::http::header;
use axum::routing::get;
use http_body_util::Empty;
use hyper::Request as HyperRequest;
use hyper::body::Bytes;
use hyper::client::conn::http2;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use qubit_web::ServerOptions;
use qubit_web::SseConnectionPolicy;
use qubit_web::WebServer;
use qubit_web::WebServerError;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::test as tokio_test;
use tokio::time;
use tower::ServiceExt;

async fn peer_address(ConnectInfo(peer): ConnectInfo<SocketAddr>) -> String {
    peer.to_string()
}

#[tokio_test]
async fn test_http1_buffer_budget_rejects_large_field_and_recovers() {
    let handled = Arc::new(AtomicUsize::new(0));
    let route_count = Arc::clone(&handled);
    let app = Router::new().route(
        "/health",
        get(move || {
            let handled = Arc::clone(&route_count);
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                "ok"
            }
        }),
    );
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http1_max_buffer_bytes(8192)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    async fn exchange(address: SocketAddr, request: Vec<u8>) -> Vec<u8> {
        let mut stream = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
            .await
            .unwrap()
            .unwrap();
        time::timeout(Duration::from_secs(3), stream.write_all(&request))
            .await
            .unwrap()
            .unwrap();
        let mut response = Vec::new();
        time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    let small = exchange(
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
    )
    .await;
    assert!(String::from_utf8_lossy(&small).starts_with("HTTP/1.1 200"));
    let mut large = b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Large: ".to_vec();
    large.extend(std::iter::repeat_n(b'a', 16 * 1024));
    large.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    let rejected = exchange(address, large).await;
    assert!(
        !rejected.is_empty(),
        "Hyper should close or respond when the parser buffer is exceeded"
    );
    let recovered = exchange(
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
    )
    .await;
    assert!(String::from_utf8_lossy(&recovered).starts_with("HTTP/1.1 200"));
    assert_eq!(
        handled.load(Ordering::SeqCst),
        2,
        "oversized request must not enter the handler"
    );

    let _ = shutdown_tx.send(());
    assert!(
        time::timeout(Duration::from_secs(3), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .graceful
    );
}

#[tokio_test]
async fn test_connect_info_http_matches_each_tcp_peer_and_ignores_proxy_headers() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = spawn(
        server.serve(Router::new().route("/peer", get(peer_address)), async move {
            let _ = shutdown_rx.await;
        }),
    );
    let result: Result<Vec<(SocketAddr, SocketAddr)>, Box<dyn std::error::Error + Send + Sync>> = async {
        let mut results = Vec::new();
        for _ in 0..2 {
            let mut stream = time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await??;
            let expected = stream.local_addr()?;
            time::timeout(Duration::from_secs(3), stream.write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nForwarded: for=203.0.113.1\r\nX-Forwarded-For: 203.0.113.1\r\n\r\n")).await??;
            let mut response = Vec::new();
            time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response)).await??;
            let response = String::from_utf8(response)?;
            let actual = response.rsplit("\r\n\r\n").next().ok_or("HTTP response has no body")?.parse::<SocketAddr>()?;
            results.push((expected, actual));
        }
        Ok(results)
    }
    .await;
    let _ = shutdown_tx.send(());
    let shutdown = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(shutdown.graceful);
    for (expected, actual) in result.expect("HTTP requests should return peer addresses") {
        assert_eq!(actual, expected);
    }
}

#[tokio_test]
async fn test_connect_info_http2_is_shared_by_requests_on_connection() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = spawn(
        server.serve(Router::new().route("/peer", get(peer_address)), async move {
            let _ = shutdown_rx.await;
        }),
    );
    let mut client_driver = None;
    let result: Result<(SocketAddr, Vec<SocketAddr>), Box<dyn std::error::Error + Send + Sync>> = async {
        let stream = time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await??;
        let expected = stream.local_addr()?;
        let (mut sender, driver) = time::timeout(
            Duration::from_secs(3),
            http2::handshake(TokioExecutor::new(), TokioIo::new(stream)),
        )
        .await??;
        client_driver = Some(spawn(driver));
        let mut results = Vec::new();
        for _ in 0..2 {
            let request = HyperRequest::builder()
                .uri(format!("http://{addr}/peer"))
                .body(Empty::<Bytes>::new())?;
            let response = time::timeout(Duration::from_secs(3), sender.send_request(request)).await??;
            let body = time::timeout(
                Duration::from_secs(3),
                http_body_util::BodyExt::collect(response.into_body()),
            )
            .await??
            .to_bytes();
            results.push(String::from_utf8(body.to_vec())?.parse::<SocketAddr>()?);
        }
        drop(sender);
        Ok((expected, results))
    }
    .await;
    let _ = shutdown_tx.send(());
    let shutdown = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(shutdown.graceful);
    let addresses = result;
    if let Some(driver) = client_driver {
        let driver_result = time::timeout(Duration::from_secs(3), driver)
            .await
            .expect("HTTP/2 client driver should stop after server shutdown")
            .expect("HTTP/2 client driver should not panic");
        assert!(driver_result.is_ok(), "HTTP/2 driver failed: {driver_result:?}");
    }
    let (expected, results) = addresses.expect("HTTP/2 requests should return peer addresses");
    for actual in results {
        assert_eq!(actual, expected);
    }
}

struct PendingEvents;

impl futures_core::Stream for PendingEvents {
    type Item = Result<axum::response::sse::Event, std::convert::Infallible>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[tokio_test]
async fn test_binds_ephemeral_http_and_serves_router_with_native_404() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    assert_ne!(server.local_addr().port(), 0);

    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(
        server.serve(Router::new().route("/health", get(|| async { "ok" })), async move {
            let _ = shutdown_rx.await;
        }),
    );

    async fn request(addr: SocketAddr, path: &str) -> String {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    }
    let health = request(addr, "/health").await;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    assert!(health.ends_with("ok"), "{health}");
    let missing = request(addr, "/missing").await;
    assert!(missing.starts_with("HTTP/1.1 404"), "{missing}");

    shutdown_tx.send(()).unwrap();
    let report = task.await.unwrap().unwrap();
    assert!(report.graceful);
    assert_eq!(report.forced_connections, None);
    assert!(TcpStream::connect(addr).await.is_err());
}

#[test]
fn rejects_transport_capacity_above_tokio_semaphore_maximum() {
    let max = tokio::sync::Semaphore::MAX_PERMITS;
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap());
    assert!(matches!(
        options.clone().with_max_transport_connections(0),
        Err(WebServerError::InvalidConfig)
    ));
    assert!(options.clone().with_max_transport_connections(1).is_ok());
    assert!(options.clone().with_max_transport_connections(max).is_ok());
    assert!(matches!(
        options.with_max_transport_connections(max + 1),
        Err(WebServerError::InvalidConfig)
    ));
}

#[tokio_test]
async fn test_rejects_invalid_options_and_reports_socket_conflicts() {
    let invalid = ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::ZERO);
    assert!(matches!(invalid.validate(), Err(WebServerError::InvalidConfig)));
    let invalid_header_timeout =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(Duration::ZERO);
    assert!(matches!(
        invalid_header_timeout.validate(),
        Err(WebServerError::InvalidConfig)
    ));
    let unrepresentable_header_timeout =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(Duration::MAX);
    assert!(matches!(
        unrepresentable_header_timeout.validate(),
        Err(WebServerError::InvalidConfig)
    ));
    assert!(matches!(
        WebServer::bind_http(unrepresentable_header_timeout).await,
        Err(WebServerError::InvalidConfig)
    ));
    let invalid_idle_timeout =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_transport_idle_timeout(Duration::ZERO);
    assert!(matches!(
        invalid_idle_timeout.validate(),
        Err(WebServerError::InvalidConfig)
    ));

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let conflict = WebServer::bind_http(ServerOptions::new(server.local_addr())).await;
    let error = conflict.expect_err("binding an occupied address must fail");
    assert!(matches!(&error, WebServerError::BindFailed { .. }));
    assert_eq!(error.code(), "bind_failed");
    let source = std::error::Error::source(&error).expect("bind errors retain their I/O source");
    let source_message = source.to_string();
    assert_eq!(
        source.downcast_ref::<std::io::Error>().map(std::io::Error::kind),
        Some(std::io::ErrorKind::AddrInUse)
    );
    assert!(!error.to_string().contains(&source_message));
    assert!(!format!("{error:?}").contains(&source_message));
}

#[tokio_test]
async fn test_transport_connection_limit_defaults_rejects_zero_and_gates_tcp_accepts() {
    let defaults = ServerOptions::new("127.0.0.1:0".parse().unwrap());
    assert_eq!(defaults.max_transport_connections(), 1024);
    assert_eq!(defaults.transport_idle_timeout(), Duration::from_secs(30));
    assert!(matches!(
        defaults.clone().with_max_transport_connections(0),
        Err(WebServerError::InvalidConfig)
    ));
    let options = defaults.with_max_transport_connections(1).unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let handled = Arc::new(AtomicUsize::new(0));
    let first_handler_entered = Arc::new(Notify::new());
    let release_first_handler = Arc::new(Notify::new());
    let handled_route = handled.clone();
    let entered_route = first_handler_entered.clone();
    let release_route = release_first_handler.clone();
    let app = Router::new()
        .route(
            "/hold",
            get(move || {
                let handled = handled_route.clone();
                let entered = entered_route.clone();
                let release = release_route.clone();
                async move {
                    handled.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    "released"
                }
            }),
        )
        .route(
            "/health",
            get({
                let handled = handled.clone();
                move || {
                    let handled = handled.clone();
                    async move {
                        handled.fetch_add(1, Ordering::SeqCst);
                        "ok"
                    }
                }
            }),
        );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    first_handler_entered.notified().await;
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    assert!(
        time::timeout(Duration::from_millis(100), second.read_to_end(&mut response))
            .await
            .is_err(),
        "the second request must wait while the first transport owns the permit"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    release_first_handler.notify_one();
    let mut first_response = Vec::new();
    time::timeout(Duration::from_secs(2), first.read_to_end(&mut first_response))
        .await
        .expect("first request should finish after its handler is released")
        .unwrap();
    assert!(String::from_utf8_lossy(&first_response).ends_with("released"));
    time::timeout(Duration::from_secs(2), second.read_to_end(&mut response))
        .await
        .expect("second connection should proceed after the permit is released")
        .unwrap();
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
    assert_eq!(handled.load(Ordering::SeqCst), 2);

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_http2_settings_advertise_configured_stream_limit() {
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http2_max_concurrent_streams(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = spawn(server.serve(Router::new(), async move {
        let _ = shutdown_rx.await;
    }));

    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n").await.unwrap();
    stream.write_all(&[0, 0, 0, 0x04, 0, 0, 0, 0, 0]).await.unwrap();
    let mut header = [0; 9];
    time::timeout(Duration::from_secs(2), stream.read_exact(&mut header))
        .await
        .expect("server SETTINGS should arrive")
        .expect("read server SETTINGS header");
    assert_eq!(header[3], 0x04, "expected a SETTINGS frame");
    let length = (usize::from(header[0]) << 16) | (usize::from(header[1]) << 8) | usize::from(header[2]);
    let mut payload = vec![0; length];
    time::timeout(Duration::from_secs(2), stream.read_exact(&mut payload))
        .await
        .expect("SETTINGS payload should arrive")
        .expect("read server SETTINGS payload");
    let max_streams = payload
        .chunks_exact(6)
        .find(|entry| u16::from_be_bytes([entry[0], entry[1]]) == 0x03)
        .map(|entry| u32::from_be_bytes([entry[2], entry[3], entry[4], entry[5]]));
    assert_eq!(max_streams, Some(1));
    stream.write_all(&[0, 0, 0, 0x04, 0x01, 0, 0, 0, 0]).await.unwrap();
    drop(stream);

    let _ = shutdown_tx.send(());
    assert!(
        time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("server shutdown should complete")
            .unwrap()
            .unwrap()
            .graceful
    );
}

#[tokio_test]
async fn test_http2_stream_limit_is_per_connection_and_releases_after_request() {
    let entered_hold = Arc::new(Notify::new());
    let release_hold = Arc::new(Notify::new());
    let entered_next = Arc::new(Notify::new());
    let app = Router::new()
        .route(
            "/hold",
            get({
                let entered = entered_hold.clone();
                let release = release_hold.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "released"
                    }
                }
            }),
        )
        .route(
            "/next",
            get({
                let entered = entered_next.clone();
                move || {
                    let entered = entered.clone();
                    async move {
                        entered.notify_one();
                        "next"
                    }
                }
            }),
        );
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http2_max_concurrent_streams(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    async fn connect(
        addr: SocketAddr,
    ) -> (
        http2::SendRequest<Empty<Bytes>>,
        tokio::task::JoinHandle<Result<(), hyper::Error>>,
    ) {
        let stream = TcpStream::connect(addr).await.unwrap();
        let (sender, connection) = time::timeout(
            Duration::from_secs(2),
            http2::handshake(TokioExecutor::new(), TokioIo::new(stream)),
        )
        .await
        .expect("HTTP/2 handshake must finish")
        .expect("HTTP/2 handshake must succeed");
        (sender, spawn(connection))
    }

    async fn request(
        mut sender: http2::SendRequest<Empty<Bytes>>,
        addr: SocketAddr,
        path: &'static str,
    ) -> hyper::Response<hyper::body::Incoming> {
        let request = HyperRequest::builder()
            .method("GET")
            .uri(format!("http://{addr}{path}"))
            .body(Empty::new())
            .unwrap();
        time::timeout(Duration::from_secs(3), sender.send_request(request))
            .await
            .expect("HTTP/2 request must finish")
            .expect("HTTP/2 request must succeed")
    }

    let (first_sender, first_driver) = connect(addr).await;
    let first = spawn(request(first_sender.clone(), addr, "/hold"));
    time::timeout(Duration::from_secs(2), entered_hold.notified())
        .await
        .expect("first handler should enter");
    let second = spawn(request(first_sender, addr, "/next"));
    assert!(
        time::timeout(Duration::from_millis(100), entered_next.notified())
            .await
            .is_err(),
        "the peer must not enter a second stream while the connection limit is one"
    );

    // A different HTTP/2 connection has its own stream budget.
    let (independent_sender, independent_driver) = connect(addr).await;
    let independent_response = request(independent_sender, addr, "/next").await;
    assert_eq!(independent_response.status(), hyper::StatusCode::OK);

    release_hold.notify_one();
    let first_response = time::timeout(Duration::from_secs(2), first)
        .await
        .expect("the first stream should finish after release")
        .unwrap();
    assert_eq!(first_response.status(), hyper::StatusCode::OK);
    let second_response = time::timeout(Duration::from_secs(2), second)
        .await
        .expect("the next stream should proceed after the first completes")
        .unwrap();
    assert_eq!(second_response.status(), hyper::StatusCode::OK);
    let report = {
        let _ = shutdown_tx.send(());
        time::timeout(Duration::from_secs(3), serving)
            .await
            .expect("server shutdown should complete")
            .unwrap()
            .unwrap()
    };
    assert!(report.graceful);
    drop(first_driver);
    drop(independent_driver);
}

#[tokio_test]
async fn test_idle_http2_preface_releases_transport_capacity() {
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_transport_idle_timeout(Duration::from_millis(50))
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(
        server.serve(Router::new().route("/", get(|| async { "ok" })), async move {
            let _ = shutdown_rx.await;
        }),
    );

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")
        .await
        .unwrap();
    let mut eof = Vec::new();
    time::timeout(Duration::from_secs(2), first.read_to_end(&mut eof))
        .await
        .expect("idle HTTP/2 connection should be closed")
        .unwrap();

    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    time::timeout(Duration::from_secs(2), second.read_to_end(&mut response))
        .await
        .expect("capacity should be released after the idle HTTP/2 connection closes")
        .unwrap();
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_active_http2_handler_survives_transport_idle_timeout() {
    let options =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_transport_idle_timeout(Duration::from_millis(50));
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let entered_handler = entered.clone();
    let release_handler = release.clone();
    let app = Router::new().route(
        "/hold",
        get(move || {
            let entered = entered_handler.clone();
            let release = release_handler.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                "ok"
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let client = Client::builder(TokioExecutor::new())
        .http2_only(true)
        .build(HttpConnector::new());
    let uri = format!("http://{addr}/hold").parse::<hyper::Uri>().unwrap();
    let request = hyper::Request::builder().uri(uri).body(Body::empty()).unwrap();
    let response = spawn(async move { client.request(request).await });
    entered.notified().await;
    time::sleep(Duration::from_millis(150)).await;
    release.notify_one();
    let response = time::timeout(Duration::from_secs(2), response)
        .await
        .expect("active HTTP/2 request should not be closed by idle timeout")
        .unwrap()
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_sse_body_keeps_connection_active_past_idle_timeout() {
    let options =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_transport_idle_timeout(Duration::from_millis(50));
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let context = server.context();
    let policy = SseConnectionPolicy::default();
    let app = Router::new().route(
        "/events",
        get(move || {
            let context = context.clone();
            let policy = policy.clone();
            async move {
                policy
                    .begin(&context)
                    .expect("SSE admission should succeed")
                    .into_sse(PendingEvents)
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n")
        .await
        .unwrap();
    let mut response_head = Vec::new();
    while !response_head.windows(4).any(|window| window == b"\r\n\r\n") {
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).await.unwrap();
        response_head.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&response_head).starts_with("HTTP/1.1 200"));
    let mut byte = [0; 1];
    assert!(
        time::timeout(Duration::from_millis(150), stream.read(&mut byte))
            .await
            .is_err(),
        "an open SSE response must retain its activity lease"
    );

    drop(stream);
    shutdown_tx.send(()).unwrap();
    assert!(
        time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .graceful
    );
}

#[tokio_test]
async fn test_idle_http1_keep_alive_releases_transport_capacity() {
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_transport_idle_timeout(Duration::from_millis(50))
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(
        server.serve(Router::new().route("/", get(|| async { "ok" })), async move {
            let _ = shutdown_rx.await;
        }),
    );

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    while !response.ends_with(b"ok") {
        let mut byte = [0; 1];
        first.read_exact(&mut byte).await.unwrap();
        response.push(byte[0]);
    }
    assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"));
    let mut eof = [0; 1];
    assert_eq!(
        time::timeout(Duration::from_secs(2), first.read(&mut eof))
            .await
            .expect("idle keep-alive connection should close")
            .unwrap(),
        0
    );

    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut second_response = Vec::new();
    time::timeout(Duration::from_secs(2), second.read_to_end(&mut second_response))
        .await
        .expect("idle keep-alive connection should release the transport cap")
        .unwrap();
    assert!(String::from_utf8_lossy(&second_response).starts_with("HTTP/1.1 200"));

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_transport_limit_releases_after_an_incomplete_header_times_out() {
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_request_header_timeout(Duration::from_millis(100))
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let second_handler_calls = Arc::new(AtomicUsize::new(0));
    let second_handler_entered = Arc::new(Notify::new());
    let release_second_handler = Arc::new(Notify::new());
    let calls_route = second_handler_calls.clone();
    let entered_route = second_handler_entered.clone();
    let release_route = release_second_handler.clone();
    let app = Router::new().route(
        "/health",
        get(move || {
            let calls = calls_route.clone();
            let entered = entered_route.clone();
            let release = release_route.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                "ok"
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Partial:")
        .await
        .unwrap();
    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();

    let mut byte = [0; 1];
    assert_eq!(
        time::timeout(Duration::from_secs(2), first.read(&mut byte))
            .await
            .expect("the first connection must reach its request-header timeout")
            .unwrap(),
        0
    );
    time::timeout(Duration::from_secs(2), second_handler_entered.notified())
        .await
        .expect("second handler should run after the first transport times out");
    assert_eq!(second_handler_calls.load(Ordering::SeqCst), 1);
    release_second_handler.notify_one();
    let mut response = String::new();
    time::timeout(Duration::from_secs(2), second.read_to_string(&mut response))
        .await
        .expect("second request should finish after its handler is released")
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert_eq!(second_handler_calls.load(Ordering::SeqCst), 1);

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_cross_origin_preflight_is_not_allowed_by_default() {
    let app = Router::new().route("/health", get(|| async { "ok" }));
    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/health")
                .header(header::ORIGIN, "https://untrusted.example")
                .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(!response.headers().contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN));
}

#[tokio_test]
async fn test_closes_a_connection_that_does_not_finish_request_headers() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(Duration::from_millis(10)),
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = spawn(
        server.serve(Router::new().route("/", get(|| async { "ok" })), async move {
            let _ = shutdown_rx.await;
        }),
    );

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Partial:")
        .await
        .unwrap();
    let mut byte = [0; 1];
    assert_eq!(
        time::timeout(Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("header timeout should close the connection promptly")
            .unwrap(),
        0
    );

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}
