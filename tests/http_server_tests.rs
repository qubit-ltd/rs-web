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
use axum::http::Request;
use axum::http::header;
use axum::routing::get;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
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
