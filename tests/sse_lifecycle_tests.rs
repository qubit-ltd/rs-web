// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::Future;
use std::future::poll_fn;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::routing::get;
use futures_core::Stream;
use hyper::body::Body as HttpBody;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use qubit_web::ServerOptions;
use qubit_web::SseConnectionPolicy;
use qubit_web::WebServer;
use qubit_web::sse::SseAdmissionError;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

struct EventStream(VecDeque<Result<Event, Infallible>>);

impl Stream for EventStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.0.pop_front())
    }
}

struct PendingEvents;

impl Stream for PendingEvents {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

struct ShutdownEvents {
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    sent_final_event: bool,
}

impl ShutdownEvents {
    fn new(cancellation: CancellationToken) -> Self {
        let cancelled = Box::pin(cancellation.clone().cancelled_owned());
        Self {
            cancelled,
            sent_final_event: false,
        }
    }
}

impl Stream for ShutdownEvents {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.sent_final_event {
            return Poll::Ready(None);
        }
        if this.cancelled.as_mut().poll(cx).is_ready() {
            this.sent_final_event = true;
            return Poll::Ready(Some(Ok(Event::default().data("shutdown-final"))));
        }
        Poll::Pending
    }
}

async fn raw_request(addr: SocketAddr, path: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect to test server");
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").as_bytes())
        .await
        .expect("write test request");
    let mut response = String::new();
    stream.read_to_string(&mut response).await.expect("read response");
    response
}

#[tokio_test]
async fn test_sse_stream_delivers_events_and_releases_its_slot() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::default();
    let handler_policy = policy.clone();
    let handler_context = context.clone();
    let app = Router::new().route(
        "/events",
        get(move || {
            let policy = handler_policy.clone();
            let context = handler_context.clone();
            async move {
                let connection = policy.begin(&context).expect("reserve SSE connection");
                let events = EventStream(VecDeque::from([
                    Ok(Event::default().data("first")),
                    Ok(Event::default().data("second")),
                ]));
                connection.into_sse(events).into_response()
            }
        }),
    );
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let response = raw_request(addr, "/events").await;
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(
        response
            .to_ascii_lowercase()
            .contains("content-type: text/event-stream"),
        "{response}"
    );
    assert!(response.contains("data: first\n\n"), "{response}");
    assert!(response.contains("data: second\n\n"), "{response}");
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);

    shutdown_tx.send(()).unwrap();
    assert!(service.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_sse_events_are_delivered_over_http2() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::default();
    let handler_policy = policy.clone();
    let handler_context = context.clone();
    let app = Router::new().route(
        "/events",
        get(move || {
            let policy = handler_policy.clone();
            let context = handler_context.clone();
            async move {
                let connection = policy.begin(&context).expect("reserve SSE connection");
                let events = EventStream(VecDeque::from([
                    Ok(Event::default().data("h2-first")),
                    Ok(Event::default().data("h2-second")),
                ]));
                connection.into_sse(events).into_response()
            }
        }),
    );
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut builder = Client::builder(TokioExecutor::new());
    builder.http2_only(true);
    let client = builder.build_http::<Body>();
    let uri = format!("http://{address}/events").parse().unwrap();
    let response = timeout(Duration::from_secs(2), client.get(uri))
        .await
        .expect("HTTP/2 request should complete")
        .expect("HTTP/2 response should be valid");
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let mut payload = Vec::new();
    timeout(Duration::from_secs(2), async {
        while let Some(frame) = poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await {
            let frame = frame.expect("HTTP/2 SSE frame");
            if let Ok(data) = frame.into_data() {
                payload.extend_from_slice(&data);
            }
        }
    })
    .await
    .expect("HTTP/2 SSE stream should finish");
    let payload = String::from_utf8(payload).unwrap();
    assert!(payload.contains("data: h2-first\n\n"), "{payload}");
    assert!(payload.contains("data: h2-second\n\n"), "{payload}");
    assert_eq!(policy.active_connections(), 0);

    shutdown_tx.send(()).unwrap();
    assert!(service.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_sse_capacity_rejection_is_a_stable_503_problem_response() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::new(NonZeroUsize::new(1).unwrap());
    let _first = policy.begin(&context).unwrap();
    let error = match policy.begin(&context) {
        Ok(_) => panic!("second SSE connection must exceed the limit"),
        Err(error) => error,
    };
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/problem+json");
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("\"type\":\"about:blank\""), "{body}");
    assert!(body.contains("\"title\":\"Service Unavailable\""), "{body}");
    assert!(body.contains("\"code\":\"capacity_exceeded\""), "{body}");
}

#[tokio_test]
async fn test_sse_admission_after_shutdown_is_rejected_without_leaking_capacity() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::new(NonZeroUsize::new(1).unwrap());
    let service = spawn(server.serve(Router::new(), async {}));
    assert!(service.await.unwrap().unwrap().graceful);

    let error = match policy.begin(&context) {
        Ok(_) => panic!("SSE admission must be closed after shutdown"),
        Err(error) => error,
    };
    assert_eq!(error, SseAdmissionError::ShuttingDown);
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);

    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/problem+json");
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();
    assert!(body.contains("\"code\":\"server_shutting_down\""), "{body}");
}

#[tokio_test]
async fn test_sse_disconnect_cancels_producer_and_does_not_block_short_requests() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::new(NonZeroUsize::new(1).unwrap())
        .with_keep_alive_interval(Duration::from_millis(10))
        .unwrap();
    let handler_policy = policy.clone();
    let handler_context = context.clone();
    let (producer_stopped_tx, producer_stopped_rx) = oneshot::channel();
    let producer_stopped_tx = Arc::new(Mutex::new(Some(producer_stopped_tx)));
    let producer_signal = producer_stopped_tx.clone();
    let app = Router::new()
        .route(
            "/events",
            get(move || {
                let policy = handler_policy.clone();
                let context = handler_context.clone();
                let producer_signal = producer_signal.clone();
                async move {
                    let connection = match policy.begin(&context) {
                        Ok(connection) => connection,
                        Err(error) => return error.into_response(),
                    };
                    let cancellation = connection.cancellation_token();
                    spawn(async move {
                        cancellation.cancelled().await;
                        if let Some(signal) = producer_signal.lock().unwrap().take() {
                            let _ = signal.send(());
                        }
                    });
                    connection.into_sse(PendingEvents).into_response()
                }
            }),
        )
        .route("/health", get(|| async { "ok" }));
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut client = BufReader::new(TcpStream::connect(addr).await.unwrap());
    client
        .get_mut()
        .write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut status = String::new();
    client.read_line(&mut status).await.unwrap();
    assert!(status.starts_with("HTTP/1.1 200"), "{status}");
    loop {
        let mut header = String::new();
        client.read_line(&mut header).await.unwrap();
        if header == "\r\n" {
            break;
        }
    }
    let mut wire = Vec::new();
    timeout(Duration::from_secs(1), async {
        let mut chunk = [0_u8; 128];
        while !String::from_utf8_lossy(&wire).contains(": keep-alive\n\n") {
            let count = client.read(&mut chunk).await.unwrap();
            assert_ne!(count, 0, "SSE response ended before its keep-alive frame");
            wire.extend_from_slice(&chunk[..count]);
        }
    })
    .await
    .expect("SSE keep-alive comment arrives promptly");
    let wire = String::from_utf8_lossy(&wire);
    assert!(!wire.contains("id:"), "keep-alive must not advance event IDs: {wire}");
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("SSE permit becomes active");

    let health = raw_request(addr, "/health").await;
    assert!(health.starts_with("HTTP/1.1 200"), "{health}");
    let rejected = raw_request(addr, "/events").await;
    assert!(rejected.starts_with("HTTP/1.1 503"), "{rejected}");

    drop(client);
    timeout(Duration::from_secs(1), producer_stopped_rx)
        .await
        .expect("producer observes client disconnect")
        .expect("producer signal is sent");
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);

    shutdown_tx.send(()).unwrap();
    assert!(service.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_sse_source_can_send_a_final_event_after_server_shutdown() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = SseConnectionPolicy::default();
    let handler_policy = policy.clone();
    let handler_context = context.clone();
    let app = Router::new().route(
        "/events",
        get(move || {
            let policy = handler_policy.clone();
            let context = handler_context.clone();
            async move {
                let connection = policy.begin(&context).expect("reserve SSE connection");
                let source = ShutdownEvents::new(connection.cancellation_token());
                connection.into_sse(source).into_response()
            }
        }),
    );
    let addr = server.local_addr();
    let shutdown_token = context.cancellation_token();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let client = spawn(raw_request(addr, "/events"));

    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("SSE response is active before shutdown");
    shutdown_tx.send(()).unwrap();
    shutdown_token.cancelled().await;

    let response = timeout(Duration::from_secs(1), client)
        .await
        .expect("SSE source finishes during graceful shutdown")
        .unwrap();
    assert!(response.contains("data: shutdown-final\n\n"), "{response}");
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);
    assert!(service.await.unwrap().unwrap().graceful);
}

#[test]
fn test_sse_policy_defaults_and_zero_keep_alive_validation() {
    let policy = SseConnectionPolicy::default();
    assert_eq!(policy.active_connections(), 0);
    assert!(
        SseConnectionPolicy::new(NonZeroUsize::new(128).unwrap())
            .with_keep_alive_interval(Duration::ZERO)
            .is_err()
    );
}
