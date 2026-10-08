// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::convert::Infallible;
use std::future::pending;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use axum::Router;
use axum::response::IntoResponse;
use axum::response::sse::Event;
use axum::routing::get;
use futures_core::Stream;
use qubit_web::ServerOptions;
use qubit_web::SessionRegistrationError;
use qubit_web::SseConnectionPolicy;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::test as tokio_test;
use tokio::time::Instant;
use tokio::time::timeout;

struct PendingSse;

impl Stream for PendingSse {
    type Item = Result<Event, Infallible>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}

#[tokio_test]
async fn test_session_registration_releases_on_drop() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let session = context.try_register_session().unwrap();
    assert_eq!(context.active_sessions(), 1);
    drop(session);
    assert_eq!(context.active_sessions(), 0);
}

#[tokio_test]
async fn test_session_registration_is_rejected_after_shutdown_starts() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_secs(1)),
    )
    .await
    .unwrap();
    let context = server.context();
    let session = context.try_register_session().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(Router::new(), async move {
        let _ = shutdown_rx.await;
    }));

    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), context.cancellation_token().cancelled())
        .await
        .expect("shutdown should close session registration");
    assert_eq!(
        context.try_register_session().unwrap_err(),
        SessionRegistrationError::ShuttingDown
    );
    assert_eq!(context.active_sessions(), 1);

    drop(session);
    assert_eq!(context.active_sessions(), 0);
    let report = timeout(Duration::from_secs(1), service)
        .await
        .expect("server shutdown should finish after the session is released")
        .unwrap()
        .unwrap();
    assert!(report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
}

#[tokio_test]
async fn test_shutdown_waits_for_in_flight_request_and_broadcasts_cancellation() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let request_finished = Arc::new(AtomicBool::new(false));
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let cancellation = context.cancellation_token();
    let entered_handler = entered.clone();
    let release_handler = release.clone();
    let finished_handler = request_finished.clone();
    let app = Router::new().route(
        "/work",
        get(move || {
            let entered = entered_handler.clone();
            let release = release_handler.clone();
            let finished = finished_handler.clone();
            async move {
                entered.notify_one();
                release.notified().await;
                finished.store(true, Ordering::SeqCst);
                "done"
            }
        }),
    );
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let client = spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /work HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    });
    entered.notified().await;
    shutdown_tx.send(()).unwrap();
    cancellation.cancelled().await;
    assert!(
        timeout(Duration::from_secs(1), TcpStream::connect(addr))
            .await
            .expect("connection refusal should be prompt")
            .is_err(),
        "listener must stop accepting connections before draining"
    );
    release.notify_one();
    assert!(client.await.unwrap().ends_with("done"));
    assert!(request_finished.load(Ordering::SeqCst));
    assert!(service.await.unwrap().unwrap().graceful);
    assert!(cancellation.is_cancelled());
}

#[tokio_test]
async fn test_shutdown_deadline_drops_a_request_that_does_not_finish() {
    let entered = Arc::new(Notify::new());
    let entered_handler = entered.clone();
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(50)),
    )
    .await
    .unwrap();
    let context = server.context();
    let cancellation = context.cancellation_token();
    let addr = server.local_addr();
    let app = Router::new().route(
        "/stuck",
        get(move || {
            let entered = entered_handler.clone();
            async move {
                entered.notify_one();
                pending::<&'static str>().await
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let client = spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    });

    entered.notified().await;
    shutdown_tx.send(()).unwrap();
    cancellation.cancelled().await;
    let deadline = context
        .shutdown_deadline()
        .expect("shutdown records its absolute deadline");
    assert!(deadline <= Instant::now() + Duration::from_millis(50));
    let report = timeout(Duration::from_secs(2), service)
        .await
        .expect("server should stop at the configured deadline")
        .unwrap()
        .unwrap();
    assert!(!report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
    assert_eq!(report.forced_connections, None);
    assert!(
        timeout(Duration::from_secs(2), client)
            .await
            .expect("client connection should be closed")
            .unwrap()
            .is_empty()
    );
}

#[tokio_test]
async fn test_http_and_sse_share_one_shutdown_deadline() {
    let entered = Arc::new(Notify::new());
    let entered_handler = entered.clone();
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(150)),
    )
    .await
    .unwrap();
    let context = server.context();
    let sse_policy = SseConnectionPolicy::default();
    let handler_context = context.clone();
    let handler_policy = sse_policy.clone();
    let app = Router::new()
        .route(
            "/events",
            get(move || {
                let context = handler_context.clone();
                let policy = handler_policy.clone();
                async move {
                    let connection = policy.begin(&context).unwrap();
                    connection.into_sse(PendingSse).into_response()
                }
            }),
        )
        .route(
            "/stuck",
            get(move || {
                let entered = entered_handler.clone();
                async move {
                    entered.notify_one();
                    pending::<&'static str>().await
                }
            }),
        );
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let serving = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let mut sse = TcpStream::connect(addr).await.unwrap();
    sse.write_all(b"GET /events HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    let mut headers = Vec::new();
    while !headers.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        sse.read_exact(&mut byte).await.unwrap();
        headers.push(byte[0]);
    }
    let mut http = TcpStream::connect(addr).await.unwrap();
    http.write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    entered.notified().await;
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    let started = Instant::now();
    shutdown_tx.send(()).unwrap();
    let report = timeout(Duration::from_secs(2), serving)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!report.graceful);
    assert!(report.unfinished_managed_sessions >= 1);
    assert!(started.elapsed() < Duration::from_millis(250));
}

#[tokio_test]
async fn test_shutdown_while_waiting_for_transport_permit_aborts_and_reaps_connection_task() {
    struct SignalOnDrop(Arc<AtomicBool>);

    impl Drop for SignalOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let entered = Arc::new(Notify::new());
    let handler_starts = Arc::new(AtomicUsize::new(0));
    let handler_dropped = Arc::new(AtomicBool::new(false));
    let entered_handler = entered.clone();
    let starts_handler = handler_starts.clone();
    let dropped_handler = handler_dropped.clone();
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_shutdown_timeout(Duration::from_millis(50))
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_http(options).await.unwrap();
    let context = server.context();
    let addr = server.local_addr();
    let app = Router::new().route(
        "/stuck",
        get(move || {
            let entered = entered_handler.clone();
            let starts = starts_handler.clone();
            let dropped = dropped_handler.clone();
            async move {
                let _drop_signal = SignalOnDrop(dropped);
                starts.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                pending::<&'static str>().await
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let service = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut first = TcpStream::connect(addr).await.unwrap();
    first
        .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    entered.notified().await;
    let mut second = TcpStream::connect(addr).await.unwrap();
    second
        .write_all(b"GET /stuck HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    assert_eq!(handler_starts.load(Ordering::SeqCst), 1);

    shutdown_tx.send(()).unwrap();
    context.cancellation_token().cancelled().await;
    let report = timeout(Duration::from_secs(2), service)
        .await
        .expect("shutdown must not remain blocked waiting for a transport permit")
        .unwrap()
        .unwrap();
    assert!(!report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
    assert!(
        handler_dropped.load(Ordering::SeqCst),
        "aborted handler task must be reaped"
    );
}
