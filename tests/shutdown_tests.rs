// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use axum::Router;
use axum::routing::get;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Notify;

#[tokio::test]
async fn session_registration_releases_on_drop() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let session = context.register_session();
    assert_eq!(context.active_sessions(), 1);
    drop(session);
    assert_eq!(context.active_sessions(), 0);
}

#[tokio::test]
async fn shutdown_waits_for_in_flight_request_and_broadcasts_cancellation() {
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
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let service = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let client = tokio::spawn(async move {
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
        tokio::time::timeout(std::time::Duration::from_secs(1), TcpStream::connect(addr))
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

#[tokio::test]
async fn shutdown_deadline_drops_a_request_that_does_not_finish() {
    let entered = Arc::new(Notify::new());
    let entered_handler = entered.clone();
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(std::time::Duration::from_millis(50)),
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
                std::future::pending::<&'static str>().await
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let service = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let client = tokio::spawn(async move {
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
    assert!(deadline <= tokio::time::Instant::now() + std::time::Duration::from_millis(50));
    let report = tokio::time::timeout(std::time::Duration::from_secs(2), service)
        .await
        .expect("server should stop at the configured deadline")
        .unwrap()
        .unwrap();
    assert!(!report.graceful);
    assert_eq!(report.forced_connections, None);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(2), client)
            .await
            .expect("client connection should be closed")
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn shutdown_while_waiting_for_transport_permit_aborts_and_reaps_connection_task() {
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
        .with_shutdown_timeout(std::time::Duration::from_millis(50))
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
                std::future::pending::<&'static str>().await
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let service = tokio::spawn(server.serve(app, async move {
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
    let report = tokio::time::timeout(std::time::Duration::from_secs(2), service)
        .await
        .expect("shutdown must not remain blocked waiting for a transport permit")
        .unwrap()
        .unwrap();
    assert!(!report.graceful);
    assert!(handler_dropped.load(Ordering::SeqCst), "aborted handler task must be reaped");
}
