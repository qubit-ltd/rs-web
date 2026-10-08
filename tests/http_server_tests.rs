// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use axum::Router;
use axum::body::Body;
use axum::http::Request;
use axum::http::header;
use axum::routing::get;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tower::ServiceExt;

#[tokio::test]
async fn binds_ephemeral_http_and_serves_router_with_native_404() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    assert_ne!(server.local_addr().port(), 0);

    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(
        server.serve(Router::new().route("/health", get(|| async { "ok" })), async move {
            let _ = shutdown_rx.await;
        }),
    );

    async fn request(addr: std::net::SocketAddr, path: &str) -> String {
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

#[tokio::test]
async fn rejects_invalid_options_and_reports_socket_conflicts() {
    let invalid = ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(std::time::Duration::ZERO);
    assert_eq!(invalid.validate(), Err(qubit_web::WebServerError::InvalidConfig));
    let invalid_header_timeout =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(std::time::Duration::ZERO);
    assert_eq!(
        invalid_header_timeout.validate(),
        Err(qubit_web::WebServerError::InvalidConfig)
    );

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let conflict = WebServer::bind_http(ServerOptions::new(server.local_addr())).await;
    assert_eq!(conflict.unwrap_err(), qubit_web::WebServerError::BindFailed);
}

#[tokio::test]
async fn cross_origin_preflight_is_not_allowed_by_default() {
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

#[tokio::test]
async fn closes_a_connection_that_does_not_finish_request_headers() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap())
            .with_request_header_timeout(std::time::Duration::from_millis(10)),
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let task = tokio::spawn(
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
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.read(&mut byte))
            .await
            .expect("header timeout should close the connection promptly")
            .unwrap(),
        0
    );

    shutdown_tx.send(()).unwrap();
    assert!(task.await.unwrap().unwrap().graceful);
}
