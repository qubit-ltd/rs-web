// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(all(feature = "json", feature = "ws", feature = "tls-rustls", feature = "config"))]

use std::convert::Infallible;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;

use axum::Router;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event;
use axum::routing::get;
use axum::routing::post;
use futures_util::stream;
use qubit_web::BoundedJson;
use qubit_web::HttpLimits;
use qubit_web::RequestLimitLayer;
use qubit_web::ServerContext;
use qubit_web::ServerOptions;
use qubit_web::SseConnectionPolicy;
use qubit_web::WebServer;
use qubit_web::json_response;
use qubit_web::mvc::ControllerRoutes;
use qubit_web::rest_controller;
use qubit_web::tls::TlsConfig;
use qubit_web::ws::WsUpgradePolicy;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::test as tokio_test;

#[derive(Clone, Deserialize, Serialize)]
struct Item {
    name: String,
}

struct Items;

#[rest_controller("/items")]
impl Items {
    #[get_mapping("")]
    async fn list(&self) -> Response {
        json_response(&["item"], &Default::default()).unwrap().into_response()
    }

    #[post_mapping("")]
    async fn create(&self, BoundedJson(item): BoundedJson<Item>) -> Response {
        json_response(&item, &Default::default()).unwrap()
    }
}

fn application(context: ServerContext) -> Router<()> {
    let items = ControllerRoutes::new().add(Arc::new(Items)).unwrap().finish().unwrap();
    let sse = SseConnectionPolicy::default();
    let ws = WsUpgradePolicy::new();
    let ws_context = context.clone();
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route(
            "/events",
            get(move || {
                let sse = sse.clone();
                let context = context.clone();
                async move {
                    let connection = sse.begin(&context).unwrap();
                    connection.into_sse(stream::iter([Ok::<_, Infallible>(Event::default().data("event"))]))
                }
            }),
        )
        .route(
            "/ws",
            get(move |headers: HeaderMap, upgrade: WebSocketUpgrade| {
                let policy = ws.clone();
                let context = ws_context.clone();
                async move {
                    policy.on_upgrade(upgrade, &headers, context, |mut session| async move {
                        while let Some(Ok(message)) = session.recv().await {
                            if let Message::Text(text) = message {
                                let _ = session.try_send(Message::text(text));
                            }
                        }
                    })
                }
            }),
        )
        .nest(
            "/bounded",
            Router::new()
                .route(
                    "/",
                    post(|BoundedJson(item): BoundedJson<Item>| async move {
                        json_response(&item, &Default::default()).unwrap()
                    }),
                )
                .layer(RequestLimitLayer::new(
                    HttpLimits::default().with_max_body_bytes(32).unwrap(),
                )),
        )
        .merge(items)
}

async fn request(address: SocketAddr, method: &str, path: &str, body: &str) -> String {
    let mut stream = TcpStream::connect(address).await.unwrap();
    stream.write_all(format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}

#[tokio_test]
async fn test_one_router_composes_controller_native_sse_ws_limits_and_shutdown() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let app = application(context.clone());
    let (stop, stopped) = oneshot::channel();
    let running = spawn(server.serve(app, async {
        let _ = stopped.await;
    }));
    let health = request(address, "GET", "/health", "").await;
    let controller = request(address, "POST", "/items", r#"{"name":"ok"}"#).await;
    let limited = request(
        address,
        "POST",
        "/bounded",
        r#"{"name":"this body is longer than the configured limit"}"#,
    )
    .await;
    let events = request(address, "GET", "/events", "").await;
    assert!(health.starts_with("HTTP/1.1 200"));
    assert!(controller.starts_with("HTTP/1.1 200") && controller.contains("\"name\":\"ok\""));
    assert!(
        limited.starts_with("HTTP/1.1 413") && limited.contains("body_too_large"),
        "{limited}"
    );
    assert!(events.contains("data: event\n\n"));
    assert_eq!(context.active_sessions(), 0);
    stop.send(()).unwrap();
    assert!(running.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_same_router_supports_https_and_wss_upgrade() {
    let fixture = |name: &str| {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/tls")
            .join(name)
    };
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .unwrap();
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let (stop, stopped) = oneshot::channel();
    let running = spawn(server.serve(application(context), async {
        let _ = stopped.await;
    }));
    let response = spawn_blocking(move || {
        openssl_request(
            address,
            b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
    })
    .await
    .unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    let wss = spawn_blocking(move || openssl_request(address, b"GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")).await.unwrap();
    assert!(wss.starts_with("HTTP/1.1 101"), "unexpected WSS handshake: {wss}");
    let _ = stop.send(());
    assert!(running.await.unwrap().unwrap().graceful);
}

fn openssl_request(address: SocketAddr, request: &[u8]) -> String {
    let mut child = Command::new("openssl")
        .args(["s_client", "-connect", &address.to_string(), "-quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    input.write_all(request).unwrap();
    drop(input);
    let mut output = String::new();
    let stdout = child.stdout.take().unwrap();
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap() > 0 {
        output.push_str(&line);
        if line == "\r\n" {
            break;
        }
        line.clear();
    }
    let _ = child.kill();
    let _ = child.wait();
    output
}
