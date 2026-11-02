// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(all(feature = "json", feature = "ws", feature = "tls", feature = "config"))]

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

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
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::test as tokio_test;
use tokio::time::timeout;

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
    let items = ControllerRoutes::new().add(Arc::new(Items)).unwrap().finish();
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
    let response = openssl_request(
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await
    .unwrap_or_else(|failure| panic!("OpenSSL {} failed; reaped={}", failure.stage, failure.reaped));
    assert!(response.reaped, "OpenSSL child was not reaped after HTTPS response");
    assert!(response.headers.starts_with("HTTP/1.1 200"));
    let wss = openssl_request(address, b"GET /ws HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
        .await
        .unwrap_or_else(|failure| panic!("OpenSSL {} failed; reaped={}", failure.stage, failure.reaped));
    assert!(wss.reaped, "OpenSSL child was not reaped after WSS response");
    assert!(
        wss.headers.starts_with("HTTP/1.1 101"),
        "unexpected WSS handshake: {}",
        wss.headers
    );
    let _ = stop.send(());
    assert!(running.await.unwrap().unwrap().graceful);
}

#[derive(Debug)]
struct OpenSslResponse {
    headers: String,
    reaped: bool,
}

#[derive(Debug)]
struct OpenSslFailure {
    stage: &'static str,
    timed_out: bool,
    reaped: bool,
}

async fn openssl_request(address: SocketAddr, request: &[u8]) -> Result<OpenSslResponse, OpenSslFailure> {
    openssl_request_with_timeout(address, request, Duration::from_secs(5)).await
}

async fn openssl_request_with_timeout(
    address: SocketAddr,
    request: &[u8],
    budget: Duration,
) -> Result<OpenSslResponse, OpenSslFailure> {
    let mut child = Command::new("openssl")
        .args(["s_client", "-connect", &address.to_string(), "-quiet"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| OpenSslFailure {
            stage: "spawn",
            timed_out: false,
            reaped: true,
        })?;

    let mut stage = "request write";
    let exchange = timeout(budget, async {
        let mut input = child.stdin.take().ok_or("stdin pipe")?;
        input.write_all(request).await.map_err(|_| "request write")?;
        stage = "stdin close";
        input.shutdown().await.map_err(|_| "stdin close")?;
        drop(input);

        stage = "response headers";
        let mut output = Vec::new();
        let mut buffer = [0_u8; 1024];
        let mut stdout = child.stdout.take().ok_or("stdout pipe")?;
        loop {
            let count = stdout.read(&mut buffer).await.map_err(|_| "response headers read")?;
            if count == 0 {
                return Err("response headers EOF");
            }
            output.extend_from_slice(&buffer[..count]);
            if output.windows(4).any(|window| window == b"\r\n\r\n") {
                return String::from_utf8(output).map_err(|_| "response headers encoding");
            }
        }
    })
    .await;

    let (headers, failure_stage, timed_out) = match exchange {
        Ok(Ok(headers)) => (Some(headers), None, false),
        Ok(Err(failure_stage)) => (None, Some(failure_stage), false),
        Err(_) => (None, Some(stage), true),
    };
    let _ = child.start_kill();
    let reaped = matches!(timeout(Duration::from_secs(1), child.wait()).await, Ok(Ok(_)));
    if !reaped {
        eprintln!("OpenSSL child reaping failed after bounded termination wait");
    }
    if let Some(stage) = failure_stage {
        return Err(OpenSslFailure {
            stage,
            timed_out,
            reaped,
        });
    }
    Ok(OpenSslResponse {
        headers: headers.expect("successful exchange includes headers"),
        reaped,
    })
}

#[tokio_test]
async fn test_openssl_request_timeout_reaps_child() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(Notify::new());
    let accepted_signal = accepted.clone();
    let peer = spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        accepted_signal.notify_one();
        let _stream = stream;
        std::future::pending::<()>().await;
    });
    let failure = openssl_request_with_timeout(address, b"GET / HTTP/1.1\r\n\r\n", Duration::from_millis(150))
        .await
        .unwrap_err();
    timeout(Duration::from_secs(1), accepted.notified()).await.unwrap();
    assert!(failure.timed_out, "unexpected failure stage: {}", failure.stage);
    assert!(failure.reaped, "OpenSSL child was not reaped after timeout");
    peer.abort();
    let _ = peer.await;
}

#[tokio_test]
async fn test_openssl_request_rejects_eof_before_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let peer = spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
    });
    let failure = openssl_request_with_timeout(address, b"GET / HTTP/1.1\r\n\r\n", Duration::from_secs(2))
        .await
        .unwrap_err();
    assert_eq!(failure.stage, "response headers EOF");
    assert!(failure.reaped, "OpenSSL child was not reaped after early EOF");
    peer.await.unwrap();
}
