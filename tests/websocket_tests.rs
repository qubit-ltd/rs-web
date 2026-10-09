// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::pending;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::get;
use qubit_web::ServerContext;
use qubit_web::ServerOptions;
use qubit_web::ShutdownReport;
use qubit_web::WebServer;

type RegisteredCheckpoint = Arc<Mutex<Option<oneshot::Sender<usize>>>>;
type DelayedUpgradeChannels = (
    Arc<Mutex<Option<oneshot::Sender<()>>>>,
    Arc<Mutex<Option<oneshot::Receiver<()>>>>,
);
type PeerCloseTestState = (
    WsUpgradePolicy,
    ServerContext,
    Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    Arc<Mutex<Option<oneshot::Sender<WsSendError>>>>,
);
use qubit_web::diagnostic::DiagnosticLayer;
use qubit_web::ws::WsSendError;
use qubit_web::ws::WsUpgradePolicy;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::runtime::Builder;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::task::yield_now;
use tokio::test as tokio_test;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::Level;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;

async fn serve(policy: WsUpgradePolicy) -> (SocketAddr, JoinHandle<ShutdownReport>) {
    serve_with(policy, CancellationToken::new()).await
}

async fn serve_with(policy: WsUpgradePolicy, shutdown: CancellationToken) -> (SocketAddr, JoinHandle<ShutdownReport>) {
    serve_with_shutdown_timeout(policy, shutdown, Duration::from_secs(30)).await
}

async fn serve_with_shutdown_timeout(
    policy: WsUpgradePolicy,
    shutdown: CancellationToken,
    shutdown_timeout: Duration,
) -> (SocketAddr, JoinHandle<ShutdownReport>) {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |mut session| async move {
            while let Some(Ok(message)) = session.recv().await {
                match message {
                    Message::Text(text) => {
                        session.try_send(Message::text(text)).unwrap();
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        })
    }

    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(shutdown_timeout);
    let server = WebServer::bind_http(options).await.unwrap();
    let addr = server.local_addr();
    let context = server.context();
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((policy, context))
        .layer(DiagnosticLayer::new());
    let dispatch = tracing::dispatcher::get_default(Clone::clone);
    let task =
        spawn(async move { server.serve(app, shutdown.cancelled_owned()).await.unwrap() }.with_subscriber(dispatch));
    (addr, task)
}

#[test]
fn test_websocket_payload_is_not_logged_at_trace() {
    const SECRET: &str = "ws-payload-secret-sentinel";
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let policy = WsUpgradePolicy::new();
                let (addr, task) = serve(policy.clone()).await;
                let (mut client, status) = handshake(addr, None).await;
                assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
                send_masked_text(&mut client, SECRET.as_bytes()).await;
                assert_eq!(read_server_text(&mut client).await, SECRET.as_bytes());
                send_masked_close(&mut client).await;
                let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
                    .await
                    .unwrap();
                assert_eq!(opcode, 8);
                task.abort();
            });
    });
    let output = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(
        !output.contains(SECRET),
        "TRACE logs leaked WebSocket payload: {output}"
    );
}

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

async fn handshake(addr: SocketAddr, origin: Option<&str>) -> (TcpStream, StatusCode) {
    handshake_path(addr, "/ws", origin).await
}

async fn handshake_path(addr: SocketAddr, path: &str, origin: Option<&str>) -> (TcpStream, StatusCode) {
    let mut stream = start_handshake(addr, path, origin).await;
    let status = read_response_status(&mut stream).await;
    (stream, status)
}

async fn start_handshake(addr: SocketAddr, path: &str, origin: Option<&str>) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    if let Some(origin) = origin {
        request.push_str(&format!("Origin: {origin}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.unwrap();
    stream
}

async fn start_handshake_without_version(addr: SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET /ws HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    stream
}

async fn read_response_status(stream: &mut TcpStream) -> StatusCode {
    let mut response = Vec::new();
    let mut buf = [0; 1];
    while !response.ends_with(b"\r\n\r\n") {
        stream.read_exact(&mut buf).await.unwrap();
        response.push(buf[0]);
    }
    let first_line = String::from_utf8_lossy(&response);
    let status = first_line.split_whitespace().nth(1).unwrap().parse::<u16>().unwrap();
    StatusCode::from_u16(status).unwrap()
}

async fn send_masked_text(stream: &mut TcpStream, text: &[u8]) {
    send_masked_frame(stream, true, 1, text).await;
}

async fn send_masked_frame(stream: &mut TcpStream, final_frame: bool, opcode: u8, text: &[u8]) {
    assert!(text.len() < 126);
    let mask = [7, 9, 3, 1];
    let first_byte = (if final_frame { 0x80 } else { 0 }) | opcode;
    let mut frame = vec![first_byte, 0x80 | text.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(text.iter().enumerate().map(|(i, byte)| byte ^ mask[i % 4]));
    stream.write_all(&frame).await.unwrap();
}

async fn send_masked_control(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
    assert!(payload.len() < 126);
    let mask = [4, 8, 2, 6];
    let mut frame = vec![0x80 | opcode, 0x80 | payload.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(payload.iter().enumerate().map(|(i, byte)| byte ^ mask[i % 4]));
    stream.write_all(&frame).await.unwrap();
}

async fn send_masked_close(stream: &mut TcpStream) {
    send_masked_control(stream, 8, &1000_u16.to_be_bytes()).await;
}

async fn read_server_text(stream: &mut TcpStream) -> Vec<u8> {
    let mut header = [0; 2];
    stream.read_exact(&mut header).await.unwrap();
    assert_eq!(header[0] & 0x0f, 1);
    assert_eq!(header[1] & 0x80, 0);
    let len = (header[1] & 0x7f) as usize;
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await.unwrap();
    body
}

async fn read_server_frame(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 2];
    stream.read_exact(&mut header).await.unwrap();
    let len = match header[1] & 0x7f {
        126 => {
            let mut bytes = [0; 2];
            stream.read_exact(&mut bytes).await.unwrap();
            u16::from_be_bytes(bytes) as usize
        }
        127 => {
            let mut bytes = [0; 8];
            stream.read_exact(&mut bytes).await.unwrap();
            u64::from_be_bytes(bytes) as usize
        }
        len => len as usize,
    };
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await.unwrap();
    (header[0] & 0x0f, body)
}

#[tokio_test]
async fn test_origin_allowlist_controls_real_loopback_upgrade() {
    let policy = WsUpgradePolicy::new().allowed_origins(["https://app.example"]);
    let (addr, task) = serve(policy).await;

    let (_, rejected) = handshake(addr, Some("https://evil.example")).await;
    assert_eq!(rejected, StatusCode::FORBIDDEN);
    let (mut accepted, status) = handshake(addr, Some("https://app.example")).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut accepted, b"hello").await;
    assert_eq!(read_server_text(&mut accepted).await, b"hello");

    accepted.shutdown().await.unwrap();
    task.abort();
}

#[tokio_test]
async fn test_browser_origin_without_allowlist_is_rejected_and_connection_limit_is_enforced() {
    let no_origins = WsUpgradePolicy::new();
    let (addr, task) = serve(no_origins).await;
    let (_, status) = handshake(addr, Some("https://app.example")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    task.abort();

    let policy = WsUpgradePolicy::new().max_connections(1);
    let (addr, task) = serve(policy.clone()).await;
    let (mut first, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let (mut rejected_stream, rejected) = handshake(addr, None).await;
    assert_eq!(rejected, StatusCode::SERVICE_UNAVAILABLE);
    let expected_body =
        br#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"capacity_exceeded"}"#;
    let mut body = vec![0; expected_body.len()];
    rejected_stream.read_exact(&mut body).await.unwrap();
    assert_eq!(body, expected_body);
    first.shutdown().await.unwrap();
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio_test]
async fn test_queue_rejects_message_count_and_byte_overflow() {
    let count_limited = WsUpgradePolicy::new().queue_limits(1, 64).send_queue();
    count_limited.try_send(Message::text("one")).unwrap();
    assert_eq!(
        count_limited.try_send(Message::text("two")),
        Err(WsSendError::Backpressure)
    );

    let byte_limited = WsUpgradePolicy::new().queue_limits(4, 3).send_queue();
    assert_eq!(
        byte_limited.try_send(Message::text("four")),
        Err(WsSendError::Backpressure)
    );
    byte_limited.try_send(Message::text("123")).unwrap();
    assert_eq!(
        byte_limited.try_send(Message::text("x")),
        Err(WsSendError::Backpressure)
    );
}

#[test]
fn test_zero_policy_limits_are_invalid() {
    assert!(WsUpgradePolicy::new().max_connections(0).validate().is_err());
    assert!(WsUpgradePolicy::new().max_frame_bytes(0).validate().is_err());
    assert!(WsUpgradePolicy::new().max_message_bytes(0).validate().is_err());
    assert!(WsUpgradePolicy::new().queue_limits(0, 1).validate().is_err());
    assert!(WsUpgradePolicy::new().queue_limits(1, 0).validate().is_err());
    assert!(WsUpgradePolicy::new().idle_timeout(Duration::ZERO).validate().is_err());
}

#[tokio_test]
async fn test_session_exits_on_shutdown_and_releases_connection_permit() {
    let policy = Arc::new(WsUpgradePolicy::new());
    let shutdown = CancellationToken::new();
    let (addr, task) =
        serve_with_shutdown_timeout((*policy).clone(), shutdown.clone(), Duration::from_millis(250)).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 1 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    shutdown.cancel();
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    assert_eq!(
        policy.active_connections(),
        1,
        "permit remains held while awaiting close ack"
    );
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio_test]
async fn test_peer_close_closes_send_queue_before_handler_resumes() {
    async fn upgrade(
        State((policy, context, release_rx, result_tx)): State<PeerCloseTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        let release = release_rx.lock().unwrap().take().unwrap();
        let result_tx = result_tx.clone();
        policy.on_upgrade(ws, &headers, context, move |session| async move {
            let _ = release.await;
            let result = session.try_send(Message::text("late")).unwrap_err();
            result_tx.lock().unwrap().take().unwrap().send(result).unwrap();
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new();
    let (release_tx, release_rx) = oneshot::channel();
    let (result_tx, result_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        policy.clone(),
        context.clone(),
        Arc::new(Mutex::new(Some(release_rx))),
        Arc::new(Mutex::new(Some(result_tx))),
    ));
    let serving = spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .expect("peer Close should release the connection permit");
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), result_rx)
            .await
            .expect("handler should resume")
            .unwrap(),
        WsSendError::Closed
    );
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 0 {
            yield_now().await;
        }
    })
    .await
    .expect("handler exit should release its managed session");
    serving.abort();
}

#[tokio_test]
async fn test_inbound_backpressure_closes_with_try_again_later() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |session| async move {
            let _session = session;
            pending::<()>().await;
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().idle_timeout(Duration::from_millis(50));
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((policy.clone(), context.clone()));
    let serving = spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, b"x").await;
    for _ in 0..64 {
        send_masked_text(&mut client, b"x").await;
    }
    let (opcode, payload) = timeout(Duration::from_secs(2), read_server_frame(&mut client))
        .await
        .expect("stalled inbound delivery should produce a Close frame");
    assert_eq!(opcode, 8);
    assert!(payload.len() >= 2, "Close frame should include a status code");
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1013);
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 || context.active_sessions() != 0 {
            yield_now().await;
        }
    })
    .await
    .expect("backpressured session should release its connection and registration");
    serving.abort();
}

#[tokio_test]
async fn test_context_upgrade_uses_the_server_absolute_shutdown_deadline() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(500)),
    )
    .await
    .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new()
        .max_connections(1)
        .allowed_origins(["https://app.example"]);
    assert_eq!(context.active_sessions(), 0);
    let app = Router::new()
        .route("/ws", get(context_ws_upgrade))
        .with_state((policy.clone(), context.clone()));
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut serving = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    let (_, rejected) = handshake(address, Some("https://evil.example")).await;
    assert_eq!(rejected, StatusCode::FORBIDDEN);
    assert_eq!(context.active_sessions(), 0);

    let (mut client, status) = handshake(address, Some("https://app.example")).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("the upgraded WebSocket should register its active session");
    shutdown_tx.send(()).unwrap();
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);

    let early_release = timeout(Duration::from_millis(100), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await;
    assert!(
        early_release.is_err(),
        "the server deadline must remain in effect before its configured timeout"
    );
    assert_eq!(context.active_sessions(), 1);
    assert_eq!(policy.active_connections(), 1);
    assert!(
        timeout(Duration::from_millis(30), &mut serving).await.is_err(),
        "serve must not report graceful completion before the peer acknowledges close"
    );

    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(context.active_sessions(), 0);
    let report = serving.await.unwrap().unwrap();
    assert!(report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
}

async fn context_ws_upgrade(
    State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    policy.on_upgrade(ws, &headers, context, |mut session| async move {
        let _ = session.recv().await;
    })
}

async fn upgrade_registration_checkpoint(
    State((policy, context, registered_tx)): State<(WsUpgradePolicy, ServerContext, RegisteredCheckpoint)>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let response = policy.on_upgrade(ws, &headers, context.clone(), |mut session| async move {
        let _ = session.recv().await;
    });
    registered_tx
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(context.active_sessions())
        .unwrap();
    response
}

async fn delayed_context_ws_upgrade(
    State((policy, context, (ready_tx, release_rx))): State<(WsUpgradePolicy, ServerContext, DelayedUpgradeChannels)>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    ready_tx.lock().unwrap().take().unwrap().send(()).unwrap();
    let release = release_rx.lock().unwrap().take().unwrap();
    release.await.unwrap();
    policy.on_upgrade(ws, &headers, context, |mut session| async move {
        let _ = session.recv().await;
    })
}

#[tokio_test]
async fn test_upgrade_is_registered_before_the_response_is_returned() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new();
    let (registered_tx, registered_rx) = oneshot::channel();
    let app = Router::new()
        .route("/ws", get(upgrade_registration_checkpoint))
        .with_state((policy, context.clone(), Arc::new(Mutex::new(Some(registered_tx)))));
    let address = server.local_addr();
    let serving = spawn(server.serve(app, pending::<()>()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(registered_rx.await.unwrap(), 1);
    assert_eq!(context.active_sessions(), 1);
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    serving.abort();
}

#[tokio_test]
async fn test_origin_and_capacity_rejections_do_not_register_sessions() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new()
        .max_connections(1)
        .allowed_origins(["https://app.example"]);
    let app = Router::new()
        .route("/ws", get(context_ws_upgrade))
        .with_state((policy, context.clone()));
    let address = server.local_addr();
    let serving = spawn(server.serve(app, pending::<()>()));

    let mut invalid_handshake = start_handshake_without_version(address).await;
    assert_eq!(
        read_response_status(&mut invalid_handshake).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(context.active_sessions(), 0);

    let (_, origin_status) = handshake(address, Some("https://evil.example")).await;
    assert_eq!(origin_status, StatusCode::FORBIDDEN);
    assert_eq!(context.active_sessions(), 0);

    let (mut client, status) = handshake(address, Some("https://app.example")).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(context.active_sessions(), 1);

    let (_, capacity_status) = handshake(address, Some("https://app.example")).await;
    assert_eq!(capacity_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(context.active_sessions(), 1);

    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    serving.abort();
}

#[tokio_test]
async fn test_upgrade_after_shutdown_starts_returns_stable_503_without_registration() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(delayed_context_ws_upgrade)).with_state((
        policy,
        context.clone(),
        (
            Arc::new(Mutex::new(Some(ready_tx))),
            Arc::new(Mutex::new(Some(release_rx))),
        ),
    ));
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let mut client = start_handshake(address, "/ws", None).await;
    ready_rx.await.unwrap();
    shutdown_tx.send(()).unwrap();
    timeout(Duration::from_secs(1), async {
        while !context.cancellation_token().is_cancelled() {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    release_tx.send(()).unwrap();

    assert_eq!(read_response_status(&mut client).await, StatusCode::SERVICE_UNAVAILABLE);
    let expected =
        br#"{"type":"about:blank","title":"Service Unavailable","status":503,"code":"server_shutting_down"}"#;
    let mut body = vec![0; expected.len()];
    client.read_exact(&mut body).await.unwrap();
    assert_eq!(body, expected);
    assert_eq!(context.active_sessions(), 0);
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_context_session_is_released_after_shutdown_deadline_without_peer_ack() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new().max_connections(1);
    let app = Router::new()
        .route("/ws", get(context_ws_upgrade))
        .with_state((policy.clone(), context.clone()));
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let mut serving = spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("the upgraded WebSocket should register its active session");

    shutdown_tx.send(()).unwrap();
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);

    assert!(
        timeout(Duration::from_millis(30), &mut serving).await.is_err(),
        "serve must wait for the managed session until its shutdown deadline"
    );
    let report = serving.await.unwrap().unwrap();
    assert!(!report.graceful);
    assert!(report.unfinished_managed_sessions >= 1);
}

#[tokio_test]
async fn test_shutdown_close_deadline_releases_connection_without_peer_ack() {
    let policy = WsUpgradePolicy::new().max_connections(1);
    let shutdown = CancellationToken::new();
    let (addr, task) = serve_with_shutdown_timeout(policy.clone(), shutdown.clone(), Duration::from_millis(30)).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    shutdown.cancel();
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    timeout(Duration::from_millis(300), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio_test]
async fn test_peer_close_is_acknowledged_before_connection_permit_is_released() {
    let policy = WsUpgradePolicy::new().max_connections(1);
    let (addr, task) = serve(policy.clone()).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_close(&mut client).await;
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    timeout(Duration::from_secs(1), async {
        while policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
}

#[tokio_test]
async fn test_ping_gets_pong_and_idle_timeout_closes_session() {
    let policy = WsUpgradePolicy::new().idle_timeout(Duration::from_millis(50));
    let (addr, task) = serve(policy).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_control(&mut client, 9, b"probe").await;
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 10);
    assert_eq!(payload, b"probe");
    let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    task.abort();
}

#[tokio_test]
async fn test_oversized_reassembled_message_is_closed_by_axum_limit() {
    let policy = WsUpgradePolicy::new().max_frame_bytes(32).max_message_bytes(4);
    let (addr, task) = serve(policy).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_frame(&mut client, false, 1, b"123").await;
    send_masked_frame(&mut client, true, 0, b"45").await;
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1009);
    task.abort();
}

#[tokio_test]
async fn test_oversized_single_frame_is_rejected_even_when_message_limit_is_larger() {
    let policy = WsUpgradePolicy::new().max_frame_bytes(4).max_message_bytes(16);
    let (addr, task) = serve(policy).await;
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, b"12345").await;
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1009);
    task.abort();
}
