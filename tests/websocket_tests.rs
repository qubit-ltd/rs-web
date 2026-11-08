// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::future::pending;
use std::io;
use std::io::ErrorKind;
use std::io::Write;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::Message;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use qubit_web::ServerContext;
use qubit_web::ServerOptions;
use qubit_web::ShutdownReport;
use qubit_web::WebServer;
use qubit_web::mvc::ControllerRoutes;
use qubit_web::rest_controller;

type RegisteredCheckpoint = Arc<Mutex<Option<oneshot::Sender<usize>>>>;
type WebSocketTestState = (
    WsUpgradePolicy,
    ServerContext,
    Arc<AtomicBool>,
    Arc<Mutex<Option<oneshot::Sender<()>>>>,
);
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
type ClosedFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type PeerClosedWaiterTestState = (
    WsUpgradePolicy,
    ServerContext,
    Arc<Mutex<Option<oneshot::Sender<ClosedFuture>>>>,
    Arc<Mutex<Option<oneshot::Receiver<()>>>>,
    Arc<Mutex<Option<oneshot::Sender<Option<WsTermination>>>>>,
);
type ClosedWaiterTestState = (
    WsUpgradePolicy,
    ServerContext,
    Arc<Mutex<Option<oneshot::Sender<ClosedFuture>>>>,
    Arc<Mutex<Option<oneshot::Sender<Option<WsTermination>>>>>,
);
type CloseNotificationTestState = (WsUpgradePolicy, ServerContext, Arc<Mutex<Option<oneshot::Sender<()>>>>);
type CloseOnlyTestState = (WsUpgradePolicy, ServerContext, Arc<Mutex<Option<oneshot::Sender<()>>>>);

struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
use qubit_web::diagnostic::DiagnosticLayer;
use qubit_web::ws::WsPolicyError;
use qubit_web::ws::WsSendError;
use qubit_web::ws::WsTermination;
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
use tokio::time::Instant;
use tokio::time::timeout;
use tokio::time::timeout_at;
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

struct ControllerUpgrade;

#[rest_controller("/controller")]
impl ControllerUpgrade {
    #[route("/ws", method = "GET", kind = "ws")]
    async fn upgrade(&self, ws: WebSocketUpgrade) -> Response {
        ws.on_upgrade(|mut socket| async move {
            if let Some(Ok(Message::Text(text))) = socket.recv().await {
                socket.send(Message::Text(text)).await.unwrap();
            }
        })
        .into_response()
    }
}

#[tokio::test]
async fn test_controller_stream_admission_preserves_native_websocket_upgrade() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let shutdown = CancellationToken::new();
    let app = ControllerRoutes::<()>::new()
        .add(Arc::new(ControllerUpgrade))
        .unwrap()
        .finish();
    let task_shutdown = shutdown.clone();
    let task = spawn(async move { server.serve(app, task_shutdown.cancelled_owned()).await.unwrap() });

    let (mut client, status) = handshake_path(address, "/controller/ws", None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, b"admission-upgrade").await;
    assert_eq!(read_server_text(&mut client).await, b"admission-upgrade");
    drop(client);
    shutdown.cancel();
    assert!(timeout(Duration::from_secs(2), task).await.unwrap().unwrap().graceful);
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
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut stream = start_handshake_until(addr, path, origin, deadline)
        .await
        .unwrap_or_else(|error| panic!("WebSocket handshake {} failed: {error}", error.kind()));
    let status = read_response_status_until(&mut stream, deadline)
        .await
        .unwrap_or_else(|error| panic!("WebSocket handshake response headers failed: {error}"));
    (stream, status)
}

async fn handshake_with_origins(addr: SocketAddr, origins: &[&str]) -> (TcpStream, StatusCode) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut stream = timeout_at(deadline, TcpStream::connect(addr)).await.unwrap().unwrap();
    let mut request = format!(
        "GET /ws HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    for origin in origins {
        request.push_str(&format!("Origin: {origin}\r\n"));
    }
    request.push_str("\r\n");
    timeout_at(deadline, stream.write_all(request.as_bytes()))
        .await
        .unwrap()
        .unwrap();
    let status = read_response_status_until(&mut stream, deadline).await.unwrap();
    (stream, status)
}

async fn start_handshake(addr: SocketAddr, path: &str, origin: Option<&str>) -> TcpStream {
    start_handshake_until(addr, path, origin, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_or_else(|error| panic!("WebSocket handshake {} failed: {error}", error.kind()))
}

async fn start_handshake_until(
    addr: SocketAddr,
    path: &str,
    origin: Option<&str>,
    deadline: Instant,
) -> io::Result<TcpStream> {
    let mut stream = timeout_at(deadline, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, "connect deadline elapsed"))?
        .map_err(|error| io::Error::new(error.kind(), format!("connect failed: {error}")))?;
    let mut request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    );
    if let Some(origin) = origin {
        request.push_str(&format!("Origin: {origin}\r\n"));
    }
    request.push_str("\r\n");
    timeout_at(deadline, stream.write_all(request.as_bytes()))
        .await
        .map_err(|_| io::Error::new(ErrorKind::TimedOut, "write deadline elapsed"))?
        .map_err(|error| io::Error::new(error.kind(), format!("write failed: {error}")))?;
    Ok(stream)
}

async fn start_handshake_without_version(addr: SocketAddr) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut stream = timeout_at(deadline, TcpStream::connect(addr))
        .await
        .unwrap_or_else(|_| panic!("WebSocket handshake connect timed out"))
        .unwrap_or_else(|error| panic!("WebSocket handshake connect failed: {error}"));
    let request = format!(
        "GET /ws HTTP/1.1\r\nHost: {addr}\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    );
    timeout_at(deadline, stream.write_all(request.as_bytes()))
        .await
        .unwrap_or_else(|_| panic!("WebSocket handshake write timed out"))
        .unwrap_or_else(|error| panic!("WebSocket handshake write failed: {error}"));
    stream
}

async fn read_response_status(stream: &mut TcpStream) -> StatusCode {
    read_response_status_until(stream, Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_or_else(|error| panic!("WebSocket response headers failed: {error}"))
}

async fn read_response_status_until(stream: &mut TcpStream, deadline: Instant) -> io::Result<StatusCode> {
    let response = timeout_at(deadline, async {
        let mut response = Vec::new();
        let mut buf = [0; 1];
        while !response.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut buf).await?;
            response.push(buf[0]);
        }
        io::Result::Ok(response)
    })
    .await
    .map_err(|_| io::Error::new(ErrorKind::TimedOut, "response headers deadline elapsed"))??;
    let first_line = String::from_utf8_lossy(&response);
    let status = first_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::new(ErrorKind::InvalidData, "missing response status"))?
        .parse::<u16>()
        .map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid response status"))?;
    StatusCode::from_u16(status).map_err(|_| io::Error::new(ErrorKind::InvalidData, "invalid HTTP status"))
}

#[tokio::test]
async fn test_handshake_headers_timeout() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(tokio::sync::Notify::new());
    let accepted_signal = accepted.clone();
    let peer = spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        accepted_signal.notify_one();
        let _stream = stream;
        std::future::pending::<()>().await;
    });
    let mut stream = start_handshake(address, "/silent", None).await;
    timeout(Duration::from_secs(1), accepted.notified()).await.unwrap();
    let result = read_response_status_until(&mut stream, Instant::now() + Duration::from_millis(100)).await;
    assert_eq!(result.unwrap_err().kind(), ErrorKind::TimedOut);
    peer.abort();
    let _ = peer.await;
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
async fn test_aborting_serve_releases_managed_websocket() {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new();
    let dropped = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = oneshot::channel();
    let ready = Arc::new(Mutex::new(Some(ready_tx)));
    async fn upgrade(
        State((policy, context, dropped, ready)): State<WebSocketTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |mut session| async move {
            struct DropFlag(Arc<AtomicBool>);
            impl Drop for DropFlag {
                fn drop(&mut self) {
                    self.0.store(true, Ordering::SeqCst);
                }
            }
            let _drop_flag = DropFlag(dropped);
            if let Some(sender) = ready.lock().unwrap().take() {
                let _ = sender.send(());
            }
            while let Some(Ok(Message::Text(text))) = session.recv().await {
                if session.try_send(Message::text(text)).is_err() {
                    break;
                }
            }
        })
    }
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((policy, context.clone(), dropped.clone(), ready));
    let address = server.local_addr();
    let serving = spawn(async move { server.serve(app, pending::<()>()).await.unwrap() });
    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(1), ready_rx)
        .await
        .expect("managed WebSocket handler should start")
        .expect("handler ready signal should be delivered");
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("upgraded session should be registered");
    send_masked_text(&mut client, b"before-abort").await;
    assert_eq!(read_server_text(&mut client).await, b"before-abort");

    serving.abort();
    let _ = serving.await;
    let cancelled = context.cancellation_token().is_cancelled();
    let post_abort_frame = timeout(Duration::from_millis(150), async {
        send_masked_text(&mut client, b"after-abort").await;
        read_server_frame(&mut client).await
    })
    .await
    .ok();
    let business_still_available = post_abort_frame
        .as_ref()
        .is_some_and(|(opcode, payload)| *opcode == 1 && payload == b"after-abort");
    let close_was_read = post_abort_frame.as_ref().is_some_and(|(opcode, _)| *opcode == 8);
    if !close_was_read {
        send_masked_close(&mut client).await;
        let _ = timeout(Duration::from_millis(250), read_server_frame(&mut client)).await;
    }
    let cleanup = timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 0 || !dropped.load(Ordering::SeqCst) {
            yield_now().await;
        }
    })
    .await
    .is_ok();
    assert!(
        cancelled,
        "aborting serve should cancel the server context (business response after abort: {business_still_available}, handler/session cleanup: {cleanup}, close frame: {close_was_read})"
    );
    assert!(
        !business_still_available,
        "managed WebSocket should stop providing business responses after abort"
    );
    assert!(
        cleanup,
        "managed WebSocket handler and session registration should be released after abort"
    );
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
        Err(WsSendError::MessageTooLarge)
    );
    byte_limited.try_send(Message::text("123")).unwrap();
    assert_eq!(
        byte_limited.try_send(Message::text("x")),
        Err(WsSendError::Backpressure)
    );
}

#[tokio_test]
async fn test_outbound_control_frame_boundaries() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        uri: axum::http::Uri,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |session| async move {
            if uri.path() == "/close" {
                assert_eq!(
                    session.try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1000,
                        reason: "x".repeat(124).into(),
                    }))),
                    Err(WsSendError::MessageTooLarge)
                );
                session
                    .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 1000,
                        reason: "x".repeat(123).into(),
                    })))
                    .unwrap();
                session.closed().await;
                return;
            }
            assert_eq!(
                session.try_send(Message::Ping(vec![0; 126].into())),
                Err(WsSendError::MessageTooLarge)
            );
            assert_eq!(
                session.try_send(Message::Pong(vec![0; 126].into())),
                Err(WsSendError::MessageTooLarge)
            );
            session.try_send(Message::Ping(vec![1; 125].into())).unwrap();
            session.try_send(Message::Pong(vec![2; 125].into())).unwrap();
            session.try_send(Message::text("control-frame-sentinel")).unwrap();
            session.closed().await;
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let app = Router::new()
        .route("/{*path}", get(upgrade))
        .with_state((WsUpgradePolicy::new(), server.context()));
    let serving = AbortOnDrop(spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    ));

    let (mut peer, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    for (expected_opcode, expected_byte) in [(9, 1), (10, 2)] {
        let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
            .await
            .expect("legal boundary control frame should arrive");
        assert_eq!(opcode, expected_opcode);
        assert_eq!(payload.len(), 125);
        assert!(payload.iter().all(|byte| *byte == expected_byte));
    }
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
        .await
        .expect("session should continue with the text sentinel");
    assert_eq!(opcode, 1);
    assert_eq!(payload, b"control-frame-sentinel");
    send_masked_close(&mut peer).await;
    drop(peer);

    let (mut peer, status) = handshake_path(address, "/close", None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
        .await
        .expect("legal Close boundary should be transmitted");
    assert_eq!(opcode, 8);
    assert_eq!(payload.len(), 125);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1000);
    assert_eq!(&payload[2..], vec![b'x'; 123].as_slice());
    send_masked_close(&mut peer).await;
    drop(peer);
    drop(serving);
}

#[tokio_test]
async fn test_invalid_close_code_does_not_close_websocket_writer() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |session| async move {
            assert_eq!(
                session.try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1005,
                    reason: "private-close-reason".into(),
                }))),
                Err(WsSendError::InvalidCloseCode)
            );
            session.try_send(Message::text("writer-remains-open")).unwrap();
            session
                .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                    code: 1000,
                    reason: "legal-close".into(),
                })))
                .unwrap();
            session.closed().await;
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((WsUpgradePolicy::new(), server.context()));
    let serving = AbortOnDrop(spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    ));

    let (mut peer, status) = handshake_path(address, "/ws", None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
        .await
        .expect("writer should send a text message after rejecting invalid Close");
    assert_eq!(opcode, 1);
    assert_eq!(payload, b"writer-remains-open");
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
        .await
        .expect("writer should send the subsequent legal Close");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1000);
    assert_eq!(&payload[2..], b"legal-close");
    send_masked_close(&mut peer).await;
    drop(peer);
    drop(serving);
}

#[test]
fn test_ws_errors_implement_standard_error() {
    fn invalid_policy() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        WsUpgradePolicy::new().max_connections(0).validate()?;
        Ok(())
    }

    fn full_queue() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let queue = qubit_web::WsSendQueue::new(0, 1024);
        queue.try_send(Message::text("private-payload"))?;
        Ok(())
    }

    fn oversized_message() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let queue = qubit_web::WsSendQueue::new(1, 0);
        queue.try_send(Message::text("private-payload"))?;
        Ok(())
    }

    let policy_error = invalid_policy().unwrap_err();
    assert_eq!(policy_error.to_string(), "WebSocket policy limits must be positive");
    assert!(!policy_error.to_string().contains("private-payload"));
    assert!(std::error::Error::source(policy_error.as_ref()).is_none());
    assert_eq!(
        policy_error.downcast_ref::<WsPolicyError>(),
        Some(&WsPolicyError::ZeroLimit)
    );

    let send_error = full_queue().unwrap_err();
    assert_eq!(send_error.to_string(), "WebSocket send queue capacity exceeded");
    assert!(!send_error.to_string().contains("private-payload"));
    assert!(std::error::Error::source(send_error.as_ref()).is_none());
    assert_eq!(
        send_error.downcast_ref::<WsSendError>(),
        Some(&WsSendError::Backpressure)
    );
    assert_eq!(
        WsPolicyError::ConnectionLimitTooLarge.to_string(),
        "WebSocket connection limit exceeds the supported maximum"
    );
    assert_eq!(
        WsPolicyError::InboundQueueLimitTooLarge.to_string(),
        "WebSocket inbound queue limit exceeds the supported maximum"
    );
    assert_eq!(WsSendError::Closed.to_string(), "WebSocket send queue is closed");
    assert_eq!(
        WsSendError::MessageTooLarge.to_string(),
        "WebSocket message exceeds the outbound limit"
    );
    assert_eq!(
        WsSendError::InvalidCloseCode.to_string(),
        "WebSocket close code is invalid for a server"
    );
    let too_large = oversized_message().unwrap_err();
    assert_eq!(too_large.to_string(), "WebSocket message exceeds the outbound limit");
    assert!(std::error::Error::source(too_large.as_ref()).is_none());
    assert_eq!(
        too_large.downcast_ref::<WsSendError>(),
        Some(&WsSendError::MessageTooLarge)
    );
}

#[test]
fn test_zero_byte_queue_payload_boundaries() {
    let empty_messages = [
        Message::text(""),
        Message::Binary(Vec::new().into()),
        Message::Ping(Vec::new().into()),
        Message::Pong(Vec::new().into()),
        Message::Close(None),
    ];
    for message in empty_messages {
        let queue = qubit_web::WsSendQueue::new(1, 0);
        assert_eq!(queue.try_send(message), Ok(()));
        assert_eq!(queue.len(), 1);
    }
    assert_eq!(
        qubit_web::WsSendQueue::new(1, 0).try_send(Message::text("x")),
        Err(WsSendError::MessageTooLarge)
    );
    assert_eq!(
        qubit_web::WsSendQueue::new(0, 0).try_send(Message::text("")),
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
    assert!(WsUpgradePolicy::new().inbound_queue_limits(0, 1).validate().is_err());
    assert!(WsUpgradePolicy::new().inbound_queue_limits(1, 0).validate().is_err());
    assert!(WsUpgradePolicy::new().idle_timeout(Duration::ZERO).validate().is_err());
}

#[test]
fn test_corrected_policy_limits_are_valid() {
    assert_eq!(
        WsUpgradePolicy::new().max_connections(0).max_connections(1).validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new().max_frame_bytes(0).max_frame_bytes(1).validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new()
            .max_message_bytes(0)
            .max_message_bytes(1)
            .validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new().queue_limits(0, 1).queue_limits(1, 1).validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new().queue_limits(1, 0).queue_limits(1, 1).validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new()
            .inbound_queue_limits(0, 0)
            .inbound_queue_limits(1, 1)
            .validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new()
            .idle_timeout(Duration::ZERO)
            .idle_timeout(Duration::from_secs(1))
            .validate(),
        Ok(())
    );
    assert_eq!(
        WsUpgradePolicy::new().idle_timeout(Duration::MAX).validate(),
        Err(WsPolicyError::IdleTimeoutTooLarge)
    );
    assert_eq!(
        WsUpgradePolicy::new()
            .idle_timeout(Duration::MAX)
            .idle_timeout(Duration::from_millis(10))
            .validate(),
        Ok(())
    );

    assert_eq!(
        WsUpgradePolicy::new()
            .max_message_bytes(0)
            .queue_limits(0, 1)
            .max_message_bytes(1)
            .validate(),
        Err(WsPolicyError::ZeroLimit)
    );

    let too_many = tokio::sync::Semaphore::MAX_PERMITS + 1;
    let policy = WsUpgradePolicy::new().max_connections(too_many);
    assert_eq!(policy.validate(), Err(WsPolicyError::ConnectionLimitTooLarge));
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(
        WsUpgradePolicy::new()
            .inbound_queue_limits(tokio::sync::Semaphore::MAX_PERMITS + 1, 1)
            .validate(),
        Err(WsPolicyError::InboundQueueLimitTooLarge)
    );
    assert_eq!(
        WsUpgradePolicy::new()
            .inbound_queue_limits(tokio::sync::Semaphore::MAX_PERMITS, 1)
            .validate(),
        Ok(())
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_read_error_is_delivered_once() {
    type ResultState = Arc<Mutex<Option<oneshot::Sender<(bool, bool, bool, Option<WsTermination>)>>>>;

    async fn upgrade(
        State((policy, context, result_tx)): State<(WsUpgradePolicy, ServerContext, ResultState)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |mut session| async move {
            let first_is_error = matches!(session.recv().await, Some(Err(_)));
            let second_is_none = session.recv().await.is_none();
            session.close().await;
            let after_close_is_none = session.recv().await.is_none();
            let _ = result_tx.lock().unwrap().take().unwrap().send((
                first_is_error,
                second_is_none,
                after_close_is_none,
                session.termination(),
            ));
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let (result_tx, result_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        WsUpgradePolicy::new(),
        context,
        Arc::new(Mutex::new(Some(result_tx))),
    ));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, &[0xff]).await;
    let (opcode, _) = timeout(Duration::from_secs(2), read_server_frame(&mut client))
        .await
        .expect("server should close after invalid UTF-8");
    assert_eq!(opcode, 8);
    send_masked_close(&mut client).await;
    let result = timeout(Duration::from_secs(2), result_rx)
        .await
        .expect("handler should report read error delivery")
        .expect("handler result should be sent");
    assert_eq!(result, (true, true, true, Some(WsTermination::ReadError)));
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_inbound_byte_budget_closes_connection() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |session| async move {
            session.closed().await;
            assert_eq!(session.termination(), Some(WsTermination::InboundBackpressure));
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().inbound_queue_limits(4, 6);
    let app = Router::new().route("/ws", get(upgrade)).with_state((policy, context));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, b"four").await;
    send_masked_text(&mut client, b"four").await;
    let (opcode, payload) = timeout(Duration::from_secs(2), read_server_frame(&mut client))
        .await
        .expect("byte budget should close the connection");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1013);
    send_masked_close(&mut client).await;
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_inbound_utf8_budget_counts_payload_bytes() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |session| async move {
            session.closed().await;
            assert_eq!(session.termination(), Some(WsTermination::InboundBackpressure));
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().inbound_queue_limits(4, 5);
    let app = Router::new().route("/ws", get(upgrade)).with_state((policy, context));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, "你".as_bytes()).await;
    send_masked_text(&mut client, "好".as_bytes()).await;
    let (opcode, payload) = timeout(Duration::from_secs(2), read_server_frame(&mut client))
        .await
        .expect("two three-byte characters should exceed a five-byte budget");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1013);
    send_masked_close(&mut client).await;
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_inbound_utf8_budget_accepts_exact_byte_limit() {
    let policy = WsUpgradePolicy::new().inbound_queue_limits(4, 6);
    let (address, serving) = serve(policy).await;
    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);

    send_masked_text(&mut client, "你好".as_bytes()).await;
    assert_eq!(
        timeout(Duration::from_secs(2), read_server_text(&mut client))
            .await
            .expect("six-byte UTF-8 payload should fit its exact budget"),
        "你好".as_bytes()
    );

    client.shutdown().await.unwrap();
    serving.abort();
}

#[tokio::test]
async fn test_inbound_reassembled_message_uses_aggregate_budget() {
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
    let policy = WsUpgradePolicy::new().max_message_bytes(16).inbound_queue_limits(4, 6);
    let app = Router::new().route("/ws", get(upgrade)).with_state((policy, context));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_frame(&mut client, false, 1, b"four").await;
    send_masked_frame(&mut client, true, 0, b"four").await;
    let (opcode, payload) = timeout(Duration::from_secs(2), read_server_frame(&mut client))
        .await
        .expect("reassembled eight-byte message should exceed the six-byte budget");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1013);
    send_masked_close(&mut client).await;
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_inbound_budget_is_released_after_receive() {
    async fn upgrade(
        State((policy, context, received_tx)): State<(
            WsUpgradePolicy,
            ServerContext,
            tokio::sync::mpsc::UnboundedSender<String>,
        )>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |mut session| async move {
            while let Some(Ok(Message::Text(text))) = session.recv().await {
                let _ = received_tx.send(text.to_string());
            }
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().inbound_queue_limits(4, 4);
    let (received_tx, mut received_rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((policy, context, received_tx));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    send_masked_text(&mut client, b"four").await;
    assert_eq!(
        timeout(Duration::from_secs(2), received_rx.recv())
            .await
            .expect("handler should receive first message"),
        Some("four".to_owned())
    );
    send_masked_text(&mut client, b"next").await;
    assert_eq!(
        timeout(Duration::from_secs(2), received_rx.recv())
            .await
            .expect("handler should receive second message after budget release"),
        Some("next".to_owned())
    );
    send_masked_close(&mut client).await;
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
}

#[tokio::test]
async fn test_shutdown_releases_binary_inbound_budget_and_capacity() {
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

    let policy = WsUpgradePolicy::new().inbound_queue_limits(1, 8);
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let app = Router::new()
        .route("/ws", get(upgrade))
        .with_state((policy.clone(), context.clone()));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(1), async {
        while context.active_sessions() != 1 {
            yield_now().await;
        }
    })
    .await
    .expect("upgraded session should register");
    send_masked_frame(&mut client, true, 2, b"four").await;
    send_masked_frame(&mut client, true, 2, b"next").await;
    for _ in 0..8 {
        yield_now().await;
    }

    shutdown.cancel();
    let _report = timeout(Duration::from_secs(2), serving)
        .await
        .expect("host shutdown should finish within its bound")
        .unwrap()
        .unwrap();
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);
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
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .unwrap();
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
    assert_eq!(&payload[2..], b"server shutdown");
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
async fn test_peer_close_notifies_owned_waiter_and_releases_single_capacity() {
    async fn upgrade(
        State((policy, context, waiter_tx, release_rx, termination_tx)): State<PeerClosedWaiterTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        let waiter_tx = waiter_tx.clone();
        let release = release_rx.lock().unwrap().take();
        policy.on_upgrade(ws, &headers, context, move |mut session| async move {
            let waiter_tx = { waiter_tx.lock().unwrap().take() };
            if let Some(waiter_tx) = waiter_tx {
                assert!(waiter_tx.send(Box::pin(session.closed())).is_ok());
            }
            if let Some(release) = release {
                let _ = release.await;
                session.close().await;
                if let Some(tx) = termination_tx.lock().unwrap().take() {
                    let _ = tx.send(session.termination());
                }
            } else {
                pending::<()>().await;
            }
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().max_connections(1).inbound_queue_limits(1, 1024);
    let (waiter_tx, waiter_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (termination_tx, termination_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        policy.clone(),
        context.clone(),
        Arc::new(Mutex::new(Some(waiter_tx))),
        Arc::new(Mutex::new(Some(release_rx))),
        Arc::new(Mutex::new(Some(termination_tx))),
    ));
    let serving = spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );

    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let ended = timeout(Duration::from_secs(1), waiter_rx)
        .await
        .expect("handler should publish an owned waiter")
        .unwrap();
    send_masked_text(&mut client, b"queued payload").await;
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), ended)
        .await
        .expect("peer Close should release both I/O resources");
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);

    let (_second_client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    release_tx.send(()).unwrap();
    assert_eq!(
        timeout(Duration::from_secs(1), termination_rx)
            .await
            .expect("handler should observe the frozen peer-close result")
            .unwrap(),
        Some(WsTermination::PeerClose { code: Some(1000) })
    );
    serving.abort();
}

#[tokio_test]
async fn test_idle_timeout_notifies_closed_waiter() {
    async fn upgrade(
        State((policy, context, notify)): State<CloseNotificationTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |mut session| async move {
            while session.recv().await.is_some() {}
            session.closed().await;
            assert_eq!(session.termination(), Some(WsTermination::IdleTimeout));
            if let Some(notify) = notify.lock().unwrap().take() {
                let _ = notify.send(());
            }
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let address = server.local_addr();
    let policy = WsUpgradePolicy::new().idle_timeout(Duration::from_millis(40));
    let (notify_tx, notify_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        policy,
        server.context(),
        Arc::new(Mutex::new(Some(notify_tx))),
    ));
    let serving = spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );
    let (mut client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .expect("idle timeout should send a Close frame");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
    assert_eq!(&payload[2..], b"idle timeout");
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), notify_rx)
        .await
        .expect("idle I/O termination should notify the handler")
        .unwrap();
    serving.abort();
}

#[tokio_test]
async fn test_server_shutdown_notifies_owned_closed_waiter() {
    async fn upgrade(
        State((policy, context, waiter_tx, termination_tx)): State<ClosedWaiterTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |session| async move {
            let waiter_tx = { waiter_tx.lock().unwrap().take() };
            if let Some(waiter_tx) = waiter_tx {
                let _ = waiter_tx.send(Box::pin(session.closed()));
            }
            tokio::spawn(async move {
                session.closed().await;
                if let Some(tx) = termination_tx.lock().unwrap().take() {
                    let _ = tx.send(session.termination());
                }
            });
        })
    }

    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let addr = server.local_addr();
    let context = server.context();
    let shutdown = CancellationToken::new();
    let (waiter_tx, waiter_rx) = oneshot::channel();
    let (termination_tx, termination_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        WsUpgradePolicy::new().max_connections(1),
        context,
        Arc::new(Mutex::new(Some(waiter_tx))),
        Arc::new(Mutex::new(Some(termination_tx))),
    ));
    let serving = spawn(
        server
            .serve(app, shutdown.clone().cancelled_owned())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );
    let (mut client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    let ended = timeout(Duration::from_secs(1), waiter_rx)
        .await
        .expect("handler should publish an owned waiter")
        .unwrap();
    shutdown.cancel();
    let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut client))
        .await
        .expect("server shutdown should send a Close frame");
    assert_eq!(opcode, 8);
    assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 1001);
    assert_eq!(&payload[2..], b"server shutdown");
    send_masked_close(&mut client).await;
    timeout(Duration::from_secs(1), ended)
        .await
        .expect("server shutdown should release both I/O resources");
    assert_eq!(
        timeout(Duration::from_secs(1), termination_rx)
            .await
            .expect("handler task should read the frozen termination snapshot")
            .unwrap(),
        Some(WsTermination::ServerShutdown)
    );
    let _report = timeout(Duration::from_secs(1), serving)
        .await
        .expect("server shutdown should finish")
        .unwrap()
        .unwrap();
}

#[tokio_test]
async fn test_peer_transport_error_notifies_closed_waiter() {
    async fn upgrade(
        State((policy, context, notify)): State<CloseNotificationTestState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, move |session| async move {
            session.closed().await;
            let notify = { notify.lock().unwrap().take() };
            if let Some(notify) = notify {
                let _ = notify.send(());
            }
        })
    }

    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let addr = server.local_addr();
    let (notify_tx, notify_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        WsUpgradePolicy::new().idle_timeout(Duration::from_secs(30)),
        server.context(),
        Arc::new(Mutex::new(Some(notify_tx))),
    ));
    let serving = spawn(
        server
            .serve(app, pending::<()>())
            .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
    );
    let (client, status) = handshake(addr, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    drop(client);
    timeout(Duration::from_secs(1), notify_rx)
        .await
        .expect("peer transport failure should release both I/O resources")
        .unwrap();
    serving.abort();
}

#[tokio_test]
async fn test_close_only_peer_without_ack_releases_session_within_budget() {
    timeout(Duration::from_secs(3), async {
        async fn upgrade(
            State((policy, context, closed_tx)): State<CloseOnlyTestState>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            policy.on_upgrade(ws, &headers, context, move |session| async move {
                session
                    .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 4001,
                        reason: "close-only probe".into(),
                    })))
                    .expect("custom Close should be queued");
                session.closed().await;
                if let Some(closed_tx) = closed_tx.lock().unwrap().take() {
                    let _ = closed_tx.send(());
                }
            })
        }

        let server = WebServer::bind_http(
            ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("test server should bind");
        let address = server.local_addr();
        let context = server.context();
        let policy = WsUpgradePolicy::new()
            .idle_timeout(Duration::from_secs(60))
            .max_connections(1);
        let (closed_tx, closed_rx) = oneshot::channel();
        let app = Router::new().route("/ws", get(upgrade)).with_state((
            policy.clone(),
            context.clone(),
            Arc::new(Mutex::new(Some(closed_tx))),
        ));
        let serving = AbortOnDrop(spawn(
            server
                .serve(app, pending::<()>())
                .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
        ));
        let (mut peer, status) = handshake(address, None).await;
        assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
        let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
            .await
            .expect("server should send the queued Close frame");
        assert_eq!(opcode, 8);
        assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 4001);
        assert_eq!(&payload[2..], b"close-only probe");

        timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("Close-only should converge within the configured shutdown budget")
            .expect("closed result sender should remain available");
        timeout(Duration::from_secs(1), async {
            while policy.active_connections() != 0 || context.active_sessions() != 0 {
                yield_now().await;
            }
        })
        .await
        .expect("Close-only should release both managed counts while the peer remains open");
        drop(peer);
        drop(serving);
    })
    .await
    .expect("Close-only experiment must finish within three seconds");
}

#[tokio_test]
async fn test_close_only_explicit_close_comparator_releases_session_within_budget() {
    timeout(Duration::from_secs(3), async {
        async fn upgrade(
            State((policy, context, closed_tx)): State<CloseOnlyTestState>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            policy.on_upgrade(ws, &headers, context, move |mut session| async move {
                session.close().await;
                if let Some(closed_tx) = closed_tx.lock().unwrap().take() {
                    let _ = closed_tx.send(());
                }
            })
        }

        let server = WebServer::bind_http(
            ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("test server should bind");
        let address = server.local_addr();
        let context = server.context();
        let policy = WsUpgradePolicy::new().idle_timeout(Duration::from_secs(60));
        let (closed_tx, closed_rx) = oneshot::channel();
        let app = Router::new().route("/ws", get(upgrade)).with_state((
            policy.clone(),
            context.clone(),
            Arc::new(Mutex::new(Some(closed_tx))),
        ));
        let serving = AbortOnDrop(spawn(
            server
                .serve(app, pending::<()>())
                .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
        ));
        let (mut peer, status) = handshake(address, None).await;
        assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
        let (opcode, _) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
            .await
            .expect("explicit close should send a Close frame");
        assert_eq!(opcode, 8);
        timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("explicit close should converge without a peer acknowledgement")
            .expect("closed result sender should remain available");
        timeout(Duration::from_secs(1), async {
            while policy.active_connections() != 0 || context.active_sessions() != 0 {
                yield_now().await;
            }
        })
        .await
        .expect("explicit close should release both managed counts");
        drop(peer);
        drop(serving);
    })
    .await
    .expect("explicit close comparator must finish within three seconds");
}

#[tokio_test]
async fn test_close_only_peer_acknowledgement_releases_session() {
    timeout(Duration::from_secs(3), async {
        async fn upgrade(
            State((policy, context, closed_tx)): State<CloseOnlyTestState>,
            headers: HeaderMap,
            ws: WebSocketUpgrade,
        ) -> Response {
            policy.on_upgrade(ws, &headers, context, move |session| async move {
                session
                    .try_send(Message::Close(Some(axum::extract::ws::CloseFrame {
                        code: 4002,
                        reason: "ack probe".into(),
                    })))
                    .expect("custom Close should be queued");
                session.closed().await;
                if let Some(closed_tx) = closed_tx.lock().unwrap().take() {
                    let _ = closed_tx.send(());
                }
            })
        }

        let server = WebServer::bind_http(
            ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("test server should bind");
        let address = server.local_addr();
        let context = server.context();
        let policy = WsUpgradePolicy::new().idle_timeout(Duration::from_secs(60));
        let (closed_tx, closed_rx) = oneshot::channel();
        let app = Router::new().route("/ws", get(upgrade)).with_state((
            policy.clone(),
            context.clone(),
            Arc::new(Mutex::new(Some(closed_tx))),
        ));
        let serving = AbortOnDrop(spawn(
            server
                .serve(app, pending::<()>())
                .with_subscriber(tracing::dispatcher::get_default(Clone::clone)),
        ));
        let (mut peer, status) = handshake(address, None).await;
        assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
        let (opcode, payload) = timeout(Duration::from_secs(1), read_server_frame(&mut peer))
            .await
            .expect("writer should send the queued Close frame");
        assert_eq!(opcode, 8);
        assert_eq!(u16::from_be_bytes([payload[0], payload[1]]), 4002);
        assert_eq!(&payload[2..], b"ack probe");
        send_masked_close(&mut peer).await;
        timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("peer acknowledgement should finish the close handshake")
            .expect("closed result sender should remain available");
        assert_eq!(policy.active_connections(), 0);
        assert_eq!(context.active_sessions(), 0);
        drop(peer);
        drop(serving);
    })
    .await
    .expect("acknowledged Close experiment must finish within three seconds");
}

#[tokio_test]
async fn test_inbound_backpressure_closes_with_try_again_later() {
    async fn upgrade(
        State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |session| async move {
            session.closed().await;
            assert_eq!(session.termination(), Some(WsTermination::InboundBackpressure));
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
async fn test_duplicate_origin_headers_are_rejected_before_registration() {
    let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap()))
        .await
        .unwrap();
    let context = server.context();
    let policy = WsUpgradePolicy::new().allowed_origins(["https://app.example"]);
    let app = Router::new()
        .route("/ws", get(context_ws_upgrade))
        .with_state((policy.clone(), context.clone()));
    let address = server.local_addr();
    let serving = spawn(server.serve(app, pending::<()>()));

    let (_, rejected_status) = handshake(address, Some("https://evil.example")).await;
    assert_eq!(rejected_status, StatusCode::FORBIDDEN);
    assert_eq!(context.active_sessions(), 0);
    assert_eq!(policy.active_connections(), 0);

    for origins in [
        ["https://app.example", "https://evil.example"],
        ["https://evil.example", "https://app.example"],
        ["https://app.example", "https://app.example"],
    ] {
        let (_, status) = handshake_with_origins(address, &origins).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "origins: {origins:?}");
        assert_eq!(context.active_sessions(), 0);
        assert_eq!(policy.active_connections(), 0);
    }

    let (_no_origin_client, no_origin_status) = handshake_with_origins(address, &[]).await;
    assert_eq!(no_origin_status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(context.active_sessions(), 1);
    assert_eq!(policy.active_connections(), 1);

    let (_allowed_client, allowed_status) = handshake(address, Some("https://app.example")).await;
    assert_eq!(allowed_status, StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(context.active_sessions(), 2);
    assert_eq!(policy.active_connections(), 2);

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
async fn test_full_application_queue_shutdown_releases_resources() {
    #[derive(Clone)]
    struct StalledWriterState {
        policy: WsUpgradePolicy,
        context: ServerContext,
        ready_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    }

    async fn upgrade(
        State(StalledWriterState {
            policy,
            context,
            ready_tx,
        }): State<StalledWriterState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |session| async move {
            let payload = vec![0; 64 * 1024];
            for _ in 0..1024 {
                match session.try_send(Message::binary(payload.clone())) {
                    Ok(()) => {}
                    Err(WsSendError::Backpressure) => {
                        ready_tx.lock().unwrap().take().unwrap().send(()).unwrap();
                        pending::<()>().await;
                    }
                    Err(error) => panic!("unexpected WebSocket send error: {error:?}"),
                }
            }
            ready_tx.lock().unwrap().take().unwrap().send(()).unwrap();
            pending::<()>().await;
        })
    }

    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(250)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new()
        .max_connections(1)
        .queue_limits(64, 4 * 1024 * 1024);
    let (ready_tx, ready_rx) = oneshot::channel();
    let ready_tx = Arc::new(Mutex::new(Some(ready_tx)));
    let app = Router::new().route("/ws", get(upgrade)).with_state(StalledWriterState {
        policy: policy.clone(),
        context: context.clone(),
        ready_tx,
    });
    let shutdown = CancellationToken::new();
    let mut serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (_client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("handler should observe a full application queue without client reads")
        .expect("handler should report the application queue checkpoint");

    shutdown.cancel();
    let _report = timeout(Duration::from_secs(2), &mut serving)
        .await
        .expect("server should stop after its bounded shutdown deadline")
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while context.active_sessions() != 0 || policy.active_connections() != 0 {
            yield_now().await;
        }
    })
    .await
    .expect("shutdown should release the managed session and connection permit");
    assert_eq!(context.active_sessions(), 0);
    assert_eq!(policy.active_connections(), 0);
}

#[tokio_test]
async fn test_session_close_is_idempotent() {
    type CloseResultSender = Arc<Mutex<Option<oneshot::Sender<bool>>>>;
    type CloseHandlerState = (WsUpgradePolicy, ServerContext, CloseResultSender);

    async fn upgrade(
        State((policy, context, result_tx)): State<CloseHandlerState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |mut session| async move {
            session.close().await;
            let first_close_succeeded = timeout(Duration::from_secs(1), session.close()).await.is_ok();
            let _ = result_tx.lock().unwrap().take().unwrap().send(first_close_succeeded);
        })
    }

    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().max_connections(1);
    let (result_tx, result_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        policy.clone(),
        context.clone(),
        Arc::new(Mutex::new(Some(result_tx))),
    ));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (_client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert!(
        timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("handler should finish both close calls")
            .expect("close result should be reported")
    );
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);
}

#[tokio_test]
async fn test_recv_after_explicit_close_returns_none() {
    type CloseResultSender = Arc<Mutex<Option<oneshot::Sender<bool>>>>;
    type CloseHandlerState = (WsUpgradePolicy, ServerContext, CloseResultSender);

    async fn upgrade(
        State((policy, context, result_tx)): State<CloseHandlerState>,
        headers: HeaderMap,
        ws: WebSocketUpgrade,
    ) -> Response {
        policy.on_upgrade(ws, &headers, context, |mut session| async move {
            session.close().await;
            let returned_none = session.recv().await.is_none();
            let _ = result_tx.lock().unwrap().take().unwrap().send(returned_none);
        })
    }

    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_millis(100)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let context = server.context();
    let policy = WsUpgradePolicy::new().max_connections(1);
    let (result_tx, result_rx) = oneshot::channel();
    let app = Router::new().route("/ws", get(upgrade)).with_state((
        policy.clone(),
        context.clone(),
        Arc::new(Mutex::new(Some(result_tx))),
    ));
    let shutdown = CancellationToken::new();
    let serving = spawn(server.serve(app, shutdown.clone().cancelled_owned()));

    let (_client, status) = handshake(address, None).await;
    assert_eq!(status, StatusCode::SWITCHING_PROTOCOLS);
    assert!(
        timeout(Duration::from_secs(2), result_rx)
            .await
            .expect("handler should report recv after close")
            .expect("recv result should be reported")
    );
    shutdown.cancel();
    let _report = serving.await.unwrap().unwrap();
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);
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
