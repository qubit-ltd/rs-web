// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::response::Response;
use axum::routing::get;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use qubit_web::WsSession;
use qubit_web::WsUpgradePolicy;
use qubit_websocket::WsClient;
use qubit_websocket::WsErrorKind;
use qubit_websocket::WsMessage;
use qubit_websocket::WsTermination;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tokio_rustls::rustls::RootCertStore;

#[derive(Clone)]
struct AppState {
    context: qubit_web::ServerContext,
    policy: WsUpgradePolicy,
    handlers: Arc<AtomicUsize>,
}

async fn echo(State(state): State<AppState>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    state.handlers.fetch_add(1, Ordering::SeqCst);
    state.policy.on_upgrade(
        upgrade.protocols(["echo.v1"]),
        &headers,
        state.context,
        |mut session: WsSession| async move {
            while let Some(Ok(message)) = session.recv().await {
                if session.try_send(message).is_err() {
                    break;
                }
            }
        },
    )
}

struct RunningServer {
    address: std::net::SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<Result<qubit_web::ShutdownReport, qubit_web::WebServerError>>,
    handlers: Arc<AtomicUsize>,
}

async fn start_server() -> RunningServer {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tls");
    let tls = qubit_web::tls::TlsConfig::from_pem_files(root.join("https-h2-cert.pem"), root.join("https-h2-key.pem"))
        .await
        .expect("load localhost WSS certificate");
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("bind WSS server");
    let address = server.local_addr();
    let handlers = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route("/ws", get(echo)).with_state(AppState {
        context: server.context(),
        policy: WsUpgradePolicy::new(),
        handlers: handlers.clone(),
    });
    let (shutdown, receiver) = oneshot::channel();
    let task = tokio::spawn(server.serve(app, async move {
        let _ = receiver.await;
    }));
    RunningServer {
        address,
        shutdown: Some(shutdown),
        task,
        handlers,
    }
}

fn roots() -> RootCertStore {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../tls/https-h2-cert.pem");
    let pem = std::fs::read(root).expect("read test trust root");
    let cert = rustls_pemfile::certs(&mut std::io::Cursor::new(pem))
        .next()
        .unwrap()
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    roots
}

fn client(roots: RootCertStore) -> WsClient {
    WsClient::builder()
        .exclusive_root_certificates(roots)
        .subprotocols(vec!["echo.v1".into()])
        .handshake_timeout(Duration::from_secs(2))
        .build()
        .unwrap()
}

fn url(address: std::net::SocketAddr) -> String {
    format!("wss://localhost:{}/ws", address.port())
}

#[tokio::test]
async fn test_trusted_wss_handshake_subprotocol_echo_and_close() {
    let mut server = start_server().await;
    let mut session = timeout(Duration::from_secs(3), client(roots()).connect(&url(server.address)))
        .await
        .unwrap()
        .expect("trusted TLS and WebSocket handshake");
    assert_eq!(session.response_status_code(), 101);
    assert_eq!(session.selected_subprotocol(), Some("echo.v1"));
    session.send(WsMessage::Text("hello over WSS".into())).await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), session.receive())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        WsMessage::Text("hello over WSS".into())
    );
    timeout(Duration::from_secs(3), session.close()).await.unwrap().unwrap();
    let _ = server.shutdown.take().unwrap().send(());
    let report = timeout(Duration::from_secs(3), server.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
}

#[tokio::test]
async fn test_empty_exclusive_root_rejects_tls_without_entering_handler() {
    let mut server = start_server().await;
    let error = match timeout(
        Duration::from_secs(3),
        client(RootCertStore::empty()).connect(&url(server.address)),
    )
    .await
    .expect("TLS rejection must not be an outer timeout")
    {
        Ok(_) => panic!("empty roots must reject the server certificate"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), WsErrorKind::Tls);
    assert_eq!(server.handlers.load(Ordering::SeqCst), 0);
    let mut valid = timeout(Duration::from_secs(3), client(roots()).connect(&url(server.address)))
        .await
        .unwrap()
        .unwrap();
    valid.close().await.unwrap();
    assert_eq!(server.handlers.load(Ordering::SeqCst), 1);
    let _ = server.shutdown.take().unwrap().send(());
    let report = timeout(Duration::from_secs(3), server.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.graceful);
}

#[tokio::test]
async fn test_server_shutdown_terminates_active_wss_session_gracefully() {
    let mut server = start_server().await;
    let mut session = timeout(Duration::from_secs(3), client(roots()).connect(&url(server.address)))
        .await
        .unwrap()
        .unwrap();
    let _ = server.shutdown.take().unwrap().send(());
    let terminal = timeout(Duration::from_secs(3), async {
        loop {
            if session.receive().await.is_none() {
                break session.termination();
            }
        }
    })
    .await
    .unwrap();
    assert!(matches!(terminal, Some(WsTermination::Closed { .. })));
    let report = timeout(Duration::from_secs(3), server.task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(report.graceful);
    assert_eq!(report.unfinished_managed_sessions, 0);
}
