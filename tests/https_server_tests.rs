// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "tls")]

#[cfg(all(feature = "tls", feature = "ws"))]
use std::future::pending;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::process::ChildStdin;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
#[cfg(all(feature = "tls", feature = "ws"))]
use std::sync::mpsc::Receiver;
#[cfg(all(feature = "tls", feature = "ws"))]
use std::sync::mpsc::channel;
use std::time::Duration;

use axum::Router;
use axum::extract::ConnectInfo;
#[cfg(feature = "ws")]
use axum::extract::State;
#[cfg(all(feature = "tls", feature = "ws"))]
use axum::extract::ws::Message;
#[cfg(all(feature = "tls", feature = "ws"))]
use axum::extract::ws::WebSocket;
#[cfg(all(feature = "tls", feature = "ws"))]
use axum::extract::ws::WebSocketUpgrade;
#[cfg(feature = "ws")]
use axum::http::HeaderMap;
#[cfg(feature = "ws")]
use axum::response::Response;
use axum::routing::get;
#[cfg(feature = "ws")]
use qubit_web::ServerContext;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
#[cfg(feature = "ws")]
use qubit_web::WsUpgradePolicy;
use qubit_web::tls::TlsConfig;
#[cfg(feature = "ws")]
use qubit_web::ws::WsSession;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::net::TcpStream as TokioTcpStream;
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::spawn_blocking;
use tokio::test as tokio_test;
use tokio::time;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::pem::PemObject;

type TestError = Box<dyn std::error::Error + Send + Sync>;

async fn peer_address(ConnectInfo(peer): ConnectInfo<SocketAddr>) -> String {
    peer.to_string()
}

#[tokio_test]
async fn test_connect_info_https_matches_tcp_peer() {
    let tls = TlsConfig::from_pem_files(fixture("https-h2-cert.pem"), fixture("https-h2-key.pem"))
        .await
        .unwrap();
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new().route("/peer", get(peer_address)), async {
        let _ = shutdown_rx.await;
    }));
    let result: Result<(SocketAddr, SocketAddr), Box<dyn std::error::Error + Send + Sync>> = async {
        let tcp = time::timeout(Duration::from_secs(3), TokioTcpStream::connect(address)).await??;
        let expected = tcp.local_addr()?;
        let certificate_pem = std::fs::read(fixture("https-h2-cert.pem"))?;
        let certificate = rustls::pki_types::CertificateDer::pem_slice_iter(&certificate_pem)
            .next()
            .ok_or("fixture has no certificate")??;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(certificate)?;
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let name = rustls::pki_types::ServerName::try_from("localhost")?.to_owned();
        let mut stream = time::timeout(
            Duration::from_secs(3),
            TlsConnector::from(Arc::new(config)).connect(name, tcp),
        )
        .await??;
        time::timeout(Duration::from_secs(3), stream.write_all(b"GET /peer HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nForwarded: for=203.0.113.1\r\nX-Forwarded-For: 203.0.113.1\r\n\r\n"))
            .await??;
        let mut response = Vec::new();
        time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response)).await??;
        let response = String::from_utf8(response)?;
        let actual = response
            .rsplit("\r\n\r\n")
            .next()
            .ok_or("HTTPS response has no body")?
            .trim()
            .parse::<SocketAddr>()?;
        Ok((expected, actual))
    }
    .await;
    let _ = shutdown_tx.send(());
    let shutdown = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(shutdown.graceful);
    let (expected, actual) = result.expect("trusted HTTPS request should return its peer address");
    assert_eq!(actual, expected);
}

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

async fn tls_snapshot_configs() -> Result<(axum_server::tls_rustls::RustlsConfig, Arc<rustls::ClientConfig>), TestError>
{
    let base =
        axum_server::tls_rustls::RustlsConfig::from_pem_file(fixture("https-h2-cert.pem"), fixture("https-h2-key.pem"))
            .await
            .expect("trusted TLS fixture should load");
    let mut initial = (*base.get_inner()).clone();
    initial.alpn_protocols = vec![b"http/1.1".to_vec()];
    let external = axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(initial));

    let certificate_pem = std::fs::read(fixture("https-h2-cert.pem"))?;
    let certificate = rustls::pki_types::CertificateDer::pem_slice_iter(&certificate_pem)
        .next()
        .expect("fixture should contain a certificate")?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate)?;
    let mut client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    client.resumption = rustls::client::Resumption::disabled();
    Ok((external, Arc::new(client)))
}

fn reloaded_h2_config(config: &axum_server::tls_rustls::RustlsConfig) -> Arc<rustls::ServerConfig> {
    let mut updated = (*config.get_inner()).clone();
    updated.alpn_protocols = vec![b"h2".to_vec()];
    Arc::new(updated)
}

async fn negotiated_alpn(address: SocketAddr, client: Arc<rustls::ClientConfig>) -> Result<Option<Vec<u8>>, TestError> {
    let tcp = time::timeout(Duration::from_secs(3), TokioTcpStream::connect(address)).await??;
    let name = rustls::pki_types::ServerName::try_from("localhost")?.to_owned();
    let connector = TlsConnector::from(client);
    let stream = time::timeout(Duration::from_secs(3), connector.connect(name, tcp)).await??;
    let protocol = stream.get_ref().1.alpn_protocol().map(ToOwned::to_owned);
    drop(stream);
    Ok(protocol)
}

#[tokio_test]
async fn tls_reload_before_bind_is_observed() {
    let (external, client) = tls_snapshot_configs().await.expect("TLS configs should build");
    external.reload_from_config(reloaded_h2_config(&external));
    let tls = TlsConfig::from_rustls(external);
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let result = negotiated_alpn(address, client).await;
    let _ = shutdown_tx.send(());
    let shutdown = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(shutdown.graceful);
    assert_eq!(result.expect("TLS handshake should succeed"), Some(b"h2".to_vec()));
}

#[tokio_test]
async fn tls_reload_after_bind_keeps_bound_snapshot() {
    let (external, client) = tls_snapshot_configs().await.expect("TLS configs should build");
    let tls = TlsConfig::from_rustls(external.clone());
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    external.reload_from_config(reloaded_h2_config(&external));
    assert_eq!(external.get_inner().alpn_protocols, vec![b"h2".to_vec()]);

    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));
    let result = async {
        let first = negotiated_alpn(address, Arc::clone(&client)).await?;
        let second = negotiated_alpn(address, client).await?;
        Ok::<_, TestError>((first, second))
    }
    .await;
    let _ = shutdown_tx.send(());
    let shutdown = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(shutdown.graceful);
    let (first, second) = result.expect("both TLS handshakes should succeed");
    assert_eq!(first, Some(b"http/1.1".to_vec()));
    assert_eq!(second, Some(b"http/1.1".to_vec()));
}

#[tokio_test]
async fn tls_reloadable_config_applies_alpn_updates_after_bind() {
    let (external, client) = tls_snapshot_configs().await.expect("TLS configs should build");
    let tls = TlsConfig::from_reloadable_rustls(external.clone());
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let result = async {
        let before = negotiated_alpn(address, Arc::clone(&client)).await?;
        external.reload_from_config(reloaded_h2_config(&external));
        let after = negotiated_alpn(address, client).await?;
        Ok::<_, TestError>((before, after))
    }
    .await;

    let _ = shutdown_tx.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.graceful);
    let (before, after) = result.expect("TLS handshakes should succeed");
    assert_eq!(before, Some(b"http/1.1".to_vec()));
    assert_eq!(after, Some(b"h2".to_vec()));
}

fn certificate_der(path: PathBuf) -> rustls::pki_types::CertificateDer<'static> {
    let pem = std::fs::read(path).expect("certificate fixture should be readable");
    rustls::pki_types::CertificateDer::pem_slice_iter(&pem)
        .next()
        .expect("fixture should contain a certificate")
        .expect("certificate PEM should parse")
}

async fn connected_peer_certificate(
    address: SocketAddr,
    client: Arc<rustls::ClientConfig>,
) -> Result<Vec<u8>, TestError> {
    let tcp = time::timeout(Duration::from_secs(3), TokioTcpStream::connect(address)).await??;
    let name = rustls::pki_types::ServerName::try_from("localhost")?.to_owned();
    let stream = time::timeout(Duration::from_secs(3), TlsConnector::from(client).connect(name, tcp)).await??;
    Ok(stream
        .get_ref()
        .1
        .peer_certificates()
        .ok_or("server sent no certificate")?[0]
        .as_ref()
        .to_vec())
}

async fn rotation_test_configs() -> Result<(axum_server::tls_rustls::RustlsConfig, Arc<rustls::ClientConfig>), TestError>
{
    let config =
        axum_server::tls_rustls::RustlsConfig::from_pem_file(fixture("https-h2-cert.pem"), fixture("https-h2-key.pem"))
            .await?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate_der(fixture("https-h2-cert.pem")))?;
    roots.add(certificate_der(fixture("rotation-b-cert.pem")))?;
    let mut client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.resumption = rustls::client::Resumption::disabled();
    Ok((config, Arc::new(client)))
}

#[tokio_test]
async fn reloadable_tls_rotates_certificates_and_retains_last_good_config() {
    let (external, client) = rotation_test_configs().await.expect("rotation configs should load");
    let server = WebServer::bind_https(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()),
        TlsConfig::from_reloadable_rustls(external.clone()),
    )
    .await
    .expect("reloadable HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let a = certificate_der(fixture("https-h2-cert.pem"));
    let b = certificate_der(fixture("rotation-b-cert.pem"));
    let result = async {
        let before = connected_peer_certificate(address, Arc::clone(&client)).await?;
        assert_eq!(before, a.as_ref());
        external
            .reload_from_pem_file(fixture("rotation-b-cert.pem"), fixture("rotation-b-key.pem"))
            .await?;
        let after = connected_peer_certificate(address, Arc::clone(&client)).await?;
        assert_eq!(after, b.as_ref());
        let failed = external
            .reload_from_pem_file(fixture("rotation-b-cert.pem"), fixture("https-h2-key.pem"))
            .await;
        assert!(failed.is_err(), "mismatched certificate and key must fail reload");
        let after_failure = connected_peer_certificate(address, client).await?;
        assert_eq!(after_failure, b.as_ref());
        Ok::<_, TestError>(())
    }
    .await;

    let _ = shutdown_tx.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.graceful);
    result.expect("certificate rotation and failed-reload retention should work");
}

#[tokio_test]
async fn reloadable_tls_uses_configuration_reloaded_before_serve_starts() {
    let (external, client) = rotation_test_configs().await.expect("rotation configs should load");
    let server = WebServer::bind_https(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()),
        TlsConfig::from_reloadable_rustls(external.clone()),
    )
    .await
    .expect("reloadable HTTPS listener binds");
    let address = server.local_addr();
    let b = certificate_der(fixture("rotation-b-cert.pem"));
    external
        .reload_from_pem_file(fixture("rotation-b-cert.pem"), fixture("rotation-b-key.pem"))
        .await
        .expect("B config should load before serving starts");

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));
    let observed = connected_peer_certificate(address, client).await;
    let _ = shutdown_tx.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.graceful);
    assert_eq!(observed.expect("first handshake should succeed"), b.as_ref());
}

#[cfg(feature = "ws")]
async fn wss_echo_once(
    stream: &mut tokio_rustls::client::TlsStream<TokioTcpStream>,
    message: &[u8],
) -> Result<Vec<u8>, TestError> {
    time::timeout(Duration::from_secs(3), async {
        let mask = [0x12_u8, 0x39, 0x7a, 0x45];
        let mut frame = vec![0x81, 0x80 | message.len() as u8];
        frame.extend_from_slice(&mask);
        frame.extend(message.iter().enumerate().map(|(i, byte)| *byte ^ mask[i % 4]));
        stream.write_all(&frame).await?;
        let mut header = [0; 2];
        stream.read_exact(&mut header).await?;
        let length = match header[1] & 0x7f {
            126 => {
                let mut length = [0; 2];
                stream.read_exact(&mut length).await?;
                u16::from_be_bytes(length) as usize
            }
            127 => {
                let mut length = [0; 8];
                stream.read_exact(&mut length).await?;
                u64::from_be_bytes(length) as usize
            }
            length => length as usize,
        };
        let mut payload = vec![0; length];
        stream.read_exact(&mut payload).await?;
        Ok::<_, TestError>(payload)
    })
    .await?
}

#[cfg(feature = "ws")]
async fn establish_wss(
    address: SocketAddr,
    client: Arc<rustls::ClientConfig>,
) -> Result<tokio_rustls::client::TlsStream<TokioTcpStream>, TestError> {
    let tcp = time::timeout(Duration::from_secs(3), TokioTcpStream::connect(address)).await??;
    let name = rustls::pki_types::ServerName::try_from("localhost")?.to_owned();
    let mut stream = time::timeout(Duration::from_secs(3), TlsConnector::from(client).connect(name, tcp)).await??;
    stream
        .write_all(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
        .await?;
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        time::timeout(Duration::from_secs(3), stream.read_exact(&mut byte)).await??;
        response.push(byte[0]);
    }
    if !response.starts_with(b"HTTP/1.1 101") {
        return Err(format!(
            "unexpected WSS upgrade response: {}",
            String::from_utf8_lossy(&response)
        )
        .into());
    }
    Ok(stream)
}

#[cfg(feature = "ws")]
async fn managed_wss_upgrade(
    State((policy, context)): State<(WsUpgradePolicy, ServerContext)>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    policy.on_upgrade(ws, &headers, context, |mut session: WsSession| async move {
        while let Some(Ok(message)) = session.recv().await {
            if session.try_send(message).is_err() {
                break;
            }
        }
    })
}

#[cfg(feature = "ws")]
#[tokio_test]
async fn reloadable_tls_keeps_existing_wss_session_and_rotates_new_sessions() {
    let (external, client) = rotation_test_configs().await.expect("rotation configs should load");
    let server = WebServer::bind_https(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()),
        TlsConfig::from_reloadable_rustls(external.clone()),
    )
    .await
    .expect("reloadable HTTPS listener binds");
    let address = server.local_addr();
    let policy = WsUpgradePolicy::new();
    let context = server.context();
    let app = Router::new()
        .route("/ws", get(managed_wss_upgrade))
        .with_state((policy.clone(), context.clone()));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let result = async {
        let mut existing = establish_wss(address, Arc::clone(&client)).await?;
        time::timeout(Duration::from_secs(3), async {
            while policy.active_connections() != 1 || context.active_sessions() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("existing WSS session should register and hold its resources");
        assert_eq!(
            wss_echo_once(&mut existing, b"session-before").await?,
            b"session-before"
        );
        external
            .reload_from_pem_file(fixture("rotation-b-cert.pem"), fixture("rotation-b-key.pem"))
            .await?;
        assert_eq!(wss_echo_once(&mut existing, b"session-after").await?, b"session-after");
        let mut new_session = establish_wss(address, client).await?;
        time::timeout(Duration::from_secs(3), async {
            while policy.active_connections() != 2 || context.active_sessions() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("new WSS session should register and hold its resources");
        let peer = new_session
            .get_ref()
            .1
            .peer_certificates()
            .expect("new session has peer certificate");
        assert_eq!(
            peer[0].as_ref(),
            certificate_der(fixture("rotation-b-cert.pem")).as_ref()
        );
        assert_eq!(wss_echo_once(&mut new_session, b"new-session").await?, b"new-session");
        drop(existing);
        drop(new_session);
        time::timeout(Duration::from_secs(3), async {
            while policy.active_connections() != 0 || context.active_sessions() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("closed WSS peers should release session and connection resources");
        Ok::<_, TestError>(())
    }
    .await;

    let _ = shutdown_tx.send(());
    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should complete")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.graceful);
    assert_eq!(policy.active_connections(), 0);
    assert_eq!(context.active_sessions(), 0);
    result.expect("existing WSS session should survive rotation and new session should use B");
}

#[tokio_test]
async fn test_serves_the_same_router_over_a_real_https_connection() {
    let rustls = axum_server::tls_rustls::RustlsConfig::from_pem_file(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let tls = TlsConfig::from_rustls(rustls);
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(
        server.serve(Router::new().route("/health", get(|| async { "tls-ok" })), async {
            let _ = shutdown_rx.await;
        }),
    );

    let response = spawn_blocking(move || https_get(address)).await.unwrap();
    let _ = shutdown_tx.send(());
    let report = serving.await.unwrap().expect("HTTPS serve completes");

    assert!(report.graceful);
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("tls-ok"), "unexpected response: {response}");
}

#[tokio_test]
async fn test_https_http1_buffer_budget_rejects_large_field_and_recovers() {
    let handled = Arc::new(AtomicUsize::new(0));
    let route_count = Arc::clone(&handled);
    let app = Router::new().route(
        "/health",
        get(move || {
            let handled = Arc::clone(&route_count);
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                "tls-ok"
            }
        }),
    );
    let tls = TlsConfig::from_pem_files(fixture("https-h2-cert.pem"), fixture("https-h2-key.pem"))
        .await
        .unwrap();
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http1_max_buffer_bytes(8192)
        .unwrap();
    let server = WebServer::bind_https(options, tls).await.unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));
    let certificate_pem = std::fs::read(fixture("https-h2-cert.pem")).unwrap();
    let certificate = rustls::pki_types::CertificateDer::pem_slice_iter(&certificate_pem)
        .next()
        .unwrap()
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(tls_config));

    async fn exchange(connector: &TlsConnector, address: SocketAddr, request: Vec<u8>) -> Vec<u8> {
        let tcp = time::timeout(Duration::from_secs(3), TokioTcpStream::connect(address))
            .await
            .unwrap()
            .unwrap();
        let name = rustls::pki_types::ServerName::try_from("localhost").unwrap().to_owned();
        let mut stream = time::timeout(Duration::from_secs(3), connector.connect(name, tcp))
            .await
            .unwrap()
            .unwrap();
        time::timeout(Duration::from_secs(3), stream.write_all(&request))
            .await
            .unwrap()
            .unwrap();
        let mut response = Vec::new();
        time::timeout(Duration::from_secs(3), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    let small = exchange(
        &connector,
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
    )
    .await;
    assert!(String::from_utf8_lossy(&small).starts_with("HTTP/1.1 200"));
    let mut large = b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Large: ".to_vec();
    large.extend(std::iter::repeat_n(b'a', 16 * 1024));
    large.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    let rejected = exchange(&connector, address, large).await;
    assert!(
        !rejected.is_empty(),
        "Hyper should close or respond when the parser buffer is exceeded"
    );
    let recovered = exchange(
        &connector,
        address,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_vec(),
    )
    .await;
    assert!(String::from_utf8_lossy(&recovered).starts_with("HTTP/1.1 200"));
    assert_eq!(
        handled.load(Ordering::SeqCst),
        2,
        "oversized request must not enter the handler"
    );
    let _ = shutdown_tx.send(());
    assert!(
        time::timeout(Duration::from_secs(3), serving)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .graceful
    );
}

#[tokio_test]
async fn test_slow_tls_handshake_holds_transport_capacity_until_handshake_aborts() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_https(options, tls).await.expect("HTTPS listener binds");
    let address = server.local_addr();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_route = handled.clone();
    let app = Router::new().route(
        "/health",
        get(move || {
            let handled = handled_route.clone();
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                "tls-limited"
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let client_hello = capture_tls_client_hello().await;
    let mut slow_handshake = TokioTcpStream::connect(address).await.unwrap();
    slow_handshake.write_all(&client_hello).await.unwrap();
    let mut record_header = [0; 5];
    time::timeout(Duration::from_secs(2), slow_handshake.read_exact(&mut record_header))
        .await
        .expect("TLS server should respond to the ClientHello")
        .expect("TLS server should send a record header");
    assert_eq!(record_header[0], 22, "server should begin a TLS handshake response");
    let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
    let mut server_hello = vec![0; record_length];
    time::timeout(Duration::from_secs(2), slow_handshake.read_exact(&mut server_hello))
        .await
        .expect("TLS ServerHello record should arrive")
        .expect("TLS ServerHello record should be complete");
    assert_eq!(server_hello.first(), Some(&2), "server should send ServerHello first");

    let (response_tx, mut response_rx) = oneshot::channel();
    spawn_blocking(move || {
        let _ = response_tx.send(https_get(address));
    });
    assert!(
        time::timeout(Duration::from_millis(100), &mut response_rx)
            .await
            .is_err(),
        "the second HTTPS request must wait while the first TLS handshake owns the permit"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 0);

    drop(slow_handshake);
    let response = time::timeout(Duration::from_secs(3), &mut response_rx)
        .await
        .expect("second TLS connection should continue after the slow handshake is dropped")
        .expect("HTTPS client task should send its response");
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("tls-limited"), "unexpected response: {response}");
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_tls_handshake_timeout_releases_transport_capacity() {
    let client_hello = capture_tls_client_hello().await;
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(1)
        .unwrap()
        .with_request_header_timeout(Duration::from_millis(500))
        .with_transport_idle_timeout(Duration::from_secs(30));
    let server = WebServer::bind_https(options, tls).await.expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(
        Router::new().route("/health", get(|| async { "tls-timeout-ok" })),
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let mut slow_handshake = TokioTcpStream::connect(address).await.unwrap();
    slow_handshake.write_all(&client_hello).await.unwrap();
    read_server_hello(&mut slow_handshake).await;

    let (response_tx, mut response_rx) = oneshot::channel();
    spawn_blocking(move || {
        let _ = response_tx.send(https_get(address));
    });
    let mut remaining_tls_records = Vec::new();
    time::timeout(
        Duration::from_secs(5),
        slow_handshake.read_to_end(&mut remaining_tls_records),
    )
    .await
    .expect("stalled handshake should close within the configured deadline")
    .expect("reading the stalled TLS connection should finish at EOF");
    let response = time::timeout(Duration::from_secs(5), &mut response_rx)
        .await
        .expect("second HTTPS request should acquire the released transport permit")
        .expect("HTTPS client task should send its response");
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("tls-timeout-ok"), "unexpected response: {response}");

    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[tokio_test]
async fn test_shutdown_cancels_pending_tls_handshake() {
    let client_hello = capture_tls_client_hello().await;
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(1)
        .unwrap()
        .with_request_header_timeout(Duration::from_secs(30))
        .with_shutdown_timeout(Duration::from_secs(1));
    let server = WebServer::bind_https(options, tls).await.expect("HTTPS listener binds");
    let address = server.local_addr();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_route = handled.clone();
    let app = Router::new().route(
        "/health",
        get(move || {
            let handled = handled_route.clone();
            async move {
                handled.fetch_add(1, Ordering::SeqCst);
                "must-not-handle"
            }
        }),
    );
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let mut pending_handshake = TokioTcpStream::connect(address).await.unwrap();
    pending_handshake.write_all(&client_hello).await.unwrap();
    read_server_hello(&mut pending_handshake).await;
    let _ = shutdown_tx.send(());

    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should finish within its one-second deadline")
        .unwrap()
        .expect("HTTPS server shutdown should be reported");
    let mut remaining_tls_records = Vec::new();
    time::timeout(
        Duration::from_secs(1),
        pending_handshake.read_to_end(&mut remaining_tls_records),
    )
    .await
    .expect("shutdown should close the pending TLS socket")
    .expect("reading the pending TLS socket should finish at EOF");
    assert!(
        report.graceful,
        "pending TLS handshake should cancel within the shutdown deadline"
    );
    assert_eq!(report.forced_connections, None);
    assert_eq!(report.unfinished_managed_sessions, 0);
    assert_eq!(
        handled.load(Ordering::SeqCst),
        0,
        "TLS handshake must not reach HTTP handler"
    );
}

async fn read_server_hello(stream: &mut TokioTcpStream) {
    let mut record_header = [0; 5];
    time::timeout(Duration::from_secs(2), stream.read_exact(&mut record_header))
        .await
        .expect("TLS server should respond to the ClientHello")
        .expect("TLS server should send a record header");
    assert_eq!(record_header[0], 22, "server should begin a TLS handshake response");
    let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
    let mut server_hello = vec![0; record_length];
    time::timeout(Duration::from_secs(2), stream.read_exact(&mut server_hello))
        .await
        .expect("TLS ServerHello record should arrive")
        .expect("TLS ServerHello record should be complete");
    assert_eq!(server_hello.first(), Some(&2), "server should send ServerHello first");
}

async fn capture_tls_client_hello() -> Vec<u8> {
    struct OpenSslClient {
        child: Child,
        _stdin: ChildStdin,
    }

    impl Drop for OpenSslClient {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    let listener = TokioTcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut child = Command::new("openssl")
        .args(["s_client", "-connect", &address.to_string(), "-tls1_2"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl is required to generate the TLS ClientHello fixture");
    let child_stdin = child.stdin.take().expect("OpenSSL stdin is piped");
    let _client = OpenSslClient {
        child,
        _stdin: child_stdin,
    };
    let (mut stream, _) = time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("OpenSSL should connect to the local capture listener")
        .unwrap();
    let mut record_header = [0; 5];
    time::timeout(Duration::from_secs(3), stream.read_exact(&mut record_header))
        .await
        .expect("OpenSSL should send a TLS record")
        .expect("ClientHello record header should be complete");
    assert_eq!(record_header[0], 22, "OpenSSL should send a TLS handshake record");
    let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
    let mut client_hello = record_header.to_vec();
    client_hello.resize(5 + record_length, 0);
    time::timeout(Duration::from_secs(3), stream.read_exact(&mut client_hello[5..]))
        .await
        .expect("OpenSSL should send the complete ClientHello")
        .expect("ClientHello payload should be complete");
    assert_eq!(client_hello.get(5), Some(&1), "TLS record should contain ClientHello");
    drop(stream);
    client_hello
}

#[cfg(all(feature = "tls", feature = "ws"))]
#[tokio_test]
async fn test_serves_a_websocket_echo_over_tls() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let app = Router::new().route(
        "/ws",
        get(|ws: WebSocketUpgrade| async move {
            ws.on_upgrade(|mut socket: WebSocket| async move {
                if let Some(Ok(Message::Text(message))) = socket.recv().await {
                    let _ = socket.send(Message::Text(message)).await;
                }
            })
        }),
    );
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let echoed = spawn_blocking(move || wss_echo(address)).await.unwrap();
    let _ = shutdown_tx.send(());
    let _ = serving.await.unwrap().expect("WSS serve completes");
    assert_eq!(echoed, "wss-echo-sentinel");
}

#[cfg(all(feature = "tls", feature = "ws"))]
#[tokio_test]
async fn test_websocket_upgrade_releases_transport_capacity_for_another_https_request() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_max_transport_connections(1)
        .unwrap();
    let server = WebServer::bind_https(options, tls).await.expect("HTTPS listener binds");
    let address = server.local_addr();
    let app = Router::new()
        .route(
            "/ws",
            get(|ws: WebSocketUpgrade| async move {
                ws.on_upgrade(|_socket: WebSocket| async move {
                    pending::<()>().await;
                })
            }),
        )
        .route("/health", get(|| async { "after-upgrade" }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let (ready_tx, ready_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = channel();
    let websocket = spawn_blocking(move || wss_open_until_stopped(address, ready_tx, stop_rx));
    let upgrade_response = time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("WebSocket upgrade response should arrive")
        .expect("WebSocket client task should send the upgrade response");
    assert!(
        upgrade_response.starts_with("HTTP/1.1 101"),
        "unexpected response: {upgrade_response}"
    );

    let response = spawn_blocking(move || https_get(address)).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("after-upgrade"), "unexpected response: {response}");

    stop_tx.send(()).unwrap();
    websocket.await.unwrap();
    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[cfg(all(feature = "tls", feature = "ws"))]
fn wss_echo(address: SocketAddr) -> String {
    let mut child = Command::new("timeout")
        .args([
            "5s",
            "openssl",
            "s_client",
            "-connect",
            &address.to_string(),
            "-quiet",
            "-ign_eof",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl is required for the loopback WSS integration test");
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    let message = b"wss-echo-sentinel";
    let mask = [0x31_u8, 0x72, 0xa4, 0x0f];
    let mut frame = vec![0x81, 0x80 | message.len() as u8];
    frame.extend_from_slice(&mask);
    frame.extend(message.iter().enumerate().map(|(index, byte)| *byte ^ mask[index % 4]));
    write!(
        input,
        "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    )
    .unwrap();
    input.write_all(&frame).unwrap();

    let mut reader = BufReader::new(output);
    let mut line = String::new();
    let mut response = String::new();
    while reader.read_line(&mut line).unwrap() > 0 {
        response.push_str(&line);
        if line == "\r\n" {
            break;
        }
        line.clear();
    }
    assert!(
        response.starts_with("HTTP/1.1 101"),
        "unexpected WSS response: {response}"
    );
    let mut header = [0_u8; 2];
    reader.read_exact(&mut header).unwrap();
    assert_eq!(header[0] & 0x0f, 0x1, "server should echo a text frame");
    assert_eq!(header[1] & 0x80, 0, "server frame must not be masked");
    let length = match header[1] & 0x7f {
        126 => {
            let mut extended = [0_u8; 2];
            reader.read_exact(&mut extended).unwrap();
            u16::from_be_bytes(extended) as usize
        }
        127 => {
            let mut extended = [0_u8; 8];
            reader.read_exact(&mut extended).unwrap();
            u64::from_be_bytes(extended) as usize
        }
        length => length as usize,
    };
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).unwrap();
    let _ = child.kill();
    let _ = child.wait();
    String::from_utf8(payload).unwrap()
}

fn https_get(address: SocketAddr) -> String {
    https_request(address, "/health")
}

fn https_request(address: SocketAddr, path: &str) -> String {
    let mut child = Command::new("timeout")
        .args([
            "5s",
            "openssl",
            "s_client",
            "-connect",
            &address.to_string(),
            "-quiet",
            "-ign_eof",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl is required for the loopback TLS integration test");
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    write!(
        input,
        "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    drop(input);

    let mut response = String::new();
    let mut reader = BufReader::new(output);
    let mut line = String::new();
    while reader.read_line(&mut line).unwrap() > 0 {
        response.push_str(&line);
        if line == "\r\n" {
            break;
        }
        line.clear();
    }
    let mut body = String::new();
    let _ = reader.read_to_string(&mut body);
    response.push_str(&body);
    let _ = child.kill();
    let _ = child.wait();
    response
}

#[cfg(all(feature = "tls", feature = "ws"))]
fn wss_open_until_stopped(address: SocketAddr, ready: oneshot::Sender<String>, stop: Receiver<()>) {
    let mut child = Command::new("timeout")
        .args([
            "5s",
            "openssl",
            "s_client",
            "-connect",
            &address.to_string(),
            "-quiet",
            "-ign_eof",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl is required for the loopback WSS integration test");
    let mut input = child.stdin.take().unwrap();
    let output = child.stdout.take().unwrap();
    write!(
        input,
        "GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
    )
    .unwrap();
    let mut reader = BufReader::new(output);
    let mut line = String::new();
    let mut response = String::new();
    while reader.read_line(&mut line).unwrap() > 0 {
        response.push_str(&line);
        if line == "\r\n" {
            break;
        }
        line.clear();
    }
    let _ = ready.send(response);
    let _ = stop.recv_timeout(Duration::from_secs(5));
    drop(reader);
    drop(input);
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio_test]
async fn test_invalid_tls_material_fails_before_binding_and_never_exposes_key_text() {
    let key_text = "PRIVATE-KEY-SENTINEL";
    let error = match TlsConfig::from_pem_files(fixture("cert.pem"), fixture("invalid-key.pem")).await {
        Err(error) => error,
        Ok(_) => panic!("invalid key must fail during configuration"),
    };
    let display = error.to_string();
    let debug = format!("{error:?}");
    assert_eq!(error.code(), "tls_invalid");
    assert!(!display.contains(key_text));
    assert!(!debug.contains(key_text));
}

#[tokio_test]
async fn test_mismatched_certificate_and_private_key_are_rejected() {
    let config = TlsConfig::from_pem_files(fixture("other-cert.pem"), fixture("key.pem")).await;
    let error = match config {
        Err(error) => error,
        Ok(_) => panic!("certificate/key mismatch must fail before serving"),
    };
    assert_eq!(error.code(), "tls_invalid");
}

#[tokio_test]
async fn test_plain_http_client_cannot_speak_to_https_listener() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .unwrap();
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let plain = spawn_blocking(move || {
        TcpStream::connect_timeout(&address, Duration::from_secs(2)).and_then(|mut stream| {
            stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut bytes = [0_u8; 64];
            let count = stream.read(&mut bytes)?;
            Ok(String::from_utf8_lossy(&bytes[..count]).starts_with("HTTP/"))
        })
    })
    .await
    .unwrap();
    let _ = shutdown_tx.send(());
    let _ = serving.await;

    assert!(!plain.unwrap(), "plaintext must not receive an HTTP response");
}
