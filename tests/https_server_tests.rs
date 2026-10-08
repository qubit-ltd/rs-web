// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "tls-rustls")]

use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::net::SocketAddr;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Router;
#[cfg(all(feature = "tls-rustls", feature = "ws"))]
use axum::extract::ws::Message;
#[cfg(all(feature = "tls-rustls", feature = "ws"))]
use axum::extract::ws::WebSocket;
#[cfg(all(feature = "tls-rustls", feature = "ws"))]
use axum::extract::ws::WebSocketUpgrade;
use axum::routing::get;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use qubit_web::tls::TlsConfig;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::net::TcpStream as TokioTcpStream;
use tokio::sync::oneshot;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

#[tokio::test]
async fn serves_the_same_router_over_a_real_https_connection() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .expect("valid test certificate and key");
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .expect("HTTPS listener binds");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(
        server.serve(Router::new().route("/health", get(|| async { "tls-ok" })), async {
            let _ = shutdown_rx.await;
        }),
    );

    let response = tokio::task::spawn_blocking(move || https_get(address)).await.unwrap();
    let _ = shutdown_tx.send(());
    let report = serving.await.unwrap().expect("HTTPS serve completes");

    assert!(report.graceful);
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("tls-ok"), "unexpected response: {response}");
}

#[tokio::test]
async fn slow_tls_handshake_holds_transport_capacity_until_handshake_aborts() {
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
    let serving = tokio::spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let client_hello = capture_tls_client_hello().await;
    let mut slow_handshake = TokioTcpStream::connect(address).await.unwrap();
    slow_handshake.write_all(&client_hello).await.unwrap();
    let mut record_header = [0; 5];
    tokio::time::timeout(Duration::from_secs(2), slow_handshake.read_exact(&mut record_header))
        .await
        .expect("TLS server should respond to the ClientHello")
        .expect("TLS server should send a record header");
    assert_eq!(record_header[0], 22, "server should begin a TLS handshake response");
    let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
    let mut server_hello = vec![0; record_length];
    tokio::time::timeout(Duration::from_secs(2), slow_handshake.read_exact(&mut server_hello))
        .await
        .expect("TLS ServerHello record should arrive")
        .expect("TLS ServerHello record should be complete");
    assert_eq!(server_hello.first(), Some(&2), "server should send ServerHello first");

    let (response_tx, mut response_rx) = oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let _ = response_tx.send(https_get(address));
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut response_rx)
            .await
            .is_err(),
        "the second HTTPS request must wait while the first TLS handshake owns the permit"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 0);

    drop(slow_handshake);
    let response = tokio::time::timeout(Duration::from_secs(3), &mut response_rx)
        .await
        .expect("second TLS connection should continue after the slow handshake is dropped")
        .expect("HTTPS client task should send its response");
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("tls-limited"), "unexpected response: {response}");
    assert_eq!(handled.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

async fn capture_tls_client_hello() -> Vec<u8> {
    struct OpenSslClient {
        child: Child,
        _stdin: std::process::ChildStdin,
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
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("OpenSSL should connect to the local capture listener")
        .unwrap();
    let mut record_header = [0; 5];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut record_header))
        .await
        .expect("OpenSSL should send a TLS record")
        .expect("ClientHello record header should be complete");
    assert_eq!(record_header[0], 22, "OpenSSL should send a TLS handshake record");
    let record_length = u16::from_be_bytes([record_header[3], record_header[4]]) as usize;
    let mut client_hello = record_header.to_vec();
    client_hello.resize(5 + record_length, 0);
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut client_hello[5..]))
        .await
        .expect("OpenSSL should send the complete ClientHello")
        .expect("ClientHello payload should be complete");
    assert_eq!(client_hello.get(5), Some(&1), "TLS record should contain ClientHello");
    drop(stream);
    client_hello
}

#[cfg(all(feature = "tls-rustls", feature = "ws"))]
#[tokio::test]
async fn serves_a_websocket_echo_over_tls() {
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
    let serving = tokio::spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let echoed = tokio::task::spawn_blocking(move || wss_echo(address)).await.unwrap();
    let _ = shutdown_tx.send(());
    serving.await.unwrap().expect("WSS serve completes");
    assert_eq!(echoed, "wss-echo-sentinel");
}

#[cfg(all(feature = "tls-rustls", feature = "ws"))]
#[tokio::test]
async fn websocket_upgrade_releases_transport_capacity_for_another_https_request() {
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
                    std::future::pending::<()>().await;
                })
            }),
        )
        .route("/health", get(|| async { "after-upgrade" }));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async {
        let _ = shutdown_rx.await;
    }));

    let (ready_tx, ready_rx) = oneshot::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel();
    let websocket = tokio::task::spawn_blocking(move || wss_open_until_stopped(address, ready_tx, stop_rx));
    let upgrade_response = tokio::time::timeout(Duration::from_secs(2), ready_rx)
        .await
        .expect("WebSocket upgrade response should arrive")
        .expect("WebSocket client task should send the upgrade response");
    assert!(upgrade_response.starts_with("HTTP/1.1 101"), "unexpected response: {upgrade_response}");

    let response = tokio::task::spawn_blocking(move || https_get(address)).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"), "unexpected response: {response}");
    assert!(response.ends_with("after-upgrade"), "unexpected response: {response}");

    stop_tx.send(()).unwrap();
    websocket.await.unwrap();
    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[cfg(all(feature = "tls-rustls", feature = "ws"))]
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
    write!(input, "GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n").unwrap();
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

#[cfg(all(feature = "tls-rustls", feature = "ws"))]
fn wss_open_until_stopped(
    address: SocketAddr,
    ready: oneshot::Sender<String>,
    stop: std::sync::mpsc::Receiver<()>,
) {
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

#[tokio::test]
async fn invalid_tls_material_fails_before_binding_and_never_exposes_key_text() {
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

#[tokio::test]
async fn mismatched_certificate_and_private_key_are_rejected() {
    let config = TlsConfig::from_pem_files(fixture("other-cert.pem"), fixture("key.pem")).await;
    let error = match config {
        Err(error) => error,
        Ok(_) => panic!("certificate/key mismatch must fail before serving"),
    };
    assert_eq!(error.code(), "tls_invalid");
}

#[tokio::test]
async fn plain_http_client_cannot_speak_to_https_listener() {
    let tls = TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .unwrap();
    let server = WebServer::bind_https(ServerOptions::new("127.0.0.1:0".parse().unwrap()), tls)
        .await
        .unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let plain = tokio::task::spawn_blocking(move || {
        TcpStream::connect_timeout(&address, Duration::from_secs(2)).and_then(|mut stream| {
            stream.write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")?;
            stream.set_read_timeout(Some(Duration::from_secs(2)))?;
            let mut bytes = [0_u8; 64];
            let count = std::io::Read::read(&mut stream, &mut bytes)?;
            Ok(String::from_utf8_lossy(&bytes[..count]).starts_with("HTTP/"))
        })
    })
    .await
    .unwrap();
    let _ = shutdown_tx.send(());
    let _ = serving.await;

    assert!(!plain.unwrap(), "plaintext must not receive an HTTP response");
}
