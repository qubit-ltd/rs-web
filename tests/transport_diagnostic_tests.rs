// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::io::Write;
#[cfg(feature = "tls")]
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use qubit_web::ServerOptions;
use qubit_web::WebServer;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::Instrument;
use tracing::Level;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<u8>>>);

struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedLog {
    type Writer = LogWriter;

    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(Arc::clone(&self.0))
    }
}

impl SharedLog {
    fn contents(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
    }
}

fn subscriber(log: &SharedLog) -> tracing::Dispatch {
    tracing::Dispatch::new(
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_target(true)
            .with_ansi(false)
            .with_writer(log.clone())
            .finish(),
    )
}

async fn serve_http(log: &SharedLog) -> (std::net::SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let server = WebServer::bind_http(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(Duration::from_millis(150)),
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let app = Router::new().route("/ok", get(|| async { "ok" }));
    let dispatch = subscriber(log);
    let span = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("test_server"));
    let task = tokio::spawn(
        async move {
            let _ = server
                .serve(app, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        }
        .instrument(span)
        .with_subscriber(dispatch),
    );
    (address, shutdown_tx, task)
}

async fn stop(shutdown: oneshot::Sender<()>, task: tokio::task::JoinHandle<()>) {
    let _ = shutdown.send(());
    timeout(Duration::from_secs(3), task).await.unwrap().unwrap();
}

#[tokio::test]
async fn test_malformed_http_is_sampled_and_normal_http_and_shutdown_are_not_failures() {
    const REQUEST_SECRET: &str = "request-secret-sentinel";
    let log = SharedLog::default();
    let (address, shutdown, task) = serve_http(&log).await;

    let mut malformed = TcpStream::connect(address).await.unwrap();
    malformed
        .write_all(
            format!("GET /ok HTTP/1.1\r\nHost: localhost\r\nX-Secret: {REQUEST_SECRET}\r\nMalformed-Header\r\n\r\n")
                .as_bytes(),
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    let _ = timeout(Duration::from_secs(2), malformed.read_to_end(&mut response)).await;
    drop(malformed);

    let mut normal = TcpStream::connect(address).await.unwrap();
    normal
        .write_all(b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut response = Vec::new();
    timeout(Duration::from_secs(2), normal.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&response).contains("200 OK"));
    stop(shutdown, task).await;

    let output = log.contents();
    let failures: Vec<_> = output
        .lines()
        .filter(|line| line.contains("failure_kind=\"http_connection_failed\""))
        .collect();
    assert_eq!(
        failures.len(),
        1,
        "expected exactly one malformed-HTTP failure, got: {output}"
    );
    assert!(failures[0].contains("qubit_web::transport"), "{output}");
    assert!(failures[0].contains("connection failure"), "{output}");
    assert!(output.contains("failure_kind=\"http_connection_failed\""), "{output}");
    assert!(output.contains("failure_count=1"), "{output}");
    assert!(output.contains("sampling=\"powers_of_two\""), "{output}");
    assert!(!output.contains(REQUEST_SECRET), "{output}");
    assert!(!output.contains("127.0.0.1"), "{output}");
}

#[cfg(feature = "tls")]
fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

#[cfg(feature = "tls")]
async fn serve_tls(log: &SharedLog) -> (std::net::SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let tls = qubit_web::tls::TlsConfig::from_pem_files(fixture("cert.pem"), fixture("key.pem"))
        .await
        .unwrap();
    let server = WebServer::bind_https(
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_request_header_timeout(Duration::from_millis(150)),
        tls,
    )
    .await
    .unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let dispatch = subscriber(log);
    let span = tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("test_tls_server"));
    let task = tokio::spawn(
        async move {
            let _ = server
                .serve(Router::new(), async {
                    let _ = shutdown_rx.await;
                })
                .await;
        }
        .instrument(span)
        .with_subscriber(dispatch),
    );
    (address, shutdown_tx, task)
}

#[cfg(feature = "tls")]
#[tokio::test]
async fn test_tls_timeout_and_handshake_failure_emit_fixed_private_events() {
    const TLS_INPUT_SECRET: &str = "tls-input-secret-sentinel";
    let log = SharedLog::default();
    let (address, shutdown, task) = serve_tls(&log).await;

    let idle = TcpStream::connect(address).await.unwrap();
    timeout(Duration::from_secs(2), tokio::time::sleep(Duration::from_millis(250)))
        .await
        .unwrap();
    drop(idle);

    let mut invalid = TcpStream::connect(address).await.unwrap();
    invalid.write_all(TLS_INPUT_SECRET.as_bytes()).await.unwrap();
    let mut reply = [0; 1];
    let _ = timeout(Duration::from_secs(2), invalid.read(&mut reply)).await;
    drop(invalid);
    timeout(Duration::from_secs(2), tokio::time::sleep(Duration::from_millis(100)))
        .await
        .unwrap();
    stop(shutdown, task).await;

    let output = log.contents();
    assert!(output.contains("failure_kind=\"tls_handshake_timeout\""), "{output}");
    assert!(output.contains("failure_kind=\"tls_handshake_failed\""), "{output}");
    assert!(!output.contains(TLS_INPUT_SECRET), "{output}");
    assert!(!output.contains("127.0.0.1"), "{output}");
}
