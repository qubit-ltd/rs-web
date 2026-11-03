// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "tls")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use http_body_util::BodyExt;
use http_body_util::Empty;
use hyper::Request;
use hyper::Response;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::client::conn::http2;
use hyper_util::rt::TokioExecutor;
use hyper_util::rt::TokioIo;
use qubit_web::ServerOptions;
use qubit_web::ShutdownReport;
use qubit_web::TlsConfig;
use qubit_web::WebServer;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::pki_types::pem::PemObject;

type Client = http2::SendRequest<Empty<Bytes>>;
type ClientDriver = JoinHandle<Result<(), hyper::Error>>;
type ServingTask = JoinHandle<Result<ShutdownReport, qubit_web::WebServerError>>;

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

async fn start_server(options: ServerOptions, app: Router) -> (SocketAddr, oneshot::Sender<()>, ServingTask) {
    let tls = TlsConfig::from_pem_files(fixture("https-h2-cert.pem"), fixture("https-h2-key.pem"))
        .await
        .expect("the HTTPS fixture certificate and key should load");
    let server = WebServer::bind_https(options, tls)
        .await
        .expect("the HTTPS listener should bind");
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(app, async move {
        let _ = shutdown_rx.await;
    }));
    (address, shutdown_tx, serving)
}

async fn connect_h2(address: SocketAddr) -> (Client, ClientDriver) {
    let tcp = time::timeout(Duration::from_secs(3), TcpStream::connect(address))
        .await
        .expect("TCP connect should finish within three seconds")
        .expect("TCP connect should succeed");

    let certificate_path = fixture("https-h2-cert.pem");
    let certificate_pem = std::fs::read(&certificate_path).expect("read the fixture certificate");
    let certificate = rustls::pki_types::CertificateDer::pem_slice_iter(&certificate_pem)
        .next()
        .expect("the fixture should contain a PEM certificate")
        .expect("the fixture certificate PEM should parse");
    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(certificate)
        .expect("add the fixture certificate as the test root");

    let mut client_config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![b"h2".to_vec()];
    let connector = TlsConnector::from(Arc::new(client_config));
    let server_name = ServerName::try_from("localhost")
        .expect("localhost should be a valid DNS server name")
        .to_owned();
    let tls = time::timeout(Duration::from_secs(3), connector.connect(server_name, tcp))
        .await
        .expect("verified TLS handshake should finish within three seconds")
        .expect("verified TLS handshake should succeed with the trusted fixture");
    assert_eq!(tls.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));

    let (sender, connection) = time::timeout(
        Duration::from_secs(3),
        http2::handshake(TokioExecutor::new(), TokioIo::new(tls)),
    )
    .await
    .expect("HTTP/2 handshake should finish within three seconds")
    .expect("the TLS connection negotiated HTTP/2");
    let driver = tokio::spawn(connection);
    (sender, driver)
}

async fn request(sender: &mut Client) -> Response<Incoming> {
    let request = Request::builder()
        .method("GET")
        .uri("https://localhost/health")
        .body(Empty::<Bytes>::new())
        .expect("build the HTTPS health request");
    time::timeout(Duration::from_secs(3), sender.send_request(request))
        .await
        .expect("HTTP/2 request should finish within three seconds")
        .expect("HTTP/2 request should succeed")
}

async fn response_body(response: Response<Incoming>) -> Bytes {
    time::timeout(Duration::from_secs(3), response.into_body().collect())
        .await
        .expect("HTTP response body should finish within three seconds")
        .expect("HTTP response body should be readable")
        .to_bytes()
}

async fn finish_server(shutdown: oneshot::Sender<()>, serving: ServingTask) -> ShutdownReport {
    let _ = shutdown.send(());
    time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should finish within three seconds")
        .expect("server task should not panic")
        .expect("server should report shutdown")
}

async fn finish_client(_sender: Client, driver: ClientDriver) {
    drop(_sender);
    time::timeout(Duration::from_secs(3), driver)
        .await
        .expect("HTTP/2 client driver should finish within three seconds")
        .expect("HTTP/2 client driver should not panic")
        .expect("HTTP/2 client driver should finish cleanly");
}

#[tokio::test]
async fn test_https_negotiates_h2_and_serves_complete_response() {
    let app = Router::new().route("/health", get(|| async { "tls-ok" }));
    let (address, shutdown, serving) = start_server(ServerOptions::new("127.0.0.1:0".parse().unwrap()), app).await;
    let (mut sender, driver) = connect_h2(address).await;

    let response = request(&mut sender).await;
    assert_eq!(response.status(), hyper::StatusCode::OK);
    assert_eq!(response.version(), hyper::Version::HTTP_2);
    assert_eq!(response_body(response).await.as_ref(), b"tls-ok");

    let report = finish_server(shutdown, serving).await;
    assert!(report.graceful);
    finish_client(sender, driver).await;
}

#[tokio::test]
async fn test_https_h2_active_request_survives_idle_timeout() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = Router::new().route(
        "/health",
        get({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    "tls-ok"
                }
            }
        }),
    );
    let options =
        ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_transport_idle_timeout(Duration::from_millis(100));
    let (address, shutdown, serving) = start_server(options, app).await;
    let (mut sender, driver) = connect_h2(address).await;
    let request = Request::builder()
        .method("GET")
        .uri("https://localhost/health")
        .body(Empty::<Bytes>::new())
        .expect("build the HTTPS health request");
    let request_task = tokio::spawn(async move { sender.send_request(request).await });

    time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("handler should report entry within three seconds");
    time::sleep(Duration::from_millis(300)).await;
    release.notify_one();
    let response = time::timeout(Duration::from_secs(3), request_task)
        .await
        .expect("active HTTPS request should finish within three seconds")
        .expect("HTTP/2 request task should not panic")
        .expect("active HTTPS request should survive the idle timeout");
    assert_eq!(response.status(), hyper::StatusCode::OK);
    assert_eq!(response.version(), hyper::Version::HTTP_2);
    assert_eq!(response_body(response).await.as_ref(), b"tls-ok");

    let report = finish_server(shutdown, serving).await;
    assert!(report.graceful);
    let _ = time::timeout(Duration::from_secs(3), driver)
        .await
        .expect("HTTP/2 client driver should finish within three seconds")
        .expect("HTTP/2 client driver should not panic");
}

#[tokio::test]
async fn test_https_h2_shutdown_drains_active_request() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let app = Router::new().route(
        "/health",
        get({
            let entered = Arc::clone(&entered);
            let release = Arc::clone(&release);
            move || {
                let entered = Arc::clone(&entered);
                let release = Arc::clone(&release);
                async move {
                    entered.notify_one();
                    release.notified().await;
                    "tls-ok"
                }
            }
        }),
    );
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap()).with_shutdown_timeout(Duration::from_secs(2));
    let (address, shutdown, serving) = start_server(options, app).await;
    let (mut sender, driver) = connect_h2(address).await;
    let request = Request::builder()
        .method("GET")
        .uri("https://localhost/health")
        .body(Empty::<Bytes>::new())
        .expect("build the HTTPS health request");
    let request_task = tokio::spawn(async move { sender.send_request(request).await });

    time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("handler should report entry within three seconds");
    shutdown.send(()).expect("shutdown request should be delivered");
    time::sleep(Duration::from_millis(100)).await;
    assert!(!serving.is_finished(), "server should wait for its active request");
    release.notify_one();

    let response = time::timeout(Duration::from_secs(3), request_task)
        .await
        .expect("active HTTPS request should finish within three seconds")
        .expect("HTTP/2 request task should not panic")
        .expect("graceful shutdown should preserve the active request");
    assert_eq!(response.status(), hyper::StatusCode::OK);
    assert_eq!(response.version(), hyper::Version::HTTP_2);
    assert_eq!(response_body(response).await.as_ref(), b"tls-ok");

    let report = time::timeout(Duration::from_secs(3), serving)
        .await
        .expect("server shutdown should finish within three seconds")
        .expect("server task should not panic")
        .expect("server should report shutdown");
    assert!(report.graceful);
    let _ = time::timeout(Duration::from_secs(3), driver)
        .await
        .expect("HTTP/2 client driver should finish within three seconds")
        .expect("HTTP/2 client driver should not panic");
}

#[tokio::test]
async fn test_https_h2_applies_per_connection_stream_limit() {
    let entered_hold = Arc::new(Notify::new());
    let release_hold = Arc::new(Notify::new());
    let entered_next = Arc::new(Notify::new());
    let app = Router::new()
        .route(
            "/hold",
            get({
                let entered = Arc::clone(&entered_hold);
                let release = Arc::clone(&release_hold);
                move || {
                    let entered = Arc::clone(&entered);
                    let release = Arc::clone(&release);
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        "released"
                    }
                }
            }),
        )
        .route(
            "/next",
            get({
                let entered = Arc::clone(&entered_next);
                move || {
                    let entered = Arc::clone(&entered);
                    async move {
                        entered.notify_one();
                        "next"
                    }
                }
            }),
        );
    let options = ServerOptions::new("127.0.0.1:0".parse().unwrap())
        .with_http2_max_concurrent_streams(1)
        .unwrap();
    let (address, shutdown, serving) = start_server(options, app).await;
    let (sender, driver) = connect_h2(address).await;
    let mut first_sender = sender.clone();
    let first_request = tokio::spawn(async move {
        let request = Request::builder()
            .method("GET")
            .uri("https://localhost/hold")
            .body(Empty::new())
            .expect("build the first HTTP/2 request");
        first_sender.send_request(request).await
    });
    time::timeout(Duration::from_secs(3), entered_hold.notified())
        .await
        .expect("first HTTPS handler should enter");

    let second_request = tokio::spawn(async move {
        let mut sender = sender;
        let request = Request::builder()
            .method("GET")
            .uri("https://localhost/next")
            .body(Empty::new())
            .expect("build the second HTTP/2 request");
        time::timeout(Duration::from_secs(3), sender.send_request(request))
            .await
            .expect("second HTTP/2 request should finish")
            .expect("second HTTP/2 request should succeed")
    });
    assert!(
        time::timeout(Duration::from_millis(100), entered_next.notified())
            .await
            .is_err(),
        "the second HTTPS stream must wait for the per-connection stream slot"
    );
    release_hold.notify_one();
    let first_response = time::timeout(Duration::from_secs(3), first_request)
        .await
        .expect("first HTTPS stream should complete after release")
        .expect("first request task should not panic")
        .expect("first HTTPS request should succeed");
    assert_eq!(first_response.status(), hyper::StatusCode::OK);
    let second_response = time::timeout(Duration::from_secs(3), second_request)
        .await
        .expect("second HTTPS stream should proceed")
        .expect("second request task should not panic");
    assert_eq!(second_response.status(), hyper::StatusCode::OK);

    let report = finish_server(shutdown, serving).await;
    assert!(report.graceful);
    drop(driver);
}
