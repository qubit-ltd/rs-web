// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "config")]

use axum::Router;
use axum::body::Body;
use axum::body::Bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::routing::post;
use qubit_config::Config;
use qubit_web::ConfiguredWeb;
use qubit_web::RequestLimitLayer;
use qubit_web::WebServer;
use tower::ServiceExt;

fn config_with(address: &str, shutdown_timeout_ms: u64) -> Config {
    let mut config = Config::new();
    config.set("address", address).unwrap();
    config.set("shutdown_timeout_ms", shutdown_timeout_ms).unwrap();
    config
}

#[tokio::test]
async fn maps_loopback_address_and_accepts_a_valid_shutdown_timeout() {
    let mut config = config_with("127.0.0.1:0", 2500);
    config.set("http.max_body_bytes", 4096u64).unwrap();
    config.set("http.max_concurrent_requests", 8u64).unwrap();
    config.set("http.request_timeout_ms", 1500u64).unwrap();

    config.set("transport.max_connections", 3u64).unwrap();
    config.set("transport.idle_timeout_ms", 900u64).unwrap();
    let configured = ConfiguredWeb::from_config(&config).unwrap();
    let (options, limits) = configured.into_parts();
    assert_eq!(options.address(), "127.0.0.1:0".parse().unwrap());
    assert_eq!(options.max_transport_connections(), 3);
    assert_eq!(options.http2_max_concurrent_streams(), 200);
    assert_eq!(options.http1_max_buffer_bytes(), None);
    assert_eq!(options.http2_max_header_list_bytes(), None);
    assert_eq!(options.transport_idle_timeout(), std::time::Duration::from_millis(900));
    assert_eq!(options.shutdown_timeout(), std::time::Duration::from_millis(2500));
    assert_eq!(limits.max_body_bytes(), 4096);
    assert_eq!(limits.max_concurrent_requests(), 8);
    assert_eq!(limits.request_timeout(), std::time::Duration::from_millis(1500));
    options.validate().unwrap();
    let server = WebServer::bind_http(options).await.unwrap();

    assert!(server.local_addr().ip().is_loopback());
    assert_ne!(server.local_addr().port(), 0);
    assert_eq!(
        server.context().shutdown_timeout(),
        std::time::Duration::from_millis(2500)
    );
}

#[test]
fn parses_optional_protocol_header_budgets_and_reports_field_errors() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();
    config.set("transport.http1_max_buffer_bytes", 8192u64).unwrap();
    config.set("transport.http2_max_header_list_bytes", 17u64).unwrap();
    let options = ConfiguredWeb::from_config(&config).unwrap().into_parts().0;
    assert_eq!(options.http1_max_buffer_bytes(), Some(8192));
    assert_eq!(options.http2_max_header_list_bytes(), Some(17));

    for (field, value) in [
        ("transport.http1_max_buffer_bytes", 8191u64),
        ("transport.http2_max_header_list_bytes", 0u64),
        ("transport.http2_max_header_list_bytes", u64::from(u32::MAX) + 1),
    ] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set(field, value).unwrap();
        let error = ConfiguredWeb::from_config(&config).unwrap_err();
        assert_eq!(error.field(), field);
        assert!(!error.to_string().contains(&value.to_string()));
    }

    for field in [
        "transport.http1_max_buffer_bytes",
        "transport.http2_max_header_list_bytes",
    ] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set(field, "private-header-budget").unwrap();
        let error = ConfiguredWeb::from_config(&config).unwrap_err();
        assert_eq!(error.field(), field);
        assert!(!error.to_string().contains("private-header-budget"));
    }

    let too_large_usize = u64::MAX;
    if usize::try_from(too_large_usize).is_err() {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set("transport.http1_max_buffer_bytes", too_large_usize).unwrap();
        let error = ConfiguredWeb::from_config(&config).unwrap_err();
        assert_eq!(error.field(), "transport.http1_max_buffer_bytes");
        assert!(!error.to_string().contains(&too_large_usize.to_string()));
    }
}

#[tokio::test]
async fn configured_http_limits_apply_when_attached_to_a_router_branch() {
    let mut config = config_with("127.0.0.1:0", 2500);
    config.set("http.max_body_bytes", 3u64).unwrap();
    let configured = ConfiguredWeb::from_config(&config).unwrap();
    let app = Router::new()
        .route("/body", post(|_: Bytes| async { "ok" }))
        .layer(RequestLimitLayer::new(configured.http_limits()));

    let response = app
        .oneshot(Request::post("/body").body(Body::from("four")).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn configured_http1_buffer_budget_is_applied_by_the_server() {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;

    let mut config = config_with("127.0.0.1:0", 2500);
    config.set("transport.http1_max_buffer_bytes", 8192u64).unwrap();
    let options = ConfiguredWeb::from_config(&config).unwrap().into_parts().0;
    let server = WebServer::bind_http(options).await.unwrap();
    let address = server.local_addr();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let serving = tokio::spawn(server.serve(Router::new(), async {
        let _ = shutdown_rx.await;
    }));

    let mut stream = TcpStream::connect(address).await.unwrap();
    let mut request = b"GET / HTTP/1.1\r\nHost: localhost\r\nX-Large: ".to_vec();
    request.extend(std::iter::repeat_n(b'a', 16 * 1024));
    request.extend_from_slice(b"\r\nConnection: close\r\n\r\n");
    stream.write_all(&request).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(!response.is_empty(), "oversized headers must be rejected");
    assert!(
        !String::from_utf8_lossy(&response).starts_with("HTTP/1.1 200"),
        "oversized headers must not reach the default route"
    );

    let _ = shutdown_tx.send(());
    assert!(serving.await.unwrap().unwrap().graceful);
}

#[tokio::test]
async fn configured_limits_do_not_change_an_unlayered_native_router() {
    let mut config = config_with("127.0.0.1:0", 2500);
    config.set("http.max_body_bytes", 3u64).unwrap();
    let configured = ConfiguredWeb::from_config(&config).unwrap();
    let app = Router::new().route("/body", post(|_: Bytes| async { "accepted" }));

    let response = app
        .oneshot(Request::post("/body").body(Body::from("four")).unwrap())
        .await
        .unwrap();

    assert_eq!(configured.http_limits().max_body_bytes(), 3);
    assert_eq!(response.status(), StatusCode::OK);
}

#[test]
fn uses_the_standard_shutdown_timeout_when_omitted() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();

    let configured = ConfiguredWeb::from_config(&config).unwrap();
    let options = configured.server_options();

    assert_eq!(options.shutdown_timeout(), std::time::Duration::from_secs(30));
    assert_eq!(options.request_header_timeout(), std::time::Duration::from_secs(10));
    assert_eq!(options.transport_idle_timeout(), std::time::Duration::from_secs(30));
    let limits = configured.http_limits();
    assert_eq!(limits.max_body_bytes(), 1024 * 1024);
    assert_eq!(limits.max_concurrent_requests(), 256);
    assert_eq!(limits.request_timeout(), std::time::Duration::from_secs(30));
    options.validate().unwrap();
}

#[test]
fn rejects_an_invalid_address_without_echoing_its_value() {
    let config = config_with("private-address-sentinel", 1000);

    let error = ConfiguredWeb::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "address");
    assert!(!error.to_string().contains("private-address-sentinel"));
}

#[test]
fn rejects_configuration_values_with_the_wrong_type() {
    let mut config = Config::new();
    config.set("address", 123u64).unwrap();
    assert_eq!(ConfiguredWeb::from_config(&config).unwrap_err().field(), "address");

    for field in ["shutdown_timeout_ms", "http.max_body_bytes", "http.request_timeout_ms"] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set(field, "private-value-sentinel").unwrap();

        let error = ConfiguredWeb::from_config(&config).unwrap_err();

        assert_eq!(error.field(), field);
        assert!(!error.to_string().contains("private-value-sentinel"));
    }
}

#[test]
fn reports_a_malformed_limit_by_field_without_echoing_its_value() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();
    config
        .set("http.max_concurrent_requests", "private-limit-sentinel")
        .unwrap();

    let error = ConfiguredWeb::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "http.max_concurrent_requests");
    assert!(!error.to_string().contains("private-limit-sentinel"));
    let debug = format!("{error:?}");
    assert!(debug.contains("http.max_concurrent_requests"));
    assert!(!debug.contains("private-limit-sentinel"));
}

#[test]
fn rejects_a_zero_shutdown_timeout() {
    let config = config_with("127.0.0.1:0", 0);

    let error = ConfiguredWeb::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "shutdown_timeout_ms");
}

#[test]
fn rejects_zero_http_limits_and_preserves_the_field_path() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();
    config.set("http.max_body_bytes", 0u64).unwrap();

    let error = ConfiguredWeb::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "http.max_body_bytes");
}

#[test]
fn rejects_zero_concurrency_and_request_timeout_limits() {
    for (field, config_key) in [
        ("http.max_concurrent_requests", "http.max_concurrent_requests"),
        ("http.request_timeout_ms", "http.request_timeout_ms"),
    ] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set(config_key, 0u64).unwrap();

        let error = ConfiguredWeb::from_config(&config).unwrap_err();

        assert_eq!(error.field(), field);
    }
}

#[test]
fn defaults_transport_connection_limit_to_1024_and_reports_invalid_values() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();

    let configured = ConfiguredWeb::from_config(&config).unwrap();
    assert_eq!(configured.server_options().max_transport_connections(), 1024);

    for invalid in ["zero", "wrong_type"] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        if invalid == "zero" {
            config.set("transport.max_connections", 0u64).unwrap();
        } else {
            config.set("transport.max_connections", "private-sentinel").unwrap();
        }

        let error = ConfiguredWeb::from_config(&config).unwrap_err();

        assert_eq!(error.field(), "transport.max_connections");
        assert!(!error.to_string().contains("private-sentinel"));
    }
}

#[test]
fn configures_per_connection_http2_stream_limit_and_reports_its_field() {
    for (configured, expected) in [(1u64, 1u32), (u64::from(u32::MAX), u32::MAX)] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config
            .set("transport.http2_max_concurrent_streams", configured)
            .unwrap();
        let parsed = ConfiguredWeb::from_config(&config).unwrap();
        assert_eq!(parsed.server_options().http2_max_concurrent_streams(), expected);
    }

    for invalid in [Some(0u64), Some(u64::from(u32::MAX) + 1), None] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        if let Some(value) = invalid {
            config.set("transport.http2_max_concurrent_streams", value).unwrap();
        } else {
            config
                .set("transport.http2_max_concurrent_streams", "private-sentinel")
                .unwrap();
        }
        let error = ConfiguredWeb::from_config(&config).unwrap_err();
        assert_eq!(error.field(), "transport.http2_max_concurrent_streams");
        assert!(!error.to_string().contains("private-sentinel"));
    }
}

#[test]
fn rejects_zero_or_malformed_transport_idle_timeout_by_field() {
    for malformed in [false, true] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        if malformed {
            config.set("transport.idle_timeout_ms", "private-sentinel").unwrap();
        } else {
            config.set("transport.idle_timeout_ms", 0u64).unwrap();
        }

        let error = ConfiguredWeb::from_config(&config).unwrap_err();

        assert_eq!(error.field(), "transport.idle_timeout_ms");
        assert!(!error.to_string().contains("private-sentinel"));
    }
}

#[test]
fn configured_transport_idle_timeout_uses_the_field_specific_deadline_check() {
    let timeout = std::time::Duration::from_millis(u64::MAX);
    if tokio::time::Instant::now().checked_add(timeout).is_none() {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set("transport.idle_timeout_ms", u64::MAX).unwrap();
        let error = ConfiguredWeb::from_config(&config).unwrap_err();
        assert_eq!(error.field(), "transport.idle_timeout_ms");
    }
}

#[test]
fn rejects_configured_semaphore_capacity_above_maximum() {
    let invalid = u64::try_from(tokio::sync::Semaphore::MAX_PERMITS).expect("Tokio semaphore maximum fits u64") + 1;
    for field in ["http.max_concurrent_requests", "transport.max_connections"] {
        let mut config = config_with("127.0.0.1:0", 2500);
        config.set(field, invalid).unwrap();
        let error = match ConfiguredWeb::from_config(&config) {
            Ok(_) => panic!("{field} must reject an oversized capacity"),
            Err(error) => error,
        };
        assert_eq!(error.field(), field);
        assert!(!error.to_string().contains(&invalid.to_string()));
    }
}
