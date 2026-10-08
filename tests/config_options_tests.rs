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
use qubit_web::RequestLimitLayer;
use qubit_web::ServerOptions;
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

    let options = ServerOptions::from_config(&config).unwrap();
    assert_eq!(options.address(), "127.0.0.1:0".parse().unwrap());
    assert_eq!(options.shutdown_timeout(), std::time::Duration::from_millis(2500));
    let limits = options.http_limits();
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

#[tokio::test]
async fn configured_http_limits_apply_when_attached_to_a_router_branch() {
    let mut config = config_with("127.0.0.1:0", 2500);
    config.set("http.max_body_bytes", 3u64).unwrap();
    let options = ServerOptions::from_config(&config).unwrap();
    let app = Router::new()
        .route("/body", post(|_: Bytes| async { "ok" }))
        .layer(RequestLimitLayer::new(options.http_limits()));

    let response = app
        .oneshot(Request::post("/body").body(Body::from("four")).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn uses_the_standard_shutdown_timeout_when_omitted() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();

    let options = ServerOptions::from_config(&config).unwrap();

    assert_eq!(options.shutdown_timeout(), std::time::Duration::from_secs(30));
    assert_eq!(options.request_header_timeout(), std::time::Duration::from_secs(10));
    let limits = options.http_limits();
    assert_eq!(limits.max_body_bytes(), 1024 * 1024);
    assert_eq!(limits.max_concurrent_requests(), 256);
    assert_eq!(limits.request_timeout(), std::time::Duration::from_secs(30));
    options.validate().unwrap();
}

#[test]
fn rejects_an_invalid_address_without_echoing_its_value() {
    let config = config_with("private-address-sentinel", 1000);

    let error = ServerOptions::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "address");
    assert!(!error.to_string().contains("private-address-sentinel"));
}

#[test]
fn rejects_configuration_values_with_the_wrong_type() {
    let mut config = Config::new();
    config.set("address", 123u64).unwrap();
    assert_eq!(ServerOptions::from_config(&config).unwrap_err().field(), "address");

    for field in ["shutdown_timeout_ms", "http.max_body_bytes", "http.request_timeout_ms"] {
        let mut config = Config::new();
        config.set("address", "127.0.0.1:0").unwrap();
        config.set(field, "private-value-sentinel").unwrap();

        let error = ServerOptions::from_config(&config).unwrap_err();

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

    let error = ServerOptions::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "http.max_concurrent_requests");
    assert!(!error.to_string().contains("private-limit-sentinel"));
    assert!(!format!("{error:?}").contains("private-limit-sentinel"));
}

#[test]
fn rejects_a_zero_shutdown_timeout() {
    let config = config_with("127.0.0.1:0", 0);

    let error = ServerOptions::from_config(&config).unwrap_err();

    assert_eq!(error.field(), "shutdown_timeout_ms");
}

#[test]
fn rejects_zero_http_limits_and_preserves_the_field_path() {
    let mut config = Config::new();
    config.set("address", "127.0.0.1:0").unwrap();
    config.set("http.max_body_bytes", 0u64).unwrap();

    let error = ServerOptions::from_config(&config).unwrap_err();

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

        let error = ServerOptions::from_config(&config).unwrap_err();

        assert_eq!(error.field(), field);
    }
}
