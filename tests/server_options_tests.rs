// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

use qubit_web::ServerOptions;

#[test]
fn test_defaults_and_builders_expose_all_server_settings() {
    let address = "127.0.0.1:8080".parse().unwrap();
    let defaults = ServerOptions::new(address);
    assert_eq!(defaults.address(), address);
    assert_eq!(defaults.shutdown_timeout(), Duration::from_secs(30));
    assert_eq!(defaults.request_header_timeout(), Duration::from_secs(10));
    assert_eq!(defaults.transport_idle_timeout(), Duration::from_secs(30));
    assert_eq!(defaults.max_transport_connections(), 1024);
    assert_eq!(defaults.http1_max_buffer_bytes(), None);
    assert_eq!(defaults.http2_max_header_list_bytes(), None);
    defaults.validate().unwrap();

    let configured = defaults
        .with_shutdown_timeout(Duration::from_secs(5))
        .with_request_header_timeout(Duration::from_secs(6))
        .with_transport_idle_timeout(Duration::from_secs(7))
        .with_max_transport_connections(8)
        .unwrap();
    assert_eq!(configured.shutdown_timeout(), Duration::from_secs(5));
    assert_eq!(configured.request_header_timeout(), Duration::from_secs(6));
    assert_eq!(configured.transport_idle_timeout(), Duration::from_secs(7));
    assert_eq!(configured.max_transport_connections(), 8);
    configured.validate().unwrap();
}

#[test]
fn test_rejects_zero_timeouts_and_transport_connection_limit() {
    let address = "127.0.0.1:8080".parse().unwrap();
    assert!(
        ServerOptions::new(address)
            .with_shutdown_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        ServerOptions::new(address)
            .with_request_header_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        ServerOptions::new(address)
            .with_transport_idle_timeout(Duration::ZERO)
            .validate()
            .is_err()
    );
    assert!(
        ServerOptions::new(address)
            .with_transport_idle_timeout(Duration::MAX)
            .validate()
            .is_err()
    );
    assert!(ServerOptions::new(address).with_max_transport_connections(0).is_err());
}

#[test]
fn test_http2_stream_limit_defaults_and_validates_positive_u32_values() {
    let address = "127.0.0.1:8080".parse().unwrap();
    assert_eq!(ServerOptions::new(address).http2_max_concurrent_streams(), 200);
    for streams in [1, u32::MAX] {
        assert_eq!(
            ServerOptions::new(address)
                .with_http2_max_concurrent_streams(streams)
                .unwrap()
                .http2_max_concurrent_streams(),
            streams
        );
    }
    assert!(
        ServerOptions::new(address)
            .with_http2_max_concurrent_streams(0)
            .is_err()
    );
}

#[test]
fn test_optional_protocol_header_budgets_validate_and_expose_values() {
    let address = "127.0.0.1:8080".parse().unwrap();
    let defaults = ServerOptions::new(address);
    assert_eq!(defaults.http1_max_buffer_bytes(), None);
    assert_eq!(defaults.http2_max_header_list_bytes(), None);
    assert!(defaults.clone().with_http1_max_buffer_bytes(8191).is_err());
    assert!(defaults.clone().with_http2_max_header_list_bytes(0).is_err());
    let configured = defaults
        .with_http1_max_buffer_bytes(8192)
        .unwrap()
        .with_http2_max_header_list_bytes(1)
        .unwrap();
    assert_eq!(configured.http1_max_buffer_bytes(), Some(8192));
    assert_eq!(configured.http2_max_header_list_bytes(), Some(1));
    configured.validate().unwrap();
}

#[test]
fn test_rejects_unrepresentable_shutdown_timeout() {
    let address = "127.0.0.1:8080".parse().unwrap();
    assert!(matches!(
        ServerOptions::new(address)
            .with_shutdown_timeout(Duration::MAX)
            .validate(),
        Err(qubit_web::WebServerError::InvalidConfig)
    ));
}
