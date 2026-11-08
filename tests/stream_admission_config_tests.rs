// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
#![cfg(feature = "config")]

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use qubit_config::Config;
use qubit_web::StreamAdmissionLayer;
use qubit_web::config::stream_admission_limits_from_config;
use tower::ServiceExt;

const BODY: &str = "stream_admission.max_body_bytes";
const CAPACITY: &str = "stream_admission.max_concurrent_admissions";
const TIMEOUT: &str = "stream_admission.admission_timeout_ms";

#[test]
fn defaults_do_not_require_an_address() {
    let limits = stream_admission_limits_from_config(&Config::new()).unwrap();
    assert_eq!(limits.max_body_bytes(), 1024 * 1024);
    assert_eq!(limits.max_concurrent_admissions(), 256);
    assert_eq!(limits.admission_timeout(), Duration::from_millis(10_000));
}

#[test]
fn partial_and_full_values_override_defaults() {
    let mut config = Config::new();
    config.set(BODY, 3_u64).unwrap();
    let partial = stream_admission_limits_from_config(&config).unwrap();
    assert_eq!(partial.max_body_bytes(), 3);
    assert_eq!(partial.max_concurrent_admissions(), 256);

    config.set(CAPACITY, 1_u64).unwrap();
    config.set(TIMEOUT, 1500_u64).unwrap();
    let limits = stream_admission_limits_from_config(&config).unwrap();
    assert_eq!(limits.max_body_bytes(), 3);
    assert_eq!(limits.max_concurrent_admissions(), 1);
    assert_eq!(limits.admission_timeout(), Duration::from_millis(1500));
}

fn assert_error(config: &Config, key: &str) -> String {
    let error = stream_admission_limits_from_config(config).unwrap_err();
    assert_eq!(error.field(), key);
    let formatted = format!("{error} {error:?}");
    assert!(formatted.contains(key));
    formatted
}

#[test]
fn wrong_types_are_field_specific_and_do_not_leak_values() {
    for (key, value) in [
        (BODY, "private-body-value"),
        (CAPACITY, "private-capacity-value"),
        (TIMEOUT, "private-timeout-value"),
    ] {
        let mut config = Config::new();
        config.set(key, value).unwrap();
        let message = assert_error(&config, key);
        assert!(!message.contains(value));
    }
    for (key, value) in [(BODY, false), (CAPACITY, false), (TIMEOUT, false)] {
        let mut config = Config::new();
        config.set(key, value).unwrap();
        assert_error(&config, key);
    }
}

#[test]
fn zero_and_capacity_overflow_are_rejected() {
    for (key, value) in [(BODY, 0), (CAPACITY, 0), (TIMEOUT, 0)] {
        let mut config = Config::new();
        config.set(key, value as u64).unwrap();
        assert_error(&config, key);
    }
    let mut config = Config::new();
    config
        .set(CAPACITY, (tokio::sync::Semaphore::MAX_PERMITS as u64) + 1)
        .unwrap();
    assert_error(&config, CAPACITY);

    let mut config = Config::new();
    config
        .set(CAPACITY, tokio::sync::Semaphore::MAX_PERMITS as u64)
        .unwrap();
    assert_eq!(
        stream_admission_limits_from_config(&config)
            .unwrap()
            .max_concurrent_admissions(),
        tokio::sync::Semaphore::MAX_PERMITS
    );
}

#[test]
fn earlier_setter_validation_error_precedes_later_type_error() {
    let mut config = Config::new();
    config.set(BODY, 0_u64).unwrap();
    config.set(CAPACITY, "malformed-later-value").unwrap();
    assert_error(&config, BODY);
}

#[test]
fn platform_conversion_and_timeout_boundaries_are_checked() {
    if usize::BITS < u64::BITS {
        let mut config = Config::new();
        config.set(BODY, u64::MAX).unwrap();
        assert_error(&config, BODY);
        let mut config = Config::new();
        config.set(CAPACITY, u64::MAX).unwrap();
        assert_error(&config, CAPACITY);
    }
    let mut config = Config::new();
    config.set(TIMEOUT, u64::MAX).unwrap();
    let timeout = Duration::from_millis(u64::MAX);
    if tokio::time::Instant::now().checked_add(timeout).is_none() {
        assert_error(&config, TIMEOUT);
    } else {
        assert_eq!(
            stream_admission_limits_from_config(&config)
                .unwrap()
                .admission_timeout(),
            timeout
        );
    }
}

#[tokio::test]
async fn configured_body_budget_is_enforced_by_the_layer() {
    async fn echo(body: Bytes) -> String {
        body.len().to_string()
    }
    let mut config = Config::new();
    config.set(BODY, 3_u64).unwrap();
    let limits = stream_admission_limits_from_config(&config).unwrap();
    let app = Router::new()
        .route("/", post(echo))
        .layer(StreamAdmissionLayer::new(limits));
    for (body, expected) in [("abc", StatusCode::OK), ("abcd", StatusCode::PAYLOAD_TOO_LARGE)] {
        let response = app
            .clone()
            .oneshot(Request::post("/").body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
    }
}

#[derive(Clone)]
struct Gate {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Semaphore>,
}

async fn held(State(gate): State<Gate>) -> &'static str {
    gate.entered.notify_one();
    gate.release.acquire().await.unwrap().forget();
    "ok"
}

#[tokio::test]
async fn configured_capacity_rejects_and_cancellation_releases_its_permit() {
    let mut config = Config::new();
    config.set(CAPACITY, 1_u64).unwrap();
    let limits = stream_admission_limits_from_config(&config).unwrap();
    let gate = Gate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let app = Router::new()
        .route("/", post(held))
        .with_state(gate.clone())
        .layer(StreamAdmissionLayer::new(limits));
    let first_app = app.clone();
    let first = tokio::spawn(async move {
        first_app
            .oneshot(Request::post("/").body(Body::empty()).unwrap())
            .await
            .unwrap()
    });
    gate.entered.notified().await;
    let rejected = app
        .clone()
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    gate.release.add_permits(1);
    let accepted = app
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
}

#[tokio::test(start_paused = true)]
async fn configured_timeout_expires_handler_and_recovers_capacity() {
    let mut config = Config::new();
    config.set(CAPACITY, 1_u64).unwrap();
    config.set(TIMEOUT, 10_u64).unwrap();
    let limits = stream_admission_limits_from_config(&config).unwrap();
    let gate = Gate {
        entered: Arc::new(tokio::sync::Notify::new()),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let app = Router::new()
        .route("/", post(held))
        .with_state(gate.clone())
        .layer(StreamAdmissionLayer::new(limits));
    let first_app = app.clone();
    let first = tokio::spawn(async move {
        first_app
            .oneshot(Request::post("/").body(Body::empty()).unwrap())
            .await
            .unwrap()
    });
    gate.entered.notified().await;
    tokio::time::advance(Duration::from_millis(10)).await;
    assert_eq!(first.await.unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    let recovered = app
        .oneshot(Request::post("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(recovered.status(), StatusCode::GATEWAY_TIMEOUT);
}
