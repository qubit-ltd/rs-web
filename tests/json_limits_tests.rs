// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "json")]

use axum::Extension;
use axum::Router;
use axum::body::Body;
use axum::body::to_bytes;
use axum::http::Request;
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use qubit_web::json::BoundedJson;
use qubit_web::json::JsonLimits;
use qubit_web::json::json_response;
use serde::de::IgnoredAny;
use tower::ServiceExt;

async fn echo_number(BoundedJson(value): BoundedJson<u64>) -> String {
    value.to_string()
}

async fn echo_array(BoundedJson(value): BoundedJson<Vec<u64>>) -> String {
    value.len().to_string()
}

async fn ignore_json(BoundedJson(_): BoundedJson<IgnoredAny>) -> &'static str {
    "accepted"
}

fn app() -> Router {
    Router::new().route("/number", post(echo_number))
}

async fn send(uri: &str, content_type: &str, body: &'static str) -> Response {
    app()
        .oneshot(
            Request::post(uri)
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn send_with_limits(body: &'static str, limits: JsonLimits) -> Response {
    Router::new()
        .route("/", post(ignore_json))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_accepts_json_and_vendor_json_media_types() {
    assert_eq!(send("/number", "application/json", "42").await.status(), StatusCode::OK);
    assert_eq!(
        send("/number", "application/vnd.example+json", "42").await.status(),
        StatusCode::OK
    );
    assert_eq!(
        send("/number", "APPLICATION/JSON; Charset=\"UTF-8\"", "42")
            .await
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        send("/number", "application/vnd.example+json; charset=utf-8", "42")
            .await
            .status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn test_rejects_non_utf8_duplicate_and_malformed_content_type_parameters() {
    for content_type in [
        "application/json; charset=us-ascii",
        "application/json; profile=example",
        "application/json; charset=utf-8; charset=utf-8",
        "application/json; charset=\"utf-8",
        "application/json; charset=utf-8;",
    ] {
        assert_eq!(
            send("/number", content_type, "42").await.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unexpectedly accepted {content_type}"
        );
    }
}

#[tokio::test]
async fn test_decodes_u64_max_without_precision_loss() {
    let response = send("/number", "application/json", "18446744073709551615").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        "18446744073709551615"
    );
}

#[tokio::test]
async fn test_rejects_unsupported_media_type() {
    assert_eq!(
        send("/number", "text/plain", "42").await.status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
}

#[tokio::test]
async fn test_rejects_invalid_json_and_target_type_mismatch() {
    assert_eq!(
        send("/number", "application/json", "42 trailing").await.status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send("/number", "application/json", "\"not a number\"").await.status(),
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn test_rejects_body_over_input_budget() {
    let limits = JsonLimits::default().with_max_input_bytes(1).unwrap();
    let response = Router::new()
        .route("/", post(echo_number))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("42"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn test_accepts_input_exactly_at_byte_budget() {
    let limits = JsonLimits::default().with_max_input_bytes(2).unwrap();
    assert_eq!(send_with_limits("42", limits).await.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_rejects_structure_budget_exceeded() {
    let limits = JsonLimits::default().with_max_sequence_items(1).unwrap();
    let response = Router::new()
        .route("/", post(echo_array))
        .layer(Extension(limits))
        .oneshot(
            Request::post("/")
                .header("content-type", "application/json")
                .body(Body::from("[1,2]"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn test_rejects_max_depth_budget() {
    let limits = JsonLimits::default().with_max_depth(1).unwrap();
    assert_eq!(
        send_with_limits("{\"a\":{\"b\":0}}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_nodes_budget() {
    let limits = JsonLimits::default().with_max_nodes(1).unwrap();
    assert_eq!(
        send_with_limits("[1]", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_map_entries_budget() {
    let limits = JsonLimits::default().with_max_map_entries(1).unwrap();
    assert_eq!(
        send_with_limits("{\"a\":1,\"b\":2}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_key_bytes_budget() {
    let limits = JsonLimits::default().with_max_key_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("{\"long\":1}", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_string_bytes_budget() {
    let limits = JsonLimits::default().with_max_string_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("\"abc\"", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_rejects_max_number_bytes_budget() {
    let limits = JsonLimits::default().with_max_number_bytes(2).unwrap();
    assert_eq!(
        send_with_limits("123", limits).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
}

#[tokio::test]
async fn test_output_budget_failure_returns_no_partial_json_response() {
    let limits = JsonLimits::default().with_max_output_bytes(1).unwrap();
    let error = json_response(&u64::MAX, &limits).unwrap_err();
    assert_eq!(error.code(), "json_output_budget_exceeded");
}

#[tokio::test]
async fn test_output_structure_budget_failure_returns_encoding_error() {
    let limits = JsonLimits::default().with_max_sequence_items(1).unwrap();
    let error = json_response(&[1_u64, 2], &limits).unwrap_err();
    assert_eq!(error.code(), "json_output_budget_exceeded");
}

#[tokio::test]
async fn test_output_preserves_u64_max_precision() {
    let response = json_response(&u64::MAX, &JsonLimits::default()).unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        u64::MAX.to_string()
    );
}
