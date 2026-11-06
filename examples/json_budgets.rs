// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates independent HTTP, JSON input, and JSON output budgets.

use std::net::SocketAddr;

use axum::Router;
use axum::extract::Extension;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::post;
use qubit_web::BoundedJson;
use qubit_web::HttpLimits;
use qubit_web::JsonLimits;
use qubit_web::RequestLimitLayer;
use qubit_web::json_response;
use serde::Deserialize;
use serde::Serialize;
use tokio::main as tokio_main;

const HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
const JSON_INPUT_BYTES: usize = 2 * 1024 * 1024;
const JSON_OUTPUT_BYTES: usize = 4096;

#[derive(Deserialize)]
struct PromptInput {
    prompt: String,
}

#[derive(Serialize)]
struct PromptResponse<'a> {
    accepted: bool,
    prompt: &'a str,
}

fn output_failure(code: &'static str) -> Response {
    let body = match code {
        "json_output_budget_exceeded" => "json_output_budget_exceeded",
        "json_encode_failed" => "json_encode_failed",
        _ => unreachable!("JsonResponseError::code() has a fixed set of values"),
    };
    (StatusCode::INTERNAL_SERVER_ERROR, body).into_response()
}

fn encode_response<T: Serialize + ?Sized>(value: &T, limits: &JsonLimits) -> Response {
    match json_response(value, limits) {
        Ok(response) => response,
        Err(error) => output_failure(error.code()),
    }
}

async fn submit(BoundedJson(input): BoundedJson<PromptInput>) -> Response {
    let output_limits = JsonLimits::default()
        .with_max_output_bytes(JSON_OUTPUT_BYTES)
        .expect("positive output budget");
    encode_response(
        &PromptResponse {
            accepted: true,
            prompt: &input.prompt,
        },
        &output_limits,
    )
}

/// Builds the prompt endpoint with separate HTTP, JSON input, and output
/// budgets.
pub fn build_app() -> Router {
    let input_limits = JsonLimits::default()
        .with_max_input_bytes(JSON_INPUT_BYTES)
        .expect("positive input budget")
        .with_max_string_bytes(JSON_INPUT_BYTES)
        .expect("positive string budget");
    let http_limits = HttpLimits::default()
        .with_max_body_bytes(HTTP_BODY_BYTES)
        .expect("positive HTTP body budget");

    Router::new()
        .route("/prompts", post(submit))
        .layer(Extension(input_limits))
        .layer(RequestLimitLayer::new(http_limits))
}

#[tokio_main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = SocketAddr::from(([127, 0, 0, 1], 3001));
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("listening on http://{address}");
    axum::serve(listener, build_app()).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::body::to_bytes;
    use axum::extract::Extension;
    use axum::http::Request;
    use axum::http::header::CONTENT_TYPE;
    use axum::routing::post;
    use qubit_json::decode::JsonDecoder;
    use qubit_web::BoundedJson;
    use qubit_web::HttpLimits;
    use qubit_web::JsonLimits;
    use qubit_web::RequestLimitLayer;
    use serde::Deserialize;
    use serde::Serialize;
    use tower::ServiceExt;

    use super::JSON_INPUT_BYTES;
    use super::PromptInput;
    use super::build_app;
    use super::encode_response;

    const RESPONSE_READ_CAP: usize = 16 * 1024;

    #[derive(Deserialize)]
    struct PromptResponse {
        accepted: bool,
        prompt: String,
    }

    fn request(body: String) -> Request<Body> {
        Request::post("/prompts")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    async fn status(app: axum::Router, body: String) -> (axum::http::StatusCode, Vec<u8>) {
        let response = app.oneshot(request(body)).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), RESPONSE_READ_CAP).await.unwrap();
        (status, bytes.to_vec())
    }

    #[tokio::test]
    async fn test_accepts_small_prompt_and_returns_json_fields() {
        let (status, body) = status(build_app(), r#"{"prompt":"hello"}"#.into()).await;
        assert_eq!(status, axum::http::StatusCode::OK);
        let value = JsonDecoder::unlimited().decode_utf8::<PromptResponse>(&body).unwrap();
        assert!(value.accepted);
        assert_eq!(value.prompt, "hello");
    }

    #[tokio::test]
    async fn test_rejects_body_above_json_input_budget() {
        let body = format!(r#"{{"prompt":"{}"}}"#, "x".repeat(JSON_INPUT_BYTES));
        let (status, _) = status(build_app(), body).await;
        assert_eq!(status, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_output_budget_failure_is_a_fixed_server_error() {
        let prompt = "x".repeat(5000);
        let (status, body) = status(build_app(), format!(r#"{{"prompt":"{prompt}"}}"#)).await;
        assert_eq!(status, axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body, b"json_output_budget_exceeded");
    }

    #[tokio::test]
    async fn test_string_budget_is_independent_from_input_byte_budget() {
        async fn tiny_string_limit(BoundedJson(_): BoundedJson<PromptInput>) -> &'static str {
            "accepted"
        }
        let limits = JsonLimits::default()
            .with_max_input_bytes(JSON_INPUT_BYTES)
            .unwrap()
            .with_max_string_bytes(4)
            .unwrap();
        let app = axum::Router::new()
            .route("/prompts", post(tiny_string_limit))
            .layer(Extension(limits));
        let (status, _) = status(app, r#"{"prompt":"hello"}"#.into()).await;
        assert_eq!(status, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn test_absent_extension_uses_default_one_mib_input_budget() {
        async fn default_input(BoundedJson(_): BoundedJson<PromptInput>) -> &'static str {
            "accepted"
        }
        let app = axum::Router::new()
            .route("/prompts", post(default_input))
            .layer(RequestLimitLayer::new(
                HttpLimits::default().with_max_body_bytes(4 * 1024 * 1024).unwrap(),
            ));
        let body = format!(r#"{{"prompt":"{}"}}"#, "x".repeat(1024 * 1024));
        let (status, _) = status(app, body).await;
        assert_eq!(status, axum::http::StatusCode::PAYLOAD_TOO_LARGE);
    }

    struct BrokenSerialize;

    impl Serialize for BrokenSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("private serialization detail"))
        }
    }

    #[tokio::test]
    async fn test_serialization_failure_uses_encode_failed_classification() {
        let response = encode_response(&BrokenSerialize, &JsonLimits::default());
        assert_eq!(response.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), RESPONSE_READ_CAP).await.unwrap();
        assert_eq!(body.as_ref(), b"json_encode_failed");
    }
}
