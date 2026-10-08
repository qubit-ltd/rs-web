// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::header;
use axum::routing::post;
use qubit_web::diagnostic::DiagnosticLayer;
use qubit_web::diagnostic::RequestDiagnostic;
use tokio::runtime::Builder;
use tower::ServiceExt;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

#[test]
fn diagnostic_formats_only_allowlisted_request_metadata() {
    let diagnostic = RequestDiagnostic::new(
        Method::POST,
        "/users/{id}",
        400,
        Duration::from_millis(12),
        "connection-7",
        Some(5),
    )
    .unwrap();
    let rendered = diagnostic.to_string();
    for expected in ["POST", "/users/{id}", "400", "12", "connection-7", "5"] {
        assert!(rendered.contains(expected), "missing {expected} in {rendered}");
    }
    for secret in [
        "secret-query",
        "authorization-sentinel",
        "prompt-sentinel",
        "cookie-sentinel",
    ] {
        assert!(!rendered.contains(secret));
    }
}

#[test]
fn diagnostic_never_accepts_a_full_url_as_the_route_template() {
    assert!(
        RequestDiagnostic::new(
            Method::GET,
            "https://example.test/path?token=secret-query",
            200,
            Duration::ZERO,
            "c1",
            None,
        )
        .is_err()
    );
}

#[test]
fn diagnostic_rejects_log_control_characters() {
    assert!(
        RequestDiagnostic::new(
            Method::GET,
            "/users/{id}",
            200,
            Duration::ZERO,
            "connection\nforged-entry",
            None,
        )
        .is_err()
    );
}

#[test]
fn request_logs_at_info_and_trace_keep_only_allowlisted_fields() {
    const QUERY_SECRET: &str = "query-secret-sentinel";
    const AUTH_SECRET: &str = "authorization-secret-sentinel";
    const COOKIE_SECRET: &str = "cookie-secret-sentinel";
    const BODY_SECRET: &str = "body-secret-sentinel";

    for level in [Level::INFO, Level::TRACE] {
        let output = capture_request_log(level);
        for secret in [QUERY_SECRET, AUTH_SECRET, COOKIE_SECRET, BODY_SECRET] {
            assert!(!output.contains(secret), "{level} log leaked {secret}: {output}");
        }
        for field in [
            "diagnostic=",
            "method=POST",
            "route=/users/{id}",
            "status=200",
            "elapsed_ms=",
            "connection_id=unavailable",
            "bytes=",
        ] {
            assert!(output.contains(field), "{level} log omitted {field}: {output}");
        }
    }
}

fn capture_request_log(level: Level) -> String {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(level)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let app = Router::new()
                .route("/users/{id}", post(|| async { "ok" }))
                .layer(DiagnosticLayer::new());
            let body = "body-secret-sentinel";
            let request = Request::builder()
                .method("POST")
                .uri("/users/42?token=query-secret-sentinel")
                .header(
                    header::AUTHORIZATION,
                    HeaderValue::from_static("Bearer authorization-secret-sentinel"),
                )
                .header(
                    header::COOKIE,
                    HeaderValue::from_static("session=cookie-secret-sentinel"),
                )
                .header(header::CONTENT_TYPE, "text/plain")
                .header(header::CONTENT_LENGTH, body.len().to_string())
                .body(Body::from(body))
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), 200);
        });
    });
    String::from_utf8(output.lock().unwrap().clone()).unwrap()
}

#[derive(Clone)]
struct SharedWriter(Arc<Mutex<Vec<u8>>>);

impl Write for SharedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for SharedWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}
