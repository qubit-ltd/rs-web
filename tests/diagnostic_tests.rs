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
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::Method;
use axum::http::Request;
use axum::http::StatusCode;
use axum::http::header;
use axum::routing::get;
use axum::routing::post;
use hyper::body::Body as HttpBody;
use qubit_web::diagnostic::DiagnosticLayer;
use qubit_web::diagnostic::RequestDiagnostic;
use tokio::runtime::Builder;
use tower::ServiceExt;
use tracing::Level;
use tracing_subscriber::fmt::MakeWriter;

#[tokio::test]
async fn diagnostic_layer_installs_on_stateless_router() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_writer(std::io::sink)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let app: Router = Router::new()
        .route("/", get(|| async { "ok" }))
        .layer(DiagnosticLayer::new());
    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[test]
fn explicit_body_diagnostics_report_complete_after_eof() {
    let output = capture_body_diagnostic(false, false);
    assert!(
        output.contains("event=\"response_body_end\""),
        "missing body event: {output}"
    );
    assert!(
        output.contains("outcome=\"complete\""),
        "missing complete outcome: {output}"
    );
    assert!(output.contains("status=200"), "missing response status: {output}");
    assert_eq!(
        output.matches("response_body_end").count(),
        1,
        "duplicate body event: {output}"
    );
    assert!(
        !output.contains("body-secret-sentinel"),
        "body content leaked: {output}"
    );
}

#[test]
fn explicit_body_diagnostics_report_body_error_without_error_text() {
    let output = capture_error_body_diagnostic();
    assert!(
        output.contains("outcome=\"body_error\""),
        "missing body error outcome: {output}"
    );
    assert_eq!(
        output.matches("response_body_end").count(),
        1,
        "duplicate body event: {output}"
    );
    assert!(!output.contains("error-secret-sentinel"), "error text leaked: {output}");
}

fn capture_error_body_diagnostic() -> String {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let app = Router::new()
                .route(
                    "/",
                    get(|| async { axum::response::Response::new(Body::new(ErrorBody)) }),
                )
                .layer(DiagnosticLayer::with_body_diagnostics());
            let response = app
                .oneshot(Request::get("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert!(axum::body::to_bytes(response.into_body(), usize::MAX).await.is_err());
        });
    });
    String::from_utf8(output.lock().unwrap().clone()).unwrap()
}

struct ErrorBody;

impl hyper::body::Body for ErrorBody {
    type Data = axum::body::Bytes;
    type Error = io::Error;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(Some(Err(io::Error::other("error-secret-sentinel"))))
    }
}

fn capture_body_diagnostic(default_layer: bool, empty: bool) -> String {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let layer = if default_layer {
                DiagnosticLayer::new()
            } else {
                DiagnosticLayer::with_body_diagnostics()
            };
            let app = Router::new()
                .route(
                    "/",
                    get(move || async move {
                        if empty {
                            Body::empty()
                        } else {
                            Body::from("body-secret-sentinel")
                        }
                    }),
                )
                .layer(layer);
            let response = app
                .oneshot(Request::get("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
            if empty {
                assert!(bytes.is_empty());
            } else {
                assert_eq!(&bytes[..], b"body-secret-sentinel");
            }
        });
    });
    String::from_utf8(output.lock().unwrap().clone()).unwrap()
}

#[test]
fn explicit_body_diagnostics_report_empty_body_complete() {
    let output = capture_body_diagnostic(false, true);
    assert!(
        output.contains("outcome=\"complete\""),
        "missing complete outcome: {output}"
    );
    assert_eq!(
        output.matches("response_body_end").count(),
        1,
        "duplicate body event: {output}"
    );
}

#[test]
fn diagnostic_body_preserves_size_hint_and_trailers() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let app = Router::new()
                .route(
                    "/",
                    get(|| async {
                        let mut trailers = HeaderMap::new();
                        trailers.insert("x-check", HeaderValue::from_static("preserved"));
                        axum::response::Response::new(Body::new(TrailerBody { step: 0, trailers }))
                    }),
                )
                .layer(DiagnosticLayer::with_body_diagnostics());
            let mut response = app
                .oneshot(Request::get("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.body().size_hint().exact(), Some(3));
            let data = std::future::poll_fn(|cx| std::pin::Pin::new(response.body_mut()).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(data.into_data().unwrap(), "abc");
            let trailers = std::future::poll_fn(|cx| std::pin::Pin::new(response.body_mut()).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(trailers.into_trailers().unwrap().get("x-check").unwrap(), "preserved");
            assert!(
                std::future::poll_fn(|cx| std::pin::Pin::new(response.body_mut()).poll_frame(cx))
                    .await
                    .is_none()
            );
        });
    });
    let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(log.contains("outcome=\"complete\""), "missing completion event: {log}");
}

struct TrailerBody {
    step: u8,
    trailers: HeaderMap,
}

impl HttpBody for TrailerBody {
    type Data = axum::body::Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        let frame = match self.step {
            0 => hyper::body::Frame::data(axum::body::Bytes::from_static(b"abc")),
            1 => hyper::body::Frame::trailers(std::mem::take(&mut self.trailers)),
            _ => return std::task::Poll::Ready(None),
        };
        self.step += 1;
        std::task::Poll::Ready(Some(Ok(frame)))
    }
    fn size_hint(&self) -> hyper::body::SizeHint {
        let mut hint = hyper::body::SizeHint::new();
        hint.set_exact(3);
        hint
    }
}

#[test]
fn default_diagnostic_layer_does_not_report_body_termination() {
    let output = capture_body_diagnostic(true, false);
    assert!(
        !output.contains("response_body_end"),
        "default layer emitted body event: {output}"
    );
}

#[test]
fn explicit_body_diagnostics_report_drop() {
    let output = capture_dropped_body_diagnostic();
    assert!(
        output.contains("outcome=\"dropped\""),
        "missing dropped outcome: {output}"
    );
    assert_eq!(
        output.matches("response_body_end").count(),
        1,
        "duplicate body event: {output}"
    );
}

fn capture_dropped_body_diagnostic() -> String {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let app = Router::new()
                .route("/", get(|| async { "dropped-secret" }))
                .layer(DiagnosticLayer::with_body_diagnostics());
            let response = app
                .oneshot(Request::get("/").body(Body::empty()).unwrap())
                .await
                .unwrap();
            drop(response);
        });
    });
    String::from_utf8(output.lock().unwrap().clone()).unwrap()
}

#[tokio::test]
async fn diagnostic_layer_installs_before_router_state() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_writer(std::io::sink)
        .finish();
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let app: Router = Router::new()
        .route("/", get(|State(value): State<&'static str>| async move { value }))
        .layer(DiagnosticLayer::new())
        .with_state("ok");
    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

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
fn test_diagnostic_accepts_dots_inside_segments() {
    for route in ["/releases/v1..v2", "/files/file...json"] {
        assert!(
            RequestDiagnostic::new(Method::GET, route, 200, Duration::ZERO, "c1", None,).is_ok(),
            "expected ordinary dots inside a path segment to be accepted: {route}"
        );
    }
}

#[test]
fn diagnostic_rejects_standalone_dot_segments() {
    for route in ["/a/../b", "/./health"] {
        assert!(
            RequestDiagnostic::new(Method::GET, route, 200, Duration::ZERO, "c1", None,).is_err(),
            "expected standalone dot segment to be rejected: {route}"
        );
    }
    assert!(RequestDiagnostic::new(Method::GET, "<unmatched>", 404, Duration::ZERO, "c1", None,).is_ok());
}

#[test]
fn diagnostic_rejects_query_fragment_and_control_characters_in_routes() {
    for route in ["/users?token=secret", "/users#fragment", "/users/\n"] {
        assert!(
            RequestDiagnostic::new(Method::GET, route, 200, Duration::ZERO, "c1", None,).is_err(),
            "expected unsafe route template to be rejected: {route:?}"
        );
    }
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

#[test]
fn diagnostic_layer_logs_route_templates_with_dots_inside_segments() {
    let output = capture_dotted_route_log();
    for field in ["route=/releases/v1..v2", "status=200"] {
        assert!(output.contains(field), "log omitted {field}: {output}");
    }
}

#[test]
fn unmatched_and_fallback_request_logs_use_safe_marker() {
    const QUERY_SECRET: &str = "unmatched-query-secret-sentinel";
    const AUTH_SECRET: &str = "unmatched-authorization-secret-sentinel";
    const COOKIE_SECRET: &str = "unmatched-cookie-secret-sentinel";

    for custom_fallback in [false, true] {
        for level in [Level::INFO, Level::TRACE] {
            let output = capture_unmatched_request_log(level, custom_fallback);
            for secret in [QUERY_SECRET, AUTH_SECRET, COOKIE_SECRET] {
                assert!(!output.contains(secret), "{level} log leaked {secret}: {output}");
            }
            for field in ["diagnostic=", "route=<unmatched>", "status=404"] {
                assert!(output.contains(field), "{level} log omitted {field}: {output}");
            }
        }
    }
}

fn capture_unmatched_request_log(level: Level, custom_fallback: bool) -> String {
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
            let router = if custom_fallback {
                Router::new().fallback(|| async { StatusCode::NOT_FOUND })
            } else {
                Router::new()
            };
            let app = router.layer(DiagnosticLayer::new());
            let request = Request::builder()
                .uri("/missing?token=unmatched-query-secret-sentinel")
                .header(
                    header::AUTHORIZATION,
                    HeaderValue::from_static("Bearer unmatched-authorization-secret-sentinel"),
                )
                .header(
                    header::COOKIE,
                    HeaderValue::from_static("session=unmatched-cookie-secret-sentinel"),
                )
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        });
    });
    String::from_utf8(output.lock().unwrap().clone()).unwrap()
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

fn capture_dotted_route_log() -> String {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = SharedWriter(output.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .without_time()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    tracing::dispatcher::with_default(&dispatch, || {
        let runtime = Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let app = Router::new()
                .route("/releases/v1..v2", post(|| async { "ok" }))
                .layer(DiagnosticLayer::new());
            let request = Request::builder()
                .method("POST")
                .uri("/releases/v1..v2")
                .body(Body::empty())
                .unwrap();
            let response = app.oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
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
