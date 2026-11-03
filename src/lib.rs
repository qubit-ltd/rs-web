// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Server-side web infrastructure for Qubit Rust services.
//!
//! The base runtime accepts a fully configured [`axum::Router<()>`], binds an
//! explicit socket address, and lets the host control shutdown:
//!
//! ```no_run
//! use axum::{Router, routing::get};
//! use qubit_web::{ServerOptions, WebServer};
//!
//! # async fn run() -> Result<(), qubit_web::WebServerError> {
//! use qubit_web::{HttpLimits, RequestLimitLayer};
//!
//! let app = Router::new().route(
//!     "/health",
//!     get(|| async { "ok" }).layer(RequestLimitLayer::new(HttpLimits::default())),
//! );
//! let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap())).await?;
//! let shutdown = std::future::pending::<()>();
//! server.serve(app, shutdown).await?;
//! # Ok(())
//! # }
//! ```
//!
//! `serve` consumes the bound server. On shutdown it stops accepting
//! connections, closes admission for managed SSE and WebSocket sessions,
//! cancels the shared [`ServerContext`] token, and waits for HTTP connections
//! and registered sessions against one grace deadline. The report records any
//! managed sessions still active at that deadline; it does not cover arbitrary
//! application background tasks or native Axum upgrades. Transport connections
//! with no active request or response body close after the configured idle
//! timeout (30 seconds by default). `WebServer` owns and tracks the connection
//! tasks, while Hyper/hyper-util drives each connection. This implementation
//! does not provide a provable count of forcibly closed connections, so
//! `ShutdownReport::forced_connections` is `None`. The shutdown future must be
//! `Send`.
//!
//! `WebServer` serves the supplied router without installing route limits.
//! Install [`StreamAdmissionLayer`] explicitly on native Axum SSE and
//! WebSocket branches; it buffers each request body before the handler and
//! its deadline ends when response headers are produced. The 1 MiB body,
//! 256-admission, and 10-second defaults apply per shared admission state
//! domain. This is separate from `RequestLimitLayer`, which is intended for
//! short requests.
//!
//! Controller macros are re-exported by this crate and can be assembled into
//! a normal Axum router:
//!
//! ```
//! use std::sync::Arc;
//! use qubit_web::{ControllerRoutes, get_mapping, rest_controller};
//!
//! struct Health;
//!
//! #[rest_controller("/health")]
//! impl Health {
//!     #[get_mapping("")]
//!     async fn check(&self) -> &'static str { "ok" }
//! }
//!
//! let router = ControllerRoutes::<()>::new()
//!     .add(Arc::new(Health)).unwrap()
//!     .finish();
//! let _ = router;
//! ```

#[doc(hidden)]
pub mod __private;

#[doc(hidden)]
pub use crate as qubit_web;

pub mod diagnostic;
/// Structured startup and request rejection errors.
pub mod error;
pub mod limit;
pub mod mvc;
/// Validated server address and timeout configuration.
pub mod options;
/// Shared classification for routes and request policies.
pub mod route_kind;
/// HTTP/HTTPS binding, request dispatch, and graceful shutdown.
pub mod server;
pub mod sse;

/// Configuration-file integration; requires the `config` feature.
#[cfg(feature = "config")]
pub mod config;
/// Bounded JSON extraction and response support; requires the `json` feature.
#[cfg(feature = "json")]
pub mod json;
/// TLS configuration and HTTPS support; requires the `tls` feature.
#[cfg(feature = "tls")]
pub mod tls;
/// Managed WebSocket sessions; requires the `ws` feature.
#[cfg(feature = "ws")]
pub mod ws;

/// Configuration validation error; requires the `config` feature.
#[cfg(feature = "config")]
pub use config::ConfigOptionsError;
/// A web server configured from a configuration file; requires the `config`
/// feature.
#[cfg(feature = "config")]
pub use config::ConfiguredWeb;
pub use error::WebServerError;
/// JSON request extractor; requires the `json` feature.
#[cfg(feature = "json")]
pub use json::BoundedJson;
/// JSON input and output budgets; requires the `json` feature.
#[cfg(feature = "json")]
pub use json::JsonLimits;
/// JSON response encoding error; requires the `json` feature.
#[cfg(feature = "json")]
pub use json::JsonResponseError;
/// Encodes a bounded JSON response; requires the `json` feature.
#[cfg(feature = "json")]
pub use json::json_response;
pub use limit::HttpLimits;
pub use limit::RequestLimitLayer;
pub use limit::StreamAdmissionLayer;
pub use limit::StreamAdmissionLimits;
pub use limit::StreamAdmissionState;
pub use limit::WebRejection;
pub use mvc::ControllerRouteError;
pub use mvc::ControllerRoutes;
pub use mvc::RouteMetadata;
pub use options::ServerOptions;
pub use qubit_web_macros::delete;
pub use qubit_web_macros::delete_mapping;
pub use qubit_web_macros::get;
pub use qubit_web_macros::get_mapping;
pub use qubit_web_macros::patch;
pub use qubit_web_macros::patch_mapping;
pub use qubit_web_macros::post;
pub use qubit_web_macros::post_mapping;
pub use qubit_web_macros::put;
pub use qubit_web_macros::put_mapping;
pub use qubit_web_macros::rest_controller;
pub use qubit_web_macros::route;
pub use route_kind::RouteKind;
pub use server::ServerContext;
pub use server::SessionGuard;
pub use server::SessionRegistrationError;
pub use server::ShutdownReport;
pub use server::WebServer;
pub use sse::SseCapacityExceeded;
pub use sse::SseConnectionPolicy;
/// TLS server configuration; requires the `tls` feature.
#[cfg(feature = "tls")]
pub use tls::TlsConfig;
/// WebSocket policy validation error; requires the `ws` feature.
#[cfg(feature = "ws")]
pub use ws::WsPolicyError;
/// WebSocket send error; requires the `ws` feature.
#[cfg(feature = "ws")]
pub use ws::WsSendError;
/// Bounded WebSocket outgoing message queue; requires the `ws` feature.
#[cfg(feature = "ws")]
pub use ws::WsSendQueue;
/// Managed WebSocket session; requires the `ws` feature.
#[cfg(feature = "ws")]
pub use ws::WsSession;
/// WebSocket upgrade and resource policy; requires the `ws` feature.
#[cfg(feature = "ws")]
pub use ws::WsUpgradePolicy;
