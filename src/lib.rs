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
//! let app = Router::new().route("/health", get(|| async { "ok" }));
//! let server = WebServer::bind_http(ServerOptions::new("127.0.0.1:0".parse().unwrap())).await?;
//! let shutdown = std::future::pending::<()>();
//! server.serve(app, shutdown).await?;
//! # Ok(())
//! # }
//! ```
//!
//! `serve` consumes the bound server. On shutdown it stops accepting
//! connections, cancels the shared [`ServerContext`] token, and waits up to
//! the configured grace period. `ShutdownReport::forced_connections` is
//! `None` because the underlying HTTP server does not expose a provable count
//! of forcibly closed connections. The shutdown future must be `Send`.

pub mod diagnostic;
/// Structured startup and request rejection errors.
pub mod error;
pub mod limit;
pub mod mvc;
/// Validated server address and timeout configuration.
pub mod options;
/// HTTP/HTTPS binding, request dispatch, and graceful shutdown.
pub mod server;
pub mod sse;

#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "json")]
pub mod json;
#[cfg(feature = "tls-rustls")]
pub mod tls;
#[cfg(feature = "ws")]
pub mod ws;

#[cfg(feature = "config")]
pub use config::ConfigOptionsError;
#[cfg(feature = "config")]
pub use config::ConfiguredWeb;
pub use error::WebServerError;
#[cfg(feature = "json")]
pub use json::BoundedJson;
#[cfg(feature = "json")]
pub use json::JsonLimits;
#[cfg(feature = "json")]
pub use json::JsonResponseError;
#[cfg(feature = "json")]
pub use json::json_response;
pub use limit::HttpLimits;
pub use limit::RequestLimitLayer;
pub use limit::WebRejection;
pub use mvc::ControllerRouteConflict;
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
pub use server::RouteKind;
pub use server::ServerContext;
pub use server::SessionGuard;
pub use server::ShutdownReport;
pub use server::WebServer;
pub use sse::SseCapacityExceeded;
pub use sse::SseConnectionPolicy;
#[cfg(feature = "tls-rustls")]
pub use tls::TlsConfig;
#[cfg(feature = "ws")]
pub use ws::WsPolicyError;
#[cfg(feature = "ws")]
pub use ws::WsSendError;
#[cfg(feature = "ws")]
pub use ws::WsSendQueue;
#[cfg(feature = "ws")]
pub use ws::WsSession;
#[cfg(feature = "ws")]
pub use ws::WsUpgradePolicy;
