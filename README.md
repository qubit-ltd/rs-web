# qubit-web

[`qubit-web`](README.zh_CN.md) gives Rust services a composable Axum server foundation: bind HTTP or HTTPS, assemble Controller and native routes, apply finite limits to selected short requests, and manage SSE and WebSocket lifecycles. It is for teams building their own service behavior; application state, authentication, authorization, and business protocols remain with the application.

## Install

For the minimal native Axum service below, declare the dependencies it uses directly:

```toml
[dependencies]
axum = "0.8"
qubit-web = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
```

The default `qubit-web` feature set is empty. Enable only the capabilities you use: `json` provides bounded JSON helpers, `ws` provides WebSocket policy support, `tls-rustls` enables HTTPS binding, and `config` enables the `qubit-config` adapter. The existing examples use additional features as noted below.

## Quick start

This small loopback service exposes a health check and an application-owned shutdown endpoint. Put the dependencies from the Install section in `Cargo.toml`, then save this as `src/main.rs`:

```rust
use std::error::Error;
use std::sync::Arc;

use axum::{http::StatusCode, routing::{get, post}, Router};
use qubit_web::{HttpLimits, RequestLimitLayer, ServerOptions, WebServer};
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let stop = Arc::new(Notify::new());
    let stop_for_route = Arc::clone(&stop);
    let app = Router::new()
        .route(
            "/health",
            get(|| async { "ok" })
                .layer(RequestLimitLayer::new(HttpLimits::default())),
        )
        .route(
            "/admin/shutdown",
            post(move || {
                let stop = Arc::clone(&stop_for_route);
                async move {
                    stop.notify_one();
                    StatusCode::ACCEPTED
                }
            }),
        );

    let options = ServerOptions::new("127.0.0.1:3000".parse()?)
        .with_max_transport_connections(128)?;
    let server = WebServer::bind_http(options).await?;
    let _report = server.serve(app, async move { stop.notified().await }).await?;
    Ok(())
}
```

In terminal A, run the service and wait for it to start:

```sh
cargo run
```

In terminal B, check the health response, then ask the application shutdown route to stop the server:

```sh
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

The first request returns `200 OK` with body `ok`; the second returns `202 Accepted` and signals the shutdown future passed to `serve`. The example binds only to loopback. The request limit is attached only to the short `/health` route; `WebServer` serves the Router as supplied and does not install HTTP limits implicitly. The shutdown route is an unauthenticated demonstration endpoint and should not be exposed beyond loopback.

For the separate in-memory CRUD example, run `cargo run --example controller_crud --all-features`. Its `/users` route belongs to that example, not the minimal service above. Creating a user there returns `201 Created`; see [`examples/controller_crud.rs`](examples/controller_crud.rs) for replacing Controller short-route defaults with `ControllerRoutes::with_http_limits`. For native Axum routes, install `RequestLimitLayer::new(http_limits)` on each selected short route branch. Controller `sse` and `ws` routes do not use the short-request limit.

When using the `config` feature, `ConfiguredWeb::from_config` returns `ServerOptions` and `HttpLimits` as separate values. Controller short routes already have `HttpLimits::default()`; pass the configured limits to `with_http_limits` to replace that policy. For native Axum routes, install `RequestLimitLayer` on selected branches. `WebServer` does not apply either policy to the Router.

## Streaming example

`prompt_stream` runs bounded JSON, a cancellable SSE producer, and a WebSocket echo handler. In terminal A, start it and wait until it prints the listening address:

```sh
cargo run --example prompt_stream --all-features
```

In terminal B, submit a prompt, observe the SSE progress events, then stop the service:

```sh
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

The sample bearer token is `demo-only`; it illustrates a handler check and is not authentication. A WebSocket client may connect to `ws://127.0.0.1:3001/prompt/demo-1/ws` with `Authorization: Bearer demo-only` and, if it sends an Origin, the allowed Origin `http://localhost:3000`. The endpoints demonstrate transport only: they do not run an Agent, save prompts, or persist jobs. See the [English user guide](doc/user_guide.md) for policy sharing, capacity planning, and shutdown details; the [Chinese user guide](doc/user_guide.zh_CN.md) covers the same workflow.

## Limits and operational boundaries

- Each `SseConnectionPolicy` and `WsUpgradePolicy` created with `new` or `default` opens an independent capacity domain; cloning a policy shares its domain. Create each policy once in `AppState` and clone it from handlers using `State<AppState>` when routes should share capacity. See the [English user guide](doc/user_guide.md) or [Chinese user guide](doc/user_guide.zh_CN.md) for an example. The per-`WebServer` transport connection limit defaults to 1024 and can be configured with `ServerOptions::with_max_transport_connections`. Transport and short-request concurrency values must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; out-of-range values return `WebServerError::InvalidConfig`. It covers TLS handshakes and HTTP connection futures. When full, accepting pauses and clients may wait in the operating system backlog or fail to connect; this does not promise an HTTP 503. These policy budgets are separate from the transport limit; SSE remains an HTTP connection.
- `ControllerRoutes::new()` applies `HttpLimits::default()` to Controller `short` routes: 1 MiB per request body, 256 concurrent requests shared by that builder, and a 30-second processing and response-body deadline. At expiry, the application response body and short-request permit are released without waiting for another body poll. A timeout after response headers can only surface as a body error when the transport polls again; it does not close the connection. Unrepresentable absolute request deadlines are rejected as `WebServerError::InvalidConfig`; an extreme runtime deadline failure returns HTTP 500 with code `invalid_configuration`. Ordinary handler timeouts remain HTTP 504. `with_http_limits` replaces those defaults and must be called before adding Controllers. Controller `sse` and `ws` routes skip the short-request layer. Native Axum routes need an explicit `RequestLimitLayer` on each selected branch; `WebServer` does not install route limits.
- `ControllerRoutes::add` returns `ControllerRouteError` before registration for duplicate methods, invalid paths, or conflicting route templates such as parameter aliases and overlapping wildcards. Different methods may share the same template; `finish()` returns the Axum `Router` directly.
- `rest_controller` supports distinct Controller types with the same terminal type name in one module.
- The default HTTP/1 request-header timeout is 10 seconds and also bounds an HTTP/1 WebSocket upgrade request before its headers arrive. The transport idle timeout defaults to 30 seconds (`ServerOptions::with_transport_idle_timeout` or `transport.idle_timeout_ms`); it closes connections with no active request or response body, including idle HTTP/2 connections, while active requests and SSE bodies keep the connection active.
- Use `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)` to reserve a managed session before returning the upgrade response. Policy validation uses the final configured field values: `max_connections` must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; zero returns `WsPolicyError::ZeroLimit`, and a value above that maximum returns `WsPolicyError::ConnectionLimitTooLarge`. Origin is optional by default: requests without Origin proceed to application authentication, while requests with Origin are rejected unless it exactly matches `allowed_origins`. An absent Origin does not authenticate a client. After the peer or session closes, `try_send` returns `Closed`; successful enqueueing alone does not guarantee network delivery. If the application does not read inbound messages and delivery makes no progress within `idle_timeout`, the session closes with WebSocket code `1013` (try again later).
- `ServerContext::active_sessions()` counts managed SSE sessions and WebSocket upgrades admitted through `on_upgrade`, including pending handshakes. Shutdown waits for HTTP connections and these sessions against one deadline set by `ServerOptions` and exposed through `ServerContext`; WebSocket policies have no separate shutdown timeout. Unrepresentable shutdown deadlines are rejected as `WebServerError::InvalidConfig`; an extreme runtime deadline failure cancels sessions and reclaims connection tasks before returning an error. `ShutdownReport::unfinished_managed_sessions` records sessions remaining at the deadline. Native Axum upgrades and arbitrary application tasks are outside this count.
- `WebServerError` keeps socket I/O details available through `std::error::Error::source()` while its default `Display` and `Debug` omit the underlying OS error text.
- Direct public deployment requires trusted TLS. `tls-rustls` supports HTTPS binding; provide valid certificates and private keys, or configure a trusted reverse proxy explicitly. Forwarded headers are not trusted by default.
- SSE producers can observe disconnect and shutdown cancellation, but event IDs, buffering, persistence, replay, and recovery remain application responsibilities. Closing a connection does not cancel business work already accepted by the application.

For setup, capacity planning, TLS, streaming, and troubleshooting, see the [English user guide](doc/user_guide.md) and [Chinese user guide](doc/user_guide.zh_CN.md). See the [API documentation](https://docs.rs/qubit-web), the [historical Chinese design record](doc/2026-10-08-rs-web-design.md), and the [current lifecycle design summary](doc/2026-10-09-rs-web-lifecycle-design.en.md) for project context.
