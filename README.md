# qubit-web

[![Rust CI](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-web/coverage-badge.json)](https://qubit-ltd.github.io/rs-web/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-web.svg?color=blue)](https://crates.io/crates/qubit-web)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

[`qubit-web`](README.zh_CN.md) gives Rust services a composable Axum server foundation: bind HTTP or HTTPS, assemble Controller and native routes, apply finite limits to selected short requests, and manage SSE and WebSocket lifecycles. It is for teams building their own service behavior; application state, authentication, authorization, and business protocols remain with the application.

## Install

For the minimal native Axum service below, declare the dependencies it uses directly:

```toml
[dependencies]
axum = "0.8"
qubit-web = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
```

The default `qubit-web` feature set is empty. Enable only the capabilities you use: `json` provides bounded JSON helpers, `ws` provides WebSocket policy support, `tls` enables HTTPS binding, and `config` enables the `qubit-config` adapter. The existing examples use additional features as noted below.

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

HTTP/2 stream limits, optional HTTP/1 and HTTP/2 header budgets, and WebSocket write diagnostics are covered in the [user guide](doc/user_guide.md#transport-and-protocol-header-budgets), including their scope and operational boundaries.

For the separate in-memory CRUD example, run `cargo run --example controller_crud --all-features`. Its `/users` route belongs to that example, not the minimal service above. Creating a user there returns `201 Created`; see [`examples/controller_crud.rs`](examples/controller_crud.rs) for replacing Controller short-route defaults with `ControllerRoutes::with_http_limits`. For native Axum routes, install `RequestLimitLayer::new(http_limits)` on selected short branches and `StreamAdmissionLayer` on SSE/WS branches. Controller `sse` and `ws` routes use a separate stream-admission budget.

When using the `config` feature, `ConfiguredWeb::from_config` returns `ServerOptions` and `HttpLimits` separately. See the [user guide](doc/user_guide.md#assemble-short-routes-and-long-lived-routes) for applying those values to Controller and native Axum routes; `WebServer` does not add route limits to the Router.

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

In [`examples/prompt_stream.rs`](examples/prompt_stream.rs), the native SSE and WebSocket routes share one `StreamAdmissionState`. Its defaults allow up to 1 MiB of buffered request body per request, 256 concurrent admissions, and a 10-second deadline from admission through handler response headers. The deadline ends when response headers are ready; it does not cover the SSE stream or the WebSocket session after upgrade. SSE and WebSocket session capacity remains controlled separately by `SseConnectionPolicy` and `WsUpgradePolicy`.

## Limits and operational boundaries

Controller path captures use Axum syntax such as `/{id}` and `/{*rest}`; legacy `/:id` and `/*rest` segments are rejected before registration. The JSON extractor accepts `application/json` and valid `application/*+json` subtypes; malformed subtype tokens receive `415 Unsupported Media Type`.

`WsSession::close()` is idempotent. If its future is cancelled while waiting, call it again to retry; after explicit close, `recv()` returns `None`. `WsSession::closed()` returns an owned signal after both managed I/O tasks release their resources, so an application can select it against cancellable work and call `close()` to release queued input. It does not finish the handler or stop application-spawned work. A transport read error can be received once as `Some(Err(_))` before the next receive returns `None`; host shutdown may cancel the handler without delivering that error. The fallback diagnostic uses the fixed `<unmatched>` route marker for unmatched requests and never records their URI or secrets. Zero or unrepresentable `request_header_timeout` and SSE keep-alive intervals are rejected as `WebServerError::InvalidConfig`.

- Controller `short` routes start with `HttpLimits::default()`; native Axum routes need an explicit `RequestLimitLayer` on each selected short-request branch. `WebServer` does not install route limits for a supplied Router.
- Controller `sse`/`ws` routes share their builder’s `StreamAdmissionState`; `with_stream_admission_limits` changes that budget for routes added afterward. Native Axum branches must install `StreamAdmissionLayer` explicitly; layers made with `new` have independent budgets, while `from_state` shares one. Defaults are 1 MiB buffered per request, 256 admissions, and 10 seconds through response headers per state domain. The resulting 256 MiB is a concurrent payload estimate, not a buffer allocation or process peak; incremental buffering releases each prior input frame but still has vector growth, copying, current-frame, trailer, allocator, and transport overhead. `RequestLimitLayer` remains the separate short-request policy. Transport and SSE/WS session limits are separate; SSE also occupies an HTTP connection.
- Applications own authentication, authorization, business state, and stream persistence or replay. Authenticate before WebSocket upgrade; Origin checks only restrict browser origins. The sample token and unauthenticated shutdown endpoint are loopback demonstrations.
- `DiagnosticLayer::new()` logs request and response-header metadata. Install `DiagnosticLayer::with_body_diagnostics()` explicitly, outside `RequestLimitLayer`, when response-body termination outcomes are needed; its allowlisted event does not include payload/error text and does not claim client disconnect or network delivery.
- WebSocket `queue_limits` configures the outbound queue. `inbound_queue_limits` separately defaults to 64 queued messages and 1 MiB of aggregate queued or pending payload per connection. A full message channel waits up to `idle_timeout`; insufficient byte budget closes immediately with `1013`. Across 128 active default connections, this queued/pending payload budget is about 128 MiB, plus up to one 1 MiB protocol-reassembled message per active connection. It excludes allocation overhead and application-owned memory, and does not bound memory retained by handlers or sessions after their I/O ends.
- `WsSendError::MessageTooLarge` identifies a permanently oversized outbound message; see the [user guide](doc/user_guide.md) for error handling and migration.
- HTTPS requires `tls` and valid certificates, or a trusted reverse proxy configured by the deployer. Shutdown is initiated by the application and follows the server's configured deadline.
- Dropping or aborting a `serve` future after it has started closes managed-session admission and notifies active sessions, but cannot await asynchronous draining or produce a shutdown report. Cleanup needs the Tokio runtime to keep running; resolve the shutdown future and await `serve` for graceful shutdown.

See the [English user guide](doc/user_guide.md) or [Chinese user guide](doc/user_guide.zh_CN.md) for limits, TLS setup, lifecycle behavior, and troubleshooting. See the [API documentation](https://docs.rs/qubit-web), [historical Chinese design record](doc/2026-10-08-rs-web-design.md), and [lifecycle design summary](doc/2026-10-09-rs-web-lifecycle-design.en.md) for more.

Deadlines use cooperative Tokio cancellation, so non-yielding synchronous work can delay checks and cleanup. HTTP and HTTPS handlers can extract the direct TCP peer through Axum `ConnectInfo<SocketAddr>`; see the [user guide](doc/user_guide.md) for proxy and execution boundaries.

The [fixed interoperability workflow](.github/workflows/downstream-interop.yml) preserves the pinned consumer baseline. The separate [current-consumer WS/WSS workflow](.github/workflows/downstream-current-interop.yml) checks rs-websocket `main` weekly or on manual dispatch, including current-consumer TLS WebSocket scenarios.

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./.infra/bin/ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public API documentation and tests current, and run `./.infra/bin/align-ci.sh` to format code and `./.infra/bin/ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-web](https://github.com/qubit-ltd/rs-web)
