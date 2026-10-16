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

For the separate in-memory CRUD example, run `cargo run --example controller_crud --all-features`. Its `/users` route belongs to that example, not the minimal service above. Creating a user there returns `201 Created`; see [`examples/controller_crud.rs`](examples/controller_crud.rs) for replacing Controller short-route defaults with `ControllerRoutes::with_http_limits`. For native Axum routes, install `RequestLimitLayer::new(http_limits)` on selected short branches and `StreamAdmissionLayer` on SSE/WS branches. Controller `sse` and `ws` routes use a separate stream-admission budget.

When using the `config` feature, `ConfiguredWeb::from_config` returns `ServerOptions` and `HttpLimits` as separate values. Controller short routes already have `HttpLimits::default()`; pass the configured limits to `with_http_limits` to replace that policy. For native Axum routes, install `RequestLimitLayer` on selected short branches and `StreamAdmissionLayer` on SSE/WS branches. `WebServer` does not apply either policy to the Router.

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

- Controller `short` routes start with `HttpLimits::default()`; native Axum routes need an explicit `RequestLimitLayer` on each selected short-request branch. `WebServer` does not install route limits for a supplied Router.
- Controller `sse`/`ws` routes share their builder’s `StreamAdmissionState`; `with_stream_admission_limits` changes that budget for routes added afterward. Native Axum branches must install `StreamAdmissionLayer` explicitly; layers made with `new` have independent budgets, while `from_state` shares one. Defaults are 1 MiB buffered per request, 256 admissions, and 10 seconds through response headers per state domain (up to 256 MiB of body buffers plus overhead). `RequestLimitLayer` remains the separate short-request policy. Transport and SSE/WS session limits are separate; SSE also occupies an HTTP connection.
- Applications own authentication, authorization, business state, and stream persistence or replay. Authenticate before WebSocket upgrade; Origin checks only restrict browser origins. The sample token and unauthenticated shutdown endpoint are loopback demonstrations.
- HTTPS requires `tls-rustls` and valid certificates, or a trusted reverse proxy configured by the deployer. Shutdown is initiated by the application and follows the server's configured deadline.

See the [English user guide](doc/user_guide.md) or [Chinese user guide](doc/user_guide.zh_CN.md) for limits, TLS setup, lifecycle behavior, and troubleshooting. See the [API documentation](https://docs.rs/qubit-web), [historical Chinese design record](doc/2026-10-08-rs-web-design.md), and [lifecycle design summary](doc/2026-10-09-rs-web-lifecycle-design.en.md) for more.

## Testing

Run the workspace checks used for this crate:

```sh
cargo test --workspace --all-features --all-targets --locked
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.

## Contributing

Contributions are welcome. Before submitting a pull request, run `./.infra/bin/ci-check.sh` and confirm the CI checks pass. Please open an [issue](https://github.com/qubit-ltd/rs-web/issues) to discuss a change or submit a [pull request](https://github.com/qubit-ltd/rs-web/pulls).

## Author

**Haixing Hu** — [Qubit Co. Ltd.](https://github.com/qubit-ltd)
