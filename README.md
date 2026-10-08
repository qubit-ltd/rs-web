# qubit-web

[`qubit-web`](README.zh_CN.md) gives Rust services a composable Axum server foundation: bind HTTP or HTTPS, assemble Controller and native routes, apply finite limits to selected short requests, and manage SSE and WebSocket lifecycles. It is for teams building their own service behavior; application state, authentication, authorization, and business protocols remain with the application.

## Install

Add the crate and enable only the capabilities you use:

```toml
[dependencies]
qubit-web = { version = "0.1", features = ["json", "ws"] }
```

The default feature set is empty. `json` enables bounded JSON helpers, `ws` enables WebSocket policy support, `tls-rustls` enables HTTPS binding, and `config` enables the `qubit-config` adapter.

## Quick start

The runnable examples show the complete path: create `ServerOptions` with a transport connection cap, construct `HttpLimits` separately, install those limits on short routes, bind, serve the router, and stop through `/admin/shutdown`.

```sh
cargo run --example controller_crud --all-features
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/users \
  -H 'content-type: application/json' -d '{"name":"Ada"}'
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

The successful health response is `200 OK` with `ok`; creating a user returns `201 Created`. The example binds only to loopback and keeps data in memory. See [`examples/controller_crud.rs`](examples/controller_crud.rs) for the explicit `ControllerRoutes::with_http_limits` setup. For native Axum routes, install `RequestLimitLayer::new(http_limits)` on the short route branches that need it. `WebServer` serves the router as supplied and does not install HTTP limits implicitly.

When using the `config` feature, `ConfiguredWeb::from_config` returns `ServerOptions` and `HttpLimits` as separate values. Pass the latter to the route assembly point; it remains inert until explicitly installed.

## Streaming example

`prompt_stream` runs bounded JSON, a cancellable SSE producer, and a WebSocket echo handler. Run it with:

```sh
cargo run --example prompt_stream --all-features
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

The sample bearer token is `demo-only`; it illustrates a handler check and is not authentication. A WebSocket client may connect to `ws://127.0.0.1:3001/prompt/demo-1/ws` with `Authorization: Bearer demo-only` and, if it sends an Origin, the allowed Origin `http://localhost:3000`. The endpoints demonstrate transport only: they do not run an Agent, save prompts, or persist jobs.

## Limits and operational boundaries

- The per-`WebServer` transport connection limit defaults to 1024 and can be configured with `ServerOptions::with_max_transport_connections`. It covers TLS handshakes and HTTP connection futures. When full, accepting pauses and clients may wait in the operating system backlog or fail to connect; this does not promise an HTTP 503. An upgraded WebSocket uses its own `WsUpgradePolicy` capacity. SSE remains an HTTP connection; size its transport and SSE budgets for the expected long-lived streams.
- `HttpLimits` applies finite body size, concurrency, and processing-time limits to ordinary short requests only where the application installs `ControllerRoutes::with_http_limits` or `RequestLimitLayer`. Controller `short` routes are covered by the configured Controller policy; `sse` and `ws` routes skip the short-request layer. Native Axum routes need an explicit layer on each selected branch.
- The default HTTP/1 request-header timeout is 10 seconds and also bounds an HTTP/1 WebSocket upgrade request before its headers arrive. It does not guarantee a timeout for an HTTP/2 first frame or idle connection.
- Prefer `WsUpgradePolicy::on_upgrade_with_context` to share shutdown cancellation and count the upgraded session in `ServerContext::active_sessions()`. The token-based `on_upgrade` has no server context and is not included in that count. `active_sessions()` counts registered SSE and context-based WS sessions, not all TCP connections.
- The library does not provide authentication or authorization. Applications must authenticate before accepting WebSocket upgrades and choose an Origin allowlist. An absent Origin does not authenticate a client.
- Direct public deployment requires trusted TLS. `tls-rustls` supports HTTPS binding; provide valid certificates and private keys, or configure a trusted reverse proxy explicitly. Forwarded headers are not trusted by default.
- SSE producers can observe disconnect and shutdown cancellation, but event IDs, buffering, persistence, replay, and recovery remain application responsibilities. Closing a connection does not cancel business work already accepted by the application.

For setup, capacity planning, TLS, streaming, and troubleshooting, see the [English user guide](doc/user_guide.md) and [Chinese user guide](doc/user_guide.zh_CN.md). See the [API documentation](https://docs.rs/qubit-web) and the [requirements](doc/2026-10-08-rs-web-prd.md) and [design notes](doc/2026-10-08-rs-web-design.md) for project context.
