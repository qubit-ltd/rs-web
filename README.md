# qubit-web

`qubit-web` is a composable server-side foundation for Rust services. It runs an Axum `Router`, offers explicit REST Controller route assembly, bounded JSON and HTTP request policies, and lifecycle helpers for SSE and WebSocket connections. Applications keep ownership of state, authentication, authorization, and business protocols.

## Examples

The examples bind to loopback and use in-memory/demo data. Run them with all optional features enabled:

```sh
cargo run --example controller_crud --all-features
cargo run --example prompt_stream --all-features
```

`controller_crud` combines `#[rest_controller]`, `#[get_mapping]`, `#[post_mapping]`, Axum `Path`/`Query`, `BoundedJson`, and a native `/health` route. It keeps records in process memory; restarting clears them. Try it with:

```sh
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/users \
  -H 'content-type: application/json' -d '{"name":"Ada"}'
curl -i 'http://127.0.0.1:3000/users?name_prefix=A'
curl -i http://127.0.0.1:3000/users/1
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

Controller instances are supplied explicitly to `ControllerRoutes`; the macro does not construct application objects or discover them globally. The resulting router can be merged with native Axum routes, state, extractors, and middleware.

`prompt_stream` demonstrates bounded JSON over HTTP, a bounded/cancellable SSE producer, and an authenticated WebSocket echo handler. The sample bearer token is `demo-only`; it is illustrative only and is not an authentication mechanism. For the WS request, allow the example's configured Origin (`http://localhost:3000`) or omit `Origin` and send `Authorization: Bearer demo-only`. These endpoints simulate transport only: they do not run an Agent, save prompts, or provide job persistence.

```sh
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

Use a WebSocket client to connect to `ws://127.0.0.1:3001/prompt/demo-1/ws` with `Authorization: Bearer demo-only` (and the allowed Origin when sending one). The sample replies with a prefixed echo.

## Security and operational boundaries

- Direct public deployment requires trusted TLS. The optional `tls-rustls` feature supports HTTPS binding; production deployments must provide valid certificates and private keys. Alternatively, configure a trusted reverse proxy explicitly. Forwarded headers are not trusted by default.
- The library does not provide authentication or authorization. Applications must authenticate before accepting a WebSocket upgrade and choose an Origin allowlist appropriate to their clients. An absent Origin does not authenticate a client.
- HTTP/1 request header blocks have a 10-second default timeout, configurable with `ServerOptions::with_request_header_timeout`; this also bounds how long an HTTP/1 upgrade request may take to send its headers. This is separate from application WebSocket session lifetime.
- SSE disconnect and shutdown cancellation are available to event producers, but event IDs, `Last-Event-ID`, buffering, persistence, replay, and recovery remain application responsibilities. A successful write does not prove client consumption.
- Closing an HTTP/SSE/WS connection does not imply cancellation of a business task already accepted by the application.
- `ServerOptions::http_limits()` supplies the service baseline; pass it to `ControllerRoutes::with_http_limits` or `RequestLimitLayer::new` when building the router. `WebServer` preserves the caller-built router and does not rewrite its route classes. Controller `short` routes receive finite limits by default, while `sse` and `ws` routes skip the short-request layer.
- When using `config`, build `ServerOptions` with `ServerOptions::from_config`, then pass `options.http_limits()` to each selected router branch before calling `serve`.
- Native Axum routes use `RequestLimitLayer` only on branches where the application installs it. Ordinary Axum extractors are not automatically protected by `BoundedJson`'s structural JSON budgets.
- Prefer `WsUpgradePolicy::on_upgrade_with_context` to share the server cancellation token and shutdown deadline. When using the lower-level token-based `on_upgrade`, configure `WsUpgradePolicy::shutdown_timeout` no greater than `ServerOptions::shutdown_timeout`.

## Optional features

The default feature set is empty. Enable only the capabilities an application uses:

| Feature | Capability |
| --- | --- |
| `json` | `BoundedJson`, structural JSON budgets, and bounded JSON responses |
| `ws` | WebSocket upgrade policy and bounded session send queue |
| `tls-rustls` | HTTPS binding with rustls |
| `config` | Adapter from `qubit-config` to server options |

See [`doc/2026-10-08-rs-web-prd.md`](doc/2026-10-08-rs-web-prd.md) and [`doc/2026-10-08-rs-web-design.md`](doc/2026-10-08-rs-web-design.md) for requirements and design details.
