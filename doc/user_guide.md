# Running an HTTP service with bounded streams

This guide is for Rust service developers using `qubit-web` 0.2.0 (Rust 1.94 or newer). It follows one service that accepts a bounded JSON prompt request, streams progress over SSE, optionally echoes a WebSocket, and shuts down under host control. The prompt and token are demonstration data; the example does not run an agent and its token is not production authentication.

Start with the [English README](../README.md) for project scope and feature selection. The [Chinese README](../README.zh_CN.md) and [Chinese guide](user_guide.zh_CN.md) are also available. This guide covers operational setup. Public API details are in [docs.rs](https://docs.rs/qubit-web) and crate Rustdoc.

## Prepare settings and start the listener

Add `qubit-web` with the features your service uses. The example below needs JSON and WebSocket support:

```toml
[dependencies]
qubit-web = { version = "0.2", features = ["json", "ws"] }
```

For HTTPS add `tls-rustls`. Add `config` only if you want `ConfiguredWeb` to parse a `qubit_config::Config` value supplied by your application; it does not load a file or global settings source for you.

Choose the listener capacity for each server instance. The default is 1024 transport connections. Set a positive value explicitly when the service's file descriptor, TLS, or memory budget calls for a smaller cap:

```rust
use qubit_web::{ServerOptions, WebServer};

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_max_transport_connections(256)?;
let server = WebServer::bind_http(options).await?;
let context = server.context();
```

Zero is rejected with `WebServerError::InvalidConfig`. The cap applies per `WebServer`; it covers accepted sockets, TLS handshakes, and HTTP connection futures. It is held until the HTTP transport future ends. Upgraded WebSocket sessions are governed by `WsUpgradePolicy` after Hyper completes the upgrade.

Transport connections with no active request or response body close after 30 seconds by default. Choose a different positive duration when the deployment needs another idle policy:

```rust
use std::time::Duration;

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_transport_idle_timeout(Duration::from_secs(45));
assert_eq!(options.transport_idle_timeout(), Duration::from_secs(45));
```

An active handler or response body pauses the idle timer, so long-lived SSE is not closed by this timeout. It also reclaims an HTTP/2 connection that never starts a request. HTTP/2 PING traffic does not count as request activity. With `config`, `transport.idle_timeout_ms` sets the same value and defaults to `30000`; zero is invalid. `request_header_timeout` remains a separate 10-second HTTP/1 header and TLS handshake limit.

With the optional `config` feature, parse settings and keep route policy separate:

```rust,ignore
let configured = ConfiguredWeb::from_config(&config)?;
let (server_options, http_limits) = configured.into_parts();
let server = WebServer::bind_http(server_options).await?;
```

`address` is required; `shutdown_timeout_ms` defaults to 30000, `transport.max_connections` to 1024, and `transport.idle_timeout_ms` to 30000. The `http.*` fields create `HttpLimits`, but do not install them. Errors identify the invalid field through `ConfigOptionsError::field()` without echoing its value.

## Assemble short routes and long-lived routes

Install request limits only on ordinary short routes. This protects consumed request-body data, concurrent handler work, and the selected branch's deadline. It does not make unprotected routes inherit a server-wide policy.

```rust
use std::time::Duration;
use axum::{Router, routing::{get, post}};
use qubit_web::{HttpLimits, RequestLimitLayer};

let http_limits = HttpLimits::default()
    .with_max_body_bytes(1024 * 1024)?
    .with_max_concurrent_requests(128)?
    .with_request_timeout(Duration::from_secs(20))?;
let app = Router::new()
    .route("/prompt", post(submit).layer(RequestLimitLayer::new(http_limits)))
    .route("/events", get(progress))
    .route("/ws", get(websocket));
```

The example fragments assume application handlers `submit`, `progress`, and `websocket`; the full runnable composition is in [`examples/prompt_stream.rs`](../examples/prompt_stream.rs). Controller routes can instead install the same policy using `ControllerRoutes::with_http_limits`. Keep SSE and WebSocket routes outside `RequestLimitLayer`: the layer's deadline also covers response streaming and is intended for short requests.

### Share SSE and WebSocket policies through application state

Create each long-lived connection policy once during startup and keep it in the Axum state. A handler clones the policy from `State<AppState>` before using it:

```rust,ignore
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use qubit_web::{ServerContext, SseConnectionPolicy, WebServer, WsUpgradePolicy};

#[derive(Clone)]
struct AppState {
    context: ServerContext,
    sse: SseConnectionPolicy,
    ws: WsUpgradePolicy,
}

// Call once while assembling the application, before Router::with_state.
fn app_state(server: &WebServer) -> AppState {
    AppState {
        context: server.context(),
        sse: SseConnectionPolicy::default(),
        ws: WsUpgradePolicy::new().allowed_origins(["http://localhost:3000"]),
    }
}

async fn progress(State(state): State<AppState>) -> Response {
    let policy = state.sse.clone();
    let connection = match policy.begin(&state.context) {
        Ok(connection) => connection,
        Err(error) => return error.into_response(),
    };
    // Pass the application's event stream here. Its cancellation token can
    // stop the producer after disconnect or server shutdown.
    connection.into_sse(event_stream).into_response()
}

async fn websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    let policy = state.ws.clone();
    policy.on_upgrade(ws, &headers, state.context.clone(), |_session| async move {})
}
```

The `event_stream` value represents the application's event stream; the runnable [`prompt_stream` example](../examples/prompt_stream.rs) shows producer construction and cancellation handling. The fragment assumes the existing `server` and an Axum state assembly function. Cloning either policy shares its connection-capacity domain, so all routes using clones draw from one configured allowance. Calling `SseConnectionPolicy::default()` or `WsUpgradePolicy::new()` again creates an independent domain; requests handled by that new instance do not consume the original policy's allowance. These are separate caps: SSE has its own session cap and remains within the HTTP transport cap, while an upgraded WebSocket uses its policy cap after the HTTP upgrade completes.

For a configuration-driven service, pass `configured.http_limits()` to the controller builder or `RequestLimitLayer::new` on the selected route branch. `WebServer` receives a completed Axum `Router` and never silently rewrites it.

For HTTPS, load trusted certificate material before binding and pass it to `WebServer::bind_https`:

```rust,ignore
let tls = TlsConfig::from_pem_files(certificate_chain, private_key).await?;
let server = WebServer::bind_https(options, tls).await?;
```

Use a certificate trusted by clients, or terminate TLS at a trusted edge and configure the application accordingly. Repository test fixtures are self-signed test material, not deployment credentials. HTTPS uses the same Router; WebSocket upgrades on it use WSS.

## Run, observe, and stop the service

Clone `ServerContext` before moving the server into its serving task. Context-aware SSE and WebSocket policy calls register active sessions. For example, an application can expose its own protected diagnostic route:

```rust,ignore
let diagnostics_context = server.context();
let app = app.route("/admin/active-sessions", get(move || {
    let context = diagnostics_context.clone();
    async move { context.active_sessions().to_string() }
}));
```

This is application code, not a built-in admin endpoint. Protect it with the application's normal authentication and network policy. `active_sessions()` counts managed SSE sessions and WebSocket upgrades admitted through `WsUpgradePolicy::on_upgrade`, including an upgrade whose response has not completed yet. It does not count ordinary HTTP requests, native Axum upgrades, or arbitrary application background tasks.

The host owns shutdown. Keep the shutdown future pending while serving, then resolve it when your process signal or supervisor requests a stop:

```rust,ignore
let report = server.serve(app, shutdown_signal).await?;
assert!(report.graceful);
```

On shutdown, the server atomically closes admission for new managed SSE and WebSocket sessions, stops accepting new connections, and notifies active sessions. It waits for HTTP connections and registered sessions against the same configured deadline. `graceful` is true only if both finish before that deadline. Otherwise `unfinished_managed_sessions` records the managed session count at the deadline; the report does not claim that arbitrary application background tasks have stopped. `forced_connections` is `None` because Axum does not expose a reliable count.

Run the demonstration service with:

```sh
cargo run --example prompt_stream --features "json ws"
```

It prints its loopback address (default `127.0.0.1:3001`). Check the short route:

```sh
curl -i -H 'content-type: application/json' -d '{"prompt":"demo"}' http://127.0.0.1:3001/prompt
```

A successful request returns HTTP 200 and JSON with `job_id` set to `demo-1`. To exercise the configured body limit, send a body larger than 1 MiB; the middleware rejects based on `Content-Length` before JSON decoding:

```sh
head -c 1048577 /dev/zero | curl -i -H 'content-type: application/json' --data-binary @- http://127.0.0.1:3001/prompt
```

Expect HTTP 413 with the stable `body_too_large` code. The demo does not include the diagnostic route above; add it to the application if you want to query a count with `curl`:

```sh
curl -i http://127.0.0.1:3001/admin/active-sessions
```

While an SSE or context-aware WebSocket remains open, the response body is the current count; after it closes, the count should return to zero. The demo SSE path is `/prompt/demo-1/events`. The WebSocket path is `/prompt/demo-1/ws`; its `Bearer demo-only` check is a placeholder only.

Stop the demo through its explicit endpoint:

```sh
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

The endpoint responds 202 when it triggers shutdown and 410 if it was already used. The serving task then exits and prints no separate report in this example; production hosts should inspect `ShutdownReport`.

## Capacity, overload, and troubleshooting

Budget transport connections separately from long-lived sessions. For example, with a transport cap of 256, a service may also allow 80 SSE sessions and 80 WebSocket sessions; these session policy capacities are separate limits and should be sized against file descriptors, memory, TLS CPU, and application work. The transport cap is per server instance, not a process-wide cap. SSE remains an HTTP transport future and occupies transport capacity while active; an upgraded WebSocket releases transport capacity when the HTTP upgrade future ends.

When all transport permits are occupied, the accept loop waits before accepting another socket. A client may remain in the OS listen backlog or fail to connect. There is no HTTP handler to return a guaranteed 503 for a socket that has not been accepted. Tune the cap and the operating system backlog as deployment-specific settings; do not treat backlog behavior as an application response.

`request_header_timeout` defaults to 10 seconds. It bounds HTTP/1 request headers and is also used for the HTTPS TLS handshake. `transport_idle_timeout` defaults to 30 seconds and closes idle HTTP/1 and HTTP/2 transports when no handler or response body is active. It does not time out a stalled active request; use route limits or deployment-level policies for that case. This is separate from `HttpLimits::with_request_timeout`, which applies only after `RequestLimitLayer` is installed on a short route.

For WebSockets, `WsUpgradePolicy::idle_timeout` also limits the time without progress while delivering an inbound message to the application. If the 64-message inbound channel is full because the application is not calling `WsSession::recv()`, expiry closes the session with code `1013` (`inbound backpressure`). Drain messages regularly and hand off longer processing after receiving each message. `WsSession::try_send` returning success means only that the message entered the outbound queue; it does not guarantee network delivery. After the peer or session closes, it returns `WsSendError::Closed` (or `Closed` when matching the variant).

| Symptom | Check |
| --- | --- |
| Client waits or connection fails during overload | Check per-instance transport capacity and OS backlog. Do not expect a 503 before accept. |
| A request exceeds the body cap but succeeds | Confirm that the route has `RequestLimitLayer` or its controller branch uses `with_http_limits`; confirm the body is consumed by the handler/extractor. |
| SSE closes at the ordinary request deadline | Remove `RequestLimitLayer` from that streaming branch and use `SseConnectionPolicy` limits. |
| `active_sessions()` stays at zero for a WS | Call `WsUpgradePolicy::on_upgrade` with the server's `ServerContext`; native Axum upgrades are outside managed-session reporting. |
| A WebSocket closes with code `1013` under inbound load | The application may not be calling `WsSession::recv()` fast enough. The inbound channel holds 64 messages; if it remains full and delivery makes no progress for `idle_timeout`, the session closes. Read messages continuously or move processing to application-managed work after receiving them; choose an `idle_timeout` that allows the expected processing gaps. |
| Configuration is rejected | Inspect `ConfigOptionsError::field()`; check address, positive timeout/limit values, and integer types. Error text intentionally does not include rejected values. |
| HTTPS or WSS cannot connect | Check certificate chain/key pairing and client trust. A TLS failure occurs before HTTP routing. |
| Shutdown exceeds expectations | Check the configured shutdown timeout and whether application handlers or stream producers respond to cancellation. |

`WebServerError` exposes retained socket errors through `std::error::Error::source()`. Its default `Display` and `Debug` intentionally show only the error category; inspect `source()` explicitly when diagnosing bind or accept failures.

See the [Chinese guide](user_guide.zh_CN.md), [README](../README.md), [API documentation](https://docs.rs/qubit-web), the [historical Chinese design record](2026-10-08-rs-web-design.md), and the [current lifecycle design summary](2026-10-09-rs-web-lifecycle-design.en.md) for next steps.
