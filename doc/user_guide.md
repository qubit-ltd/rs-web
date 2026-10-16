# Running an HTTP service with bounded streams

This guide is for Rust service developers using `qubit-web` 0.2.0 (Rust 1.94 or newer). It follows one service that accepts a bounded JSON prompt request, streams progress over SSE, optionally echoes a WebSocket, and shuts down under host control. The prompt and token are demonstration data; the example does not run an agent and its token is not production authentication.

Start with the [English README](../README.md) for project scope and feature selection. The [Chinese README](../README.zh_CN.md) and [Chinese guide](user_guide.zh_CN.md) are also available. This guide covers operational setup. Public API details are in [docs.rs](https://docs.rs/qubit-web) and crate Rustdoc.

## Prepare settings and start the listener

The portable `prompt_stream` scenario imports Axum, `futures-core`, Serde, Tokio, and `qubit-web` directly. Declare each crate your application imports; transitive dependencies of `qubit-web` are not available as direct dependencies in your crate:

```toml
[dependencies]
axum = "0.8"
futures-core = "0.3"
qubit-web = { version = "0.2", features = ["json", "ws"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

These are the direct dependencies used to move the [`prompt_stream` example](../examples/prompt_stream.rs) into a separate application. To run the repository example itself, use the repository's manifest and the command in [Run, observe, and stop the service](#run-observe-and-stop-the-service). For HTTPS add `tls-rustls`. Add `config` only if you want `ConfiguredWeb` to parse a `qubit_config::Config` value supplied by your application; it does not load a file or global settings source for you. If your application imports `qubit_config::Config` directly, declare `qubit-config` as a direct dependency too.

Choose the listener capacity for each server instance. The default is 1024 transport connections. Set a positive value explicitly when the service's file descriptor, TLS, or memory budget calls for a smaller cap:

```rust
use qubit_web::{ServerOptions, WebServer};

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_max_transport_connections(256)?;
let server = WebServer::bind_http(options).await?;
let context = server.context();
```

The transport and short-request concurrency values must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; zero and larger values are rejected with `WebServerError::InvalidConfig`. The cap applies per `WebServer`; it covers accepted sockets, TLS handshakes, and HTTP connection futures. It is held until the HTTP transport future ends. Upgraded WebSocket sessions are governed by `WsUpgradePolicy` after Hyper completes the upgrade.

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

`address` is required; `shutdown_timeout_ms` defaults to 30000, `transport.max_connections` to 1024, and `transport.idle_timeout_ms` to 30000. `transport.max_connections` and `http.max_concurrent_requests` must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`. The `http.*` fields create `HttpLimits` but do not install them: Controller short routes already use `HttpLimits::default()`, and passing the configured value to `with_http_limits` replaces that policy. Install `RequestLimitLayer` on selected native Axum short routes. `WebServer` does not modify the Router. Errors identify the invalid field through `ConfigOptionsError::field()` without echoing its value; oversized capacities report the corresponding configuration key.

## Assemble short routes and long-lived routes

Controller short routes have a default budget of 1 MiB per request body, 256 concurrent requests shared by the builder, and 30 seconds for handler work plus response-body streaming. Call `with_http_limits` before adding Controllers to replace that policy. Native Axum routes do not inherit Controller limits; install `RequestLimitLayer` on each selected ordinary short route. `WebServer` serves the supplied Router without adding route layers. These limits protect consumed request-body data, concurrent handler work, and the selected branch's deadline. If the handler misses the deadline, the layer returns `504 Gateway Timeout`. Unrepresentable absolute request deadlines are rejected as `WebServerError::InvalidConfig`; if the deadline becomes unrepresentable at runtime, the layer returns HTTP 500 with code `invalid_configuration`. If the deadline expires after response headers have been sent, the layer immediately drops the application response body and releases the short-request permit; the status cannot be changed, and the body reports one timeout error on its next poll. A slow client or transport backpressure may delay when that error is observed. This deadline does not close the TCP or HTTP/2 connection, which remains governed by the independent transport policy. For streamed short-route bodies without an oversized declared `Content-Length`, the handler may perform side effects before consuming the over-limit frame; the resulting 413 does not roll those effects back. If validation must precede a side effect, consume and validate the complete body first.

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

The example fragments assume application handlers `submit`, `progress`, and `websocket`; the full runnable composition is in [`examples/prompt_stream.rs`](../examples/prompt_stream.rs). Controller routes already receive default limits; call `ControllerRoutes::with_http_limits` before adding them to replace those limits. Controller `sse` and `ws` routes use a separate stream-admission layer; `with_http_limits` only changes short-route limits. Keep native SSE and WebSocket branches outside `RequestLimitLayer`: its deadline also covers response streaming. The macro treats `kind` as a limit-policy classification, requires `GET` for `ws`, and does not verify that the handler implements the declared wire protocol.

`ControllerRoutes::add` checks the complete set of Controller route declarations before registering them. It returns `ControllerRouteError` for duplicate method/path pairs, invalid paths, or matching-template conflicts such as parameter aliases and overlapping wildcards. Different methods can share one template, and `finish()` returns the `Router` directly. This check covers declarations collected by the builder; later merges with native Axum routers retain Axum's own behavior. `rest_controller` also supports distinct Controller types with the same terminal type name in one module.

### Admit native SSE and WebSocket requests

`WebServer` does not add middleware to a supplied router. Install one `StreamAdmissionLayer` on each native SSE or WebSocket branch. Reuse a `StreamAdmissionState` when branches should share capacity:

```rust,ignore
use axum::{Router, routing::get};
use qubit_web::{StreamAdmissionLayer, StreamAdmissionLimits, StreamAdmissionState};

let limits = StreamAdmissionLimits::default();
let shared = StreamAdmissionState::new(limits);
let app = Router::new()
    .route("/events", get(progress).layer(StreamAdmissionLayer::from_state::<()>(shared.clone())))
    .route("/ws", get(websocket).layer(StreamAdmissionLayer::from_state::<()>(shared)));
```

The runnable [`prompt_stream` example](../examples/prompt_stream.rs) now uses this same arrangement: its native SSE and WebSocket routes receive clones of one `StreamAdmissionState`. With the default limits, that shared state buffers at most 1 MiB per request, allows up to 256 concurrent admissions, and applies a 10-second deadline from admission through handler response headers. The deadline ends when response headers are ready; it does not cover an SSE stream or a WebSocket session after upgrade. Session capacity remains controlled separately by `SseConnectionPolicy` and `WsUpgradePolicy`.

Each `StreamAdmissionLayer::new::<()>(limits)` creates a separate capacity domain. The `ControllerRoutes` builder creates one shared stream domain for its `sse`/`ws` routes; call `with_stream_admission_limits(limits)` before `add` to replace it. `with_http_limits` configures only `short` routes, so a builder has two independent request-policy domains.

Defaults per stream-admission state are 1 MiB per buffered body, 256 concurrent admissions, and 10 seconds from entering admission through handler response headers. The theoretical body-buffer budget is 256 MiB per state domain, plus request and runtime overhead; lower the body or concurrency limit to fit your memory budget. The layer reads the complete body before invoking the handler, so an oversized or unreadable body cannot reach handler code. An active SSE response body or WebSocket session is outside this deadline.

| Result | Meaning and next step |
| --- | --- |
| `413` `body_too_large` | Declared or buffered body exceeded the limit; reduce the request or raise the configured body budget. |
| `400` `body_read_failed` | The request body could not be read; inspect the client/proxy request delivery and retry only when appropriate. |
| `503` `capacity_exceeded` | All admission permits in this state domain are occupied; share state as intended or tune capacity and load. |
| `504` `request_timeout` | Body buffering or handler response-header creation missed the admission deadline; inspect slow uploads and handler work. |
| `500` `invalid_configuration` | The absolute admission deadline could not be represented at runtime; inspect the configured timeout and report a configuration/runtime issue. |

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
    let connection = match state.sse.begin(&state.context) {
        Ok(connection) => connection,
        Err(error) => return error.into_response(),
    };
    let cancellation = connection.cancellation_token();
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        for update in ["queued", "working", "complete"] {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                result = sender.send(
                    axum::response::sse::Event::default()
                        .event("progress")
                        .data(update),
                ) => if result.is_err() { break; }
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });
    connection
        .into_sse(EventReceiver(receiver))
        .into_response()
}

// This adapter is declared in the same application module as `progress`.
struct EventReceiver(tokio::sync::mpsc::Receiver<axum::response::sse::Event>);

impl futures_core::Stream for EventReceiver {
    type Item = Result<axum::response::sse::Event, std::convert::Infallible>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.0.poll_recv(cx).map(|event| event.map(Ok))
    }
}

async fn websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // Demonstration-only authentication. Replace this check with the
    // application's authentication and authorization before calling on_upgrade.
    if headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        != Some("Bearer demo-only")
    {
        return axum::http::StatusCode::UNAUTHORIZED.into_response();
    }

    let policy = state.ws.clone();
    policy.on_upgrade(ws, &headers, state.context.clone(), |mut session| async move {
        while let Some(Ok(message)) = session.recv().await {
            match message {
                axum::extract::ws::Message::Text(text) => {
                    if session
                        .try_send(axum::extract::ws::Message::text(format!("demo echo: {text}")))
                        .is_err()
                    {
                        break;
                    }
                }
                axum::extract::ws::Message::Close(_) => break,
                _ => {}
            }
        }
    })
}
```

The code is an application-module excerpt: it assumes `AppState` and the state assembly function above, and requires `tokio`, `futures-core`, and Axum as direct dependencies. The executable source of truth, including its imports, event producer, route assembly, and `main`, is [`examples/prompt_stream.rs`](../examples/prompt_stream.rs) (`EventReceiver`, `progress`, `websocket`, and `Router::with_state`). `Router::with_state(state)` supplies the single startup-created state instance to these handlers. The example's `Bearer demo-only` check only demonstrates authentication ordering and is not production authentication; replace it with the application's identity and authorization checks. `WsUpgradePolicy::allowed_origins` independently restricts browser origins; an Origin allowlist does not authenticate a client. Cloning either policy shares its connection-capacity domain, so all routes using clones draw from one configured allowance. Calling `SseConnectionPolicy::default()` or `WsUpgradePolicy::new()` again creates an independent domain; requests handled by that new instance do not consume the original policy's allowance. These are separate caps: SSE has its own session cap and remains within the HTTP transport cap, while an upgraded WebSocket uses its policy cap after the HTTP upgrade completes. A successful `try_send` means only that a message entered the outbound queue, not that the network delivered it.

For a configuration-driven service, pass `configured.http_limits()` to `ControllerRoutes::with_http_limits` before adding controllers to replace its default policy, or to `RequestLimitLayer::new` on selected native Axum branches. Controller `sse` and `ws` routes use their separate stream-admission budget. Native Axum SSE/WS branches need `StreamAdmissionLayer`; `WebServer` receives a completed Axum `Router` and does not add route limits.

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

On shutdown, the server atomically closes admission for new managed SSE and WebSocket sessions, stops accepting new connections, and notifies active sessions. It waits for HTTP connections and registered sessions against the same configured deadline, set by `ServerOptions` and shared with WebSocket sessions through `ServerContext`; `WsUpgradePolicy` has no separate shutdown timeout. Unrepresentable shutdown deadlines are rejected as `WebServerError::InvalidConfig`; an extreme runtime failure cancels sessions and drains connection tasks before returning an error. `graceful` is true only if both finish before that deadline. Otherwise `unfinished_managed_sessions` records the managed session count at the deadline; the report does not claim that arbitrary application background tasks have stopped. `forced_connections` is `None` because Axum does not expose a reliable count.

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

While an SSE or context-aware WebSocket remains open, the response body is the current count; after it closes, the count should return to zero. The demo SSE path is `/prompt/demo-1/events`. Observe its named progress events with:

```sh
curl -N http://127.0.0.1:3001/prompt/demo-1/events
```

The response sends `queued`, `working`, and `complete` as `progress` events. The WebSocket path is `/prompt/demo-1/ws`; connect with `Authorization: Bearer demo-only` and the allowed Origin `http://localhost:3000`, then send a text message to receive `demo echo: <message>`. A missing or different bearer value is rejected with HTTP 401 before upgrade. Both credentials and the unrestricted shutdown endpoint are for this loopback demonstration only; replace authentication and protect shutdown in a real service.

Stop the demo through its explicit endpoint:

```sh
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

The endpoint responds 202 when it triggers shutdown and 410 if it was already used. The serving task then exits and prints no separate report in this example; production hosts should inspect `ShutdownReport`.

## Capacity, overload, and troubleshooting

Budget transport connections separately from long-lived sessions. For example, with a transport cap of 256, a service may also allow 80 SSE sessions and 80 WebSocket sessions; these session policy capacities are separate limits and should be sized against file descriptors, memory, TLS CPU, and application work. The transport cap is per server instance, not a process-wide cap. SSE remains an HTTP transport future and occupies transport capacity while active; an upgraded WebSocket releases transport capacity when the HTTP upgrade future ends.

When all transport permits are occupied, the accept loop waits before accepting another socket. A client may remain in the OS listen backlog or fail to connect. There is no HTTP handler to return a guaranteed 503 for a socket that has not been accepted. Tune the cap and the operating system backlog as deployment-specific settings; do not treat backlog behavior as an application response.

`request_header_timeout` defaults to 10 seconds. It bounds HTTP/1 request headers and is also used for the HTTPS TLS handshake. `transport_idle_timeout` defaults to 30 seconds and closes idle HTTP/1 and HTTP/2 transports when no handler or response body is active. It does not time out a stalled active request; use route limits or deployment-level policies for that case. This is separate from `HttpLimits::with_request_timeout`, which applies on Controller `short` routes by default and on native Axum routes only after `RequestLimitLayer` is installed.

For WebSockets, `WsUpgradePolicy::idle_timeout` also limits the time without progress while delivering an inbound message to the application. If the 64-message inbound channel is full because the application is not calling `WsSession::recv()`, expiry closes the session with code `1013` (`inbound backpressure`). Drain messages regularly and hand off longer processing after receiving each message. `WsSession::try_send` returning success means only that the message entered the outbound queue; it does not guarantee network delivery. After the peer or session closes, it returns `WsSendError::Closed` (or `Closed` when matching the variant).

`WsUpgradePolicy::validate()` checks the final configured field values, so a later setter value replaces an earlier invalid value for validation. The valid `max_connections` range is `1..=tokio::sync::Semaphore::MAX_PERMITS`; zero returns `WsPolicyError::ZeroLimit`, and a larger value returns `WsPolicyError::ConnectionLimitTooLarge`. An invalid policy is rejected by `on_upgrade` with HTTP 500 and cannot upgrade. For example, correcting a zero value before validation succeeds:

```rust,ignore
assert!(WsUpgradePolicy::new()
    .max_connections(0)
    .max_connections(1)
    .validate()
    .is_ok());
```

| Symptom | Check |
| --- | --- |
| Client waits or connection fails during overload | Check per-instance transport capacity and OS backlog. Do not expect a 503 before accept. |
| A request exceeds the body cap but succeeds | For a Controller `short` route, confirm its handler/extractor consumes the body and check whether `with_http_limits` replaced the default with a larger budget. For a native Axum route, install `RequestLimitLayer` on that branch and confirm the body is consumed. |
| A slow client does not immediately observe a short-response timeout | After response headers are sent, expiry releases the application body and short-request permit but can only report a body error when the transport polls again. It does not change the response status or close the connection; transport connections use their separate policy. |
| SSE closes at the ordinary request deadline | Remove `RequestLimitLayer` from that streaming branch and use `SseConnectionPolicy` limits. |
| `active_sessions()` stays at zero for a WS | Call `WsUpgradePolicy::on_upgrade` with the server's `ServerContext`; native Axum upgrades are outside managed-session reporting. |
| `WsUpgradePolicy::validate()` reports an invalid connection limit | Check the final `max_connections` value: zero is `WsPolicyError::ZeroLimit`; values above `tokio::sync::Semaphore::MAX_PERMITS` are `WsPolicyError::ConnectionLimitTooLarge`. Correct the setting and validate again; an invalid policy cannot upgrade. |
| A WebSocket closes with code `1013` under inbound load | The application may not be calling `WsSession::recv()` fast enough. The inbound channel holds 64 messages; if it remains full and delivery makes no progress for `idle_timeout`, the session closes. Read messages continuously or move processing to application-managed work after receiving them; choose an `idle_timeout` that allows the expected processing gaps. |
| Configuration is rejected | Inspect `ConfigOptionsError::field()`; check address, positive timeout/limit values, concurrency capacities within `1..=tokio::sync::Semaphore::MAX_PERMITS`, and integer types. The capacity errors identify `http.max_concurrent_requests` or `transport.max_connections`; error text intentionally does not include rejected values. |
| HTTPS or WSS cannot connect | Check certificate chain/key pairing and client trust. A TLS failure occurs before HTTP routing. |
| Shutdown exceeds expectations | Check the configured shutdown timeout and whether application handlers or stream producers respond to cancellation. |

`WebServerError` exposes retained socket errors through `std::error::Error::source()`. Its default `Display` and `Debug` intentionally show only the error category; inspect `source()` explicitly when diagnosing bind or accept failures.

See the [Chinese guide](user_guide.zh_CN.md), [README](../README.md), [API documentation](https://docs.rs/qubit-web), the [historical Chinese design record](2026-10-08-rs-web-design.md), and the [current lifecycle design summary](2026-10-09-rs-web-lifecycle-design.en.md) for next steps.
