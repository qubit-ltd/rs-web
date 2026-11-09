# Running an HTTP service with bounded streams

This guide is for Rust service developers using `qubit-web` 0.3.0 (Rust 1.94 or newer). It follows one service that accepts a bounded JSON prompt request, streams progress over SSE, optionally echoes a WebSocket, and shuts down under host control. The prompt and token are demonstration data; the example does not run an agent and its token is not production authentication.

Start with the [English README](../README.md) for project scope and feature selection. The [Chinese README](../README.zh_CN.md) and [Chinese guide](user_guide.zh_CN.md) are also available. This guide covers operational setup. Public API details are in [docs.rs](https://docs.rs/qubit-web) and crate Rustdoc.

## Prepare settings and start the listener

The portable `prompt_stream` scenario imports Axum, `futures-core`, Serde, Tokio, and `qubit-web` directly. Declare each crate your application imports; transitive dependencies of `qubit-web` are not available as direct dependencies in your crate:

```toml
[dependencies]
axum = "0.8"
futures-core = "0.3"
qubit-web = { version = "0.3", features = ["json", "ws"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

These are the direct dependencies used to move the [`prompt_stream` example](../examples/prompt_stream.rs) into a separate application. To run the repository example itself, use the repository's manifest and the command in [Run, observe, and stop the service](#run-observe-and-stop-the-service). For HTTPS add `tls`. Add `config` only if you want `ConfiguredWeb` to parse a `qubit_config::Config` value supplied by your application; it does not load a file or global settings source for you. If your application imports `qubit_config::Config` directly, declare `qubit-config` as a direct dependency too.

Choose the listener capacity for each server instance. The default is 1024 transport connections. Set a positive value explicitly when the service's file descriptor, TLS, or memory budget calls for a smaller cap:

```rust
use qubit_web::{ServerOptions, WebServer};

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_max_transport_connections(256)?;
let server = WebServer::bind_http(options).await?;
let context = server.context();
```

The transport and short-request concurrency values must be within `1..=tokio::sync::Semaphore::MAX_PERMITS`; zero and larger values are rejected with `WebServerError::InvalidConfig`. The cap applies per `WebServer`; it covers accepted sockets, TLS handshakes, and HTTP connection futures. It is held until the HTTP transport future ends. Upgraded WebSocket sessions are governed by `WsUpgradePolicy` after Hyper completes the upgrade.

For a batch of native listener settings, `validate_detailed()` identifies the first invalid field and a stable reason code without exposing the configured value. Its field order is shutdown timeout, request-header timeout, transport idle timeout, transport connection capacity, HTTP/1 parser buffer, and HTTP/2 header-list budget. The existing `validate()` still returns `WebServerError::InvalidConfig`, and binding performs its own checks:

```rust,ignore
let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_request_header_timeout(Duration::ZERO);
if let Err(error) = options.validate_detailed() {
    eprintln!("invalid {} ({})", error.field(), error.code());
}
```

Transport connections with no active request or response body close after 30 seconds by default. Choose a different positive duration when the deployment needs another idle policy:

```rust
use std::time::Duration;

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_transport_idle_timeout(Duration::from_secs(45));
assert_eq!(options.transport_idle_timeout(), Duration::from_secs(45));
```

An active handler or response body pauses the idle timer, so long-lived SSE is not closed by this timeout. It also reclaims an HTTP/2 connection that never starts a request. HTTP/2 PING traffic does not count as request activity. The timeout must be positive and form a representable Tokio deadline; `Duration::MAX` is rejected. With `config`, `transport.idle_timeout_ms` sets the same value and defaults to `30000`; zero and values that cannot form a deadline are invalid. `request_header_timeout` remains a separate 10-second HTTP/1 header and TLS handshake limit.

HTTP/2 connections advertise a maximum of 200 concurrent streams by default. Set a positive per-connection value when constructing the server:

```rust,ignore
let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_http2_max_concurrent_streams(32)?;
```

With `config`, `transport.http2_max_concurrent_streams` accepts `1..=u32::MAX` and defaults to 200. This controls streams on each HTTP/2 connection; it is independent of transport connections, route concurrency, and sessions, and does not cap total process requests, header bytes, or memory. `request_header_timeout` applies to HTTP/1 request headers and TLS handshakes, not HTTP/2 header blocks.

## Transport and protocol header budgets

Hyper's protocol defaults remain in effect unless an optional budget is set. `ServerOptions::with_http1_max_buffer_bytes` accepts values of at least 8,192 bytes; `http1_max_buffer_bytes()` returns `None` by default. This limits HTTP/1 parser buffering and is not an exact header-list byte count. `ServerOptions::with_http2_max_header_list_bytes` accepts a positive `u32`; `http2_max_header_list_bytes()` also defaults to `None`. Hyper applies this to the decoded HTTP/2 header list. It is not a limit on dynamic-table state, stream count, connections, body data, or total process memory. These budgets apply to HTTP and HTTPS connections. Hyper may reject an over-budget request or connection, and the rejection is not guaranteed to use one application HTTP status.

With `config`, set `transport.http1_max_buffer_bytes` (at least `8192`) and `transport.http2_max_header_list_bytes` (positive `u32` bytes). Both keys are optional. Values are checked before constructing the Hyper builders; invalid values identify their configuration field without echoing the value.

With the optional `config` feature, parse settings and keep route policy separate:

```rust,ignore
let configured = ConfiguredWeb::from_config(&config)?;
let (server_options, http_limits) = configured.into_parts();
let server = WebServer::bind_http(server_options).await?;
```

`address` is required; `shutdown_timeout_ms` defaults to 30000, `transport.max_connections` to 1024, `transport.idle_timeout_ms` to 30000, `transport.request_header_timeout_ms` to 10000, and `transport.http2_max_concurrent_streams` to 200. The request-header timeout must be positive and construct a Tokio deadline; it applies to HTTP/1 request headers and TLS handshakes, not HTTP/2 header blocks. The HTTP/2 stream value must fit in a positive `u32`. The optional `transport.http1_max_buffer_bytes` and `transport.http2_max_header_list_bytes` have no explicit default and preserve Hyper's defaults when omitted. The `http.*` fields create `HttpLimits` but do not install them: Controller short routes already use `HttpLimits::default()`, and passing the configured value to `with_http_limits` replaces that policy. Install `RequestLimitLayer` on selected native Axum short routes. `WebServer` does not modify the Router. Errors identify the invalid field through `ConfigOptionsError::field()` without echoing its value; invalid values report the corresponding configuration key.

Stream admission has a separate optional parser. `stream_admission_limits_from_config` does not require `address`, does not change `ConfiguredWeb::from_config`, and returns limits without creating state or installing a layer. Omitted keys retain `StreamAdmissionLimits::default()` values:

| Key | Type | Default |
| --- | --- | --- |
| `stream_admission.max_body_bytes` | `u64` | 1 MiB |
| `stream_admission.max_concurrent_admissions` | `u64` | 256 |
| `stream_admission.admission_timeout_ms` | `u64` | 10000 ms |

Parse the policy independently, then explicitly install one state in each consumer. Cloning the state shares its capacity domain between Controller long-lived routes and native Axum branches:

```rust,ignore
use axum::{Router, routing::get};
use qubit_config::Config;
use qubit_web::config::stream_admission_limits_from_config;
use qubit_web::{ControllerRoutes, StreamAdmissionLayer, StreamAdmissionState};

let config = Config::new();
let limits = stream_admission_limits_from_config(&config)?;
let state = StreamAdmissionState::new(limits);
let controller_routes = ControllerRoutes::new()
    .with_stream_admission_state(state.clone()); // set before add
let native_routes = Router::new().route(
    "/events",
    get(|| async { "ok" }).layer(StreamAdmissionLayer::from_state(state)),
);
```

Invalid types, integer conversions, zero or out-of-range values, and unrepresentable deadlines return `ConfigOptionsError` with the corresponding full key in `field()`; rejected values are not included in the error text. The parser does not load files or global settings and does not reject unrelated keys.

## Assemble short routes and long-lived routes

Controller short routes have a default budget of 1 MiB per request body, 256 concurrent requests shared by the builder, and 30 seconds for handler work plus response-body streaming. Call `with_http_limits` before adding Controllers to replace that policy. Native Axum routes do not inherit Controller limits; install `RequestLimitLayer` on each selected ordinary short route. `WebServer` serves the supplied Router without adding route layers. These limits protect consumed request-body data, concurrent handler work, and the selected branch's deadline. If the handler misses the deadline, the layer returns `504 Gateway Timeout`. Unrepresentable absolute request deadlines are rejected as `WebServerError::InvalidConfig`; if the deadline becomes unrepresentable at runtime, the layer returns HTTP 500 with code `invalid_configuration`. Deadline enforcement depends on Tokio scheduling: non-yielding synchronous work can delay this check and cleanup, and may finish successfully after the deadline. Once the timeout is observed after response headers have been sent, the layer drops the application response body and releases the short-request permit; the status cannot be changed. With `DiagnosticLayer::with_body_diagnostics()` installed outside `RequestLimitLayer`, the timeout event is logged when the deadline fires even if the transport never polls the body again; the body reports its timeout error only on a later poll. Without that opt-in diagnostic layer, existing logging remains unchanged. A slow client or transport backpressure may delay when the body error is observed. This deadline does not close the TCP or HTTP/2 connection, which remains governed by the independent transport policy. For streamed short-route bodies without an oversized declared `Content-Length`, the layer converts an over-limit signal to 413 only if it observes the signal before the handler returns. If the handler returns an unread request body for later consumption, its response status is not changed afterward; the response body reports a read error at the over-limit frame. To guarantee a rejection status, applications must first read and validate the complete body. The handler may still perform side effects before observing an over-limit frame, and a resulting 413 does not roll them back. If validation must precede a side effect, consume and validate the complete body first.

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

To share short-request concurrency across native Axum branches, create one `LimitState` and pass clones to each layer. Each `RequestLimitLayer::new` call creates an independent capacity domain, even when its `HttpLimits` values match:

```rust,ignore
use axum::{Router, routing::get};
use qubit_web::{HttpLimits, RequestLimitLayer};
use qubit_web::limit::LimitState;

let shared = LimitState::new(HttpLimits::default());
let app: Router = Router::new()
    .route("/a", get(|| async { "a" })
        .layer(RequestLimitLayer::from_state(shared.clone())))
    .route("/b", get(|| async { "b" })
        .layer(RequestLimitLayer::from_state(shared)));
```

The shared allowance applies only to routes using clones of this state. It is not an automatic process-wide cap. To include Controller short routes in the same domain, pass a clone to `with_limit_state` before adding their Controller; adding a Controller captures the builder's current state, so a later replacement applies only to later additions. The last short-request state or limits setter called before an addition selects that Controller's domain. Independent state values remain independent.

This complete assembly shares one short-request domain between a Controller and a native Axum route:

```rust
use std::sync::Arc;
use axum::{Router, routing::get};
use qubit_web::{ControllerRoutes, HttpLimits, RequestLimitLayer, rest_controller};
use qubit_web::limit::LimitState;

fn main() -> Result<(), qubit_web::mvc::ControllerRouteError> {
struct Health;
#[rest_controller("/health")]
impl Health {
    #[get_mapping("")]
    async fn check(&self) -> &'static str { "ok" }
}

let state = LimitState::new(HttpLimits::default());
let controllers = ControllerRoutes::<()>::new()
    .with_limit_state(state.clone())
    .add(Arc::new(Health))?
    .finish();
let native: Router<()> = Router::new()
    .route("/native", get(|| async { "ok" }))
    .layer(RequestLimitLayer::from_state(state));
let app = controllers.merge(native);
let _ = app;
Ok(())
}
```

Stream admission has a separate state and setter. Share one `StreamAdmissionState` with the Controller builder and native long-lived branch; do not use the short-request `LimitState` for this budget:

```rust
use std::sync::Arc;
use axum::{Router, routing::get};
use qubit_web::{ControllerRoutes, StreamAdmissionLayer, StreamAdmissionLimits, StreamAdmissionState, rest_controller};

fn main() -> Result<(), qubit_web::mvc::ControllerRouteError> {
struct Events;
#[rest_controller("/events")]
impl Events {
    #[route("", method = "GET", kind = "sse")]
    async fn stream(&self) -> &'static str { "event" }
}

let state = StreamAdmissionState::new(StreamAdmissionLimits::default());
let controllers = ControllerRoutes::<()>::new()
    .with_stream_admission_state(state.clone())
    .add(Arc::new(Events))?
    .finish();
let native: Router<()> = Router::new()
    .route("/native-events", get(|| async { "event" }))
    .layer(StreamAdmissionLayer::from_state(state));
let app = controllers.merge(native);
let _ = app;
Ok(())
}
```

The short-request and stream-admission states remain independent capacity domains even when both are configured on the same builder.

`ControllerRoutes::add` checks the complete set of Controller route declarations before registering them. It returns `ControllerRouteError` for duplicate method/path pairs, invalid paths, more than 25 non-wildcard parameters, or matching-template conflicts such as parameter aliases and overlapping wildcards. A final catch-all parameter does not count toward the limit. Different methods can share one template, and `finish()` returns the `Router` directly. Use Axum capture syntax such as `/{id}` and `/{*rest}`; legacy `/:id` and `/*rest` segments are rejected. This check covers declarations collected by the builder; later merges with native Axum routers retain Axum's own behavior. `rest_controller` also supports distinct Controller types with the same terminal type name in one module.

### Give Controllers one root state type

Controller methods extract state directly as `State<AppState>`. Every enabled method on a Controller that uses `State` must use the same state type, and the macro compares the written types without resolving aliases or renamed imports. It does not combine several child states into an inferred root state through `FromRef`. Keep the values the Controller needs on one cloneable root state and read them from its fields:

```rust,ignore
use std::sync::Arc;
use axum::extract::State;
use qubit_web::{ControllerRoutes, get_mapping, rest_controller};

#[derive(Clone)]
struct AppState { service_name: Arc<str> }
struct Health;

#[rest_controller("/health")]
impl Health {
    #[get_mapping("")]
    async fn check(&self, State(state): State<AppState>) -> String {
        format!("{}: ok", state.service_name)
    }
}

let state = AppState { service_name: Arc::from("catalog") };
let router = ControllerRoutes::<AppState>::new()
    .add(Arc::new(Health))?
    .finish()
    .with_state(state);
```

This assembles a `Router<()>` ready for `WebServer::serve`. If a handler needs a child component, access it through a field or method on `AppState`; do not rely on `FromRef<Child>` to infer the Controller state. The compile-fail contract for mixed state types is covered by the existing conditional-state UI test.

Controller methods can return opaque response types and use `Self` in extractor types, including const expressions such as `Extension<[u8; Self::N]>` and qualified associated constants such as `Extension<[u8; <Self as BufferLimit>::N]>`. A method may also accept `Extension<Arc<Self>>` and return `impl IntoResponse`, or return `Result<impl IntoResponse, StatusCode>`. The generated route converts the value to Axum's concrete `Response` while the Controller instance remains alive; the method's declared return type does not need to change.

### Combine Axum extractor limits with request budgets

For native Axum `Bytes`, `String`, and `Json` extractors, Axum's `DefaultBodyLimit` and the `RequestLimitLayer` or `StreamAdmissionLayer` budget are independent. Neither rs-web setter installs or disables Axum's setting. A body is accepted only when every applicable limit admits it. To accept a 3 MiB native extractor body, for example, set the Axum route limit to at least that size and configure the rs-web layer budget accordingly:

```rust,ignore
use axum::{Router, body::Bytes, extract::DefaultBodyLimit, routing::post};
use qubit_web::{HttpLimits, RequestLimitLayer};

const BODY_LIMIT: usize = 4 * 1024 * 1024;
let limits = HttpLimits::default().with_max_body_bytes(BODY_LIMIT)?;
let app = Router::new()
    .route("/upload", post(|body: Bytes| async move { body.len().to_string() }))
    .layer(DefaultBodyLimit::max(BODY_LIMIT))
    .layer(RequestLimitLayer::new(limits));
```

With Axum's default, a native extractor may reject a body above its default before the handler runs, even when the rs-web budget is larger. Raising Axum's limit lets the rs-web limit govern the request; disabling Axum's limit still leaves the rs-web body budget in force. `StreamAdmissionLayer` follows the same independent-budget rule on SSE/WS branches. `BoundedJson` is different: it reads through its own `JsonLimits` budget and is not governed by Axum's native JSON extractor default. The router assembly and 3/4/5 MiB boundaries are covered by the layer documentation and tests.

### Configure JSON input and output budgets

For `BoundedJson`, install the input policy as an Axum `Extension<JsonLimits>` on the route or router. `BoundedJson` reads that extension and falls back to `JsonLimits::default()` when it is absent; the default input byte limit is 1 MiB. The runnable [`json_budgets` example](../examples/json_budgets.rs) sets a 4 MiB `RequestLimitLayer` body ceiling, then installs input limits of 2 MiB and a 2 MiB maximum for each string. The HTTP ceiling is an outer transport limit; the JSON byte and string limits remain separate checks, and the remaining structure limits (depth, nodes, collections, keys, and numbers) retain their own `JsonLimits` defaults.

Output encoding has its own explicit budget. The example creates a separate `JsonLimits` value with `with_max_output_bytes(4096)` and passes it to `json_response`; the input extension does not configure output encoding. On success, the handler returns the encoded response. On failure, it checks `JsonResponseError::code()` and emits a fixed HTTP 500 body: `json_output_budget_exceeded` for an output byte or structural budget violation, or `json_encode_failed` for another encoding failure. The first code does not identify which budget was exceeded. No partial JSON is returned. This 500 mapping is the example's application policy; the library does not force it. Do not retry the same unencodable value. Record the safe code for operators, then reduce the response data or review the serializer; when investigating a budget error, check the relevant output byte and structural budgets. If the intended response is larger, adjust the applicable output budget deliberately. Keep prompt contents and error details out of the response.

Run the example and submit a small prompt:

```sh
cargo run -p qubit-web --features json --example json_budgets
curl -i http://127.0.0.1:3001/prompts \
  -H 'content-type: application/json' \
  -d '{"prompt":"hello"}'
```

The request returns HTTP 200 with `accepted: true` and the prompt. A body that fits the 4 MiB HTTP ceiling but exceeds the 2 MiB JSON input budget receives HTTP 413 from `BoundedJson`. When `max_string_bytes` is configured below `max_input_bytes`, exceeding the per-string budget also receives HTTP 413 even if the whole JSON body remains within its byte budget. An output that exceeds 4096 bytes receives the fixed HTTP 500 code described above, with no partial JSON body. JSON input rejections from `BoundedJson` otherwise retain its 400/413/415 behavior (or an application handler's chosen mapping). The 4 MiB request middleware can independently reject an oversized body before the handler runs; that middleware response is not rewritten by the handler. See [`JsonLimits`](../src/json/json_limits.rs), [`BoundedJson`](../src/json/bounded_json.rs), [`json_response`](../src/json/json_response.rs), and [`JsonResponseError`](../src/json/json_response_error.rs) for the API details.

While reading JSON input, `BoundedJson` copies each data frame into a contiguous buffer and drops that frame's `Bytes` handle before polling the next one. `max_input_bytes` limits JSON payload bytes, not total process memory: the vector can retain spare capacity, growth can temporarily keep allocations alive together, and the current frame, transport buffers, decoding, and the resulting value add memory use. This budget is not a process RSS limit. Decoding is synchronous, so byte and structure budgets do not bound CPU time spent in a custom `Deserialize` implementation. This implementation does not change the public API, so callers do not need to migrate.

### Handle bounded JSON rejection categories

With the `json` feature, a handler can accept `Result<BoundedJson<T>, JsonRejection>` and choose its own response for each failure category. `JsonRejection` is available at `qubit_web::json::JsonRejection`; its default response remains `400` for invalid JSON and body-read failure, `413` for an exceeded JSON budget, and `415` for an unsupported media type. This application response only handles `BoundedJson` rejections; middleware-generated responses, such as request-limit rejection, are not automatically replaced.

```rust,ignore
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use qubit_web::json::{BoundedJson, JsonRejection};

#[derive(serde::Deserialize)]
struct NewPrompt { prompt: String }

async fn submit(
    input: Result<BoundedJson<NewPrompt>, JsonRejection>,
) -> Response {
    match input {
        Ok(BoundedJson(prompt)) => (StatusCode::OK, prompt.prompt).into_response(),
        Err(error) => {
            let (status, message) = match error {
                JsonRejection::InvalidJson => (StatusCode::BAD_REQUEST, "Invalid JSON"),
                JsonRejection::BodyReadFailed => (StatusCode::BAD_REQUEST, "Could not read request body"),
                JsonRejection::BudgetExceeded => (StatusCode::PAYLOAD_TOO_LARGE, "JSON budget exceeded"),
                JsonRejection::UnsupportedMediaType => (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Use a JSON content type"),
            };
            (status, message).into_response()
        }
    }
}
```

Choose the application's response body and logging policy for these errors. If the handler returns the rejection unchanged, `JsonRejection`'s existing `IntoResponse` behavior supplies the stable problem codes and statuses.

### Admit native SSE and WebSocket requests

`WebServer` does not add middleware to a supplied router. Install one `StreamAdmissionLayer` on each native SSE or WebSocket branch. Reuse a `StreamAdmissionState` when branches should share capacity:

```rust,ignore
use axum::{Router, routing::get};
use qubit_web::{StreamAdmissionLayer, StreamAdmissionLimits, StreamAdmissionState};

let limits = StreamAdmissionLimits::default();
let shared = StreamAdmissionState::new(limits);
let app = Router::new()
    .route("/events", get(progress).layer(StreamAdmissionLayer::from_state(shared.clone())))
    .route("/ws", get(websocket).layer(StreamAdmissionLayer::from_state(shared)));
```

The runnable [`prompt_stream` example](../examples/prompt_stream.rs) now uses this same arrangement: its native SSE and WebSocket routes receive clones of one `StreamAdmissionState`. With the default limits, that shared state buffers at most 1 MiB per request, allows up to 256 concurrent admissions, and applies a 10-second deadline from admission through handler response headers. The 256 MiB product is a concurrent payload estimate, not a bound on buffer allocation or process peak memory. Incremental buffering releases each prior data-frame owner after copying it, while vector spare capacity, temporary coexistence during growth, the current frame, trailers, allocator behavior, and transport buffers remain additional costs. The deadline ends when response headers are ready; it does not cover an SSE stream or a WebSocket session after upgrade. Session capacity remains controlled separately by `SseConnectionPolicy` and `WsUpgradePolicy`.

The layer constructors no longer take a Router-state type parameter. Migrate explicit calls by removing the turbofish:

```rust
// Before
RequestLimitLayer::new::<()>(http_limits);
StreamAdmissionLayer::from_state::<()>(shared.clone());

// Now
RequestLimitLayer::new(http_limits);
StreamAdmissionLayer::from_state(shared.clone());
```

Each `StreamAdmissionLayer::new(limits)` creates a separate capacity domain. The `ControllerRoutes` builder creates its own stream domain by default for `sse`/`ws` routes; call `with_stream_admission_limits(limits)` before `add` to replace it, or use `with_stream_admission_state(state.clone())` to share a domain with native branches or another builder. A Controller captures the current state when added. `with_http_limits` and `with_limit_state` configure only `short` routes, so a builder has two independent request-policy domains.

Defaults per stream-admission state are 1 MiB per buffered body, 256 concurrent admissions, and 10 seconds from entering admission through handler response headers. The 256 MiB product estimates concurrent payload bytes; it is not a bound on buffer allocation or process peak memory. Incremental buffering releases each prior input data-frame owner after copying it, while vector spare capacity, temporary coexistence during growth, the current frame, trailers, allocator behavior, and transport buffers remain additional costs. Lower the body or concurrency limit to fit your memory budget. The layer reads the complete body before invoking the handler, so an oversized or unreadable body cannot reach handler code. An active SSE response body or WebSocket session is outside this deadline.

The layer preserves request trailers only as delivered by the transport. The 2026-11-08 investigation observed that, with Hyper 1.12.0 and h2 0.4.20, an over-limit HTTP/2 trailer field can be omitted while request processing continues; an HTTP 200 response therefore does not prove that every sent trailer reached the handler. Applications that require a trailer field, such as a digest or signature, must check that it is present and valid before treating the request as successful. The configured body budget counts DATA bytes; trailers do not count toward that byte limit, and the layer does not set a separate trailers memory limit. This is a transport behavior, not an HTTP status guarantee; see the bounded [trailers budget investigation](trailers-budget-investigation-20261108.md) and [network regression tests](../tests/stream_admission_trailers_tests.rs).

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

The code is an application-module excerpt: it assumes `AppState` and the state assembly function above, and requires `tokio`, `futures-core`, and Axum as direct dependencies. The executable source of truth, including its imports, event producer, route assembly, and `main`, is [`examples/prompt_stream.rs`](../examples/prompt_stream.rs) (`EventReceiver`, `progress`, `websocket`, and `Router::with_state`). `Router::with_state(state)` supplies the single startup-created state instance to these handlers. The example's `Bearer demo-only` check only demonstrates authentication ordering and is not production authentication; replace it with the application's identity and authorization checks. `WsUpgradePolicy::allowed_origins` independently restricts browser origins; an Origin allowlist does not authenticate a client. Requests with two or more `Origin` fields are rejected with HTTP 403, including repeated identical values; no Origin and a single value retain their existing semantics. Cloning either policy shares its connection-capacity domain, so all routes using clones draw from one configured allowance. Calling `SseConnectionPolicy::default()` or `WsUpgradePolicy::new()` again creates an independent domain; requests handled by that new instance do not consume the original policy's allowance. These are separate caps: SSE has its own session cap and remains within the HTTP transport cap, while an upgraded WebSocket uses its policy cap after the HTTP upgrade completes. A successful `try_send` means only that a message entered the outbound queue, not that the network delivered it.

For a configuration-driven service, pass `configured.http_limits()` to `ControllerRoutes::with_http_limits` before adding controllers to replace its default policy, or to `RequestLimitLayer::new` on selected native Axum branches. Controller `sse` and `ws` routes use their separate stream-admission budget. Native Axum SSE/WS branches need `StreamAdmissionLayer`; `WebServer` receives a completed Axum `Router` and does not add route limits.

For HTTPS, load trusted certificate material before binding and pass it to `WebServer::bind_https`:

```rust,ignore
let tls = TlsConfig::from_pem_files(certificate_chain, private_key).await?;
let server = WebServer::bind_https(options, tls).await?;
```

Use a certificate trusted by clients, or terminate TLS at a trusted edge and configure the application accordingly. Repository test fixtures are self-signed test material, not deployment credentials. HTTPS uses the same Router; WebSocket upgrades on it use WSS.

`TlsConfig::from_pem_files` and `TlsConfig::from_rustls` keep the existing bind-time snapshot behavior. A reload completed before binding can be observed; a reload after binding does not change that server's later handshakes. To rotate settings online, keep a clone of an `axum_server::tls_rustls::RustlsConfig` and explicitly use `TlsConfig::from_reloadable_rustls`:

```rust,ignore
let rustls = axum_server::tls_rustls::RustlsConfig::from_pem_file(certificate_chain, private_key).await?;
let server = WebServer::bind_https(
    options,
    TlsConfig::from_reloadable_rustls(rustls.clone()),
).await?;
rustls.reload_from_pem_file(next_certificate_chain, next_private_key).await?;
```

The application must declare `axum-server` as a direct dependency to use `RustlsConfig`. A successful reload applies to handshakes that take their configuration snapshot afterward; each handshake reads one snapshot, so a reload racing that read may leave that handshake on the previous configuration. Existing HTTP, SSE, and WSS connections retain their negotiated TLS state. A failed PEM reload leaves the last successfully loaded configuration active. Applications that retain snapshot mode must still coordinate shutdown and rebinding; seamless rebinding of the same address is not guaranteed.

## Run, observe, and stop the service

The server emits sampled connection failure events through the existing `tracing` subscriber. Enable the `qubit_web::transport` target at `DEBUG` (for example, with the filter `qubit_web::transport=debug`) to observe `tls_handshake_timeout`, `tls_handshake_failed`, and `http_connection_failed`. Each `serve` call and failure class has its own cumulative `failure_count`; events are emitted at counts 1, 2, 4, 8, and subsequent powers of two, with `sampling="powers_of_two"`. This sampling limits log volume, so it is not a per-failure audit log or a strict per-second rate limit. Events contain only the fixed failure category and count; they omit underlying errors, request data, peer addresses, and TLS secrets. The crate does not install a global subscriber.

Install request diagnostics with `DiagnosticLayer::new()` on the router branch you want to observe. This preserves the existing response-header log behavior. To also log response-body termination, explicitly use `DiagnosticLayer::with_body_diagnostics()` on the selected branch and place it outside `RequestLimitLayer`, so it can share the request's termination observer. The `response_body_end` event reports one of `complete`, `timeout`, `body_error`, or `dropped`; `status` is the response-header status, and elapsed time ends when the body reaches that state. `dropped` does not prove a client disconnect, and `complete` does not prove network delivery. The event only includes allowlisted request metadata and never logs body bytes or error text. Avoid wrapping a Controller router that already emits the same response-header diagnostic unless duplicate header events are intended. The constructor no longer takes a type parameter: remove any `::<...>` turbofish from existing calls. Neither entry changes the router's `AppState` or requires changes to its state type.

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

If a running `serve` future is dropped or its task is aborted, the server synchronously closes managed-session admission and notifies active sessions. This drop path cannot wait for asynchronous draining or return a `ShutdownReport`, and cleanup still needs the Tokio runtime to run. For graceful shutdown and its report, resolve the shutdown future and await `serve` as shown above. This guarantee applies after `serve` has been polled; dropping a never-started future does not start shutdown.

On shutdown, the server atomically closes admission for new managed SSE and WebSocket sessions, stops accepting new connections, and notifies active sessions. It waits for HTTP connections and registered sessions against the same configured deadline, set by `ServerOptions` and shared with WebSocket sessions through `ServerContext`; `WsUpgradePolicy` has no separate shutdown timeout. Unrepresentable shutdown deadlines are rejected as `WebServerError::InvalidConfig`; an extreme runtime failure cancels sessions and drains connection tasks before returning an error. `graceful` is true only if both groups actually finish at or before the deadline. Session accounting uses its deadline snapshot, and transport tasks record when they release their connection resources. A delayed report can therefore have an `elapsed` value greater than the configured timeout while still reporting graceful shutdown if the work finished on time; conversely, a wait that returns `Ok` after scheduler delay does not make late work graceful. Otherwise `unfinished_managed_sessions` records the managed session count at the deadline; the report does not claim that arbitrary application background tasks have stopped. `WebServer` owns and tracks the connection tasks, while Hyper/hyper-util drives each connection. This implementation does not provide a provable count of forcibly closed connections, so `forced_connections` is `None`.

When a managed connection task panics, the server reaps it and emits a `WARN` tracing event named `connection_task_panic` with `task_kind` set to `transport` or `hyper`. The event omits the panic payload, join error text, request data, peer address, and task ID. Cancellation does not emit this event. It does not change the shutdown deadline or `ShutdownReport::graceful`; the host's panic hook remains independent of this tracing event.

Run the demonstration service with:

```sh
cargo run --example prompt_stream --features "json ws"
```

It prints its loopback address (default `127.0.0.1:3001`). Check the short route:

```sh
curl -i -H 'content-type: application/json' -d '{"prompt":"demo"}' http://127.0.0.1:3001/prompt
```

A successful request returns HTTP 200 and JSON with `job_id` set to `demo-1`. With the `json` feature, `BoundedJson` accepts `application/json` and valid `application/*+json` subtype tokens; malformed subtype syntax is rejected with HTTP 415 before the handler runs. To exercise the configured body limit, send a body larger than 1 MiB; the middleware rejects based on `Content-Length` before JSON decoding:

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

The WebSocket writer emits at most one structured `websocket_io_failure` warning per writer for a failed sink send or control-frame flush, including a real error from the bounded final flush after a peer Close. The event has `direction=write` and `operation=send` or `flush`; it omits payloads and underlying error text. Successful peer-close flushes are quiet, and cancellation or close-confirmation timeout is not logged as a write failure. A successful `WsSession::try_send` still confirms enqueue only, not network delivery.

Budget transport connections separately from long-lived sessions. For example, with a transport cap of 256, a service may also allow 80 SSE sessions and 80 WebSocket sessions; these session policy capacities are separate limits and should be sized against file descriptors, memory, TLS CPU, and application work. The transport cap is per server instance, not a process-wide cap. SSE remains an HTTP transport future and occupies transport capacity while active; an upgraded WebSocket releases transport capacity when the HTTP upgrade future ends.

When `accept` encounters a socket I/O error, `InvalidInput`, `PermissionDenied`, `Unsupported`, and `NotConnected` are terminal. Other errors get at most eight consecutive retries, delayed by 10, 20, 40, 80, 160, 320, 640, and 1,000 ms, for a maximum cumulative wait of 2.27 seconds; the ninth consecutive error terminates serving. A successful accept resets the consecutive-error count. Shutdown or a completed connection task can cancel the current wait; if the accept future is rebuilt after a task completes, it retains the original absolute retry deadline instead of restarting the delay. A terminal error closes the listener, cancels managed sessions, and waits for connection tasks against the shutdown deadline; remaining tasks are aborted and reaped after the deadline. `WebServerError::ServeFailed` preserves the last socket error, available through `std::error::Error::source()`.

When all transport permits are occupied, the accept loop waits before accepting another socket. A client may remain in the OS listen backlog or fail to connect. There is no HTTP handler to return a guaranteed 503 for a socket that has not been accepted. Tune the cap and the operating system backlog as deployment-specific settings; do not treat backlog behavior as an application response.

`request_header_timeout` defaults to 10 seconds; zero or a duration Tokio cannot represent with `Instant::checked_add` (including `Duration::MAX`) is rejected as `WebServerError::InvalidConfig`. The SSE keep-alive interval has the same positive, representable-duration requirement. Unmatched requests use the fixed `<unmatched>` marker in diagnostics; their URI, query, and secrets are not logged. This timeout bounds HTTP/1 request headers and is also used for the HTTPS TLS handshake; it does not set an HTTP/2 header-block deadline. The WEB-004 protocol experiment found SSE continued while a second stream's valid header block was incomplete, but did not measure long-term resource retention; see the [investigation report](http2-resource-investigation-20261103.md). `transport_idle_timeout` defaults to 30 seconds and closes idle HTTP/1 and HTTP/2 transports when no handler or response body is active. It does not time out a stalled active request; use route limits or deployment-level policies for that case. This is separate from `HttpLimits::with_request_timeout`, which applies on Controller `short` routes by default and on native Axum routes only after `RequestLimitLayer` is installed.

For WebSockets, `queue_limits` configures the outbound queue; `inbound_queue_limits(messages, bytes)` configures the separate inbound queue and its per-connection aggregate payload budget. Each ordinary outbound send and notification-driven flush uses the independent `write_timeout` completion deadline (60 seconds by default), measured once from operation start; inbound messages and unrelated queue notifications do not reset it. This is an operation wait bound, not byte-level progress detection. On expiry, the writer closes the queue, establishes the shared close deadline, cancels the session, and drops the sink without retrying or guaranteeing that a Close frame reaches a backpressured peer. Applications can change `write_timeout` without changing inbound waits; `idle_timeout` remains a compatibility setter for both limits. Defaults are 64 queued messages and 1 MiB. The reader may hold one additional message while waiting for a channel slot, and that payload is charged to the same byte budget. A full message channel waits up to `idle_timeout` before closing with code `1013` (`inbound backpressure`); a message that cannot fit the remaining byte budget closes immediately with `1013`. For 128 active default connections, queued and pending payloads are bounded to about 128 MiB, plus up to one 1 MiB protocol-reassembled message per active connection. This estimate excludes allocation overhead and application-owned memory, and does not bound sessions or handlers retained after their I/O ends. `WsSession::recv()` transfers message ownership to the application, outside this queue budget. Drain messages regularly and hand off longer processing after receiving each message. A transport read error is returned once as `Some(Err(_))`, then later receives return `None`; explicit `close()` clears that pending error and makes `recv()` return `None`. Host shutdown may cancel the handler without delivering the error. `WsSession::try_send` returning success means only that the message entered the outbound queue; it does not guarantee network delivery. Queue admission checks `Closed`, `InvalidCloseCode`, `MessageTooLarge`, then `Backpressure`. `InvalidCloseCode` rejects Close codes a server cannot send, including reserved codes and client-only 1010; it is permanent for that message. `MessageTooLarge` applies when the message exceeds the total byte budget or violates a fixed control-frame limit, while `Backpressure` means message-count or remaining-byte capacity is temporarily unavailable. `MessageTooLarge` applies when a Ping or Pong payload exceeds 125 bytes, or a Close reason exceeds 123 UTF-8 bytes; lengths are measured in bytes, and increasing the queue budget cannot relax these protocol limits. Apply backoff to `Backpressure` according to your policy. For an oversized data message, shrink it, split it into application-level messages, or reject it; for an oversized control frame, shorten its payload or reason. Do not fragment control frames. `MessageTooLarge` is permanent for that message; stop sending after `Closed`. Adding `InvalidCloseCode` is a breaking enum change for exhaustive `WsSendError` matches: add a branch that corrects the Close code and does not retry it. The older `try_send` methods retain their signatures and discard a rejected message. Use `WsSession::try_send_recoverable` or `WsSendQueue::try_send_recoverable` when the application needs to retain it. `MessageTooLarge` is permanent for the same message; tests that expected `Backpressure` for an individually oversized message must be updated. After the peer or session closes, it returns `WsSendError::Closed` (or `Closed` when matching the variant). `WsSession::close()` is idempotent; if its future is cancelled while waiting, call it again to retry. A `Close` queued with `try_send` is sent with its code and reason preserved; when the writer begins processing it, the send queue closes, later sends return `Closed`, queued messages behind it may be discarded, and the peer acknowledgement wait is bounded by the shared shutdown deadline. Calls accepted before the writer reaches that Close only mean the messages entered the queue.

When a queue rejection must leave the message available to the application, use `try_send_recoverable` and take the message from `WsSendFailure`. Retry only `Backpressure`, and bound the policy by both an attempt count and a deadline; also cap the number or total bytes of messages retained for retry. Stop on `Closed`, and correct an `InvalidCloseCode` before sending again. Do not busy-loop on a full queue or move rejected messages into an unbounded application buffer. This API does not wait for capacity or provide a capacity notification; successful enqueue still does not mean network delivery.

```rust
use axum::extract::ws::Message;
use qubit_web::ws::{WsSendError, WsSendQueue};

let queue = WsSendQueue::new(0, 1024);
let failure = queue.try_send_recoverable(Message::text("retryable"))
    .expect_err("zero item capacity rejects admission");
let (kind, message) = failure.into_parts();
assert_eq!(kind, WsSendError::Backpressure);
assert_eq!(message.into_text().unwrap(), "retryable");
```

This fragment demonstrates ownership recovery; it does not implement a retry scheduler. The application must choose its finite retry count, deadline, and retained-message budget.

Use `WsSession::closed()` when application work should stop after both WebSocket I/O tasks release their resources. It returns an owned future, so it can be selected alongside a cancellation-safe application operation:

```rust,ignore
let ended = session.closed();
tokio::select! {
    _ = ended => session.close().await,
    result = process_next_item() => apply_result(result),
}
```

Here `process_next_item` and `apply_result` belong to the application. If `ended` wins, `close()` discards unread queued messages. The notification does not mean that a Close frame reached the peer or that the handler, business work, or spawned tasks have finished. Work that must continue should take ownership of its required input and be managed separately by the application.

### Separate inbound and write deadlines

`idle_timeout` retains its historical combined behavior: it sets the inbound frame wait, inbound channel-delivery wait, `WsSession::recv()` wait, and ordinary write deadline. For independent values, call `idle_timeout` first and `write_timeout` second. The later setter wins for each field it changes; calling `idle_timeout` after `write_timeout` restores the write limit to the idle value. Both default to 60 seconds and must be positive and representable as Tokio deadlines. Close handshakes continue to use the shared shutdown deadline. These are cooperative operation-wait limits: they do not measure byte-level progress or stop non-yielding synchronous work, `spawn_blocking` jobs, or application-owned tasks.

```rust
use std::time::Duration;
use qubit_web::WsUpgradePolicy;

let policy = WsUpgradePolicy::new()
    .idle_timeout(Duration::from_secs(600))
    .write_timeout(Duration::from_secs(10));
policy.validate().expect("valid WebSocket policy");
```

This order gives inbound operations ten minutes and ordinary writes ten seconds. `Duration::ZERO` returns `WsPolicyError::ZeroLimit`; an unrepresentable write deadline returns `WsPolicyError::WriteTimeoutTooLarge`. Adding that enum variant requires a new arm in exhaustive `WsPolicyError` matches. The older `idle_timeout` calls and their combined behavior remain source-compatible.

### Read the final termination reason

Use `closed()` to wait until both managed I/O tasks have released their connection permit and session registration, then read the stable snapshot:

```rust
let ended = session.closed();
ended.await;
let cause = session.termination();
```

The snapshot is `None` before publication and `Some(...)` after `closed()` resolves. The first cause recorded wins; simultaneous causes are ordered by whichever records under the state lock first. `PeerClose` means the reader observed a peer Close frame; it does not prove the close handshake completed or that the frame was delivered. `IoEnded` is a fallback and does not mean success. Existing `recv()` behavior is retained, including one-time delivery of a read error. Close frames with code 1001 use `idle timeout` for an idle deadline, `server shutdown` for server shutdown, and `session closed` for local close/handler cleanup.

`WsUpgradePolicy::validate()` checks the final configured field values, so a later setter value replaces an earlier invalid value for validation. The valid `max_connections` and `inbound_queue_limits` message-count ranges are `1..=tokio::sync::Semaphore::MAX_PERMITS`; zero returns `WsPolicyError::ZeroLimit`, while an oversized connection limit returns `WsPolicyError::ConnectionLimitTooLarge` and an oversized inbound message count returns `WsPolicyError::InboundQueueLimitTooLarge`. The inbound byte budget must also be positive. An idle timeout that cannot form a Tokio deadline returns `WsPolicyError::IdleTimeoutTooLarge`; the corresponding write limit returns `WsPolicyError::WriteTimeoutTooLarge`. Callers that exhaustively match `WsPolicyError` should add branches for `InboundQueueLimitTooLarge`, `IdleTimeoutTooLarge`, and `WriteTimeoutTooLarge`, or use a fallback branch. An invalid policy is rejected by `on_upgrade` with HTTP 500 and cannot upgrade. For example, correcting a zero value before validation succeeds:

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
| The accept loop stops after a socket error | `InvalidInput`, `PermissionDenied`, `Unsupported`, and `NotConnected` are terminal. Other I/O errors get at most eight consecutive retries with delays of 10, 20, 40, 80, 160, 320, 640, and 1,000 ms (2.27 seconds total); the ninth consecutive error terminates serving. A successful accept resets the retry count. Inspect the returned `WebServerError::ServeFailed` source for the underlying socket error. |
| A request exceeds the body cap but succeeds | For a Controller `short` route, confirm its handler/extractor consumes the body and check whether `with_http_limits` replaced the default with a larger budget. For a native Axum route, install `RequestLimitLayer` on that branch and confirm the body is consumed. |
| A slow client does not immediately observe a short-response timeout | After response headers are sent, expiry releases the application body and short-request permit but can only return the body error when the transport polls again. The opt-in body diagnostic event is emitted at expiry even without another poll. It does not change the response status or close the connection; transport connections use their separate policy. |
| SSE closes at the ordinary request deadline | Remove `RequestLimitLayer` from that streaming branch and use `SseConnectionPolicy` limits. |
| `active_sessions()` stays at zero for a WS | Call `WsUpgradePolicy::on_upgrade` with the server's `ServerContext`; native Axum upgrades are outside managed-session reporting. |
| `WsUpgradePolicy::validate()` reports an invalid connection or inbound queue limit | Check the final `max_connections` and `inbound_queue_limits` values. Zero limits return `WsPolicyError::ZeroLimit`; an oversized connection count returns `ConnectionLimitTooLarge`; an inbound message count above `tokio::sync::Semaphore::MAX_PERMITS` returns `InboundQueueLimitTooLarge`. The inbound byte budget must be positive. Correct the setting and validate again; an invalid policy cannot upgrade. |
| A WebSocket closes with code `1013` under inbound load | The application may not be calling `WsSession::recv()` fast enough, or the aggregate inbound payload budget may be exhausted. The defaults are 64 queued messages and 1 MiB per connection; a full channel waits up to `idle_timeout`, while insufficient byte budget closes immediately. Use `inbound_queue_limits` to size the byte budget, keep receiving, and move processing to application-managed work after receipt. |
| Configuration is rejected | Inspect `ConfigOptionsError::field()`; check address, positive timeout/limit values, concurrency capacities within `1..=tokio::sync::Semaphore::MAX_PERMITS`, and integer types. The capacity errors identify `http.max_concurrent_requests` or `transport.max_connections`; error text intentionally does not include rejected values. |
| HTTPS or WSS cannot connect | Check certificate chain/key pairing and client trust. A TLS failure occurs before HTTP routing. |
| Shutdown exceeds expectations | Check the configured shutdown timeout and whether application handlers or stream producers respond to cancellation. |

`WebServerError` exposes retained socket errors through `std::error::Error::source()`. Its default `Display` and `Debug` intentionally show only the error category; inspect `source()` explicitly when diagnosing bind or accept failures.

See the [Chinese guide](user_guide.zh_CN.md), [README](../README.md), [API documentation](https://docs.rs/qubit-web), and [documentation index](README.md) for related guidance and investigation records.

## Deadlines and peer address

Timeouts in this crate use cooperative Tokio cancellation. A synchronous operation that does not yield can delay timeout checks and cleanup, and a handler may finish successfully after its deadline. Cancelling a wait does not stop an already running `spawn_blocking` task or application-spawned work. Byte and structure limits on JSON do not bound the CPU time of user `Serialize` or `Deserialize` implementations. Run CPU-heavy or blocking work with an application-owned concurrency budget on suitable execution resources; use process isolation when work must be forcibly terminable. The application owns cancellation cooperation, side effects, and detached work.

Each HTTP and HTTPS request carries Axum `ConnectInfo<SocketAddr>` with the accepted TCP peer; HTTP/2 streams on one connection share it. A reverse proxy will appear as the peer. Forwarded headers are not trusted or converted, and this address is not an authenticated user identity. Resolve forwarded addresses only under an application-configured proxy trust policy. A Router called directly with `oneshot` has no transport peer, so add one explicitly in such tests:

```rust
let mut request = axum::http::Request::new(axum::body::Body::empty());
let peer: std::net::SocketAddr = "127.0.0.1:12345".parse().unwrap();
request.extensions_mut().insert(axum::extract::ConnectInfo(peer));
```

A handler can extract the value with `async fn handler(axum::extract::ConnectInfo(peer): axum::extract::ConnectInfo<std::net::SocketAddr>) -> String { peer.to_string() }`.
