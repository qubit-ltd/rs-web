# rs-web architecture overview

> Date: 2026-10-09. This document describes the current `qubit-web` 0.2.0 module layout. See the [lifecycle and resource boundary summary](2026-10-09-rs-web-lifecycle-design.en.md) for shutdown and session contracts, and the [English user guide](user_guide.md) for integration steps. The [2026-10-08 design record](2026-10-08-rs-web-design.md) is historical.

## Request path and module map

`WebServer` owns the bound TCP listener, per-server transport policy, and shared `ServerContext`. It accepts and caps transport connections, optionally performs TLS negotiation, and adapts each stream into Hyper with upgrade support. Hyper dispatches requests through the application's Axum `Router<()>`.

Applications can construct that router directly or use `ControllerRoutes` with the controller macros. Controller routes receive method/path conflict checks and route metadata. Controller `short` routes receive the builder's default request budget; native Axum routes receive no such budget unless the application installs a layer on selected branches. Limits are scoped to those routes and are not a server-wide policy. Diagnostic middleware logs allowlisted request metadata: method, matched route template, response status, elapsed time, connection ID, and optional declared request size.

The main module responsibilities are:

| Module | Responsibility |
| --- | --- |
| `server` | Bind HTTP/HTTPS listeners, run the Axum router, cap transport connections, coordinate shutdown, and report shutdown results. |
| `mvc` | Assemble controller routes, reject duplicate method/path declarations, and expose route metadata. |
| `limit` | Provide body, concurrency, and time budgets for ordinary short requests. Controller short routes receive defaults; native Axum branches opt in by installing a layer. |
| `json` | Enforce strict JSON input/output byte and structural limits when the `json` feature is enabled. |
| `sse` | Admit bounded server-sent event streams and track them as managed sessions. |
| `ws` | Validate WebSocket policy and Origin, reserve capacity, and manage inbound/outbound session I/O when the `ws` feature is enabled. |
| `config` | Parse explicit configuration values into listener options and separate HTTP route limits when the `config` feature is enabled. |
| `tls` | Load or wrap rustls configuration for HTTPS when `tls-rustls` is enabled. |
| `diagnostic`, `error`, `route_kind` | Supply request diagnostics, stable public errors, and shared route classification. |

## Capacity domains and defaults

Each limit protects a specific resource. Its configured count must not be read as a single service-wide concurrency ceiling.

| Domain | Default | Scope and overload behavior |
| --- | --- | --- |
| Accepted transport connections | 1,024 per `WebServer` | Includes TLS handshakes and HTTP connection futures. When full, the accept loop stops accepting; clients may wait in the OS backlog or fail to connect. No HTTP 503 is generated. |
| Controller short requests | 256 in flight per `ControllerRoutes` builder | `ControllerRoutes::new()` applies `HttpLimits::default()`: 1 MiB per body and a 30-second handler/response-body deadline as well. `with_http_limits` replaces these defaults and should be called before adding controllers. Each builder has its own shared state. Native Axum branches require an explicit `RequestLimitLayer`; `WebServer` does not install route limits. Full capacity returns a capacity rejection. |
| SSE connections | 128 per `SseConnectionPolicy` by default | `new`/`default` policies create independent capacity domains; clones share their domain. Exhaustion is reported separately from shutdown rejection. |
| WebSocket connections | 128 per `WsUpgradePolicy` by default | `new`/`default` policies create independent capacity domains; clones share their domain. Failed admission produces a rejection before the session is managed. |
| Managed server sessions | No independent numeric cap | `ServerContext` tracks admitted SSE and WebSocket sessions for shutdown accounting. It is not a global connection limit and does not include ordinary requests, native Axum upgrades, or application background jobs. |

To share an SSE or WebSocket capacity across routes, create one policy at application startup, put it in application state, and pass its clones to handlers. Multiple separately created policies multiply the effective aggregate capacity. The same planning principle applies to short-request branches: configure and share the intended branch state explicitly.

## Request budgets and long-lived streams

`HttpLimits::default()` allows 1 MiB per request body, 256 concurrent in-flight short requests, and 30 seconds for handler work plus response-body streaming. `ControllerRoutes::new()` applies this policy to Controller `short` routes by default; `with_http_limits` replaces it for routes added afterward. Controller `sse` and `ws` routes skip the short-request layer. Native Axum branches require an explicit `RequestLimitLayer`, because `WebServer` serves the Router as supplied and does not add route limits. The request body is counted as the selected route consumes it; the middleware does not drain an otherwise unread body just to enforce the budget. Capacity exhaustion, an oversized body, and a request deadline have stable rejection outcomes. If a deadline expires after response headers are sent, the response body terminates with an error.

When enabled, `JsonLimits::default()` independently bounds input and output payloads to 1 MiB, nesting depth to 64, value nodes to 100,000, array items and object entries to 10,000 each, key bytes to 16 KiB, string bytes to 256 KiB, and number bytes to 128. JSON limits are separate from the generic HTTP body budget; configure compatible values when both apply.

At the input stage, `BoundedJson` rejects an oversized body or decode budget through `JsonRejection::BudgetExceeded` (`413`); malformed JSON and unsupported media types have distinct extractor rejections. At the output stage, `json_response` encodes the complete response before returning it and reports output-budget or serialization failures as `JsonResponseError` (`json_output_budget_exceeded` or `json_encode_failed`). The application must handle extractor rejections and the returned response-encoding error; the library does not choose an application-specific fallback response.

SSE and WebSocket routes are long-lived and must not be put behind short-request concurrency/deadline middleware. SSE keep-alives and WebSocket frame/message/queue limits belong to their respective policy modules. WebSocket's inbound application channel holds 64 messages. The [lifecycle summary](2026-10-09-rs-web-lifecycle-design.en.md) documents session shutdown, queue closure, inbound backpressure, and Origin behavior.

## Timeouts, shutdown, and configuration

`ServerOptions` defaults to a 10-second HTTP/1 header and TLS handshake timeout, a 30-second idle transport timeout, and a 30-second graceful shutdown deadline. The idle timeout closes a TCP/TLS connection only while no request handler or response body is active; it does not bound a stalled active request. Long-lived response bodies therefore pause transport-idle expiry.

On shutdown, the server stops accepting sockets, closes managed-session admission, signals active sessions, and waits for HTTP connection tasks and managed SSE/WS sessions against one absolute deadline. `ShutdownReport::graceful` is true only if both groups finish in time. The report records unfinished managed sessions; it cannot claim arbitrary application background work has stopped. `forced_connections` is `None` because Axum does not expose a provable count.

`ConfiguredWeb` parses an explicit configuration object. It returns `ServerOptions` separately from `HttpLimits`. Controller short routes already use `HttpLimits::default()`; pass the configured limits to `with_http_limits` to replace that policy. For native Axum routes, install `RequestLimitLayer` on selected short branches. `WebServer` does not modify the Router. With `config`, transport defaults are `transport.max_connections = 1024` and `transport.idle_timeout_ms = 30000`; HTTP policy keys include `http.max_body_bytes`, `http.max_concurrent_requests`, and `http.request_timeout_ms`. TLS is optional and configured separately through `TlsConfig`; PEM load failures are reduced to `TlsInvalid` without exposing key material or paths.

## Failure stages and application responsibilities

| Stage | Library behavior | Application responsibility |
| --- | --- | --- |
| Startup and bind | Validates options, binds the socket, queries its local address, and validates TLS configuration before serving. Public errors distinguish invalid configuration, bind/address failures, TLS errors, and serve/accept failures. | Supply a valid address and positive timeouts/capacity; load valid certificates and private keys; decide how startup errors are surfaced. |
| Route assembly | Detects duplicate controller method/path declarations. Request-policy middleware returns stable body-size, capacity, and timeout rejections on Controller short routes and on native branches where installed. | Assemble intended routes, replace Controller defaults when needed, install limits on appropriate native branches, and translate public rejections into application behavior where needed. |
| SSE admission | Reserves policy capacity and a managed server session before creating the stream. Capacity exhaustion and shutdown rejection are distinct. | Choose shared policy state; implement event IDs, replay, and event buffering; handle client reconnects. |
| WebSocket admission and I/O | Checks policy and Origin, reserves policy/server-session capacity, and manages reader/writer lifecycle. It bounds frames, messages, queues, and inbound delivery. | Authenticate before `on_upgrade`, configure allowed Origins, keep calling `WsSession::recv()`, and handle application-level processing and delivery semantics. Origin is not authentication. |
| Shutdown | Uses one deadline for HTTP connections and managed sessions; reports remaining managed sessions. | Stop or supervise application background jobs and decide what to do when the report is not graceful. Native Axum upgrades and arbitrary jobs are outside managed-session accounting. |

The [lifecycle summary](2026-10-09-rs-web-lifecycle-design.en.md) is the concise reference for current session and shutdown guarantees; the [user guide](user_guide.md) covers concrete integration.
