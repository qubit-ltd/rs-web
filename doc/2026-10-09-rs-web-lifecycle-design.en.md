# rs-web lifecycle and resource boundary design

> Date: 2026-10-09. This summary describes the current `qubit-web` 0.2.0 contract. For operational steps, see the [English user guide](user_guide.md) and [Chinese user guide](user_guide.zh_CN.md). The [2026-10-08 design record](2026-10-08-rs-web-design.md) is historical and includes superseded API proposals.

## Managed session admission and shutdown

`ServerContext::try_register_session()` atomically reserves a managed session unless shutdown has begun. `active_sessions()` includes managed SSE connections and WebSocket upgrades admitted through `WsUpgradePolicy::on_upgrade`, even while the upgrade response is pending. SSE and WS admission after shutdown begins is rejected. Ordinary HTTP requests, native Axum upgrades, and arbitrary application background tasks are not managed sessions.

The server stops accepting connections, closes session admission, and notifies active sessions when shutdown starts. It waits for HTTP connection tasks and managed sessions against one absolute deadline. `ShutdownReport::graceful` is true only when both groups finish before that deadline. `unfinished_managed_sessions` records the managed session count at the deadline. The report does not claim that arbitrary application work has stopped. `forced_connections` remains `None` because Axum does not expose a provable count.

## Transport idle timeout

`ServerOptions::transport_idle_timeout` defaults to 30 seconds and must be nonzero. Configuration uses `transport.idle_timeout_ms`, also defaulting to `30000`. A TCP/TLS transport with no active request handler or response body is closed after this idle interval, including a connection that never sends its first HTTP/2 request. Active handlers and bodies pause the idle timer, so a long-lived SSE response is not closed as idle. HTTP/2 PING frames do not reset request activity. The separate `request_header_timeout` remains 10 seconds for HTTP/1 headers and TLS handshake; an idle timeout does not bound a stalled active request.

## Origin and application authentication

`WsUpgradePolicy::new()` permits a request with no Origin to proceed to application authentication. If a request includes Origin, the default policy rejects it; configure `allowed_origins` to accept exact values. Applications remain responsible for authenticating before calling `on_upgrade`. Origin validation is not authentication.

## I/O error boundary and API migration

`WebServerError` retains socket bind, local-address, and accept failures as `io::Error` sources. Callers can inspect `std::error::Error::source()` for diagnostics. Default `Display` and `Debug` output omit the underlying OS error text; TLS and configuration errors do not expose potentially sensitive source chains.

Version 0.2.0 has intentional breaking changes: token-only WebSocket upgrades are removed in favor of `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`, `WebServerError` is no longer a copyable source-free enum, and SSE admission distinguishes capacity exhaustion from shutdown rejection.
