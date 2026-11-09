# Request and session lifecycle design

[简体中文版本](lifecycle-design.zh_CN.md) · [Documentation index](README.md)

This is a maintainer map of the current implementation. It describes the code and tested contracts; it does not add runtime guarantees. For setup and configuration, see the [English user guide](user_guide.md) and [Chinese user guide](user_guide.zh_CN.md).

## Modules and capacity domains

| Domain | Owner and scope | What is counted | Boundary |
| --- | --- | --- | --- |
| Transport connection | `WebServer` accept/serve path and per-connection `ConnectionActivity` | Accepted transport connections and active request/body activity | A request deadline does not itself close a transport. An activity lease can remain until body completion or drop. |
| Short HTTP request | `LimitState` shared by a `RequestLimitLayer` branch | Concurrent handler plus response-body work; request data bytes; one absolute request deadline | Independently constructed layers have independent state. Cloned state shares capacity. Defaults: 1 MiB body, 256 concurrent requests, 30 seconds (`HttpLimits`). |
| Stream admission | `StreamAdmissionState` shared by `StreamAdmissionLayer` branches | Concurrent request-body buffering and handler work through response headers | Its permit ends after admission, not when the SSE/WS session ends. Defaults: 1 MiB, 256 admissions, 10 seconds (`StreamAdmissionLimits`). |
| SSE policy | One `SseConnectionPolicy` state | Active admitted SSE connections, plus a server session registration per connection | Policy connection capacity and server session count are different counters. |
| WebSocket policy | One `WsUpgradePolicy` state | Active admitted WebSocket sessions; each session also has one server session registration | Shared reader/writer references retain resources until both I/O sides release them. |
| Server tasks | `ConnectionTaskExecutor`, `ConnectionCompletion`, and `SessionTracker` in one server context | Registered Hyper tasks, transport completion, and managed sessions | These are server lifecycle counters, not application-wide task tracking. |
| Application tasks | Tasks created by application code | Not tracked by rs-web unless explicitly registered through its managed session APIs | Request cancellation cannot stop arbitrary spawned or blocking work. |

## Resource ownership

| Resource | Acquired at | Owner while active | Normal release | Error, drop, timeout, or shutdown release | Evidence |
| --- | --- | --- | --- | --- | --- |
| Short-request semaphore permit | `RequestLimitLayer::enforce_limits` after declared-length check | `PermitBody` state after handler response is produced; handler future before that | Body EOF/end-of-stream, including final data frame | Handler timeout drops processing; body error, explicit body drop, or `watch_deadline` takes it. Body and permit are dropped outside the state lock. | `src/limit/request_limit_layer.rs`, `src/limit/internal/permit_body.rs`; `tests/http_limits_tests.rs` |
| Application response body | Handler response wrapping | `PermitBody` state | EOF/end-of-stream | Body error, wrapper drop, or deadline watcher; timeout yields one body read error if polled later. A response status already sent cannot be changed. | `src/limit/internal/permit_body.rs`; `tests/http_limits_tests.rs::unpolled_response_body_deadline_releases_capacity_and_drops_source` |
| Connection activity lease | Request accepted on a connection | `ActivityBody` for a non-empty response body | Inner body reports end, including after a final frame, or reaches EOF | Body error, wrapper drop, or connection teardown | `src/server/internal/connection_activity.rs`, `activity_lease.rs`, `activity_body.rs`; `tests/http_server_tests.rs` |
| Admission permit and buffered body | `StreamAdmissionLayer` before request body collection | Admission future through response headers | Handler response headers are ready | Body read/rejection, timeout, handler error, or future cancellation drops the permit and buffer | `src/limit/stream_admission_layer.rs`, `stream_admission_state.rs`; `tests/stream_admission_tests.rs`, `tests/stream_admission_network_tests.rs` |
| SSE policy slot and server session guard | Successful `SseConnectionPolicy` admission | `SseConnectionGuard` attached to `SessionStream` | Source returns `None` | Stream error followed by body drop, response/body drop, or server cancellation/drop releases the guard and cancels producer token | `src/sse/internal/session_stream.rs`, `sse_connection_guard.rs`; `tests/sse_lifecycle_tests.rs` |
| WebSocket policy permit and server session guard | Successful upgrade admission and session registration | Shared `SessionResources` held by session reader/writer | Both I/O task references end | I/O termination, peer close, task exit, or shutdown; final reference drop releases both resources, finalizes termination, then signals closed | `src/ws/internal/session_resources.rs`, `reader.rs`, `writer.rs`, `session_lifecycle.rs`; `tests/websocket_tests.rs`, `src/ws/internal/writer/tests.rs` |
| Hyper connection tasks | Hyper invokes `ConnectionTaskExecutor::spawn` | Server-owned `JoinSet` and tracked future | Future completes and is joined/reaped | Shutdown closes admission, waits to deadline, aborts unfinished tasks as needed, then joins them | `src/server/internal/connection_task_executor.rs`, `finish_serve.rs`; unit tests in those modules |

A successful `StreamAdmissionLayer` admission only reserves resources while the request is buffered and response headers are constructed. It is not the same reservation as a long-lived session guard. The response being accepted by Axum or queued to a transport also does not prove that a peer received its bytes.

## Short response state transitions

| Current state | Trigger | Next state | Body / permit action | Observable result |
| --- | --- | --- | --- | --- |
| `Active` | final frame or EOF | `Finished` | Take and drop body and permit; cancel watcher | Frame may be returned, then stream ends; terminal diagnostic may be `complete` when body observation is active. |
| `Active` | body error | `Finished` | Take and drop body and permit; cancel watcher | Original body error is returned. |
| `Active` | wrapper drop | `Finished` | Take and drop body and permit; cancel watcher | No further body poll is possible. |
| `Active` | deadline watcher wins | `TimedOut` | Take and drop body and permit, then wake registered body waker | Permit is recovered even if transport is no longer polling. |
| `TimedOut` | next body poll | `Finished` | Watcher already took body and permit; cancel watcher if still present | One timeout error is returned; subsequent polls yield EOF. |
| `Active` | poll observes deadline first | `Finished` after timeout transition | Poll path takes/drops resources and cancels watcher | One timeout error is returned. |

`PermitBody` and `watch_deadline` coordinate through `BodyState` under a mutex, so the first terminal transition owns the resources. Destructors, permit release, wake, and diagnostic emission are performed after releasing the body-state lock. The deadline spans handler work and response streaming, but is cooperative: non-yielding synchronous work can delay observation, and spawned or `spawn_blocking` application work is outside cancellation control.

## SSE and WebSocket lifecycles

SSE first passes request admission, then `SseConnectionPolicy` reserves policy capacity and registers a server session. `SessionStream` owns the application stream and `SseConnectionGuard`; natural stream completion or dropping the response releases the guard and cancels the producer token. The ordinary short-request deadline must not wrap a long-lived SSE route. Producer cancellation is a signal; application producer code must observe it.

WebSocket upgrade admission likewise does not end with request-body admission. The policy reserves a session slot and the server registers a managed session. `SessionResources` shares the policy permit and session guard across I/O work. The reader processes peer frames and close state; the writer drains the outbound queue and performs sink sends/flushes. Each ordinary outbound send and notification-triggered flush has its own absolute `write_timeout` deadline; inbound activity and unrelated notifications do not reset it. `idle_timeout` sets both inbound waits and ordinary write deadlines. A later `write_timeout` changes only the write deadline; a later `idle_timeout` sets both values again. There is no separate inbound-only timeout setter. Close processing uses its separate close deadline, so the ordinary write deadline is not a deadline for every Close operation. See the [user guide's separate inbound and write deadlines](user_guide.md#separate-inbound-and-write-deadlines) for configuration details. The final I/O resource reference releases capacity and session registration. Queue insertion success means the item entered the local queue; it does not mean a socket write completed or the peer received it. A peer Close, writer completion, server cancellation, and queue closure are distinct lifecycle events; see `src/ws/internal/reader.rs`, `writer.rs`, `close.rs`, and `session_lifecycle.rs`.

## Server shutdown sequence

`finish_serve` drops the listener, asks `ServerContext` to begin shutdown and establish the shared absolute deadline, and closes managed Hyper-task registration. It concurrently waits for Hyper graceful shutdown and registered sessions, and waits for tracked Hyper tasks against that same deadline. At the deadline it snapshots unfinished sessions and transport completion, determines the `graceful` result from actual completion by that deadline, aborts unfinished connection tasks when needed, and always joins the connection tasks. `SessionTracker::begin_shutdown` closes managed-session admission; registered guards release their count on drop.

`ShutdownReport::forced_connections` is currently `None`: `finish_serve` does not produce a forced-connection count. A graceful result means the tracked transport, managed-session, and Hyper-task conditions completed by the deadline; it does not establish delivery of every response byte. Shutdown cancellation is cooperative for application session code. Arbitrary application-spawned tasks are not automatically registered, cancelled, or joined by the server.

## Diagnostic semantics

`BodyObservation` records response status and first body terminal outcome independently, then claims and emits a single event when both are known. The short-body terminal labels include `complete`, `dropped`, and `timeout`; request byte fields such as parsed `Content-Length` describe declared length, not measured bytes or limit accounting. `complete` means the observed body ended in the wrapper, not that a client received it. `dropped` means the wrapper was dropped, not that a client disconnected. A timeout after headers cannot revise the already-produced status. Connection and task diagnostics describe their corresponding local lifecycle observations.

See `src/diagnostic/internal/body_observation.rs`, `body_observation_state.rs`, `body_termination.rs`, and `tests/diagnostic_tests.rs`.

## Tests and historical evidence

| Evidence | What it covers | Boundary |
| --- | --- | --- |
| `tests/http_limits_tests.rs` | In-process body/permit completion, drop, timeout and capacity behavior | Does not exercise transport delivery. |
| `tests/sse_lifecycle_tests.rs`, `tests/websocket_tests.rs`, `src/ws/internal/writer/tests.rs` | Managed session, stream, reader/writer and close behavior in their fixtures | Assertions apply to the constructed test lifecycle. |
| `tests/http_server_tests.rs`, `tests/shutdown_tests.rs`, `tests/transport_diagnostic_tests.rs` | Local server, transport activity and shutdown outcomes | Do not imply all network peers or scheduling conditions behave identically. |
| [Backpressure/request-timeout investigation](backpressure-timeout-investigation-20261104.md) | Locked-stack HTTP/2 flow-control observation | Historical bounded loopback experiment; application timeout did not itself close the transport. |
| [Close lifecycle investigation](close-lifecycle-investigation-20261104.md) | WebSocket close handling and prior repair evidence | Conclusions are tied to the recorded source baseline and tests. |
| [HTTP/2 resource investigation](http2-resource-investigation-20261103.md) | Incomplete-header behavior and bounded experiments | Does not establish a general header deadline or unbounded-duration behavior. |
| [Stream admission buffer investigation](stream-admission-buffer-investigation-20261106.md) | Synthetic ownership and local HTTP/1/HTTP/2 fragmented-body tests | Does not quantify production RSS or remote exploitability. |
| [WebSocket write-stall investigation](ws-write-stall-investigation-20261107.md) | Recorded network reproduction and repair evidence | This historical experiment used the then-current `idle_timeout` configuration and is bounded to its recorded host/harness; it is not a result for the current independent `write_timeout` configuration. See the [user guide's separate inbound and write deadlines](user_guide.md#separate-inbound-and-write-deadlines) for current ordinary-write behavior. |
| [Connection activity investigation](connection-activity-investigation-20261108.md) | Local body lease regression and transport controls | Local count evidence is distinct from network-visible protocol failure. |
| [Trailers budget investigation](trailers-budget-investigation-20261108.md) | Locked HTTP/1 and HTTP/2 trailer parser observations | Tested shapes and locked dependency versions only; oversized HTTP/2 fields may be omitted. |

Historical commands and outcomes in these reports are not results of the current checkout. Consult each record's baseline and limitations before using it as evidence for a later revision.

## Runtime and evidence limits

Deadlines use Tokio monotonic time and are observed when the relevant task or body poll is scheduled. They do not preempt synchronous work. A request timeout releases the application response body and short-request permit, but cannot rewrite sent headers or guarantee transport closure. Stream-admission limits cover buffering through response headers, not long-lived sessions. SSE/WS capacity is held by session lifecycle resources. Shutdown waits only for the transport tasks, sessions, and managed Hyper tasks described above; it cannot guarantee arbitrary application work termination or peer delivery. Investigation results are finite observations under their recorded versions, platform, and harness, not universal protocol guarantees.
