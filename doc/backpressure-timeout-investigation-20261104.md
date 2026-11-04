# WEB-RE03: Backpressure and request-timeout investigation

## Ongoing diagnostic entry point (WEB-RE01)

The [Transport backpressure diagnostics workflow](../.github/workflows/transport-diagnostics.yml) runs every Tuesday at 03:37 UTC and can also be started manually from GitHub Actions. It runs the three ignored scenarios serially, prints the checked-out revision, Rust/Cargo versions, and `Cargo.lock`, and uploads the complete output as a 14-day artifact. A failing test makes the workflow fail; if checkout or local dependency preparation fails before the test step, the summary reports that no diagnostic log was produced. Review the workflow log and artifact to distinguish setup failures from scenario failures, then investigate the recorded revision and locked dependencies. These timing-sensitive loopback experiments are diagnostic signals, not performance thresholds or release gates.

The local equivalent is `cargo test -p qubit-web --locked --test transport_backpressure_tests -- --ignored --nocapture --test-threads=1`.

Date: 2026-11-04. Locked HTTP/2 dependency versions are recorded below.

## Static lifecycle evidence

The existing source currently separates three resources:

1. `src/limit/internal/permit_body.rs::watch_deadline` moves an active request body to `TimedOut`, takes and drops the application body and short-request semaphore permit, then wakes the last registered body waker.
2. `src/server/internal/activity_body.rs::poll_frame` holds the per-request connection activity lease until the wrapped body returns EOF/error; dropping the wrapper also drops its optional lease.
3. `src/server/internal/connection_activity.rs::wait_until_idle_for` only starts its idle interval when the active request count reaches zero. A retained lease therefore keeps that connection from being considered idle.

The bounded HTTP/2 experiment below confirms temporary divergence under a zero stream window: the application body and short-request permit are released at deadline, while a second connection cannot reach its health handler until the original response stream is reset and the idle connection is reaped. This observation is finite and matches the current contract that an application deadline does not itself guarantee transport closure.

## Event timeline

Latest successful event timeline, relative to test start (`t0`):

| Event | Relative time | Evidence |
| --- | ---: | --- |
| Zero-window SETTINGS acknowledged | 1.229 ms | server ACK received before request was sent |
| Application response body created | 42.930 ms | creation notification |
| Response headers received | 42.979 ms | HEADERS frame received; no WINDOW_UPDATE sent |
| DATA withheld, then body poll observed | 144.212–144.231 ms | no DATA for 100 ms; one poll produced 1,024 bytes; total under 1 MiB |
| Deadline disposed application producer | 246.296 ms | body-drop notification after configured 200 ms request deadline |
| Second-connection health handler before reset | 547.050 ms | no HEADERS in 300 ms observation; TCP connect alone not counted |
| Same-connection health handler | 547.893 ms | response HEADERS after application deadline, proving short-request permit recovery |
| RST_STREAM sent, original TCP connection kept open | 548.386 ms | stream reset action |
| Second-connection health response after reset/idle reap | 850.581 ms | HEADERS received about 302 ms after RST_STREAM; transport permit recovered |
| SSE control | passed | separate ignored case; SSE outside request deadline, DATA observed after health response completion and again after 250 ms |
| Tasks and server cleanup | within 1.15 s total run | all test-owned server tasks joined; drop guard aborts on timeout/panic |

## Validity and platform limits

A valid paused-window phase must show (a) zero stream credit sent before the response is produced, (b) response headers arriving, (c) the body source being polled and producing data before the 1 MiB cap, (d) no DATA arriving and no WINDOW_UPDATE being sent during the pause. The final run satisfies these checks. Normal consumption and explicit connection-plus-stream WINDOW_UPDATE resume controls passed. An RST_STREAM while keeping the original TCP connection open led to transport recovery after the configured idle interval. A separate peer-disconnect control passed. The SSE route is deliberately outside `RequestLimitLayer`; its DATA continued after the short health response completed and after an additional 250 ms, beyond the 200 ms short-request deadline.

OS scheduling, kernel socket buffering, Tokio scheduling, and Hyper's internal polling can shift observed events. These timings are loopback observations on this locked dependency set, not cross-platform guarantees. The diagnostic remains ignored rather than a routine timing-sensitive CI gate. Each scenario has a 10 s outer bound, each observation wait is at most 3 s, and server work is joined or aborted during cleanup.

## Existing test evidence and limits

- `tests/http_limits_tests.rs::unpolled_response_body_deadline_releases_capacity_and_drops_source` is deterministic body-layer evidence for application producer and short-request permit disposal. It does not exercise a network transport or connection lease.
- `tests/http_server_tests.rs` has HTTP/2 active-handler / active-SSE idle-timeout tests and transport-capacity recovery controls. These establish current idle behavior for their tested states, not for a response timed out behind HTTP/2 flow control.
- `tests/http2_resource_tests.rs` has a raw HTTP/2 preface/frame helper and is the closest protocol fixture. Its current investigation concerns incomplete request headers; it does not establish response-side backpressure behavior.

## Current decision

**Design branch 2: expected finite resource retention; keep production implementation.** At the 200 ms application deadline, the producer was dropped and the single short-request permit became available (same-connection health handler entered). The transport permit remained occupied while the response DATA stream was flow-control blocked, as expected because request timeout does not promise transport closure. After RST_STREAM, while the original TCP connection remained open, the lease/idle lifecycle allowed the server to reap that connection after approximately the configured 300 ms idle interval; the pending second connection then reached its health handler. Full peer disconnect also released capacity. No persistent leak or activity-count mismatch was reproduced, so no runtime or public documentation change is warranted.

The experiment test file is ignored by default, so it is diagnostic coverage only. Current validation commands and results are listed in the run log below. The old invalid harness attempts are retained there and are not included in this conclusion.

## WEB-RE03 run update (2026-11-04)

### Options and versions

The first valid run used `max_transport_connections=1`, short request concurrency `1`, request timeout `200 ms`, and transport idle timeout `300 ms`. The response source was capped at `1 MiB`, emitting 1,024-byte chunks. The first client sent an HTTP/2 preface and `SETTINGS_INITIAL_WINDOW_SIZE=0` before its request; the client read response headers and sent no stream WINDOW_UPDATE during the pause. Individual waits were capped at `3 s`; the diagnostic total was capped at `10 s`. The target is an ignored diagnostic because its result depends on transport scheduling. Locked versions from `Cargo.lock`: axum `0.8.9`, h2 `0.4.20`, hyper `1.12.0`, tokio `1.54.1`; `rust-toolchain.toml` pins Rust `1.94.0`.

### Commands and output

- `cargo test --locked --test transport_backpressure_tests -- --nocapture` — exit `0`; one test discovered and ignored by default. Initial compile had an unused shutdown-report warning; this was fixed by asserting graceful shutdown.
- First `cargo test --locked --test transport_backpressure_tests -- --ignored --nocapture` — exit `101` after `3.25 s`, panic `HTTP/2 settings deadline: Elapsed(())`. Harness setup error: it waited for server SETTINGS on a second connection while the first connection held the sole transport permit. No lifecycle phase ran in that attempt. The second-connection probe now sends the HTTP/2 preface/settings and health request without waiting for server SETTINGS, and only response HEADERS count as handler entry.
- Second `cargo test --locked --test transport_backpressure_tests -- --ignored --nocapture` — exit `0`, runtime `0.85 s`. Output: `t+545.427107ms: second-connection health response before reset=false`; `body polls=1 produced_bytes=1024 health_before_reset=false health_after_reset=true`. The test observed response headers, no DATA over a 100 ms zero-window observation, application producer drop at the configured deadline, no health response before reset, and a health response after RST_STREAM plus closing the original peer connection. The test joined server shutdown successfully.

Historical first-run finding: the client advertised a zero stream window before sending the request, response headers arrived, the application body was polled and produced 1,024 bytes, no DATA was received, and no WINDOW_UPDATE was sent. It supported finite retention, but that run did not yet distinguish RST_STREAM from peer disconnect or complete the other controls. The final run below resolves those remaining questions.

### Historical initial run precision

The first successful run did not print monotonic timestamps for body creation, headers, body polling, or producer drop. Those exact earlier timestamps are unavailable and are not inferred. Later control runs below added per-event timestamps and completed the normal, resume, SSE, reset, and disconnect controls; the current decision is in “Current decision” and “Final run” below.

The diagnostic remains ignored under the regular test command; an ignored test result must not be read as ordinary suite coverage. Host scheduling and Hyper's poll behavior bound generalization: this is a local loopback observation on the locked dependency set, not a cross-platform timing guarantee.

### Control-run update

A later ignored run used the timestamp-instrumented harness. The `zero_window_timeout_and_reset_recover_transport_capacity` case passed with these captured relative times: client preface/zero-window SETTINGS sent at `t+1.131902 ms`; response body created at `t+42.890279 ms`; response headers received at `t+42.949592 ms`; no DATA during the 100 ms zero-window observation by `t+144.262619 ms`; source poll observed at `t+144.287505 ms` with 1,024 bytes produced; producer dropped at `t+245.005329 ms`; health response absent before reset at `t+546.955029 ms`; RST_STREAM and original connection close at `t+547.069689 ms`; health response after reset at `t+548.800484 ms`. Test cleanup completed within the 10 s cap. This positively demonstrates a real flow-control-blocked phase and temporary transport-capacity retention/recovery after reset/close.

Intermediate control-run history: active SSE and peer-disconnect passed; normal consumption passed; explicit resume initially failed because the client increased the stream window but not the connection window. This was a harness omission, not a server result. The control was corrected to send WINDOW_UPDATE for both connection and stream. The failed run exited 101 and is retained for traceability; the final run below passed the corrected control.

An intermediate ignored run passed all three cases. Its combined command exited `0`, but the reset case still coupled RST_STREAM with immediately closing the original client connection. Its timestamps are retained as historical evidence; the final run below isolates reset while keeping the original connection open.

That intermediate run did not isolate reset from peer disconnect. The final run below completes the same-connection health response and holds the original TCP connection open after RST_STREAM; it counts recovery only when the pending second-connection health request receives response HEADERS. The separate SSE control exercises full peer disconnect independently.

## Final run

The final source separates `/events` from `RequestLimitLayer`, so the SSE control has no 200 ms short-body deadline. It waits for the short health DATA frame with END_STREAM, observes further SSE DATA afterward, waits 250 ms (longer than the configured short deadline), and observes more SSE DATA. `RunningServer` owns the shutdown sender and server task; each ignored scenario is inside `timeout(MAX_TOTAL, ...)`, with a drop guard that aborts server work if the future times out or panics.

Commands were run separately and serially:

- `cargo test --locked --test transport_backpressure_tests -- --nocapture` — exit `0`; three diagnostics discovered and ignored by default.
- `cargo test --locked --test transport_backpressure_tests -- --ignored --nocapture` — exit `0`; 3 passed, 0 failed, runtime `1.15 s`. Normal body consumption, dual connection/stream WINDOW_UPDATE resume to 1 MiB END_STREAM, active SSE after short-response completion and beyond 200 ms, peer disconnect recovery, zero-window timeout, same-connection short-request permit recovery, and isolated RST_STREAM/idle recovery all passed.

Final zero-window timeline from the latest run: server acknowledged the client's zero-window SETTINGS at `t+1.229435 ms`; response body created `t+42.929994 ms`; response headers `t+42.979418 ms`; no DATA after the 100 ms observation by `t+144.212176 ms`; body poll and 1,024 bytes produced `t+144.231226 ms`; producer dropped `t+246.295615 ms`; second-connection health response absent before reset `t+547.049808 ms`; same-connection health handler response `t+547.892741 ms`; RST_STREAM sent while the original TCP connection remained open `t+548.386494 ms`; second-connection health response after reset and idle reap `t+850.581203 ms`. The transport recovery point is approximately 302 ms after RST_STREAM, matching the 300 ms idle timeout. All test-owned tasks were joined during normal cleanup.

Latest exact commands/results: `cargo test --locked --test transport_backpressure_tests -- --nocapture` exited `0` (3 ignored by default); `cargo test --locked --test transport_backpressure_tests -- --ignored --nocapture` exited `0` (3 passed, 0 failed, 1.15 s). The final run explicitly waited for the server ACK of the client's zero-window SETTINGS before sending the test request. `rustfmt --check --edition 2024 tests/transport_backpressure_tests.rs` exited `0`.

These observations select branch 2, as stated above. The earlier initial setup timeout, missing connection-level WINDOW_UPDATE control, and compile typo were harness failures and have been corrected; they do not alter the final decision.

## Style alignment and final validation update

The initial full workspace format probe was `cargo fmt --all -- --check` — exit `1`. It emitted 4,284 diff blocks across 686 unique files (2,684,654 bytes of output). Of these, 685 paths belonged to clean sibling local-path repositories: rs-config (112), rs-json (168), rs-redact (294), and rs-value (111). The remaining path was the untouched, tracked baseline file `qubit-web-macros/src/controller/internal/resolve_self_type.rs`. Neither T3 file appeared in the formatter output. Full captured output was `/tmp/rs-web-cargo-fmt-all-20261104.log`. The package-scoped `cargo fmt --package qubit-web -- --check` exited `0`.

After correcting the seven-line copyright/license header and splitting imports, the first `.infra/bin/align-ci.sh` run failed on a brace-list import (`use tokio::time::{self}`). After splitting that import, the second alignment run passed style but its Clippy phase found a nested `if` that triggered `clippy::collapsible_if`; that condition was collapsed. The final `.infra/bin/align-ci.sh` exited `0`: Rust style operation and `rs-infra-ci check` both succeeded. No sibling files were formatted or changed.

After final alignment, the following serial checks all exited `0`:

- `cargo test --locked --test transport_backpressure_tests -- --nocapture` — 3 diagnostics found, all ignored by default.
- `cargo test --locked --test transport_backpressure_tests -- --ignored --nocapture` — 3 passed, 0 failed, 1.15 s. The zero-window handshake waited for the server's SETTINGS ACK before sending the request; all measured results and the branch-2 decision above remain unchanged.
- `rustfmt --check --edition 2024 tests/transport_backpressure_tests.rs`
- `git diff --check`
