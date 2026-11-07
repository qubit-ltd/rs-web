# WebSocket write stall investigation (WEB-001)

## Scope and baseline

The first section records T1's diagnostic evidence. The final section records the conditional T2 repair and T3 validation.

- Repository: `rs-web`, worktree branch `codex/web-001-write-stall-20261107`
- Baseline HEAD: `134d747ba80b31124408af9fa628d9a6f8fcf8b7` (`feat(ws): validate close codes and preserve rejected messages`, 2026-11-07)
- Worktree status at investigation start: clean
- Repository instructions: no `AGENTS.md` found under the worktree
- Toolchain: Rust `1.94.0` (`4a4ef49`), Cargo `1.94.0` (`85eff8c`), from `rust-toolchain.toml`
- Platform: Linux x86_64, Ubuntu kernel `6.8.0-147-generic`
- Locked WebSocket dependencies: Axum `0.8.9`, Tokio `1.54.1`, Tungstenite / tokio-tungstenite `0.29.0`

## Harness

The writer's existing `PendingSend` fake sink covered a pending `poll_flush`, but did not cover pending readiness. Two deterministic tests now exercise both points. They hold the peer-close acknowledgement sender, leave the shutdown token uncancelled, advance paused time through three 500 ms windows, and check that no poll or send completion occurs and that the writer still retains its session registration and connection permit. They explicitly abort the writer only after observation; the test then releases the abandoned in-flight queue charge as harness cleanup.

A raw TCP WebSocket test also runs within the writer unit-test module so it can use a private, `cfg(test)` sink wrapper. The wrapper records `Sink::start_send` and `poll_flush` Pending/Ready transitions only for a binary message whose payload begins with a unique marker. The wrapper and observer channel are absent outside unit-test builds; no public API or production telemetry was added.

The test starts a real `WebServer` and Axum WebSocket route with `max_connections=1`, `idle_timeout=500 ms`, a one-message outbound queue, and a 32 MiB message/frame limit. The handler enqueues one 32 MiB marked message, then continuously calls `WsSession::recv` and counts text frames. The raw TCP peer completes the WebSocket handshake, sends masked inbound text frames every 50 ms, and does not read server data. The write-stall window begins only after the actual server sink reports `poll_flush == Pending`. During three consecutive 500 ms windows, the test watches for a matching `poll_flush == Ready`, samples inbound progress and capacity, and then attempts a second WebSocket upgrade.

## Results

The pre-repair real-network test passed on the recorded Linux host. Its monotonic event times were:

| Event | Monotonic timestamp | Relative to first Pending |
| --- | ---: | ---: |
| Marked send started | `449989.831247858 s` | `-39.237 ms` |
| Actual sink `poll_flush` returned Pending | `449989.870485039 s` | `0 ms` |
| End of window 1 | `449990.371997265 s` | `501.512 ms` |
| End of window 2 | `449990.872374448 s` | `1,001.889 ms` |
| End of window 3 | `449991.371553955 s` | `1,501.069 ms` |

No matching Ready event occurred during the 1.5-second observation. At each window end, the handler's inbound message count had increased:

| Window | Inbound text count | `active_connections` | `active_sessions` |
| --- | ---: | ---: | ---: |
| 1 | 10 | 1 | 1 |
| 2 | 20 | 1 | 1 |
| 3 | 30 | 1 | 1 |

The second upgrade returned `503 Service Unavailable` while both active counters remained 1. This is direct evidence of connection-capacity denial while the first session's actual network writer remained in a pending flush and its application continued receiving inbound frames. The test did not use queue admission/backpressure as a proxy for network write completion.

After the evidence window, the test stopped its inbound sender, dropped the client TCP halves, and explicitly cancelled server shutdown. The server returned a graceful report within its configured 200 ms shutdown bound; `active_connections` and `active_sessions` both reached 0. These cleanup actions happened only after the observation; the test does not rely on which of peer EOF or server shutdown wins cleanup.

The test total runtime was 1.57 seconds, below its 15-second observation bound. The deterministic pending-readiness and pending-flush tests also passed, each preserving Pending for three simulated 500 ms windows without an external close.

## Conclusion

WEB-001 was confirmed on this platform and harness: while inbound frames continued to arrive, an actual WebSocket sink flush stayed Pending for three idle-timeout intervals, the writer retained the session and connection permit, and a second upgrade was rejected for capacity. The test establishes a bounded 1.5-second stall, not an assertion that every platform or socket buffer has identical thresholds. This evidence justified proceeding to the plan's conditional T2 repair branch.

## Commands and outcomes

All commands were run from `/data/working/qubit/rust-common/.worktrees/rs-web/web-001-write-stall-20261107`.

| Command | Exit | Result |
| --- | ---: | --- |
| `rustfmt /data/working/qubit/rust-common/.worktrees/rs-web/web-001-write-stall-20261107/src/ws/internal/writer.rs` | 0 | Formatted the touched Rust file |
| `cargo test -p qubit-web --locked --no-default-features --features ws --lib ws::internal::writer::tests::test_real_network_write_stall_keeps_inbound_live_and_capacity_occupied -- --nocapture --test-threads=1` | 0 | Real-network diagnostic passed; 1 test, 0 failed |
| `cargo test -p qubit-web --locked --no-default-features --features ws --lib ws::internal::writer::tests -- --nocapture --test-threads=1` | 0 | Writer unit module passed; 26 tests, 0 failed, 86 filtered out |

The network diagnostic resides in the private writer unit-test module rather than `tests/ws_write_stall_tests.rs`: the test-only observer intentionally uses private sink instrumentation to prove the actual writer's Pending state. No separate integration-test binary or consumer migration was needed for this T1 evidence.

## T2 repair

The confirmed failure mode was addressed by using `idle_timeout` as a single absolute deadline for each ordinary outbound `Sink::send` and notification-triggered flush. The deadline is not refreshed by inbound frames or unrelated queue notifications. A checked-add overflow produces an immediate deadline. When an ordinary send expires, its in-flight queue charge is finished exactly once, the queue is closed, the shared close deadline is recorded, the session token is cancelled, and the sink is dropped. A notification flush has no in-flight message to finish and follows the same queue/session cleanup. The timed-out sink is never retried or reused; a Close frame is not guaranteed to reach a backpressured peer. Existing peer-Close and server-shutdown handling still uses its shared close deadline.

Regression coverage includes pending send readiness, pending send flush, notification flush held pending while unrelated notifications continue, an unrepresentable operation deadline, queue accounting and permit/session release, and the real TCP/WebSocket reproduction. The real-network test now confirms inbound frames progress during the write deadline, the first session releases capacity without a test-triggered close, a second upgrade succeeds with `101`, and server cleanup returns both active counters to zero. The test-only sink observer remains private and absent from non-test builds.

Public setter signatures and error types are unchanged. `idle_timeout` now also bounds outbound operation completion, so applications with slow readers may need to increase it; doing so also increases inbound waits. The limit measures operation wait, not byte-level progress. This is the only behavioral compatibility change in scope, documented in both READMEs, both user guides, both lifecycle documents, and the Rustdoc for `WsUpgradePolicy::idle_timeout`.

Two narrow baseline validation repairs were also required and are included: the `rs-json` local-path CI pin now points at the already-existing clean commit `6b72c09a044f9dd3352998fc3521b09c7ba5cfaa` (version 0.12.2), because its prior 0.12.1 pin conflicted with `qubit-config`'s `qubit-json = ^0.12` during `cargo package`; and the WSS fixture lock now records registry `qubit-redact` 0.10.1 instead of an unversioned local 0.10.0 entry so `--locked` resolves without local path patches. The full CI initially failed only at package verification with the JSON version conflict; package verification passed after aligning the pin. The root `Cargo.lock` changes produced by local-path preparation are generated and were restored before staging.

## Final verification

All commands below ran against the implementation worktree at `/data/working/qubit/rust-common/.worktrees/rs-web/web-001-write-stall-20261107` unless stated otherwise.

| Command | Exit | Result |
| --- | ---: | --- |
| `./.infra/bin/align-ci.sh` (after formatter/lint corrections) | 0 | Formatting/style alignment and infra checks succeeded |
| `./.infra/bin/ci-check.sh` (first run) | 1 | Build, full tests, feature matrix, docs and other suites completed; package verification exposed the stale `qubit-json` 0.12.1 local pin conflict |
| Manual `cargo ... package --package qubit-web --allow-dirty` with local path patches at `qubit-json` 0.12.2 and the workspace macro patch | 0 | Packaged and verified after pin diagnosis |
| `./.infra/bin/align-ci.sh` (after updating the pin) | 0 | Style alignment and infra checks succeeded |
| `./.infra/bin/ci-check.sh` (rerun after pin update) | 0 | Build, all tests and feature-matrix configurations, strict docs, README, package verification, coverage, advisory scan and CI checks passed; coverage was functions 95.16%, lines 95.34%, regions 94.93% |
| `cargo test -p qubit-web --locked --no-default-features --features ws --lib ws::internal::writer::tests` | 0 | 28 passed, 0 failed |
| `cargo test -p qubit-web --locked --no-default-features --features ws --test websocket_tests` | 0 | 44 passed, 0 failed |
| `cargo test --manifest-path tests/fixtures/wss_consumer/Cargo.toml --locked` | 0 | 3 passed, 0 failed |
| `python3 scripts/check-downstream-interop.py --upstream <worktree> --downstream <disposable fixed checkout> --disposable-marker <external marker>` | 0 | Fixed downstream SHA `94f9da8c486998efcae693a15c5c021c7cece601`: 5 WS tests passed; metadata resolved `qubit-web` to this worktree |
| `python3 scripts/check-downstream-interop.py --upstream <worktree> --downstream <disposable current checkout> --disposable-marker <external marker> --with-wss` | 0 | Current downstream main SHA `e76cce01d50f75c30353cf282cf33eb90f758005`: 5 WS and 3 WSS tests passed; metadata resolved both direct packages to the intended worktrees |
| `git diff --check` | 0 | No whitespace errors |
| `./.infra/bin/align-ci.sh` (after documenting the 60-second default) | 0 | Style and CI alignment succeeded |
| `./.infra/bin/ci-check.sh` (final rerun after the documentation update) | 0 | All suites passed again, including package verification and unchanged coverage thresholds; coverage was functions 95.16%, lines 95.35%, regions 94.94% |

The plan named a separate `tests/ws_write_stall_tests.rs` target; the diagnostic and repaired real-network regression remain inside the private writer unit-test module because the test-only sink observer must instrument the private production writer. The final 28-test writer command includes that real-network test. No test was skipped, and CI coverage thresholds were unchanged.
