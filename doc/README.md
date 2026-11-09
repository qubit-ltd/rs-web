# Documentation index

## Guides and design references

- [User guide (English)](user_guide.md): end-to-end setup, routing, operation, acceptance, and troubleshooting.
- [用户指南（简体中文）](user_guide.zh_CN.md)：按接入场景独立编写的完整指南。
- [Request and session lifecycle design (English)](lifecycle-design.md): maintainer reference for ownership, capacity domains, deadlines, diagnostics, and shutdown.
- [请求与会话生命周期设计（简体中文）](lifecycle-design.zh_CN.md)：生命周期维护者设计说明。

## Investigation records

These reports preserve historical experiments and their own environment, baseline, and limitations. Their results are not validation results for the current checkout.

| Topic | Date | Baseline / version evidence | Finding and limitation |
| --- | --- | --- | --- |
| [HTTP/2 resource investigation](http2-resource-investigation-20261103.md) | 2026-11-03; bounded follow-up 2026-11-04 | Baseline is recorded in the bounded resource experiment section: [`2ab1cb41e316d456e26636b6cb222075c619a7fc`](http2-resource-investigation-20261103.md#bounded-resource-experiment-2026-11-04). Locked Hyper 1.12.0 and h2 0.4.20. | Tested incomplete header blocks did not stop the concurrent SSE response; no general header deadline or unbounded-duration conclusion. The experiments are bounded and do not establish arbitrary-client memory behavior. |
| [Backpressure and request-timeout investigation](backpressure-timeout-investigation-20261104.md) | 2026-11-04 | Original record does not state a source baseline commit. Final run versions: [options and versions](backpressure-timeout-investigation-20261104.md#options-and-versions): Rust 1.94.0, Axum 0.8.9, h2 0.4.20, Hyper 1.12.0, Tokio 1.54.1. | A bounded zero-window HTTP/2 run observed application-body and short-request permit release while transport retention continued until reset/idle reaping. It is a locked-stack loopback observation, not a transport-close guarantee. |
| [WebSocket Close lifecycle investigation](close-lifecycle-investigation-20261104.md) | 2026-11-04 UTC | Target baseline `3adf93dca8cd2218b8e9ce665d0a9eaca39c1446`; Axum 0.8.9, Tokio 1.54.1, Tungstenite and Tokio-Tungstenite 0.29.0. See [record header](close-lifecycle-investigation-20261104.md#websocket-close-lifecycle-investigation). | Captured Close-only behavior and subsequent repair contract. `try_send(Close)` means enqueue, not delivery; findings are tied to the recorded harness and source history. |
| [Stream admission buffer investigation](stream-admission-buffer-investigation-20261106.md) | 2026-11-06 | Source baseline `d599ad24a0fda2f563b4b744c9bde9cd0446a954`; locked `http-body-util` 0.1.5, bytes 1.12.1, Hyper 1.12.0, h2 0.4.20. See [environment and method](stream-admission-buffer-investigation-20261106.md#environment-and-method). | Synthetic and local network experiments support the tested buffering/owner behavior; they do not establish process RSS amplification, remote exploitability, or portable performance. |
| [WebSocket write-stall investigation](ws-write-stall-investigation-20261107.md) | 2026-11-07 | Baseline `134d747ba80b31124408af9fa628d9a6f8fcf8b7`; locked Axum 0.8.9, Tokio 1.54.1, Tungstenite/Tokio-Tungstenite 0.29.0. See [scope and baseline](ws-write-stall-investigation-20261107.md#scope-and-baseline). | Recorded a bounded real-network stall and subsequent repair. Slow-reader operation completion is now bounded by `idle_timeout`; peer delivery of Close is not guaranteed. Timing is specific to the recorded host and fixture. |
| [Connection activity lease investigation](connection-activity-investigation-20261108.md) | 2026-11-08 | Starting commit `e255d353594be4d460f1de6a922a26a5d34e1001`; locked Axum 0.8.9, Hyper 1.12.0, Tokio 1.54.1. See [scope and environment](connection-activity-investigation-20261108.md#scope-and-environment). | A local body test reproduced lease retention after a final frame and motivated a narrow repair. Network controls passed; the report does not claim an externally visible protocol defect. |
| [Trailers budget investigation](trailers-budget-investigation-20261108.md) | 2026-11-08 | Baseline HEAD `320803bb1e3396e803d40acce83d738a41400373`; locked Hyper 1.12.0 and h2 0.4.20. See [scope and environment](trailers-budget-investigation-20261108.md#scope-and-environment). | In tested HTTP/1 and HTTP/2 shapes, oversized trailer behavior differed; HTTP/2 may omit over-budget fields while continuing. Conclusions cover the tested versions and sizes only. |

## Interoperability and diagnostics workflows

The [consumer contracts workflow](../.github/workflows/consumer-contracts.yml) checks the WebSocket dependency graph against both the committed lockfile and a fresh resolution, then runs the two existing oversized-message tests that assert close code 1009. Its documentation job compiles six selected examples extracted directly from the English and Chinese README and user guides. The workflow runs for relevant pull request paths, every Monday at 04:29 UTC, and on manual dispatch. Run the locked checks locally with:

```bash
python3 -m unittest discover -s scripts/tests -v
python3 scripts/check-ws-backend.py
cargo test --locked --features ws --test websocket_tests test_oversized_reassembled_message_is_closed_by_axum_limit
cargo test --locked --features ws --test websocket_tests test_oversized_single_frame_is_rejected_even_when_message_limit_is_larger
python3 scripts/check-documentation-contracts.py
```

For the fresh dependency signal, run `cargo update` before `python3 scripts/check-ws-backend.py`, then run both Cargo tests above; use a disposable checkout because `cargo update` changes `Cargo.lock`.

The guides above document current behavior. The [fixed WebSocket interoperability workflow](../.github/workflows/downstream-interop.yml) preserves the pinned `rs-websocket` baseline and tests it against the event checkout of `rs-web`. The separate [current-consumer WS/WSS workflow](../.github/workflows/downstream-current-interop.yml) runs against `rs-websocket` `main` when a pull request changes `src/**`, `qubit-web-macros/**`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `tests/**`, `scripts/check-downstream-interop.py`, `.infra/**`, or `.github/workflows/downstream*interop.yml`; it also runs every Monday at 03:17 UTC and on manual dispatch. It covers both WS and WSS, copies the fixed WSS scenarios into a disposable crate, and verifies that Cargo resolves both `qubit-web` and `qubit-websocket` directly to the intended local checkouts before running them. Cargo-relative local dependencies are prepared in one shared parent directory so both repository checkouts resolve the same dependency paths. The current-consumer job uses `continue-on-error`, making it a non-blocking compatibility signal; inspect its job and step outcomes and summary because overall workflow success does not prove this check passed. The pinned baseline and current `main` are separate signals.

The [transport backpressure diagnostics workflow](../.github/workflows/transport-diagnostics.yml) runs weekly on Tuesday at 03:37 UTC and supports manual dispatch. Its three ignored HTTP/2 experiments remain diagnostic observations, not performance gates. Run them locally with `cargo test -p qubit-web --locked --test transport_backpressure_tests -- --ignored --nocapture --test-threads=1`; inspect the test output and captured dependency baseline on failure. See the [backpressure timeout investigation](backpressure-timeout-investigation-20261104.md) for the preserved experiment history and interpretation.
