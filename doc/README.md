# Documentation index

## Current architecture and usage

- [rs-web architecture overview (English, 2026-10-09)](2026-10-09-rs-web-architecture.en.md): current module map, resource boundaries, failure stages, and application responsibilities.
- [rs-web 架构概览（简体中文，2026-10-09）](2026-10-09-rs-web-architecture.zh_CN.md)：现行模块关系、资源边界、失败阶段和应用责任。
- [rs-web lifecycle and resource boundary design (English, 2026-10-09)](2026-10-09-rs-web-lifecycle-design.en.md): current shutdown, connection, Origin, and I/O error contracts.
- [rs-web 生命周期与资源边界设计摘要（简体中文，2026-10-09）](2026-10-09-rs-web-lifecycle-design.zh_CN.md)：现行生命周期边界与 API 合同。
- [User guide (English)](user_guide.md): end-to-end setup, routing, operation, acceptance, and troubleshooting.
- [用户指南（简体中文）](user_guide.zh_CN.md)：按接入场景独立编写的完整指南。

## Historical decisions

- [rs-web first PRD (2026-10-08)](2026-10-08-rs-web-prd.md): original requirements and acceptance baseline, with implementation status at the top.
- [rs-web detailed design record (2026-10-08)](2026-10-08-rs-web-design.md): technical decisions and implementation notes, preserving the original decision history.

The historical records above preserve past decisions and may describe superseded proposals. Current behavior is documented in the architecture overview, lifecycle summaries, and user guides above. The [fixed WebSocket interoperability workflow](../.github/workflows/downstream-interop.yml) preserves the pinned `rs-websocket` baseline and tests it against the event checkout of `rs-web`. The separate [current-consumer WS/WSS workflow](../.github/workflows/downstream-current-interop.yml) runs against `rs-websocket` `main` when a pull request changes `src/**`, `qubit-web-macros/**`, `Cargo.toml`, `Cargo.lock`, `rust-toolchain.toml`, `tests/**`, `scripts/check-downstream-interop.py`, `.infra/**`, or `.github/workflows/downstream*interop.yml`; it also runs every Monday at 03:17 UTC and on manual dispatch. It covers both WS and WSS, copies the fixed WSS scenarios into a disposable crate, and verifies that Cargo resolves both `qubit-web` and `qubit-websocket` directly to the intended local checkouts before running them. Cargo-relative local dependencies are prepared in one shared parent directory so both repository checkouts resolve the same dependency paths. The current-consumer job uses `continue-on-error`, making it a non-blocking compatibility signal; inspect its job and step outcomes and summary because overall workflow success does not prove this check passed. The pinned baseline and current `main` are separate signals. The [HTTP/2 resource investigation](http2-resource-investigation-20261103.md) records the WEB-004 protocol experiment and its limits.

The [transport backpressure diagnostics workflow](../.github/workflows/transport-diagnostics.yml) runs weekly on Tuesday at 03:37 UTC and supports manual dispatch. Its three ignored HTTP/2 experiments remain diagnostic observations, not performance gates. Run them locally with `cargo test -p qubit-web --locked --test transport_backpressure_tests -- --ignored --nocapture --test-threads=1`; inspect the test output and captured dependency baseline on failure. The [backpressure timeout investigation](backpressure-timeout-investigation-20261104.md) preserves the experiment history and interpretation.
