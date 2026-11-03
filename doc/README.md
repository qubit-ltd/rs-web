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

The historical records above preserve past decisions and may describe superseded proposals. Current behavior is documented in the architecture overview, lifecycle summaries, and user guides above. The [fixed WebSocket interoperability workflow](../.github/workflows/downstream-interop.yml) preserves the pinned `rs-websocket` baseline and tests it against the event checkout of `rs-web`. The separate [current-consumer WS/WSS workflow](../.github/workflows/downstream-current-interop.yml) runs weekly and on manual dispatch against `rs-websocket` `main`, covering both WS and WSS. It copies the fixed WSS scenarios into a disposable crate and verifies that Cargo resolves both `qubit-web` and `qubit-websocket` directly to the intended local checkouts before running them. Cargo-relative local dependencies are prepared in one shared parent directory so both repository checkouts resolve the same dependency paths. The [HTTP/2 resource investigation](http2-resource-investigation-20261103.md) records the WEB-004 protocol experiment and its limits.
