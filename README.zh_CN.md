# qubit-web

[![Rust CI](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-web/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-web/coverage-badge.json)](https://qubit-ltd.github.io/rs-web/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-web.svg?color=blue)](https://crates.io/crates/qubit-web)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

[`qubit-web`](README.md) 为 Rust 服务提供可组合的 Axum 服务端基础设施：绑定 HTTP 或 HTTPS、装配 Controller 与原生路由、为选定的短请求设置有限额，并管理 SSE 和 WebSocket 生命周期。它适合需要自行实现服务行为的团队；应用状态、认证、授权和业务协议仍由应用负责。

## 安装

下面的原生 Axum 最小服务会直接使用这些依赖：

```toml
[dependencies]
axum = "0.8"
qubit-web = "0.3"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
```

`qubit-web` 默认不启用可选 feature。按需启用即可：`json` 提供有界 JSON 辅助能力，`ws` 提供 WebSocket 策略支持，`tls` 用于 HTTPS 绑定，`config` 提供 `qubit-config` 配置适配器。下方已有示例会注明所需 feature。

## 快速开始

先用一个只监听本机的服务提供健康检查和应用自有的停服入口。将“安装”一节的依赖写入 `Cargo.toml`，再把下面代码保存为 `src/main.rs`：

```rust
use std::error::Error;
use std::sync::Arc;

use axum::{http::StatusCode, routing::{get, post}, Router};
use qubit_web::{HttpLimits, RequestLimitLayer, ServerOptions, WebServer};
use tokio::sync::Notify;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let stop = Arc::new(Notify::new());
    let stop_for_route = Arc::clone(&stop);
    let app = Router::new()
        .route(
            "/health",
            get(|| async { "ok" })
                .layer(RequestLimitLayer::new(HttpLimits::default())),
        )
        .route(
            "/admin/shutdown",
            post(move || {
                let stop = Arc::clone(&stop_for_route);
                async move {
                    stop.notify_one();
                    StatusCode::ACCEPTED
                }
            }),
        );

    let options = ServerOptions::new("127.0.0.1:3000".parse()?)
        .with_max_transport_connections(128)?;
    let server = WebServer::bind_http(options).await?;
    let _report = server.serve(app, async move { stop.notified().await }).await?;
    Ok(())
}
```

在终端 A 运行服务并等待启动完成：

```sh
cargo run
```

在终端 B 查看健康检查响应，再调用应用提供的停服路由：

```sh
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

健康检查返回 `200 OK`，正文为 `ok`；停服路由返回 `202 Accepted`，并通知传给 `serve` 的关闭 future。示例仅绑定 loopback。请求限额只安装在短请求 `/health` 上；`WebServer` 直接运行传入的 Router，不会自动安装 HTTP 限额。停服路由没有认证，仅供本机演示，不能对外开放。

每条 HTTP/2 连接默认允许同时开启 200 个流。可通过 `ServerOptions::with_http2_max_concurrent_streams` 设置每连接上限，也可在启用 `config` 时使用 `transport.http2_max_concurrent_streams`。该协议流上限不是进程级请求数、连接数或内存上限。

`ServerOptions` 还可选限制 HTTP/1 解析缓冲区（最小 8192 字节）和 HTTP/2 解压后 header list（正 `u32` 字节）；两项默认沿用 Hyper 默认值。启用 `config` 时，对应配置键为 `transport.http1_max_buffer_bytes` 和 `transport.http2_max_header_list_bytes`。两种协议计量不同，也都不是进程内存上限。详细边界与拒绝行为见[用户指南](doc/user_guide.zh_CN.md#传输与协议请求头预算)。WebSocket 写入失败会发出脱敏的 `websocket_io_failure` tracing 事件；成功入队仍不表示消息已通过网络送达。

另一个独立的内存 CRUD 示例可用 `cargo run --example controller_crud --all-features` 启动。`/users` 属于该示例，并不存在于上面的最小服务中；创建用户会返回 `201 Created`。在 [`examples/controller_crud.rs`](examples/controller_crud.rs) 可查看如何用 `ControllerRoutes::with_http_limits` 替换 Controller 短路由的默认限额。原生 Axum 路由需要在选定的短路由分支安装 `RequestLimitLayer::new(http_limits)`，并在 SSE/WS 分支安装 `StreamAdmissionLayer`。Controller 的 `sse` 和 `ws` 路由使用独立的流式接纳预算。

启用 `config` feature 后，`ConfiguredWeb::from_config` 会分别返回 `ServerOptions` 和 `HttpLimits`。Controller 短路由已有 `HttpLimits::default()`；将配置所得限额传给 `with_http_limits` 可替换默认策略。原生 Axum 路由需在选定的短路由分支安装 `RequestLimitLayer`，并在 SSE/WS 分支安装 `StreamAdmissionLayer`；`WebServer` 不会替 Router 应用限额。

## 流式示例

`prompt_stream` 演示有界 JSON 请求、可取消的 SSE 进度流和 WebSocket echo。先在终端 A 启动服务，等它打印出监听地址：

```sh
cargo run --example prompt_stream --all-features
```

然后在终端 B 提交请求、观察 SSE 事件，最后停服：

```sh
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

示例 bearer token `demo-only` 只用于演示 handler 检查，不构成认证。WebSocket 客户端可连接 `ws://127.0.0.1:3001/prompt/demo-1/ws`，发送 `Authorization: Bearer demo-only`；若发送 Origin，则须使用允许的 `http://localhost:3000`。这些端点只演示传输，不会运行 Agent、保存 Prompt 或持久化任务。policy 的共享方式、容量规划和关闭流程见[中文用户指南](doc/user_guide.zh_CN.md)；[英文用户指南](doc/user_guide.md)也介绍了同一场景。

在 [`examples/prompt_stream.rs`](examples/prompt_stream.rs) 中，原生 SSE 和 WebSocket 路由共用一个 `StreamAdmissionState`。默认每个请求最多缓冲 1 MiB 请求体，共享接纳容量为 256 个并发许可，从开始接纳到 handler 生成响应头的期限为 10 秒。响应头就绪后期限即结束，不会覆盖 SSE 流本身或 WebSocket 升级后的会话。SSE 和 WebSocket 会话容量仍分别由 `SseConnectionPolicy` 和 `WsUpgradePolicy` 管理。

## 限额与运行边界

Controller 路径捕获使用 Axum 语法，例如 `/{id}` 和 `/{*rest}`；旧式 `/:id`、`/*rest` 路径段会在注册前被拒绝。JSON 提取器接受 `application/json` 和符合 token 语法的 `application/*+json` subtype；畸形 subtype 返回 `415 Unsupported Media Type`。

`WsSession::close()` 可重复调用；如果等待期间取消了 close future，可以再次调用完成关闭。显式关闭后，`recv()` 返回 `None`。`WsSession::closed()` 提供一个 owned 通知：两个受管 I/O 任务释放资源后 future 才会就绪，应用可将它与可取消工作一起 select，并调用 `close()` 释放队列中的入站消息。它不会自动结束 handler 或应用自行启动的任务。传输读取错误可通过一次 `Some(Err(_))` 交付，后续读取返回 `None`；宿主停服可能直接取消 handler，不保证交付该错误。未匹配请求的诊断日志只使用固定标记 `<unmatched>`，不会记录请求 URI 或秘密信息。`request_header_timeout` 和 SSE keep-alive interval 若为零或无法表示为 Tokio 截止时间，会以 `WebServerError::InvalidConfig` 拒绝。

- Controller 的 `short` 路由默认使用 `HttpLimits::default()`；原生 Axum 路由必须在需要限额的短请求分支上显式安装 `RequestLimitLayer`。`WebServer` 不会替传入的 Router 自动添加路由限额。
- Controller 的 `sse`/`ws` 路由共用 builder 的 `StreamAdmissionState`；添加路由前可用 `with_stream_admission_limits` 替换预算。原生 Axum 分支必须显式安装 `StreamAdmissionLayer`：`new` 创建独立预算，`from_state` 可共享预算。默认每个状态域最多缓冲 1 MiB/请求、接纳 256 个请求，并在响应头产生前限时 10 秒；请求体缓冲理论上限为每个状态域 256 MiB，另有请求和运行时开销。`RequestLimitLayer` 仍是独立的短请求策略。传输连接数和 SSE/WS 会话额度另行计算；SSE 也占用 HTTP 连接。
- 认证、授权、业务状态，以及流式数据的保存和重放由应用负责。WebSocket 升级前必须先完成认证；Origin 检查只限制浏览器来源。示例 token 和无认证停服入口仅供 loopback 演示。
- `DiagnosticLayer::new()` 记录请求及响应头元数据。需要响应体终止结果时，在 `RequestLimitLayer` 外侧显式安装 `DiagnosticLayer::with_body_diagnostics()`；事件使用允许列表字段，不含正文或错误文本，也不声称客户端已断连或网络已送达。
- WebSocket 的 `queue_limits` 配置出站队列；`inbound_queue_limits` 单独配置入站预算，默认每连接最多排队 64 条消息和累计 1 MiB payload。通道满时最多等待至 `idle_timeout`，字节预算不足则立即以 `1013` 关闭。128 个活跃默认连接的排队/待交付 payload 约为 128 MiB，另有每个活跃连接最多一条 1 MiB 协议重组消息；该估算不含分配开销和应用自有内存，也不是 I/O 结束后仍被 handler 或 session 保留的内存上限。
- 启用 HTTPS 需要 `tls` 与有效证书，也可由部署方配置可信反向代理。停服由应用发起，并遵守服务端配置的截止时间。
- `serve` 已开始运行后，丢弃其 future 或 abort 所在任务会关闭受管会话登记并通知现有会话，但无法等待异步排空或生成停服报告；后续清理需要 Tokio runtime 继续运行。优雅停服请完成 shutdown future 并等待 `serve` 返回。

限额细节、TLS 配置、生命周期行为和排障方法见[中文用户指南](doc/user_guide.zh_CN.md)或[英文用户指南](doc/user_guide.md)。更多背景可查阅 [API 文档](https://docs.rs/qubit-web)、[历史中文设计记录](doc/2026-10-08-rs-web-design.md)和[生命周期设计摘要](doc/2026-10-09-rs-web-lifecycle-design.en.md)。

## 测试

运行当前 crate 的检查：

```sh
cargo test
cargo test --all-features
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

如需单独检查 WebSocket 合同，可运行：

```sh
cargo test -p qubit-web --no-default-features --features ws --test websocket_tests --locked
```

## 许可证

本项目采用 Apache License 2.0 授权，完整条款见 [LICENSE](LICENSE)。

## 贡献

欢迎参与贡献。提交 Pull Request 前，可用 `./.infra/bin/style-check.sh` 检查代码风格，并用 `./.infra/bin/ci-check.sh` 完成本 crate 的验收检查。[固定互操作 workflow](.github/workflows/downstream-interop.yml) 保留固定消费者基线；独立的[当前消费者 WS/WSS workflow](.github/workflows/downstream-current-interop.yml) 每周或手动检查 rs-websocket `main`，也覆盖当前消费者的 TLS WebSocket 场景。提出改进前可先到 [Issues](https://github.com/qubit-ltd/rs-web/issues) 讨论，也可直接提交 [Pull Request](https://github.com/qubit-ltd/rs-web/pulls)。

## 作者

**Haixing Hu** — [Qubit Co. Ltd.](https://github.com/qubit-ltd)

期限采用 Tokio 协作式取消，不让出执行权的同步工作会延迟检查和清理。HTTP/HTTPS handler 可通过 Axum `ConnectInfo<SocketAddr>` 提取直接 TCP 对端；代理与执行边界见[用户指南](doc/user_guide.zh_CN.md)。
