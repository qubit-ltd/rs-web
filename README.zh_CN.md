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

HTTP/2 流数、可选的 HTTP/1 与 HTTP/2 请求头预算，以及 WebSocket 写入诊断均在[用户指南](doc/user_guide.zh_CN.md#传输与协议请求头预算)中说明，包含各自的适用范围和运行边界。

另一个独立的内存 CRUD 示例可用 `cargo run --example controller_crud --all-features` 启动。`/users` 属于该示例，并不存在于上面的最小服务中；创建用户会返回 `201 Created`。在 [`examples/controller_crud.rs`](examples/controller_crud.rs) 可查看如何用 `ControllerRoutes::with_http_limits` 替换 Controller 短路由的默认限额。原生 Axum 路由需要在选定的短路由分支安装 `RequestLimitLayer::new(http_limits)`，并在 SSE/WS 分支安装 `StreamAdmissionLayer`。Controller 的 `sse` 和 `ws` 路由使用独立的流式接纳预算。

启用 `config` feature 后，`ConfiguredWeb::from_config` 会分别返回 `ServerOptions` 和 `HttpLimits`。如何将两者应用于 Controller 与原生 Axum 路由，见[用户指南](doc/user_guide.zh_CN.md#装配普通接口与长连接)；`WebServer` 不会替 Router 添加路由限额。

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

在 [`examples/prompt_stream.rs`](examples/prompt_stream.rs) 中，原生 SSE 和 WebSocket 路由共用一个 `StreamAdmissionState`。接纳期限在响应头就绪后结束，不覆盖后续 SSE 流或 WebSocket 会话。会话容量仍分别由 `SseConnectionPolicy` 和 `WsUpgradePolicy` 管理。请求预算和运行边界见[用户指南](doc/user_guide.zh_CN.md)。

## 限额与运行边界

`WebServer` 按调用方提供的 Router 运行。原生短请求分支需显式安装 `RequestLimitLayer`，原生 SSE/WS 分支需显式安装 `StreamAdmissionLayer`；Controller 路由使用 builder 的策略。短请求和流式接纳属于不同容量域。每个 builder 默认拥有独立状态域；调用方也可显式共享 `LimitState` 或 `StreamAdmissionState`，让 Controller、原生路由和其他 builder 使用同一容量。期限依赖 Tokio 协作式取消，不让出执行权的同步工作会延迟检查和清理。

认证、授权、业务状态以及流式数据的保存和重放由应用负责。WebSocket 升级前应完成认证；Origin 检查只限制浏览器来源。示例 token 和无认证停服入口仅供 loopback 演示。队列与请求体预算、setter 顺序、停服和读取错误语义、TLS 轮换、迁移及排障细节见[中文用户指南](doc/user_guide.zh_CN.md)或[英文用户指南](doc/user_guide.md)。现行边界与资料导航见[架构概览](doc/2026-10-09-rs-web-architecture.zh_CN.md)、[生命周期设计摘要](doc/2026-10-09-rs-web-lifecycle-design.zh_CN.md)和[文档索引](doc/README.md)。

[固定互操作 workflow](.github/workflows/downstream-interop.yml) 保留固定消费者基线；[当前消费者 WS/WSS workflow](.github/workflows/downstream-current-interop.yml) 则提供针对 rs-websocket `main` 的非阻塞兼容信号。两者是独立信号；CI 触发条件、调度和结果解释集中见[文档索引](doc/README.md)。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./.infra/bin/ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交 Pull Request 前运行 `./.infra/bin/align-ci.sh` 格式化代码，运行 `./.infra/bin/ci-check.sh` 对齐 CI 要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-web](https://github.com/qubit-ltd/rs-web)
