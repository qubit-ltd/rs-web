# qubit-web

[`qubit-web`](README.md) 为 Rust 服务提供可组合的 Axum 服务端基础设施：绑定 HTTP 或 HTTPS、装配 Controller 与原生路由、为选定的短请求设置有限额，并管理 SSE 和 WebSocket 生命周期。它适合需要自行实现服务行为的团队；应用状态、认证、授权和业务协议仍由应用负责。

## 安装

下面的原生 Axum 最小服务会直接使用这些依赖：

```toml
[dependencies]
axum = "0.8"
qubit-web = "0.2"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync"] }
```

`qubit-web` 默认不启用可选 feature。按需启用即可：`json` 提供有界 JSON 辅助能力，`ws` 提供 WebSocket 策略支持，`tls-rustls` 用于 HTTPS 绑定，`config` 提供 `qubit-config` 配置适配器。下方已有示例会注明所需 feature。

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

另一个独立的内存 CRUD 示例可用 `cargo run --example controller_crud --all-features` 启动。`/users` 属于该示例，并不存在于上面的最小服务中；创建用户会返回 `201 Created`。在 [`examples/controller_crud.rs`](examples/controller_crud.rs) 可查看如何用 `ControllerRoutes::with_http_limits` 替换 Controller 短路由的默认限额。原生 Axum 路由需要在每个选定的短路由分支上安装 `RequestLimitLayer::new(http_limits)`；Controller 的 `sse` 和 `ws` 路由不会使用短请求限额。

启用 `config` feature 后，`ConfiguredWeb::from_config` 会分别返回 `ServerOptions` 和 `HttpLimits`。Controller 短路由已有 `HttpLimits::default()`；将配置所得限额传给 `with_http_limits` 可替换默认策略。原生 Axum 路由则需在选定分支显式安装 `RequestLimitLayer`；`WebServer` 不会替 Router 应用限额。

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

## 限额与运行边界

- Controller 的 `short` 路由默认使用 `HttpLimits::default()`；原生 Axum 路由必须在需要限额的短请求分支上显式安装 `RequestLimitLayer`。`WebServer` 不会替传入的 Router 自动添加路由限额。
- 新建的 SSE 或 WebSocket policy 各自管理独立容量。需要多个路由共用预算时，应将同一 policy 放入应用共享状态并克隆使用。传输连接上限单独计算；SSE 也会占用 HTTP 连接。
- 认证、授权、业务状态，以及流式数据的保存和重放由应用负责。WebSocket 升级前必须先完成认证；Origin 检查只限制浏览器来源。示例 token 和无认证停服入口仅供 loopback 演示。
- 启用 HTTPS 需要 `tls-rustls` 与有效证书，也可由部署方配置可信反向代理。停服由应用发起，并遵守服务端配置的截止时间。

限额细节、TLS 配置、生命周期行为和排障方法见[中文用户指南](doc/user_guide.zh_CN.md)或[英文用户指南](doc/user_guide.md)。更多背景可查阅 [API 文档](https://docs.rs/qubit-web)、[历史中文设计记录](doc/2026-10-08-rs-web-design.md)和[生命周期设计摘要](doc/2026-10-09-rs-web-lifecycle-design.en.md)。

## 测试

可运行以下命令检查整个 workspace：

```sh
cargo test --workspace --all-features --all-targets --locked
```

## 许可证

本项目采用 Apache License 2.0 授权，完整条款见 [LICENSE](LICENSE)。

## 贡献

欢迎参与贡献。提交 Pull Request 前，请运行 `./.infra/bin/ci-check.sh` 并确认 CI 检查通过。提出改进前可先到 [Issues](https://github.com/qubit-ltd/rs-web/issues) 讨论，也可直接提交 [Pull Request](https://github.com/qubit-ltd/rs-web/pulls)。

## 作者

**Haixing Hu** — [Qubit Co. Ltd.](https://github.com/qubit-ltd)
