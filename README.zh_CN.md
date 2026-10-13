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

- 每次通过 `new` 或 `default` 创建 `SseConnectionPolicy`、`WsUpgradePolicy` 都会开启独立容量域；克隆 policy 才会共享容量。需要路由共享时，在 `AppState` 中各创建一次，再由 handler 通过 `State<AppState>` 取出并克隆。示例见[中文用户指南](doc/user_guide.zh_CN.md)或[英文用户指南](doc/user_guide.md)。每个 `WebServer` 的传输连接上限默认为 1024，可通过 `ServerOptions::with_max_transport_connections` 配置。传输连接数与短请求并发数必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`；超界值返回 `WebServerError::InvalidConfig`。该上限覆盖 TLS 握手和 HTTP 连接 future。达到上限时服务会暂停接收连接；客户端可能在操作系统 backlog 中等待，也可能连接失败，因此不保证返回 HTTP 503。policy 容量与传输上限分别计算；SSE 仍占用 HTTP 连接。
- `ControllerRoutes::new()` 会为 Controller 的 `short` 路由安装 `HttpLimits::default()`：每个请求体 1 MiB、每个 builder 共享 256 个并发请求，处理及响应体期限为 30 秒。期限到达时会主动释放应用响应体和短请求许可，不必等到再次轮询响应体。响应头发出后，超时只能在传输再次轮询时通过 body error 体现，不会关闭连接。无法表示的绝对请求期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限变得无法表示，layer 返回 HTTP 500 和代码 `invalid_configuration`。普通 handler 超时仍返回 HTTP 504。`with_http_limits` 用于替换默认值，并应在添加 Controller 前调用。Controller 的 `sse` 和 `ws` 路由会跳过短请求 layer。原生 Axum 路由需在选定分支显式安装 `RequestLimitLayer`；`WebServer` 不会安装路由限额。
- `ControllerRoutes::add` 会在注册前检查重复方法、无效路径和冲突的路由模板（包括参数别名及重叠通配符），并返回 `ControllerRouteError`。不同方法可以共用同一模板；`finish()` 直接返回 Axum `Router`。
- `rest_controller` 支持在同一模块中装配末段名称相同、但实际类型不同的 Controller。
- HTTP/1 请求头默认最多等待 10 秒；WebSocket 的 HTTP/1 upgrade 也必须在此期限内发送完请求头。传输空闲期限默认 30 秒，可通过 `ServerOptions::with_transport_idle_timeout` 或配置键 `transport.idle_timeout_ms` 设置。连接在没有正在处理的请求或响应体时会被回收，包括空闲 HTTP/2 连接；活跃请求和 SSE 响应体会暂停空闲计时。
- 使用 `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`，可在返回升级响应前预留受管会话。策略按最终配置字段值校验：`max_connections` 必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`；零值返回 `WsPolicyError::ZeroLimit`，超过上限返回 `WsPolicyError::ConnectionLimitTooLarge`。Origin 默认可选：未携带 Origin 的请求会继续进入应用认证；携带 Origin 时，必须与 `allowed_origins` 中的值完全匹配，否则拒绝。没有 Origin 不代表已通过认证。对端或会话关闭后，`try_send` 会返回 `Closed`；成功入队本身不保证消息已通过网络送达。若应用不读取入站消息，且交付在 `idle_timeout` 内没有进展，会话会以 WebSocket 关闭码 `1013`（暂时无法处理）关闭。
- `ServerContext::active_sessions()` 统计受管 SSE 和通过 `on_upgrade` 接纳的 WebSocket，包括握手尚未完成的升级。停服会使用由 `ServerOptions` 设置、经 `ServerContext` 提供的同一个截止时间等待 HTTP 连接和这些会话；WebSocket policy 不再单独设置停服期限。无法表示的停服期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限异常，服务会先取消会话并回收连接任务，再返回错误。`ShutdownReport::unfinished_managed_sessions` 记录截止时仍未结束的受管会话。原生 Axum 升级和应用后台任务不在统计范围内。
- `WebServerError` 通过 `std::error::Error::source()` 保留 socket I/O 错误供调用方诊断；默认 `Display` 和 `Debug` 不输出底层操作系统错误文本。
- 对公网提供服务必须配置可信 TLS。`tls-rustls` 支持 HTTPS 绑定，需提供有效证书和私钥；也可以显式配置可信反向代理。默认不信任转发头。
- SSE producer 可以获知客户端断开和服务关闭，但事件 ID、缓冲、持久化、重放和恢复由应用负责。连接关闭不会取消应用已经受理的业务工作。

安装、容量规划、TLS、流式连接和排障步骤见[中文用户指南](doc/user_guide.zh_CN.md)与[英文用户指南](doc/user_guide.md)。API 详情见 [API 文档](https://docs.rs/qubit-web)；另见[历史中文设计记录](doc/2026-10-08-rs-web-design.md)和[当前生命周期设计摘要（English）](doc/2026-10-09-rs-web-lifecycle-design.en.md)。
