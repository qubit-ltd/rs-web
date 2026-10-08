# qubit-web

[`qubit-web`](README.md) 为 Rust 服务提供可组合的 Axum 服务端基础设施：绑定 HTTP 或 HTTPS、装配 Controller 与原生路由、为选定的短请求设置有限额，并管理 SSE 和 WebSocket 生命周期。它适合需要自行实现服务行为的团队；应用状态、认证、授权和业务协议仍由应用负责。

## 安装

将 crate 加入项目，并只启用需要的能力：

```toml
[dependencies]
qubit-web = { version = "0.2", features = ["json", "ws"] }
```

默认不启用任何可选 feature。`json` 提供有界 JSON 辅助能力，`ws` 提供 WebSocket 策略支持，`tls-rustls` 用于 HTTPS 绑定，`config` 提供 `qubit-config` 配置适配器。

## 快速开始

可运行示例展示完整接入流程：用 `ServerOptions` 设置传输连接上限，单独创建 `HttpLimits`，将其装配到短请求路由，绑定并运行 Router，最后通过 `/admin/shutdown` 停服。

```sh
cargo run --example controller_crud --all-features
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/users \
  -H 'content-type: application/json' -d '{"name":"Ada"}'
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

健康检查成功时返回 `200 OK` 和 `ok`；创建用户返回 `201 Created`。示例只监听 loopback，数据保存在进程内存中。显式调用 `ControllerRoutes::with_http_limits` 的代码见 [`examples/controller_crud.rs`](examples/controller_crud.rs)。原生 Axum 路由需要在要保护的短路由分支上安装 `RequestLimitLayer::new(http_limits)`。`WebServer` 按调用方提供的 Router 运行，不会暗中安装 HTTP 限额。

启用 `config` feature 后，`ConfiguredWeb::from_config` 会分别返回 `ServerOptions` 和 `HttpLimits`。后者仍需由调用方传给路由装配处，显式安装后才会生效。

## 流式示例

`prompt_stream` 提供有界 JSON、可取消的 SSE producer 和 WebSocket echo handler：

```sh
cargo run --example prompt_stream --all-features
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

示例 bearer token `demo-only` 只用于演示 handler 检查，不构成认证。WebSocket 客户端可连接 `ws://127.0.0.1:3001/prompt/demo-1/ws`，发送 `Authorization: Bearer demo-only`；若发送 Origin，则须使用允许的 `http://localhost:3000`。这些端点只演示传输，不会运行 Agent、保存 Prompt 或持久化任务。

## 限额与运行边界

- 每个 `WebServer` 的传输连接上限默认为 1024，可通过 `ServerOptions::with_max_transport_connections` 配置。该上限覆盖 TLS 握手和 HTTP 连接 future。达到上限时服务会暂停接收连接；客户端可能在操作系统 backlog 中等待，也可能连接失败，因此不保证返回 HTTP 503。WebSocket 升级后由独立的 `WsUpgradePolicy` 容量控制。SSE 仍占用 HTTP 连接；应按预期长连接数量分别规划传输和 SSE 容量。
- `HttpLimits` 为普通短请求提供请求体大小、并发数和处理时间上限；只有应用通过 `ControllerRoutes::with_http_limits` 或 `RequestLimitLayer` 显式安装后才生效。Controller 的 `short` 路由受对应策略约束，`sse` 和 `ws` 路由会跳过短请求 layer。原生 Axum 路由需在选定分支单独安装 layer。
- HTTP/1 请求头默认最多等待 10 秒；WebSocket 的 HTTP/1 upgrade 也必须在此期限内发送完请求头。传输空闲期限默认 30 秒，可通过 `ServerOptions::with_transport_idle_timeout` 或配置键 `transport.idle_timeout_ms` 设置。连接在没有正在处理的请求或响应体时会被回收，包括空闲 HTTP/2 连接；活跃请求和 SSE 响应体会暂停空闲计时。
- 使用 `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`，可在返回升级响应前预留受管会话。Origin 默认可选：未携带 Origin 的请求会继续进入应用认证；携带 Origin 时，必须与 `allowed_origins` 中的值完全匹配，否则拒绝。没有 Origin 不代表已通过认证。
- `ServerContext::active_sessions()` 统计受管 SSE 和通过 `on_upgrade` 接纳的 WebSocket，包括握手尚未完成的升级。停服会用同一个截止时间等待 HTTP 连接和这些会话；`ShutdownReport::unfinished_managed_sessions` 记录截止时仍未结束的受管会话。原生 Axum 升级和应用后台任务不在统计范围内。
- `WebServerError` 通过 `std::error::Error::source()` 保留 socket I/O 错误供调用方诊断；默认 `Display` 和 `Debug` 不输出底层操作系统错误文本。
- 对公网提供服务必须配置可信 TLS。`tls-rustls` 支持 HTTPS 绑定，需提供有效证书和私钥；也可以显式配置可信反向代理。默认不信任转发头。
- SSE producer 可以获知客户端断开和服务关闭，但事件 ID、缓冲、持久化、重放和恢复由应用负责。连接关闭不会取消应用已经受理的业务工作。

安装、容量规划、TLS、流式连接和排障步骤见[中文用户指南](doc/user_guide.zh_CN.md)与[英文用户指南](doc/user_guide.md)。API 详情见 [API 文档](https://docs.rs/qubit-web)；另见[历史中文设计记录](doc/2026-10-08-rs-web-design.md)和[当前生命周期设计摘要（English）](doc/2026-10-09-rs-web-lifecycle-design.en.md)。
