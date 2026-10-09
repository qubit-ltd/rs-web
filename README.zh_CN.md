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

可运行示例展示完整接入流程：用 `ServerOptions` 设置传输连接上限，Controller 短路由使用默认 HTTP 限额（也可用 `HttpLimits` 替换），在选定的原生 Axum 短路由上显式安装限额，绑定并运行 Router，最后通过 `/admin/shutdown` 停服。

先在终端 A 启动示例。等它打印出监听地址后，再打开终端 B 发请求：

```sh
cargo run --example controller_crud --all-features
```

终端 B 中依次检查健康接口、创建用户，最后关闭服务：

```sh
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/users \
  -H 'content-type: application/json' -d '{"name":"Ada"}'
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

健康检查成功时返回 `200 OK` 和 `ok`；创建用户返回 `201 Created`。示例只监听 loopback，数据保存在进程内存中。用 `ControllerRoutes::with_http_limits` 替换 Controller 短路由默认限额的代码见 [`examples/controller_crud.rs`](examples/controller_crud.rs)。原生 Axum 路由需要在要保护的短路由分支上安装 `RequestLimitLayer::new(http_limits)`。`WebServer` 按调用方提供的 Router 运行，不会暗中安装 HTTP 限额。

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

- 每次通过 `new` 或 `default` 创建 `SseConnectionPolicy`、`WsUpgradePolicy` 都会开启独立容量域；克隆 policy 才会共享容量。需要路由共享时，在 `AppState` 中各创建一次，再由 handler 通过 `State<AppState>` 取出并克隆。示例见[中文用户指南](doc/user_guide.zh_CN.md)或[英文用户指南](doc/user_guide.md)。每个 `WebServer` 的传输连接上限默认为 1024，可通过 `ServerOptions::with_max_transport_connections` 配置。该上限覆盖 TLS 握手和 HTTP 连接 future。达到上限时服务会暂停接收连接；客户端可能在操作系统 backlog 中等待，也可能连接失败，因此不保证返回 HTTP 503。policy 容量与传输上限分别计算；SSE 仍占用 HTTP 连接。
- `ControllerRoutes::new()` 会为 Controller 的 `short` 路由安装 `HttpLimits::default()`：每个请求体 1 MiB、每个 builder 共享 256 个并发请求，处理及响应体期限为 30 秒。`with_http_limits` 用于替换默认值，并应在添加 Controller 前调用。Controller 的 `sse` 和 `ws` 路由会跳过短请求 layer。原生 Axum 路由需在选定分支显式安装 `RequestLimitLayer`；`WebServer` 不会安装路由限额。
- `ControllerRoutes::add` 会在注册前检查重复方法、无效路径和冲突的路由模板（包括参数别名及重叠通配符），并返回 `ControllerRouteError`。不同方法可以共用同一模板；`finish()` 直接返回 Axum `Router`。
- HTTP/1 请求头默认最多等待 10 秒；WebSocket 的 HTTP/1 upgrade 也必须在此期限内发送完请求头。传输空闲期限默认 30 秒，可通过 `ServerOptions::with_transport_idle_timeout` 或配置键 `transport.idle_timeout_ms` 设置。连接在没有正在处理的请求或响应体时会被回收，包括空闲 HTTP/2 连接；活跃请求和 SSE 响应体会暂停空闲计时。
- 使用 `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`，可在返回升级响应前预留受管会话。Origin 默认可选：未携带 Origin 的请求会继续进入应用认证；携带 Origin 时，必须与 `allowed_origins` 中的值完全匹配，否则拒绝。没有 Origin 不代表已通过认证。对端或会话关闭后，`try_send` 会返回 `Closed`；成功入队本身不保证消息已通过网络送达。若应用不读取入站消息，且交付在 `idle_timeout` 内没有进展，会话会以 WebSocket 关闭码 `1013`（暂时无法处理）关闭。
- `ServerContext::active_sessions()` 统计受管 SSE 和通过 `on_upgrade` 接纳的 WebSocket，包括握手尚未完成的升级。停服会使用由 `ServerOptions` 设置、经 `ServerContext` 提供的同一个截止时间等待 HTTP 连接和这些会话；WebSocket policy 不再单独设置停服期限。`ShutdownReport::unfinished_managed_sessions` 记录截止时仍未结束的受管会话。原生 Axum 升级和应用后台任务不在统计范围内。
- `WebServerError` 通过 `std::error::Error::source()` 保留 socket I/O 错误供调用方诊断；默认 `Display` 和 `Debug` 不输出底层操作系统错误文本。
- 对公网提供服务必须配置可信 TLS。`tls-rustls` 支持 HTTPS 绑定，需提供有效证书和私钥；也可以显式配置可信反向代理。默认不信任转发头。
- SSE producer 可以获知客户端断开和服务关闭，但事件 ID、缓冲、持久化、重放和恢复由应用负责。连接关闭不会取消应用已经受理的业务工作。

安装、容量规划、TLS、流式连接和排障步骤见[中文用户指南](doc/user_guide.zh_CN.md)与[英文用户指南](doc/user_guide.md)。API 详情见 [API 文档](https://docs.rs/qubit-web)；另见[历史中文设计记录](doc/2026-10-08-rs-web-design.md)和[当前生命周期设计摘要（English）](doc/2026-10-09-rs-web-lifecycle-design.en.md)。
