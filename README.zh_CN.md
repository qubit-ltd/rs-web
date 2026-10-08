# qubit-web

`qubit-web` 为 Rust 服务提供可组合的服务端基础设施。它运行 Axum `Router`，支持显式装配 REST Controller 路由、受限 JSON 与 HTTP 请求策略，以及 SSE 和 WebSocket 连接生命周期辅助能力。应用仍自行管理状态、认证、授权和业务协议。

## 示例

示例仅监听 loopback，使用内存或演示数据。启用全部可选 feature 运行：

```sh
cargo run --example controller_crud --all-features
cargo run --example prompt_stream --all-features
```

`controller_crud` 组合使用 `#[rest_controller]`、`#[get_mapping]`、`#[post_mapping]`、Axum `Path`/`Query`、`BoundedJson`，以及原生 `/health` 路由。记录只存于进程内存，重启后会清空。可以这样请求：

```sh
curl -i http://127.0.0.1:3000/health
curl -i -X POST http://127.0.0.1:3000/users \
  -H 'content-type: application/json' -d '{"name":"Ada"}'
curl -i 'http://127.0.0.1:3000/users?name_prefix=A'
curl -i http://127.0.0.1:3000/users/1
curl -i -X POST http://127.0.0.1:3000/admin/shutdown
```

`ControllerRoutes` 由应用显式传入 Controller 实例；宏不会创建应用对象，也不会全局自动发现 Controller。生成的 Router 可与原生 Axum 路由、状态、提取器和中间件合并。

`prompt_stream` 演示 HTTP 受限 JSON、带取消能力且使用有界通道的 SSE producer，以及经过演示认证的 WebSocket echo handler。示例 bearer token 是 `demo-only`，仅用于演示，不构成认证机制。WebSocket 客户端可使用示例允许的 Origin（`http://localhost:3000`）；若不发送 `Origin`，仍须发送 `Authorization: Bearer demo-only`。这些端点只模拟传输，不运行 Agent、不保存 Prompt，也不提供任务持久化。

```sh
curl -i -X POST http://127.0.0.1:3001/prompt \
  -H 'content-type: application/json' -d '{"prompt":"Summarize this"}'
curl -N http://127.0.0.1:3001/prompt/demo-1/events
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

使用 WebSocket 客户端连接 `ws://127.0.0.1:3001/prompt/demo-1/ws`，并发送 `Authorization: Bearer demo-only`（若发送 Origin，则必须使用允许的 Origin）。示例会返回带前缀的 echo 消息。

## 安全与运行边界

- 直接对公网提供服务必须配置可信 TLS。可选 `tls-rustls` feature 支持 HTTPS 绑定；生产环境必须提供有效证书和私钥。也可以显式配置可信反向代理。默认不信任转发头。
- 库不提供认证或授权。应用必须在接受 WebSocket 升级前完成认证，并根据客户端选择 Origin allowlist。缺少 Origin 不代表客户端已通过认证。
- HTTP/1 请求头块默认有 10 秒超时，可通过 `ServerOptions::with_request_header_timeout` 配置；该设置也限制 HTTP/1 upgrade 请求发送完请求头的等待时间。它与应用 WebSocket 会话的持续时间不同。
- SSE 断开和服务关闭时会向事件 producer 提供取消通知，但事件 ID、`Last-Event-ID`、缓冲、持久化、重放和恢复都由应用负责。写入成功不代表客户端已消费事件。
- 关闭 HTTP/SSE/WS 连接，不代表应用取消了已经受理的业务任务。
- `ServerOptions::http_limits()` 提供服务限额基线；构建 Router 时将其传给 `ControllerRoutes::with_http_limits` 或 `RequestLimitLayer::new`。`WebServer` 保留调用方构造的 Router，不会重写路由类别。Controller 的 `short` 路由默认获得有限限额；`sse` 和 `ws` 路由跳过短请求 layer。
- 使用 `config` 时，通过 `ServerOptions::from_config` 构造选项，再在调用 `serve` 前将 `options.http_limits()` 传给需要保护的 Router 分支。
- 原生 Axum 路由只在应用安装 `RequestLimitLayer` 的分支上受限。普通 Axum extractor 不会自动获得 `BoundedJson` 的结构化 JSON 预算保护。
- 推荐使用 `WsUpgradePolicy::on_upgrade_with_context`，让 WebSocket 共用服务取消 token 和关闭期限。若使用较底层的 token 版 `on_upgrade`，需将 `WsUpgradePolicy::shutdown_timeout` 设为不大于 `ServerOptions::shutdown_timeout`。

## 可选 feature

默认 feature 集为空。应用可按需启用：

| Feature | 能力 |
| --- | --- |
| `json` | `BoundedJson`、结构化 JSON 预算和有界 JSON 响应 |
| `ws` | WebSocket upgrade 策略和有界会话发送队列 |
| `tls-rustls` | 使用 rustls 绑定 HTTPS |
| `config` | 将 `qubit-config` 配置转换为服务选项 |

需求和设计详情见 [`doc/2026-10-08-rs-web-prd.md`](doc/2026-10-08-rs-web-prd.md) 与 [`doc/2026-10-08-rs-web-design.md`](doc/2026-10-08-rs-web-design.md)。
