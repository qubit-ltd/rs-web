# 为 HTTP 服务配置连接限额与流式接口

本文面向使用 `qubit-web` 0.2.0（Rust 1.94 或更新版本）的 Rust 服务开发者。我们用一个具体服务串起完整流程：接收有大小上限的 JSON 请求，通过 SSE 推送进度，可选地提供 WebSocket 回显，并由宿主控制关闭。示例里的 prompt 和 token 都是演示数据；它不会运行 Agent，演示 token 也不能用于生产认证。

先看[中文版 README](../README.zh_CN.md)了解项目范围和 feature 选择；也可查看[英文 README](../README.md)。本文聚焦接入、运行和排障；[英文指南](user_guide.md)介绍同一场景。公开 API 细节可查[在线 API 文档](https://docs.rs/qubit-web)和 crate Rustdoc。

## 配置并启动监听器

按服务所需功能添加 `qubit-web`。下面的示例需要 JSON 和 WebSocket：

```toml
[dependencies]
qubit-web = { version = "0.2", features = ["json", "ws"] }
```

启用 HTTPS 时再加 `tls-rustls`。只有希望把应用显式提供的 `qubit_config::Config` 转成服务配置时才需要 `config`；`ConfiguredWeb` 不会替应用读取文件或全局配置。

连接额度按每个服务实例分别计算。默认上限是 1024；文件描述符、TLS 握手成本或内存预算较紧时，可以调小，但必须大于零：

```rust
use qubit_web::{ServerOptions, WebServer};

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_max_transport_connections(256)?;
let server = WebServer::bind_http(options).await?;
let context = server.context();
```

传入 0 会得到 `WebServerError::InvalidConfig`。额度覆盖已接受的 socket、TLS 握手和 HTTP 连接 future；HTTP transport future 结束后，升级出的 WebSocket 不再占用这项额度，由 `WsUpgradePolicy` 管理。SSE 仍占用 HTTP transport 额度。

连接在没有活跃请求或响应体时，默认空闲 30 秒后关闭。部署需要其他回收周期时，可设置一个大于零的期限：

```rust
use std::time::Duration;

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_transport_idle_timeout(Duration::from_secs(45));
assert_eq!(options.transport_idle_timeout(), Duration::from_secs(45));
```

handler 正在运行或响应体仍活跃时，空闲计时会暂停，因此长时间 SSE 不会被此期限关闭。它也会回收尚未开始处理请求的 HTTP/2 连接；HTTP/2 PING 不算请求活动。启用 `config` 后，`transport.idle_timeout_ms` 可设置同一期限，缺省为 `30000`，不能设为 0。`request_header_timeout` 仍是独立的 10 秒 HTTP/1 请求头和 TLS 握手期限。

启用 `config` feature 后，可以拆分监听设置和路由限额：

```rust,ignore
let configured = ConfiguredWeb::from_config(&config)?;
let (server_options, http_limits) = configured.into_parts();
let server = WebServer::bind_http(server_options).await?;
```

配置必须包含 `address`；`shutdown_timeout_ms` 缺省为 30000，`transport.max_connections` 和 `transport.idle_timeout_ms` 分别缺省为 1024 和 30000。`http.*` 配置只生成 `HttpLimits`，不会自动装到 Router。解析错误可通过 `ConfigOptionsError::field()` 定位字段，错误不会回显输入值。

## 装配普通接口与长连接

普通短请求要显式安装限额。它约束被读取的请求体数据、处理中的 handler 并发数和该路由分支的请求期限。没有安装的路由不会自动继承这些设置：

```rust
use std::time::Duration;
use axum::{Router, routing::{get, post}};
use qubit_web::{HttpLimits, RequestLimitLayer};

let http_limits = HttpLimits::default()
    .with_max_body_bytes(1024 * 1024)?
    .with_max_concurrent_requests(128)?
    .with_request_timeout(Duration::from_secs(20))?;
let app = Router::new()
    .route("/prompt", post(submit).layer(RequestLimitLayer::new(http_limits)))
    .route("/events", get(progress))
    .route("/ws", get(websocket));
```

片段中的 `submit`、`progress` 和 `websocket` 是应用自己的 handler；完整组合可参考 [`examples/prompt_stream.rs`](../examples/prompt_stream.rs)。Controller 路由也可以通过 `ControllerRoutes::with_http_limits` 安装同一策略。SSE/WS 路由不要套用 `RequestLimitLayer`：它的期限还会覆盖响应流，只适合短请求。

使用配置适配器时，把 `configured.http_limits()` 传给 Controller builder，或在选定的短路由上创建 `RequestLimitLayer`。`WebServer` 接收完整的 Axum `Router`，不会暗中修改它。

启用 HTTPS 时，在绑定前加载可信证书材料，再传给 `WebServer::bind_https`：

```rust,ignore
let tls = TlsConfig::from_pem_files(certificate_chain, private_key).await?;
let server = WebServer::bind_https(options, tls).await?;
```

部署时应使用客户端信任的证书，或在可信边缘终止 TLS 并由应用明确处理相关配置。仓库测试证书是自签名 fixture，不能直接作为部署凭据。同一个 Router 可用于 HTTPS；其 WebSocket 升级自然使用 WSS。

## 运行、观察和关闭

在把 server 移入服务任务前复制一份 `ServerContext`。通过 context-aware 入口创建的 SSE 和 WebSocket 会登记到活跃会话数。应用可以自行添加诊断路由：

```rust,ignore
let diagnostics_context = server.context();
let app = app.route("/admin/active-sessions", get(move || {
    let context = diagnostics_context.clone();
    async move { context.active_sessions().to_string() }
}));
```

这段是应用代码，不是库自带的管理端点。生产环境要用应用已有的认证和网络策略保护它。`active_sessions()` 统计受管 SSE 和通过 `WsUpgradePolicy::on_upgrade` 接纳的 WebSocket，包括升级响应尚未完成的握手；普通 HTTP 请求、原生 Axum 升级和任意应用后台任务不计入。

关闭由宿主触发：服务正常运行时让 shutdown future 保持等待，进程信号或 supervisor 请求停止时再让它完成：

```rust,ignore
let report = server.serve(app, shutdown_signal).await?;
assert!(report.graceful);
```

关闭开始后，服务会原子地禁止新的受管 SSE/WS 会话登记、停止接收新连接，并通知现有会话。HTTP 连接和已登记会话共用同一个关闭截止时间；只有两者都在期限内结束，`graceful` 才为 `true`。超时则由 `unfinished_managed_sessions` 记录截止时仍未结束的受管会话数。该报告不表示任意应用后台任务都已停止；`forced_connections` 为 `None`，因为 Axum 无法提供可证明的强制关闭数量。

运行演示服务：

```sh
cargo run --example prompt_stream --features "json ws"
```

它会打印 loopback 地址，默认是 `127.0.0.1:3001`。请求普通接口：

```sh
curl -i -H 'content-type: application/json' -d '{"prompt":"demo"}' http://127.0.0.1:3001/prompt
```

成功时返回 HTTP 200，JSON 中的 `job_id` 是 `demo-1`。下面发送超过 1 MiB 的请求体，`Content-Length` 会让 middleware 在 JSON 解码前返回 413：

```sh
head -c 1048577 /dev/zero | curl -i -H 'content-type: application/json' --data-binary @- http://127.0.0.1:3001/prompt
```

预期状态为 HTTP 413，错误码为 `body_too_large`。示例程序没有内置前面展示的活跃数诊断路由；将该路由加到应用后即可查询：

```sh
curl -i http://127.0.0.1:3001/admin/active-sessions
```

SSE 或 context-aware WebSocket 未关闭时，响应正文是当前计数；会话结束后应回到 0。演示 SSE 地址为 `/prompt/demo-1/events`，WebSocket 地址为 `/prompt/demo-1/ws`。其 `Bearer demo-only` 检查只是占位示范。

向演示服务发起关闭：

```sh
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

首次调用触发关闭并返回 202，重复调用返回 410。示例不会额外打印 shutdown report；实际宿主应检查 `ShutdownReport`。

## 容量估算与故障定位

传输连接与长连接要分别预算。例如 transport 上限设为 256，SSE 策略再允许 80 个会话、WebSocket 策略允许 80 个会话；这些策略上限彼此独立，部署时还要结合文件描述符、内存、TLS CPU 和业务处理能力评估。transport 上限按 `WebServer` 实例计算，不是进程级总额度。SSE 仍是 HTTP transport future，会持续占用 transport 额度；WebSocket 完成 HTTP upgrade 后释放这项额度。

transport 许可用满后，accept loop 会在 accept 新 socket 前等待。客户端可能留在操作系统 listen backlog 中，也可能连接失败；尚未 accept 的连接没有 HTTP handler，因此不能保证返回 503。backlog 行为取决于部署平台，不应当作应用响应。

`request_header_timeout` 默认 10 秒，用于限制 HTTP/1 请求头；HTTPS TLS 握手也沿用该期限。`transport_idle_timeout` 默认 30 秒，在 handler 和响应体都不活跃时回收空闲 HTTP/1/2 连接。活跃请求停滞不会由空闲期限中断；此类情况应使用路由限额或部署层策略处理。它与 `HttpLimits::with_request_timeout` 不同，后者只在短路由显式安装 `RequestLimitLayer` 后生效。

| 现象 | 检查方式 |
| --- | --- |
| 过载时客户端等待或连接失败 | 检查该实例的 transport 额度和系统 backlog。accept 之前不会有保证的 503。 |
| 请求体超过上限但仍成功 | 确认路由装了 `RequestLimitLayer`，或 Controller 分支使用 `with_http_limits`；并确认 handler/extractor 实际读取请求体。 |
| SSE 在普通请求期限到达时关闭 | 从该流式分支移除 `RequestLimitLayer`，改用 `SseConnectionPolicy` 的连接策略。 |
| WS 建立后 `active_sessions()` 仍为 0 | 调用 `WsUpgradePolicy::on_upgrade` 并传入服务的 `ServerContext`；原生 Axum upgrade 不属于受管会话统计。 |
| 配置解析失败 | 查看 `ConfigOptionsError::field()`，检查地址、正数限额/期限和整数类型。错误文本不会包含配置值。 |
| HTTPS/WSS 无法连接 | 检查证书链、私钥匹配和客户端信任；TLS 阶段失败时请求还没进入 HTTP 路由。 |
| 关闭时间超过预期 | 检查 shutdown timeout，并确认 handler/流生产者会响应取消。 |

`WebServerError` 通过 `std::error::Error::source()` 暴露保留的 socket 错误。默认 `Display` 和 `Debug` 只显示错误类别；需要排查绑定或 accept 故障时，再由调用方显式检查 `source()`。

继续阅读[英文指南](user_guide.md)、[中文版 README](../README.zh_CN.md)、[API 文档](https://docs.rs/qubit-web)、[历史中文设计记录](2026-10-08-rs-web-design.md)和[当前中文生命周期设计摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)。
