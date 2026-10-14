# 为 HTTP 服务配置连接限额与流式接口

本文面向使用 `qubit-web` 0.2.0（Rust 1.94 或更新版本）的 Rust 服务开发者。我们用一个具体服务串起完整流程：接收有大小上限的 JSON 请求，通过 SSE 推送进度，可选地提供 WebSocket 回显，并由宿主控制关闭。示例里的 prompt 和 token 都是演示数据；它不会运行 Agent，演示 token 也不能用于生产认证。

先看[中文版 README](../README.zh_CN.md)了解项目范围和 feature 选择；也可查看[英文 README](../README.md)。阅读顺序是：准备可移植依赖、运行完整示例确认响应，再按需把路由和长连接 policy 接入自己的 Axum 应用，最后根据验证结果和故障现象排查。公开 API 细节可查[在线 API 文档](https://docs.rs/qubit-web)和 crate Rustdoc；[英文指南](user_guide.md)记录同一组事实。

## 配置并启动监听器

如果要把 `prompt_stream` 示例移植到独立项目，直接依赖必须覆盖示例实际导入的 crate。以下版本和 feature 与仓库示例一致：

```toml
[dependencies]
axum = "0.8"
futures-core = "0.3"
qubit-web = { version = "0.2", features = ["json", "ws"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "sync", "time"] }
```

这里列的是移植场景所需的直接依赖；只运行仓库自带示例时，Cargo 会使用仓库 manifest 中已有的依赖。启用 HTTPS 时按需给 `qubit-web` 增加 `tls-rustls`。只有希望把应用显式提供的 `qubit_config::Config` 转成服务配置时才需要 `config`；若代码直接引用 `qubit_config::Config`，还要在应用 manifest 中单独声明 `qubit-config`。`ConfiguredWeb` 不会替应用读取文件或全局配置。

先运行仓库里的完整示例，确认服务能启动并观察最小成功结果：

```sh
cargo run --example prompt_stream --features "json ws"
```

服务监听 loopback 地址，默认是 `127.0.0.1:3001`。另开终端提交 JSON：

```sh
curl -i -H 'content-type: application/json' -d '{"prompt":"demo"}' http://127.0.0.1:3001/prompt
```

预期为 HTTP 200，响应 JSON 含 `job_id: "demo-1"` 和提交的 prompt。完整响应式流与关闭检查见后文“运行、观察和关闭”；完成这一步后，再按下文将 handler、state 和连接策略接入自己的 Router。

连接额度按每个服务实例分别计算。默认上限是 1024；文件描述符、TLS 握手成本或内存预算较紧时，可以调小，但必须大于零：

```rust
use qubit_web::{ServerOptions, WebServer};

let options = ServerOptions::new("127.0.0.1:3001".parse()?)
    .with_max_transport_connections(256)?;
let server = WebServer::bind_http(options).await?;
let context = server.context();
```

传输连接数与短请求并发数必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`；传入 0 或更大值会得到 `WebServerError::InvalidConfig`。额度覆盖已接受的 socket、TLS 握手和 HTTP 连接 future；HTTP transport future 结束后，升级出的 WebSocket 不再占用这项额度，由 `WsUpgradePolicy` 管理。SSE 仍占用 HTTP transport 额度。

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

配置必须包含 `address`；`shutdown_timeout_ms` 缺省为 30000，`transport.max_connections` 和 `transport.idle_timeout_ms` 分别缺省为 1024 和 30000。`transport.max_connections` 和 `http.max_concurrent_requests` 必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`。`http.*` 配置只生成 `HttpLimits`，不会自动装到 Router：Controller 短路由已有 `HttpLimits::default()`，将配置值传给 `with_http_limits` 可替换该策略。原生 Axum 短路由需在选定分支安装 `RequestLimitLayer`；`WebServer` 不会修改 Router。解析错误可通过 `ConfigOptionsError::field()` 定位字段；容量错误分别指向原键 `http.max_concurrent_requests` 或 `transport.max_connections`，错误不会回显输入值。

## 装配普通接口与长连接

Controller 短路由默认使用每个请求体 1 MiB、每个 builder 共享 256 个并发请求、handler 工作及响应体传输 30 秒的预算。添加 Controller 前调用 `with_http_limits` 可替换该策略。原生 Axum 路由不会继承 Controller 限额，需在选定的普通短路由分支显式安装 `RequestLimitLayer`。`WebServer` 按原样服务调用方提供的 Router，不会添加路由 layer。限额约束被读取的请求体数据、处理中的 handler 并发数和所选分支的请求期限。handler 超过期限时 layer 返回 `504 Gateway Timeout`。无法表示的绝对请求期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限异常，layer 返回 HTTP 500 和代码 `invalid_configuration`。若响应头已经发出，期限到达会立即丢弃应用响应体并归还短请求许可，但无法更改状态码；下一次轮询响应体时才会报告一次超时错误。慢客户端或传输背压可能推迟客户端观察到该错误的时间。此期限不会关闭 TCP 或 HTTP/2 连接，连接仍由独立的传输策略管理：

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

片段中的 `submit`、`progress` 和 `websocket` 是应用自己的 handler；完整组合可参考 [`examples/prompt_stream.rs`](../examples/prompt_stream.rs)。Controller 短路由已有默认限额；调用 `ControllerRoutes::with_http_limits` 可替换默认策略。Controller 的 `sse` 和 `ws` 路由会跳过短请求 layer。SSE/WS 路由不要套用 `RequestLimitLayer`：它的期限还会覆盖响应流，只适合短请求。

`ControllerRoutes::add` 会在注册前检查当前 Controller 的全部路由声明。重复的 method/path、无效路径或匹配模板冲突（例如参数别名和重叠通配符）会返回 `ControllerRouteError`。不同 method 可以共用同一模板，`finish()` 直接返回 `Router`。检查范围是 builder 收集的声明；之后与原生 Axum Router 合并仍遵循 Axum 自身行为。`rest_controller` 也支持在同一模块中装配末段名称相同、但实际类型不同的 Controller。

### 在应用状态中共享 SSE 和 WebSocket policy

先运行 [`examples/prompt_stream.rs`](../examples/prompt_stream.rs) 看完整装配，再按应用职责移植。该示例启动时各创建一次长连接 policy，放进 `AppState`，最后由 `Router::with_state(state)` 提供给 handler。handler 从 `State<AppState>` 取出 policy 的克隆；克隆共享同一连接额度，重新调用 `default()` 或 `new()` 则会创建独立额度。

```rust,ignore
use axum::extract::State;
use axum::extract::WebSocketUpgrade;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::{IntoResponse, Response};
use axum::extract::ws::Message;
use qubit_web::{ServerContext, SseConnectionPolicy, WebServer, WsUpgradePolicy};

#[derive(Clone)]
struct AppState {
    context: ServerContext,
    sse: SseConnectionPolicy,
    ws: WsUpgradePolicy,
}

// 在组装应用、调用 Router::with_state 之前调用一次。
fn app_state(server: &WebServer) -> AppState {
    AppState {
        context: server.context(),
        sse: SseConnectionPolicy::default(),
        ws: WsUpgradePolicy::new().allowed_origins(["http://localhost:3000"]),
    }
}

async fn progress(State(state): State<AppState>) -> Response {
    let connection = match state.sse.begin(&state.context) {
        Ok(connection) => connection,
        Err(error) => return error.into_response(),
    };
    let cancellation = connection.cancellation_token();
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        for data in ["queued", "working", "complete"] {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                result = sender.send(axum::response::sse::Event::default()
                    .event("progress").data(data)) => {
                    if result.is_err() { break; }
                }
            }
        }
    });
    // 需同时复制示例中的 EventReceiver 类型及其 Stream 实现。
    connection.into_sse(EventReceiver(receiver)).into_response()
}

async fn websocket(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    // 示例占位认证：应用应替换为自己的身份认证，并在 upgrade 之前完成。
    if headers.get(header::AUTHORIZATION).and_then(|value| value.to_str().ok())
        != Some("Bearer demo-only")
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    state.ws.clone().on_upgrade(ws, &headers, state.context.clone(), |mut session| async move {
        while let Some(Ok(message)) = session.recv().await {
            match message {
                Message::Text(text) => {
                    if session.try_send(Message::text(format!("demo echo: {text}"))).is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    })
}
```

SSE handler 先通过 `begin` 登记会话，再把取消令牌交给事件生产任务；`EventReceiver` 将 mpsc 接收端适配为 `Stream`，`into_sse` 负责生成响应。客户端断开或服务关闭时，生产者收到取消信号并停止。上面的片段省略了 `EventReceiver` 的 `Stream` 实现，完整代码见示例链接。WebSocket handler 必须先检查 `Authorization`，验证失败返回 401，只有通过后才调用 `on_upgrade`。升级后的任务持续 `recv()` 入站消息，用 `try_send()` 回显文本；对端关闭或出站队列无法接纳消息时结束循环。示例中的 `Bearer demo-only` 只是演示占位符，不能提供生产认证。

示例把 `http://localhost:3000` 配置为 `WsUpgradePolicy::allowed_origins` 的 Origin allowlist。Origin 检查只约束浏览器来源，不能代替身份认证；应用应在 `on_upgrade` 前认证，并按自身部署设置允许的来源。SSE 有独立会话上限，同时仍占用 HTTP transport 额度；WebSocket 完成 HTTP upgrade 后改由其 policy 的会话上限管理。

使用配置适配器时，把 `configured.http_limits()` 在添加 Controller 前传给 `ControllerRoutes::with_http_limits`，即可替换 Controller 短路由默认策略；原生 Axum 短路由则在选定分支创建 `RequestLimitLayer`。Controller 的 `sse` 和 `ws` 路由跳过短请求 layer。`WebServer` 接收完整的 Axum `Router`，不会为路由添加限额。

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

关闭开始后，服务会原子地禁止新的受管 SSE/WS 会话登记、停止接收新连接，并通知现有会话。HTTP 连接和已登记会话共用同一个关闭截止时间，该期限由 `ServerOptions` 设置并经 `ServerContext` 提供给 WebSocket 会话；`WsUpgradePolicy` 不单独设置停服期限。无法表示的停服期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限异常，服务先取消会话并回收连接任务，再返回错误。只有两者都在期限内结束，`graceful` 才为 `true`。超时则由 `unfinished_managed_sessions` 记录截止时仍未结束的受管会话数。该报告不表示任意应用后台任务都已停止；`forced_connections` 为 `None`，因为 Axum 无法提供可证明的强制关闭数量。

服务按前文命令启动后，下面发送超过 1 MiB 的请求体，`Content-Length` 会让 middleware 在 JSON 解码前返回 413：

```sh
head -c 1048577 /dev/zero | curl -i -H 'content-type: application/json' --data-binary @- http://127.0.0.1:3001/prompt
```

预期状态为 HTTP 413，错误码为 `body_too_large`。示例程序没有内置前面展示的活跃数诊断路由；将该路由加到应用后即可查询：

```sh
curl -i http://127.0.0.1:3001/admin/active-sessions
```

SSE 地址为 `/prompt/demo-1/events`，连接后依次收到 `progress` 事件及 `queued`、`working`、`complete` 数据。WebSocket 地址为 `/prompt/demo-1/ws`：带 `Authorization: Bearer demo-only` 才会进入升级流程，文本消息会收到 `demo echo: ...` 回显；缺少或错误 token 时返回 401。该 token 是占位示范，Origin allowlist 也只限制浏览器来源，两者都不能代替应用的生产身份认证。若已添加前述 context-aware 活跃数诊断路由，SSE 或 WebSocket 未关闭时响应正文是当前计数；会话结束后应回到 0。

向演示服务发起关闭：

```sh
curl -i -X POST http://127.0.0.1:3001/admin/shutdown
```

首次调用触发关闭并返回 202，重复调用返回 410。示例不会额外打印 shutdown report；实际宿主应检查 `ShutdownReport`。

## 容量估算与故障定位

传输连接与长连接要分别预算。例如 transport 上限设为 256，SSE 策略再允许 80 个会话、WebSocket 策略允许 80 个会话；这些策略上限彼此独立，部署时还要结合文件描述符、内存、TLS CPU 和业务处理能力评估。transport 上限按 `WebServer` 实例计算，不是进程级总额度。SSE 仍是 HTTP transport future，会持续占用 transport 额度；WebSocket 完成 HTTP upgrade 后释放这项额度。

transport 许可用满后，accept loop 会在 accept 新 socket 前等待。客户端可能留在操作系统 listen backlog 中，也可能连接失败；尚未 accept 的连接没有 HTTP handler，因此不能保证返回 503。backlog 行为取决于部署平台，不应当作应用响应。

`request_header_timeout` 默认 10 秒，用于限制 HTTP/1 请求头；HTTPS TLS 握手也沿用该期限。`transport_idle_timeout` 默认 30 秒，在 handler 和响应体都不活跃时回收空闲 HTTP/1/2 连接。活跃请求停滞不会由空闲期限中断；此类情况应使用路由限额或部署层策略处理。`HttpLimits::with_request_timeout` 与传输空闲期限不同：Controller `short` 路由默认使用该请求期限，原生 Axum 路由则只有显式安装 `RequestLimitLayer` 后才使用。

对 WebSocket，`WsUpgradePolicy::idle_timeout` 也限制入站消息交付给应用时无进展的最长时间。若应用没有调用 `WsSession::recv()`，导致容量为 64 条消息的入站通道满载，期限到达后会话会以关闭码 `1013`（原因 `inbound backpressure`）关闭。应定期排空消息，并在接收后再转交较长的业务处理。`WsSession::try_send` 成功只表示消息进入出站队列，不保证消息已通过网络送达；对端或会话关闭后，它会返回 `WsSendError::Closed`（匹配变体时为 `Closed`）。

`WsUpgradePolicy::validate()` 按最终配置字段值校验；同一 setter 后续设置的值会覆盖此前的值。`max_connections` 的合法范围是 `1..=tokio::sync::Semaphore::MAX_PERMITS`：零值返回 `WsPolicyError::ZeroLimit`，超过上限返回 `WsPolicyError::ConnectionLimitTooLarge`。无效策略会使 `on_upgrade` 返回 HTTP 500，不能完成升级。例如，先设为零、再修正为 1 后，校验通过：

```rust,ignore
assert!(WsUpgradePolicy::new()
    .max_connections(0)
    .max_connections(1)
    .validate()
    .is_ok());
```

| 现象 | 检查方式 |
| --- | --- |
| 过载时客户端等待或连接失败 | 检查该实例的 transport 额度和系统 backlog。accept 之前不会有保证的 503。 |
| 请求体超过上限但仍成功 | 对 Controller `short` 路由，确认 handler/extractor 实际读取请求体，并检查是否用 `with_http_limits` 替换成了更大的预算。对原生 Axum 路由，在该分支安装 `RequestLimitLayer`，并确认请求体被读取。 |
| SSE 在普通请求期限到达时关闭 | 从该流式分支移除 `RequestLimitLayer`，改用 `SseConnectionPolicy` 的连接策略。 |
| 慢客户端没有立即观察到短响应超时 | 响应头发出后，期限会释放应用响应体和短请求许可；只有传输再次轮询时才能报告 body error。它不会更改响应状态或关闭连接；传输连接由独立策略管理。 |
| WS 建立后 `active_sessions()` 仍为 0 | 调用 `WsUpgradePolicy::on_upgrade` 并传入服务的 `ServerContext`；原生 Axum upgrade 不属于受管会话统计。 |
| `WsUpgradePolicy::validate()` 报告连接上限无效 | 检查最终的 `max_connections`：零值对应 `WsPolicyError::ZeroLimit`；超过 `tokio::sync::Semaphore::MAX_PERMITS` 对应 `WsPolicyError::ConnectionLimitTooLarge`。修正设置后重新校验；无效策略不能升级。 |
| 入站负载下 WebSocket 以 `1013` 关闭 | 应用可能没有及时调用 `WsSession::recv()`。入站通道容量为 64 条消息；若通道持续满载且 `idle_timeout` 内交付没有进展，会话就会关闭。应持续读取消息，或先接收再交给应用管理的任务处理；根据预期处理间隔设置合适的 `idle_timeout`。 |
| 配置解析失败 | 查看 `ConfigOptionsError::field()`，检查地址、正数限额/期限、处于 `1..=tokio::sync::Semaphore::MAX_PERMITS` 的并发容量和整数类型。容量错误会指向 `http.max_concurrent_requests` 或 `transport.max_connections`，错误文本不会包含配置值。 |
| HTTPS/WSS 无法连接 | 检查证书链、私钥匹配和客户端信任；TLS 阶段失败时请求还没进入 HTTP 路由。 |
| 关闭时间超过预期 | 检查 shutdown timeout，并确认 handler/流生产者会响应取消。 |

`WebServerError` 通过 `std::error::Error::source()` 暴露保留的 socket 错误。默认 `Display` 和 `Debug` 只显示错误类别；需要排查绑定或 accept 故障时，再由调用方显式检查 `source()`。

继续阅读[英文指南](user_guide.md)、[中文版 README](../README.zh_CN.md)、[API 文档](https://docs.rs/qubit-web)、[历史中文设计记录](2026-10-08-rs-web-design.md)和[当前中文生命周期设计摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)。
