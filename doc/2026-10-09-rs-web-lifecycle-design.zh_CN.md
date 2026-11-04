# rs-web 生命周期与资源边界设计摘要

> 日期：2026-10-09。本文记录 `qubit-web` 0.3.0 的现行合同。接入步骤见[中文用户指南](user_guide.zh_CN.md)和[英文用户指南](user_guide.md)；[2026-10-08 设计记录](2026-10-08-rs-web-design.md)属于历史文档，其中部分 API 草案已被取代。

## 长连接请求接纳

Controller 的 `sse`/`ws` 路由和原生 Axum 长连接路由，应在启动流或 upgrade 前限制请求接纳。Controller 路由共用 builder 创建的 `StreamAdmissionState`；原生 Axum 路由需显式安装 `StreamAdmissionLayer`，多个分支要共享容量时可通过 `StreamAdmissionState`/`from_state` 共用状态。每个状态域默认每个请求体缓冲 1 MiB、同时接纳 256 个请求，且响应头产生前的期限为 10 秒；请求体缓冲理论上限为每个状态域 256 MiB，另有请求和运行时开销。中间件会在 handler 执行前收齐完整请求体；413 `body_too_large`、400 `body_read_failed`、503 `capacity_exceeded` 和 504 `request_timeout` 分别表示请求体超限、读取失败、容量耗尽和接纳超时。运行时无法表示绝对接纳期限时，layer 返回 HTTP 500 `invalid_configuration`。该期限不覆盖 SSE 响应体或 WebSocket 会话。此接纳预算独立于短请求 `RequestLimitLayer`，也独立于下文的 SSE/WebSocket 会话策略。

## 响应体诊断

`DiagnosticLayer::new()` 仍保持只记录响应头的行为。应用显式安装 `DiagnosticLayer::with_body_diagnostics()` 后即可记录响应体终止事件；该 layer 应放在 `RequestLimitLayer` 外侧，以便即使传输停止轮询，也能收到限流层的超时通知。每个请求至多发出一次 `response_body_end`，结果为 `complete`、`timeout`、`body_error` 或 `dropped`。`status` 是响应头状态，耗时在观察到终止状态时结束。事件只使用允许列表中的请求元数据，不含正文和错误文本。`dropped` 不能证明客户端断连，`complete` 也不能证明网络送达。WebSocket upgrade 的 HTTP 响应体结束不表示后续 WebSocket 会话结束；SSE 也只按 HTTP 响应体生命周期观察。

## 受管会话登记与停服

`ServerContext::try_register_session()` 会在一个原子操作中登记会话；停服开始后，新的登记失败。`active_sessions()` 包含受管 SSE，以及通过 `WsUpgradePolicy::on_upgrade` 接纳的 WebSocket；即使升级响应尚未完成，计数也已增加。停服后，新的 SSE/WS 接纳会被拒绝。普通 HTTP 请求、原生 Axum upgrade 和任意应用后台任务不属于受管会话。

SSE 与 WebSocket 容量限制属于彼此独立的策略域，不是整个服务统一的会话上限。每次调用 `SseConnectionPolicy::new`/`default` 或 `WsUpgradePolicy::new`/`default` 都会创建独立容量域；同一策略的克隆共享该域。两类策略各自的默认容量都是每个策略实例 128 个连接。`WsUpgradePolicy` 按最终字段值校验：`max_connections` 必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`；零值返回 `WsPolicyError::ZeroLimit`，超过上限返回 `WsPolicyError::ConnectionLimitTooLarge`。无效策略不能升级。因此，为不同路由分别创建策略会累加可接纳容量。若要让多条路由共用一个上限，应在应用启动时创建一个策略并放入应用状态，再使用从该状态取得的克隆。`ServerContext::active_sessions()` 用于停服统计，不是额外的数值接纳上限。

停服时服务停止接收连接、关闭会话登记入口并通知现有会话。HTTP 连接任务与受管会话共用一个绝对截止时间。只有两者实际在截止时刻或之前结束，`ShutdownReport::graceful` 才为 `true`。服务在连接任务释放连接资源时记录完成时刻，并使用会话 tracker 的截止快照；调度迟滞或报告任务晚恢复不会把按时完成误判为超时，而等待返回 `Ok` 也不会使逾期完成变成优雅停服。`unfinished_managed_sessions` 记录截止时仍未结束的受管会话数。报告不表示任意应用工作都已停止。`WebServer` 拥有并跟踪连接任务，Hyper/hyper-util 驱动各连接。本实现尚不提供具有明确定义且可证明的强制关闭连接计数，因此 `forced_connections` 为 `None`。

停服时长仅通过 `ServerOptions` 配置，并由 `ServerContext` 统一提供；`WsUpgradePolicy` 不再设置独立的停服期限。WebSocket reader 和 writer 的停服使用服务器共享的绝对截止时间。

`serve` 已被轮询后，其 future 被丢弃或任务被 abort 时，会同步关闭受管会话登记并通知现有会话。Drop 无法等待异步排空或生成 `ShutdownReport`；Tokio 必须继续调度后续清理。需要优雅排空并取得报告的宿主应完成 shutdown future 并等待 `serve` 返回。未轮询的 `serve` future 不在此取消保证范围内。

## 传输空闲期限

`ServerOptions::transport_idle_timeout` 默认 30 秒，且不能为零。配置键是 `transport.idle_timeout_ms`，缺省值为 `30000`。TCP/TLS 连接在没有活跃 handler 或响应体时达到空闲期限后关闭；这也包括尚未发送首个 HTTP/2 请求的连接。handler 和响应体活跃期间，空闲计时暂停，因此长连接 SSE 不会被误判为空闲。HTTP/2 PING 不会重置请求活动时间。独立的 `request_header_timeout` 仍为 HTTP/1 请求头及 TLS 握手设置 10 秒期限；空闲期限不会中断停滞中的活跃请求。

## WebSocket 队列与入站交付

WebSocket 发送队列的所有克隆共享关闭状态。对端或会话关闭后，`try_send` 会返回 `Closed`；成功入队不保证消息已通过网络送达。出站 `queue_limits` 与入站 `inbound_queue_limits(messages, bytes)` 分开配置。入站默认每连接最多排队 64 条消息、累计 1 MiB payload；reader 等待通道空位时最多另持有一条消息，且同样计入字节预算。128 个活跃默认连接的排队/待交付 payload 约为 128 MiB，每个活跃连接另最多有一条 1 MiB 协议重组消息。该估算不含分配开销和应用自有内存，也不限定 I/O 结束后仍被 handler 或 session 保留的内存。字节预算不足以容纳新消息时，消息会被丢弃并立即以关闭码 `1013` 关闭；仅通道槽满时，`WsUpgradePolicy::idle_timeout` 限制无进展等待时间，到期后以 `1013`、原因 `inbound backpressure` 关闭。`WsSession::recv()` 将消息所有权交给应用后，该消息不再计入队列预算。应用应持续接收消息，并在收到后再转交较长的业务处理。

服务取消时，普通写入会停止等待刷新完成。对端发送 Close 后，writer 的最终刷新受服务器共享的关闭截止时间约束。发生背压时，Close 帧可能无法送达。writer 退出时会释放其连接策略许可，并解除受管会话登记。

## 会话关闭与时间配置边界

`WsSession::close()` 可幂等调用。如果等待期间取消 close future，可以再次调用重试；显式关闭后 `recv()` 返回 `None`，并丢弃尚未交付的读错误。传输读取错误保存在有界通道之外；取消清理完成后，可通过一次 `Some(Err(_))` 交付，之后读取返回 `None`。如果 `recv()` 在清理等待期间被取消，错误仍保留，可由后续调用取得。宿主停服可能直接取消 handler，不保证交付该错误。清空通道时会 drop 尚未交付的入站消息封装并归还其字节预算。writer 退出时会归还出站队列字节预算、策略许可和受管会话登记，包括 sink shutdown 一直处于 Pending 的情况。HTTP 请求头期限和 SSE keep-alive interval 若为零，或无法由 `Tokio Instant::checked_add` 表示（包括 `Duration::MAX`），会以 `WebServerError::InvalidConfig` 拒绝。

`WsSession::closed()` 返回拥有自身状态的 `Send + 'static` future，可在 I/O 结束前或之后创建，也可由多个等待者同时使用。reader 与 writer 都释放连接许可和受管会话登记后，该 future 才就绪；这不表示 Close 帧已送达、handler 或应用工作已结束，也不表示入站消息已消费。应用可将它与可安全取消的工作一起 select，收到通知后调用 `close()` 清理队列。该通知不会取消 handler 或应用自行启动的任务。若业务工作必须继续，应先转移所需输入，再由应用独立管理。

## Origin 与应用认证

`WsUpgradePolicy::new()` 默认允许不携带 Origin 的请求继续进入应用认证。请求若携带 Origin，则默认拒绝；只有配置 `allowed_origins` 并精确匹配后才会接纳。应用仍须在调用 `on_upgrade` 前完成认证。Origin 校验不等同于身份认证。

## I/O 错误边界与 API 迁移

`WebServerError` 会保留 socket 绑定、本地地址查询和 accept 故障的 `io::Error` source，调用方可通过 `std::error::Error::source()` 主动诊断。默认 `Display` 和 `Debug` 不输出底层操作系统错误文本；TLS 与配置错误也不会暴露可能包含敏感信息的 source 链。

历史说明：0.2.0 曾引入以下破坏性变更：移除只接收 token 的 WebSocket 升级入口，统一使用 `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`；WebSocket 停服期限统一通过 `ServerOptions` 配置并由 `ServerContext` 提供；`WebServerError` 不再是可复制、无 source 的枚举；SSE 接纳错误区分容量耗尽和停服拒绝。

## 协作式取消边界

期限依赖 Tokio 获得调度机会。不让出执行权的同步工作会延迟超时检查和清理，也可能在期限之后完成。取消等待不会停止已经运行的 `spawn_blocking` 任务或应用自行启动的工作。应用应为 CPU 密集或阻塞任务设置独立并发预算，需要强制终止时使用进程隔离。字节和结构限额不限制用户 JSON `Serialize` 或 `Deserialize` 实现的 CPU 时间；取消配合和业务副作用仍由应用负责。
