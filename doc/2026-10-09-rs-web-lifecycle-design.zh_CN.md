# rs-web 生命周期与资源边界设计摘要

> 日期：2026-10-09。本文记录 `qubit-web` 0.2.0 的现行合同。接入步骤见[中文用户指南](user_guide.zh_CN.md)和[英文用户指南](user_guide.md)；[2026-10-08 设计记录](2026-10-08-rs-web-design.md)属于历史文档，其中部分 API 草案已被取代。

## 受管会话登记与停服

`ServerContext::try_register_session()` 会在一个原子操作中登记会话；停服开始后，新的登记失败。`active_sessions()` 包含受管 SSE，以及通过 `WsUpgradePolicy::on_upgrade` 接纳的 WebSocket；即使升级响应尚未完成，计数也已增加。停服后，新的 SSE/WS 接纳会被拒绝。普通 HTTP 请求、原生 Axum upgrade 和任意应用后台任务不属于受管会话。

SSE 与 WebSocket 容量限制属于彼此独立的策略域，不是整个服务统一的会话上限。每次调用 `SseConnectionPolicy::new`/`default` 或 `WsUpgradePolicy::new`/`default` 都会创建独立容量域；同一策略的克隆共享该域。两类策略各自的默认容量都是每个策略实例 128 个连接。因此，为不同路由分别创建策略会累加可接纳容量。若要让多条路由共用一个上限，应在应用启动时创建一个策略并放入应用状态，再使用从该状态取得的克隆。`ServerContext::active_sessions()` 用于停服统计，不是额外的数值接纳上限。

停服时服务停止接收连接、关闭会话登记入口并通知现有会话。HTTP 连接任务与受管会话共用一个绝对截止时间。只有两者都在期限内结束，`ShutdownReport::graceful` 才为 `true`；`unfinished_managed_sessions` 记录截止时仍未结束的受管会话数。报告不表示任意应用工作都已停止。由于 Axum 无法提供可证明的强制关闭连接数量，`forced_connections` 保持为 `None`。

停服时长仅通过 `ServerOptions` 配置，并由 `ServerContext` 统一提供；`WsUpgradePolicy` 不再设置独立的停服期限。WebSocket reader 和 writer 的停服使用服务器共享的绝对截止时间。

## 传输空闲期限

`ServerOptions::transport_idle_timeout` 默认 30 秒，且不能为零。配置键是 `transport.idle_timeout_ms`，缺省值为 `30000`。TCP/TLS 连接在没有活跃 handler 或响应体时达到空闲期限后关闭；这也包括尚未发送首个 HTTP/2 请求的连接。handler 和响应体活跃期间，空闲计时暂停，因此长连接 SSE 不会被误判为空闲。HTTP/2 PING 不会重置请求活动时间。独立的 `request_header_timeout` 仍为 HTTP/1 请求头及 TLS 握手设置 10 秒期限；空闲期限不会中断停滞中的活跃请求。

## WebSocket 队列与入站交付

WebSocket 发送队列的所有克隆共享关闭状态。对端或会话关闭后，`try_send` 会返回 `Closed`；成功入队不保证消息已通过网络送达。入站通道容量为 64 条消息。`WsUpgradePolicy::idle_timeout` 表示读取入站消息或向应用交付消息时无进展的最长时间。若应用不调用 `WsSession::recv()` 且交付在该期限内持续受阻，会话会以关闭码 `1013`、原因 `inbound backpressure` 关闭。应用应持续接收消息，并在收到后再转交较长的业务处理。

## Origin 与应用认证

`WsUpgradePolicy::new()` 默认允许不携带 Origin 的请求继续进入应用认证。请求若携带 Origin，则默认拒绝；只有配置 `allowed_origins` 并精确匹配后才会接纳。应用仍须在调用 `on_upgrade` 前完成认证。Origin 校验不等同于身份认证。

## I/O 错误边界与 API 迁移

`WebServerError` 会保留 socket 绑定、本地地址查询和 accept 故障的 `io::Error` source，调用方可通过 `std::error::Error::source()` 主动诊断。默认 `Display` 和 `Debug` 不输出底层操作系统错误文本；TLS 与配置错误也不会暴露可能包含敏感信息的 source 链。

0.2.0 包含明确的破坏性变更：移除只接收 token 的 WebSocket 升级入口，统一使用 `WsUpgradePolicy::on_upgrade(ws, headers, context, handler)`；WebSocket 停服期限统一通过 `ServerOptions` 配置并由 `ServerContext` 提供；`WebServerError` 不再是可复制、无 source 的枚举；SSE 接纳错误区分容量耗尽和停服拒绝。
