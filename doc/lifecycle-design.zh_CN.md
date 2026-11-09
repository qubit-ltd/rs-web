# 请求与会话生命周期设计

[English version](lifecycle-design.md) · [文档索引](README.md)

本文为维护者梳理当前实现，不新增运行时保证。接入和配置请见[英文用户指南](user_guide.md)与[简体中文用户指南](user_guide.zh_CN.md)。

## 模块与容量域

| 域 | 所有者与范围 | 计数内容 | 边界 |
| --- | --- | --- | --- |
| 传输连接 | `WebServer` 接收/服务流程及每连接 `ConnectionActivity` | 已接收的传输连接，以及活跃请求/响应体活动 | 请求 deadline 本身不会关闭传输。活动租约可保留至响应体完成或被丢弃。 |
| 短 HTTP 请求 | `RequestLimitLayer` 分支共享的 `LimitState` | handler 与响应体并发、请求数据字节、一个绝对请求 deadline | 独立创建的 layer 使用独立状态；克隆状态共享容量。`HttpLimits` 默认：请求体 1 MiB、并发 256、30 秒。 |
| 流接纳 | `StreamAdmissionLayer` 分支共享的 `StreamAdmissionState` | 请求体缓冲及响应头生成期间的并发请求 | permit 在接纳阶段结束，不随 SSE/WS 会话持续。`StreamAdmissionLimits` 默认：1 MiB、256 个接纳、10 秒。 |
| SSE 策略 | 一个 `SseConnectionPolicy` 状态 | 已接纳 SSE 连接数；每个连接另有一项 server session 注册 | 策略连接容量和 server session 数是不同计数。 |
| WebSocket 策略 | 一个 `WsUpgradePolicy` 状态 | 已接纳 WebSocket 会话数；每个会话另有一项 server session 注册 | 共享 reader/writer 引用会持有资源，直到两侧都释放。 |
| Server 任务 | 一个 server 实例中的 `ConnectionTaskExecutor`、`ConnectionCompletion` 与 `SessionTracker` | 已注册 Hyper 任务、传输完成状态、托管会话 | 这是 server 生命周期计数，不代表跟踪整个应用的任务。 |
| 应用任务 | 应用代码创建的任务 | 除非通过托管 session API 显式注册，否则 rs-web 不跟踪 | 请求取消不能停止任意 spawn 或阻塞任务。 |

## 资源所有权

| 资源 | 获取位置 | 活跃期间所有者 | 正常释放 | 错误、drop、timeout 或 shutdown 时释放 | 证据 |
| --- | --- | --- | --- | --- | --- |
| 短请求 semaphore permit | `RequestLimitLayer::enforce_limits` 的声明长度检查之后 | handler 返回响应前由 handler future 持有，之后进入 `PermitBody` 状态 | 响应体 EOF/end-of-stream，包括最后一个数据帧 | handler timeout 会丢弃处理 future；响应体错误、wrapper drop 或 `watch_deadline` 会取走 permit。响应体和 permit 在状态锁外析构。 | `src/limit/request_limit_layer.rs`、`src/limit/internal/permit_body.rs`；`tests/http_limits_tests.rs` |
| 应用响应体 | handler 响应包装时 | `PermitBody` 状态 | EOF/end-of-stream | 响应体错误、wrapper drop 或 deadline watcher；之后轮询超时体时只返回一次读取超时错误。已发送的响应状态不能更改。 | `src/limit/internal/permit_body.rs`；`tests/http_limits_tests.rs::unpolled_response_body_deadline_releases_capacity_and_drops_source` |
| 连接活动租约 | 连接接收请求时 | 非空响应体对应的 `ActivityBody` | 内层响应体报告结束（包括最后一帧之后）或 EOF | 响应体错误、wrapper drop 或连接终止 | `src/server/internal/connection_activity.rs`、`activity_lease.rs`、`activity_body.rs`；`tests/http_server_tests.rs` |
| 接纳 permit 和缓冲体 | `StreamAdmissionLayer` 收集请求体之前 | 接纳 future，持续至响应头就绪 | handler 响应头就绪 | 读取错误、拒绝、超时、handler 错误或 future 取消会丢弃 permit 和缓冲区 | `src/limit/stream_admission_layer.rs`、`stream_admission_state.rs`；`tests/stream_admission_tests.rs`、`tests/stream_admission_network_tests.rs` |
| SSE 策略槽位和 server session guard | `SseConnectionPolicy` 接纳成功时 | 附着在 `SessionStream` 上的 `SseConnectionGuard` | 源 stream 返回 `None` | stream 出错后响应体被丢弃、响应/响应体 drop 或 server 取消/丢弃时释放 guard 并取消 producer token | `src/sse/internal/session_stream.rs`、`sse_connection_guard.rs`；`tests/sse_lifecycle_tests.rs` |
| WebSocket policy permit 和 server session guard | upgrade 接纳成功并注册 session 时 | reader/writer 共享的 `SessionResources` | 两个 I/O task 均结束 | I/O 终止、peer close、task 退出或 shutdown；最后一个引用释放时先释放两项资源并 finalize termination，再通知 closed | `src/ws/internal/session_resources.rs`、`reader.rs`、`writer.rs`、`session_lifecycle.rs`；`tests/websocket_tests.rs`、`src/ws/internal/writer/tests.rs` |
| Hyper 连接任务 | Hyper 调用 `ConnectionTaskExecutor::spawn` 时 | server 所有的 `JoinSet` 和跟踪 future | future 完成并被 join/reap | shutdown 关闭接纳，在 deadline 前等待；必要时 abort 未完成任务，然后 join | `src/server/internal/connection_task_executor.rs`、`finish_serve.rs`；相应模块单测 |

`StreamAdmissionLayer` 成功接纳只表示请求缓冲和响应头生成期间占有资源，不等同长期会话 guard。Axum 接受响应或传输层将响应排队，也不证明对端收到字节。

## 短响应状态转换

| 当前状态 | 触发条件 | 下一状态 | body / permit 操作 | 可观察结果 |
| --- | --- | --- | --- | --- |
| `Active` | 最后一帧或 EOF | `Finished` | 取出并析构 body 与 permit；取消 watcher | 可先返回数据帧后结束；启用 body observation 时终止诊断可为 `complete`。 |
| `Active` | body 错误 | `Finished` | 取出并析构 body 与 permit；取消 watcher | 返回原始 body 错误。 |
| `Active` | wrapper drop | `Finished` | 取出并析构 body 与 permit；取消 watcher | 后续无法轮询该 body。 |
| `Active` | deadline watcher 先获胜 | `TimedOut` | 取出并析构 body 与 permit，再唤醒已注册的 body waker | 即使传输不再轮询，也会归还 permit。 |
| `TimedOut` | 下一次 body poll | `Finished` | watcher 已取走 body 和 permit；如仍存在则取消 watcher | 返回一次 timeout 错误；之后轮询为 EOF。 |
| `Active` | poll 先观察到 deadline | timeout 转换后为 `Finished` | poll 路径取出并析构资源、取消 watcher | 返回一次 timeout 错误。 |

`PermitBody` 和 `watch_deadline` 通过 `BodyState` 互斥协调，首个终止状态转换取得资源。析构、permit 释放、唤醒和诊断输出都在释放 body 状态锁后进行。deadline 覆盖 handler 工作和响应体流式输出，但采用协作式调度：不让出执行权的同步工作会延迟观察；spawn 或 `spawn_blocking` 的应用工作不受该取消控制。

## SSE 与 WebSocket 生命周期

SSE 先经过请求接纳，然后由 `SseConnectionPolicy` 预留策略容量并注册 server session。`SessionStream` 持有应用 stream 和 `SseConnectionGuard`；自然结束或响应被丢弃时释放 guard 并取消 producer token。长期 SSE 路由不应套用普通短请求 deadline。取消 token 只是信号，应用 producer 需要主动检查它。

WebSocket upgrade 接纳同样不会在请求体接纳后结束。策略预留 session 槽位并注册托管 session。`SessionResources` 在 I/O 工作间共享 policy permit 和 session guard。reader 处理 peer 帧与 close 状态；writer 从出站队列取数据并执行 sink send/flush。每次普通出站发送和通知触发的 flush 都有独立的绝对 `write_timeout` deadline；入站活动和无关通知不会重置它。`idle_timeout` 同时设置入站等待与普通写操作期限；之后调用 `write_timeout` 仅覆盖写期限，再次调用 `idle_timeout` 则重新设置两者。目前没有仅修改入站期限的独立 setter。Close 流程使用独立的关闭 deadline，因此普通写期限不是所有 Close 操作的期限。配置细节见[用户指南中的入站与写入期限](user_guide.zh_CN.md#分别设置入站与写入期限)。最后一个 I/O 资源引用释放时才归还容量和 session 注册。队列插入成功仅表示数据进入本地队列，不表示 socket 写入完成或对端收到。peer Close、writer 完成、server 取消和队列关闭是不同事件；见 `src/ws/internal/reader.rs`、`writer.rs`、`close.rs` 与 `session_lifecycle.rs`。

## Server 关闭时序

`finish_serve` 先丢弃 listener，再请 `ServerContext` 开始关闭并建立共享绝对 deadline，然后关闭托管 Hyper task 的注册。它并发等待 Hyper graceful shutdown 和已注册 session，并让被跟踪的 Hyper task 使用同一 deadline 等待。到期后，代码快照未完成 session 与传输状态，依据这些实际完成情况计算 `graceful`，必要时 abort 未完成连接任务，并始终 join 连接任务。`SessionTracker::begin_shutdown` 关闭托管 session 接纳；注册 guard 在 drop 时递减计数。

`ShutdownReport::forced_connections` 当前为 `None`：`finish_serve` 没有提供强制关闭连接数。`graceful` 表示被跟踪的传输、托管 session 和 Hyper task 在 deadline 前满足完成条件；它不代表每个响应字节都已送达。应用 session 代码的取消依赖协作。任意应用自行 spawn 的任务不会自动注册、取消或 join。

## 诊断语义

`BodyObservation` 分别记录响应状态和首个 body 终止结果；两者都已知后才认领并发出单个事件。短响应 body 的终止标签包括 `complete`、`dropped` 和 `timeout`；解析到的 `Content-Length` 等字段表示声明长度，不是实测字节数或限额核算值。`complete` 表示观察到 wrapper 内 body 结束，不表示客户端收到响应；`dropped` 表示 wrapper 被丢弃，不表示客户端断开。响应头发出后的 timeout 不能改写原状态。连接与 task 诊断对应各自的本地生命周期观察。

参见 `src/diagnostic/internal/body_observation.rs`、`body_observation_state.rs`、`body_termination.rs` 和 `tests/diagnostic_tests.rs`。

## 测试与历史证据

| 证据 | 覆盖内容 | 边界 |
| --- | --- | --- |
| `tests/http_limits_tests.rs` | 进程内 body/permit 完成、drop、timeout 和容量行为 | 不测试传输交付。 |
| `tests/sse_lifecycle_tests.rs`、`tests/websocket_tests.rs`、`src/ws/internal/writer/tests.rs` | fixture 中的托管 session、stream、reader/writer 和 close 行为 | 断言只适用于构造的测试生命周期。 |
| `tests/http_server_tests.rs`、`tests/shutdown_tests.rs`、`tests/transport_diagnostic_tests.rs` | 本地 server、传输活动和 shutdown 结果 | 不意味着所有网络对端或调度条件相同。 |
| [Backpressure/request-timeout investigation](backpressure-timeout-investigation-20261104.md) | 锁定依赖栈下的 HTTP/2 流控观察 | 有界历史 loopback 实验；应用 timeout 本身不会关闭传输。 |
| [Close lifecycle investigation](close-lifecycle-investigation-20261104.md) | WebSocket close 处理及既有修复证据 | 结论对应记录的源码基线与测试。 |
| [HTTP/2 resource investigation](http2-resource-investigation-20261103.md) | 不完整 header 行为和有界实验 | 不证明普遍存在 header deadline，也不说明无限时长行为。 |
| [Stream admission buffer investigation](stream-admission-buffer-investigation-20261106.md) | 合成所有权实验及本地 HTTP/1、HTTP/2 分片请求体测试 | 不量化生产 RSS 或远程可利用性。 |
| [WebSocket write-stall investigation](ws-write-stall-investigation-20261107.md) | 记录的网络复现和修复证据 | 这项历史实验使用当时的 `idle_timeout` 配置，结论受记录主机和 harness 限制；它不是当前独立 `write_timeout` 配置的验证结果。当前普通写行为见[用户指南中的入站与写入期限](user_guide.zh_CN.md#分别设置入站与写入期限)。 |
| [Connection activity investigation](connection-activity-investigation-20261108.md) | 本地 body 租约回归和传输控制 | 本地计数证据不等于网络可见协议故障。 |
| [Trailers budget investigation](trailers-budget-investigation-20261108.md) | 锁定 HTTP/1、HTTP/2 trailer parser 观察 | 仅覆盖测试形状及锁定依赖；超限 HTTP/2 字段可能被省略。 |

这些报告中的历史命令与结果不是当前 checkout 的运行结果。作为后续版本证据使用前，应核对各自基线和限制。

## 运行时与证据限制

deadline 使用 Tokio 单调时钟，由相关 task 或 body poll 获得调度时观察，不会抢占同步工作。请求 timeout 会释放应用响应体和短请求 permit，但不能改写已发送响应头，也不能保证传输关闭。流接纳限制覆盖缓冲到响应头生成阶段，不覆盖长期 session。SSE/WS 容量由 session 生命周期资源持有。shutdown 只等待前述传输 task、session 和托管 Hyper task；不能保证任意应用工作终止或对端收到数据。调查结论是在记录的平台、版本和 harness 下进行的有限观察，不是普遍协议保证。
