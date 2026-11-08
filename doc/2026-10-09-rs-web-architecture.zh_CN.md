# rs-web 架构概览

> 日期：2026-10-09。本文说明 `qubit-web` 0.3.0 的现行模块布局。停服与会话合同见[生命周期与资源边界摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)，接入步骤见[中文用户指南](user_guide.zh_CN.md)。[2026-10-08 设计记录](2026-10-08-rs-web-design.md)属于历史文档。

## 当前 API 与运行约定

原生 Axum 路由使用 `RequestLimitLayer::new(limits)`、`StreamAdmissionLayer::new(limits)` 或 `StreamAdmissionLayer::from_state(state)`；这些构造器不再接受 Router 状态类型参数。原先显式写了 turbofish 的调用需要删除类型参数。`WsSession::close()` 可幂等调用，取消等待后可以重试；关闭后 `recv()` 返回 `None`。未匹配路由的诊断只记录固定标记 `<unmatched>`。请求头期限或 SSE 保活间隔若为零或无法表示，会以 `InvalidConfig` 拒绝。

`ServerOptions::validate_detailed()` 返回首个无效 listener 字段及稳定原因码，不包含配置值；旧 `validate()` 仍返回 `WebServerError::InvalidConfig`。HTTPS 默认保留绑定时 TLS 快照。显式使用 `TlsConfig::from_reloadable_rustls` 后，每条新连接握手前会取得一次当前 rustls 配置快照；应用通过自己保留的 `RustlsConfig` clone 更新配置。

## 请求路径与模块关系

`WebServer` 持有已绑定的 TCP listener、每个服务实例的传输策略和共享的 `ServerContext`。它接收并限制传输连接，可选执行 TLS 握手，再将连接适配为支持 upgrade 的 Hyper 连接。Hyper 将请求交给应用的 Axum `Router<()>`。

accept 遇到可重试的 socket I/O 错误时，最多连续重试 8 次，等待间隔依次为 10、20、40、80、160、320、640 和 1,000 毫秒，累计 2.27 秒。第 9 次连续错误会终止服务；成功 accept 会清零计数。`InvalidInput`、`PermissionDenied`、`Unsupported` 和 `NotConnected` 会立即终止。停服或连接任务结束可以中断等待；重建 accept future 时会保留原来的绝对重试截止时间。终止失败时会关闭 listener、取消受管会话，并在优雅停服期限内等待连接任务；期限后 abort 并回收剩余任务。`WebServerError::ServeFailed` 会保留最后一次 I/O 错误作为 source。

应用可以直接构造 router，也可以使用 `ControllerRoutes` 和 controller 宏。末段名称相同但实际类型不同的 Controller，也可以在同一模块分别使用 `rest_controller` 并装配到一起。`ControllerRoutes::add` 会增量复用路由 matcher 来检查完整路由模板，并在注册前对重复 method/path、无效路径、超过 25 个普通参数和匹配模板冲突返回 `ControllerRouteError`；末尾 catch-all 不计入参数数量。这只改变装配成本；错误分类、注册前完整预检和路由行为保持不变。`finish()` 直接返回 Axum `Router`。之后与原生 Axum Router 合并仍遵循 Axum 自身的冲突行为。Controller 的 `short` 路由默认使用 builder 的短请求预算，`sse`/`ws` 路由则使用独立的流式接纳状态；原生 Axum 路由需由应用显式安装相应 layer。`WebServer` 按原样服务 Router，不会向整个 Router 添加路由限额。诊断中间件记录允许列表中的请求元数据：method、匹配的路由模板、响应状态码、耗时、连接 ID，以及可选的声明请求体长度。`DiagnosticLayer::new()` 仍只记录响应头阶段；应用显式安装 `DiagnosticLayer::with_body_diagnostics()` 后，还会在响应体终止时记录一次允许列表 `response_body_end` 事件（`complete`、`timeout`、`body_error` 或 `dropped`）。该 layer 应放在 `RequestLimitLayer` 外侧以观察其超时。body 被丢弃不证明客户端断连，complete 也不证明网络已送达；事件不含正文或错误文本。

主要模块职责如下：

| 模块 | 职责 |
| --- | --- |
| `server` | 绑定 HTTP/HTTPS listener、运行 Axum router、限制传输连接、协调停服并生成停服报告。 |
| `mvc` | 装配 controller 路由，在注册前拒绝重复 method 和冲突的路由模板，并提供路由元数据。 |
| `limit` | 为短请求和长连接接纳提供请求体、并发和时间预算。Controller 两类路由默认使用彼此独立的状态域；原生 Axum 路由需显式安装相应 layer。`WebServer` 不向整个 Router 添加路由限额。 |
| `json` | 启用 `json` feature 时限制严格 JSON 输入/输出的字节数和结构规模。 |
| `sse` | 接纳有容量上限的 server-sent event 流，并将其登记为受管会话。 |
| `ws` | 启用 `ws` feature 时校验 WebSocket 策略与 Origin、预留容量并管理会话 I/O。 |
| `config` | 启用 `config` feature 时，将显式配置解析为 listener 选项及独立的 HTTP 路由限制。 |
| `tls` | 启用 `tls` 时，为 HTTPS 加载或封装 rustls 配置。 |
| `diagnostic`、`error`、`route_kind` | 提供请求诊断、稳定的公开错误和共享的路由分类。 |

## 容量域与默认值

每项限制保护特定资源。不能把其中某个容量值理解成整个服务的统一并发上限。

| 容量域 | 默认值 | 范围与超限行为 |
| --- | --- | --- |
| 已接收的传输连接 | 每个 `WebServer` 1,024 个 | 配置值必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`。包含 TLS 握手和 HTTP connection future。达到上限时 accept 循环暂停接收；客户端可能等待操作系统 backlog，也可能连接失败；不会生成 HTTP 503。 |
| HTTP/2 并发流 | 每条连接 200 个 | 可通过 `ServerOptions::with_http2_max_concurrent_streams` 或 `config` 的 `transport.http2_max_concurrent_streams`（`1..=u32::MAX`）设置。这是每连接的协议上限，与传输连接、路由和会话额度独立；不是进程级请求数、header 字节或内存预算。 |
| HTTP/1 请求头及 TLS 握手期限 | 10 秒 | 通过 `ServerOptions::with_request_header_timeout` 设置。启用 `config` 时，`transport.request_header_timeout_ms` 缺省为 10000，必须为正且能表示为 Tokio 截止时间；它不限制 HTTP/2 请求头块。 |
| HTTP 请求头缓冲 | 未配置时使用 Hyper 默认值 | 可选 `http1_max_buffer_bytes`（至少 8192 字节）限制 HTTP/1 解析缓冲区，不是精确的 header list 字节数；可选 `http2_max_header_list_bytes`（正 `u32`）限制解压后的 HTTP/2 header list。启用 `config` 时使用 `transport.http1_max_buffer_bytes` 和 `transport.http2_max_header_list_bytes`。两者是不同的协议预算，不是整个进程的内存上限。 |
| Controller 短请求 | 每个 `ControllerRoutes` builder 同时 256 个 | 配置值必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`。`ControllerRoutes::new()` 默认采用 `HttpLimits::default()`：每个请求体 1 MiB，handler/响应体期限 30 秒。期限到达时会主动释放应用响应体和短请求许可，无需等待再次轮询。响应头发出后，超时只能在传输再次轮询时通过 body error 体现，不会关闭连接。`with_http_limits` 替换这些默认值，应在添加 Controller 前调用。每个 builder 的路由共享自己的状态。原生 Axum 分支需显式安装 `RequestLimitLayer`；`WebServer` 不安装路由限额。容量耗尽时返回容量拒绝。 |
| SSE 连接 | 每个 `SseConnectionPolicy` 默认 128 个 | 每次 `new`/`default` 都建立独立容量域；克隆共享其容量域。容量耗尽与停服拒绝分别报告。 |
| WebSocket 连接 | 每个 `WsUpgradePolicy` 默认 128 个 | 每次 `new`/`default` 都建立独立容量域；克隆共享其容量域。策略按最终字段值校验；`max_connections` 必须处于 `1..=tokio::sync::Semaphore::MAX_PERMITS`，零值对应 `WsPolicyError::ZeroLimit`，超上限对应 `ConnectionLimitTooLarge`。`inbound_queue_limits` 的消息条数接受 `1..=Semaphore::MAX_PERMITS`，字节数必须为正；条数超上限返回 `WsPolicyError::InboundQueueLimitTooLarge`。穷尽匹配该枚举的调用方需增加此分支。接纳失败会在会话纳入管理前拒绝。 |
| 服务受管会话 | 没有单独的数值上限 | `ServerContext` 追踪已接纳的 SSE 和 WebSocket 会话，以便停服统计。它不是全局连接上限，也不统计普通请求、原生 Axum upgrade 或应用后台任务。 |

若要让多条路由共用 SSE 或 WebSocket 容量，应在应用启动时创建一个策略并放入应用状态，再向 handler 传递该策略的克隆。分别创建多个策略会叠加总体可接纳容量。短请求分支也应按同样原则显式规划和共享预期的分支状态。

## 请求预算与长连接流

Controller 的 `sse`/`ws` 长连接路由使用独立于短请求 `LimitState` 的 `StreamAdmissionState`。每个接纳状态域默认每请求缓冲 1 MiB、同时接纳 256 个请求，并在 handler 生成响应头前限时 10 秒；256 MiB 是每个状态域并发请求体载荷的估算，不是缓冲分配或进程峰值上限。增量缓冲在复制后释放已处理的数据帧，但 Vec 增长余量、扩容期间的临时共存、当前帧、trailers、分配器行为和传输缓冲仍会占用额外资源。添加 Controller 前可用 `with_stream_admission_limits` 替换 builder 的接纳预算。原生 Axum SSE/WS 分支必须显式安装 `StreamAdmissionLayer`；通过 `StreamAdmissionState` 和 `from_state` 共享多个分支的容量。新 layer 会创建独立状态域。请求体超限、读取失败、容量耗尽和接纳超时分别返回 413 `body_too_large`、400 `body_read_failed`、503 `capacity_exceeded` 和 504 `request_timeout`。运行时无法表示绝对接纳期限时，layer 返回 HTTP 500 `invalid_configuration`。期限止于 handler 返回响应头，不覆盖 SSE 响应流或 WebSocket 会话。

`HttpLimits::default()` 为每个请求体设置 1 MiB 上限，为并发短请求设置 256 个额度，并为 handler 工作及响应体传输设置 30 秒期限。`ControllerRoutes::new()` 默认将此策略用于 Controller 的 `short` 路由；`with_http_limits` 替换默认值，后续添加的路由使用新策略。Controller 的 `sse` 和 `ws` 路由使用流式接纳策略，不使用短请求 layer。原生 Axum 分支需显式安装 `RequestLimitLayer`；`WebServer` 按原样服务 Router，不添加路由限额。中间件在选定路由读取请求体时累计字节；不会为了执行限制而读取本来未消费的请求体。容量耗尽、请求体超限和处理超时都有稳定的拒绝结果。无法表示的绝对请求期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限异常，layer 返回 HTTP 500 和代码 `invalid_configuration`。普通 handler 超时仍返回 `504 Gateway Timeout`。如果响应头已发送后期限到达，会立即丢弃应用响应体并归还短请求许可，但无法更改响应状态。启用响应体诊断时，即使传输不再轮询响应体，期限触发时也会发出超时事件；body error 本身仍只在之后再次轮询时观察到。未启用该观察器时，现有日志行为不变。传输背压可能推迟客户端观察到错误的时间；该期限不会关闭连接，TCP/HTTP/2 连接仍受独立传输策略管理。

启用 `json` 时，`JsonLimits::default()` 分别将输入和输出 payload 限制为 1 MiB，嵌套深度限制为 64，值节点数限制为 100,000，数组元素数和对象成员数各限制为 10,000，键长度限制为 16 KiB，字符串长度限制为 256 KiB，数字文本长度限制为 128 字节。JSON 限制与通用 HTTP 请求体限制相互独立；两者同时生效时，应配置兼容的值。`BoundedJson` 会把每个输入数据帧复制到连续缓冲区，并在轮询下一帧前丢弃当前帧的 `Bytes` 句柄。输入字节上限只计算 payload，不限制进程总内存；`Vec` 容量与扩容、当前帧、传输缓冲、解码和应用值都会增加内存占用，因此它不是进程 RSS 上限。

输入阶段中，`BoundedJson` 会通过 `JsonRejection::BudgetExceeded`（`413`）拒绝超限请求体或解码预算；格式错误的 JSON 和不支持的媒体类型分别产生不同的 extractor 拒绝结果。输出阶段中，`json_response` 会先完成整个响应的编码，再返回响应；输出预算或序列化失败通过 `JsonResponseError` 返回（错误码为 `json_output_budget_exceeded` 或 `json_encode_failed`）。应用负责处理 extractor 拒绝和响应编码错误；库不会替应用选择特定的降级响应。

SSE 和 WebSocket 都是长连接路由，不应套用短请求并发/期限中间件。SSE keep-alive 与 WebSocket frame、message、队列限制由各自策略模块管理。WebSocket 入站默认每连接最多排队 64 条消息和累计 1 MiB payload，通过 `inbound_queue_limits` 配置；出站 `queue_limits` 独立生效。reader 等待时最多另持有一条消息，且也计入该字节预算。默认 128 个连接对应约 128 MiB 排队/待交付 payload，此外每连接最多一条 1 MiB 协议重组消息、分配开销和应用自有内存。会话停服、队列关闭、入站背压和 Origin 行为见[生命周期摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)。

## 超时、停服与配置

`ServerOptions` 默认每条连接最多同时开启 200 个 HTTP/2 流，HTTP/1 请求头及 TLS 握手期限为 10 秒，传输空闲期限和优雅停服期限各为 30 秒。HTTP/1 解析缓冲区和 HTTP/2 解压后 header list 可选设置预算；省略时沿用 Hyper 默认值。HTTP/1 请求头期限不是 HTTP/2 header block 的期限。WEB-004 协议实验中，第二条流的合法请求头尚未结束时 SSE 仍继续发送；独立资源调查报告会记录有界场景下的资源观测与回收结果。版本化源码检查和证据见[核查报告](http2-resource-investigation-20261103.md)。仅在没有活跃请求 handler 或响应体时，空闲期限才会关闭 TCP/TLS 连接；它不会限制已经开始但停滞的活跃请求。长连接响应体活跃期间，传输空闲计时暂停。

停服开始时，服务停止接收 socket、关闭受管会话登记入口、通知已有会话，并使用同一个绝对期限等待 HTTP connection task 和受管 SSE/WS 会话。期限由 `ServerOptions` 设置，并由 `ServerContext` 提供给 WebSocket 会话；`WsUpgradePolicy` 不再单独设置停服期限。无法表示的停服期限会以 `WebServerError::InvalidConfig` 拒绝；若运行时期限异常，服务会先取消会话并回收连接任务，再返回错误。只有两组任务都按期结束，`ShutdownReport::graceful` 才为 `true`。报告记录未结束的受管会话，但不保证任意应用后台工作也已停止。`WebServer` 拥有并跟踪连接任务，Hyper/hyper-util 驱动各连接。本实现尚不提供具有明确定义且可证明的强制关闭连接计数，因此 `forced_connections` 为 `None`。

`ConfiguredWeb` 解析应用显式传入的配置对象，并分别返回 `ServerOptions` 与 `HttpLimits`。Controller 短路由已有 `HttpLimits::default()`；把配置值传给 `with_http_limits` 可替换该策略。原生 Axum 路由需在选定的短路由分支安装 `RequestLimitLayer`；`WebServer` 不会修改 Router。启用 `config` 时，传输配置默认值为 `transport.max_connections = 1024`、`transport.idle_timeout_ms = 30000`、`transport.request_header_timeout_ms = 10000` 和每条 HTTP/2 连接 `transport.http2_max_concurrent_streams = 200`。请求头期限必须为正且能表示为 Tokio 截止时间；它同时用于 HTTP/1 请求头和 TLS 握手，不限制 HTTP/2 请求头块。流数值须为正 u32。可选的 `transport.http1_max_buffer_bytes` 和 `transport.http2_max_header_list_bytes` 省略时沿用 Hyper 默认值，显式值分别至少为 8192 和正 u32。HTTP 策略键包括 `http.max_body_bytes`、`http.max_concurrent_requests` 和 `http.request_timeout_ms`。两个并发配置键的有效范围都是 `1..=tokio::sync::Semaphore::MAX_PERMITS`；无效值通过 `ConfigOptionsError::field()` 指向原配置键。TLS 是可选项，通过独立的 `TlsConfig` 配置；PEM 加载失败会归并为 `TlsInvalid`，不会暴露密钥材料或路径。

如需在线轮换 TLS，宿主保留 `axum_server::tls_rustls::RustlsConfig` 的 clone，把另一个 clone 交给 `TlsConfig::from_reloadable_rustls`，并在绑定后调用该配置的重载方法。PEM 重载失败会保留最近一次成功配置。已经取得握手快照的连接及已建立的 HTTP、SSE、WSS 连接不受更新影响；重载顺序由宿主负责。

## 失败阶段与应用责任

| 阶段 | 库的行为 | 应用责任 |
| --- | --- | --- |
| 启动与绑定 | 在开始服务前校验选项、绑定 socket、查询本地地址并校验 TLS 配置。公开错误区分配置无效、绑定/地址失败、TLS 错误和服务/accept 错误。 | 提供有效地址及正数超时/容量；加载有效证书和私钥；决定如何向上层报告启动错误。 |
| 路由装配 | 检测重复的 controller method/path 声明。已安装请求策略中间件的路由会对请求体、容量和期限返回稳定拒绝。 | 装配预期路由，将短请求限制安装到合适分支，并在需要时转换公开拒绝结果。 |
| SSE 接纳 | 在创建流之前预留策略容量并登记受管服务会话。容量耗尽与停服拒绝彼此区分。 | 决定是否共享策略状态；实现 event ID、重放和事件缓冲；处理客户端重连。 |
| WebSocket 接纳与 I/O | 检查策略和 Origin，在升级前拒绝重复 Origin 字段，预留策略/服务会话容量，并管理读写生命周期；限制 frame、message、队列和入站交付。 | 在调用 `on_upgrade` 前完成身份认证，配置允许的 Origin 且至多发送一个 Origin 字段，持续调用 `WsSession::recv()`，并处理业务逻辑与交付语义。Origin 校验不等于身份认证。 |
| 停服 | 对 HTTP 连接和受管会话使用同一个期限，并报告仍未结束的受管会话。 | 停止或监管应用后台任务，并决定如何处理非优雅停服报告。原生 Axum upgrade 和任意应用任务不在受管会话统计范围内。 |

[生命周期摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)是现行会话和停服保证的简明参考；[用户指南](user_guide.zh_CN.md)介绍具体接入方式。

WebSocket 策略将入站等待与普通写操作期限分开。`idle_timeout` 为兼容旧调用仍同时设置两者；需要拆分时先设 `idle_timeout`，再设 `write_timeout`。默认均为 60 秒，Close 握手仍使用共享停服期限。`WsSession::closed()` 在连接许可和受管会话登记释放后就绪；此后 `termination()` 提供不可变的首因快照。关闭码 1001 根据原因使用 `idle timeout`、`server shutdown` 或 `session closed`。`PeerClose` 仅代表观察到对端帧，不确认握手完成；`IoEnded` 也不代表成功。这些期限是协作式操作等待上限，不保证字节级进度；字节和内存预算不限制 CPU 时间或应用保留的内存。

## 协作式期限与传输对端

请求、流式接纳和停服期限依赖 Tokio 调度与协作式取消。不让出执行权的同步工作会延迟超时检查和清理，也可能在期限之后成功完成。取消等待不会停止已经运行的 `spawn_blocking` 任务或应用自行启动的工作。JSON 字节和结构限额不限制用户序列化或反序列化实现的 CPU 时间。应用应为 CPU 密集或阻塞工作单独设置预算；需要强制终止时使用进程隔离。取消配合和业务副作用由应用负责。

accept 循环会向每个 HTTP/HTTPS 请求注入 Axum `ConnectInfo<SocketAddr>`，HTTP/2 同一连接的每个流也使用该地址。它标识直接 TCP 对端，可能是反向代理，并非经过验证的用户身份。服务不信任转发头；代理来源解析属于应用策略。
