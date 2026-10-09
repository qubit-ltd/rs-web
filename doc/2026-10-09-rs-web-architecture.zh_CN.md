# rs-web 架构概览

> 日期：2026-10-09。本文说明 `qubit-web` 0.2.0 的现行模块布局。停服与会话合同见[生命周期与资源边界摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)，接入步骤见[中文用户指南](user_guide.zh_CN.md)。[2026-10-08 设计记录](2026-10-08-rs-web-design.md)属于历史文档。

## 请求路径与模块关系

`WebServer` 持有已绑定的 TCP listener、每个服务实例的传输策略和共享的 `ServerContext`。它接收并限制传输连接，可选执行 TLS 握手，再将连接适配为支持 upgrade 的 Hyper 连接。Hyper 将请求交给应用的 Axum `Router<()>`。

应用可以直接构造 router，也可以使用 `ControllerRoutes` 和 controller 宏。Controller 路由会检查 method/path 冲突并提供路由元数据。短请求预算由应用安装在选定的 controller 路由或 router 分支上；除非应用明确安装到相应位置，否则不会全局生效。诊断中间件记录允许列表中的请求元数据：method、匹配的路由模板、响应状态码、耗时、连接 ID，以及可选的声明请求体长度。

主要模块职责如下：

| 模块 | 职责 |
| --- | --- |
| `server` | 绑定 HTTP/HTTPS listener、运行 Axum router、限制传输连接、协调停服并生成停服报告。 |
| `mvc` | 装配 controller 路由、拒绝重复的 method/path 声明并提供路由元数据。 |
| `limit` | 为普通短请求提供可选的请求体、并发和处理时间预算。 |
| `json` | 启用 `json` feature 时限制严格 JSON 输入/输出的字节数和结构规模。 |
| `sse` | 接纳有容量上限的 server-sent event 流，并将其登记为受管会话。 |
| `ws` | 启用 `ws` feature 时校验 WebSocket 策略与 Origin、预留容量并管理会话 I/O。 |
| `config` | 启用 `config` feature 时，将显式配置解析为 listener 选项及独立的 HTTP 路由限制。 |
| `tls` | 启用 `tls-rustls` 时，为 HTTPS 加载或封装 rustls 配置。 |
| `diagnostic`、`error`、`route_kind` | 提供请求诊断、稳定的公开错误和共享的路由分类。 |

## 容量域与默认值

每项限制保护特定资源。不能把其中某个容量值理解成整个服务的统一并发上限。

| 容量域 | 默认值 | 范围与超限行为 |
| --- | --- | --- |
| 已接收的传输连接 | 每个 `WebServer` 1,024 个 | 包含 TLS 握手和 HTTP connection future。达到上限时 accept 循环暂停接收；客户端可能等待操作系统 backlog，也可能连接失败；不会生成 HTTP 503。 |
| Controller 短请求 | 每个 `ControllerRoutes` builder 同时 256 个 | `ControllerRoutes::new()` 默认采用 `HttpLimits::default()`：每个请求体 1 MiB，handler/响应体期限 30 秒。`with_http_limits` 替换这些默认值，应在添加 Controller 前调用。每个 builder 的路由共享自己的状态。原生 Axum 分支需显式安装 `RequestLimitLayer`；`WebServer` 不安装路由限额。容量耗尽时返回容量拒绝。 |
| SSE 连接 | 每个 `SseConnectionPolicy` 默认 128 个 | 每次 `new`/`default` 都建立独立容量域；克隆共享其容量域。容量耗尽与停服拒绝分别报告。 |
| WebSocket 连接 | 每个 `WsUpgradePolicy` 默认 128 个 | 每次 `new`/`default` 都建立独立容量域；克隆共享其容量域。接纳失败会在会话纳入管理前拒绝。 |
| 服务受管会话 | 没有单独的数值上限 | `ServerContext` 追踪已接纳的 SSE 和 WebSocket 会话，以便停服统计。它不是全局连接上限，也不统计普通请求、原生 Axum upgrade 或应用后台任务。 |

若要让多条路由共用 SSE 或 WebSocket 容量，应在应用启动时创建一个策略并放入应用状态，再向 handler 传递该策略的克隆。分别创建多个策略会叠加总体可接纳容量。短请求分支也应按同样原则显式规划和共享预期的分支状态。

## 请求预算与长连接流

`HttpLimits::default()` 为每个请求体设置 1 MiB 上限，为并发短请求设置 256 个额度，并为 handler 工作及响应体传输设置 30 秒期限。`ControllerRoutes::new()` 默认将此策略用于 Controller 的 `short` 路由；`with_http_limits` 替换默认值，后续添加的路由使用新策略。Controller 的 `sse` 和 `ws` 路由跳过短请求 layer。原生 Axum 分支需显式安装 `RequestLimitLayer`；`WebServer` 按原样服务 Router，不添加路由限额。中间件在选定路由读取请求体时累计字节；不会为了执行限制而读取本来未消费的请求体。容量耗尽、请求体超限和处理超时都有稳定的拒绝结果。如果响应头已发送后期限到达，响应体会以错误结束。

启用 `json` 时，`JsonLimits::default()` 分别将输入和输出 payload 限制为 1 MiB，嵌套深度限制为 64，值节点数限制为 100,000，数组元素数和对象成员数各限制为 10,000，键长度限制为 16 KiB，字符串长度限制为 256 KiB，数字文本长度限制为 128 字节。JSON 限制与通用 HTTP 请求体限制相互独立；两者同时生效时，应配置兼容的值。

输入阶段中，`BoundedJson` 会通过 `JsonRejection::BudgetExceeded`（`413`）拒绝超限请求体或解码预算；格式错误的 JSON 和不支持的媒体类型分别产生不同的 extractor 拒绝结果。输出阶段中，`json_response` 会先完成整个响应的编码，再返回响应；输出预算或序列化失败通过 `JsonResponseError` 返回（错误码为 `json_output_budget_exceeded` 或 `json_encode_failed`）。应用负责处理 extractor 拒绝和响应编码错误；库不会替应用选择特定的降级响应。

SSE 和 WebSocket 都是长连接路由，不应套用短请求并发/期限中间件。SSE keep-alive 与 WebSocket frame、message、队列限制由各自策略模块管理。WebSocket 的应用入站通道容量为 64 条消息。会话停服、队列关闭、入站背压和 Origin 行为见[生命周期摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)。

## 超时、停服与配置

`ServerOptions` 默认将 HTTP/1 请求头及 TLS 握手期限设为 10 秒，将传输空闲期限和优雅停服期限各设为 30 秒。仅在没有活跃请求 handler 或响应体时，空闲期限才会关闭 TCP/TLS 连接；它不会限制已经开始但停滞的活跃请求。长连接响应体活跃期间，传输空闲计时暂停。

停服开始时，服务停止接收 socket、关闭受管会话登记入口、通知已有会话，并使用同一个绝对期限等待 HTTP connection task 和受管 SSE/WS 会话。只有两组任务都按期结束，`ShutdownReport::graceful` 才为 `true`。报告记录未结束的受管会话，但不保证任意应用后台工作也已停止。Axum 无法提供可证明的强制关闭连接数，因此 `forced_connections` 为 `None`。

`ConfiguredWeb` 解析应用显式传入的配置对象，并分别返回 `ServerOptions` 与 `HttpLimits`。Controller 短路由已有 `HttpLimits::default()`；把配置值传给 `with_http_limits` 可替换该策略。原生 Axum 路由需在选定的短路由分支安装 `RequestLimitLayer`；`WebServer` 不会修改 Router。启用 `config` 时，传输配置默认值为 `transport.max_connections = 1024` 和 `transport.idle_timeout_ms = 30000`；HTTP 策略键包括 `http.max_body_bytes`、`http.max_concurrent_requests` 和 `http.request_timeout_ms`。TLS 是可选项，通过独立的 `TlsConfig` 配置；PEM 加载失败会归并为 `TlsInvalid`，不会暴露密钥材料或路径。

## 失败阶段与应用责任

| 阶段 | 库的行为 | 应用责任 |
| --- | --- | --- |
| 启动与绑定 | 在开始服务前校验选项、绑定 socket、查询本地地址并校验 TLS 配置。公开错误区分配置无效、绑定/地址失败、TLS 错误和服务/accept 错误。 | 提供有效地址及正数超时/容量；加载有效证书和私钥；决定如何向上层报告启动错误。 |
| 路由装配 | 检测重复的 controller method/path 声明。已安装请求策略中间件的路由会对请求体、容量和期限返回稳定拒绝。 | 装配预期路由，将短请求限制安装到合适分支，并在需要时转换公开拒绝结果。 |
| SSE 接纳 | 在创建流之前预留策略容量并登记受管服务会话。容量耗尽与停服拒绝彼此区分。 | 决定是否共享策略状态；实现 event ID、重放和事件缓冲；处理客户端重连。 |
| WebSocket 接纳与 I/O | 检查策略和 Origin，预留策略/服务会话容量，并管理读写生命周期；限制 frame、message、队列和入站交付。 | 在调用 `on_upgrade` 前完成身份认证，配置允许的 Origin，持续调用 `WsSession::recv()`，并处理业务逻辑与交付语义。Origin 校验不等于身份认证。 |
| 停服 | 对 HTTP 连接和受管会话使用同一个期限，并报告仍未结束的受管会话。 | 停止或监管应用后台任务，并决定如何处理非优雅停服报告。原生 Axum upgrade 和任意应用任务不在受管会话统计范围内。 |

[生命周期摘要](2026-10-09-rs-web-lifecycle-design.zh_CN.md)是现行会话和停服保证的简明参考；[用户指南](user_guide.zh_CN.md)介绍具体接入方式。
