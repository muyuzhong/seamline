# Seamline v2 架构演进与核心边界规范

**双速热窗口（Record + 多路 InFlight） · 因果水位线对齐 · Agent 有状态会话控制面**

本文档定义 Seamline 在 Phase 1 基础上的 Phase 2 / Phase 3 演进规范，重点解决以下四个核心工程边界问题：
1. **短期热窗口（TTL + 三层字节配额）与未定稿长任务保护**；
2. **业务主库快照与 Seamline 游标的因果水位线对齐契约**；
3. **纪元变更（`epoch_mismatch`）与窗口截断（`cursor_expired`）的显式断层通知**；
4. **Seamline 与通用推送网关（如 Centrifugo）的核心定位边界（有状态会话控制面）**。

---

## 1. 存储边界与双速热窗口（保留 `inflight_snapshot` + 三层字节配额）

### 1.1 职责边界：不做永久数据库，只管“短期热恢复窗口”
在生产级 Agent 架构中，**永久历史记录（System of Record）归属客户端本地存储（SQLite / IndexedDB / JSONL）或业务后端主数据库（PostgreSQL / MySQL）**。Seamline 不自研本地磁盘 WAL，仅提供：
- **默认内置 `MemoryStorage`（带 TTL 与字节水位淘汰）**：单机零外部依赖开箱即用；
- **可选插件 `RedisStorage`（Phase 3）**：基于 Redis 承载跨重启热窗口与多实例共享状态。

### 1.2 为什么在 TTL 热窗口下必须保留“双速结构（`Record Offset` + `InFlight`）”？
业务主库通常只在**一轮消息或工具调用定稿结束时**才执行落库，**正在生成途中的半截内容并不存在于业务主库中**。如果去掉双速聚合、改为按 Token 级事件定长环形缓存（例如 `max_events = 4096`）：
- 单轮长回复或代码生成极易超过 4,096 个 Token 事件，导致**当前正在生成的轮次在落入业务主库前就被环形窗口截断首部**——此时刷新页面，无论查主库还是查 Seamline 都无法找回前半段内容；
- 接 Redis 时若逐个 Token 写 Redis，会产生百倍网络 RTT 与写放大。

因此，v2 **保留并扩展 Phase 1 的双速架构**，将 **“已定稿历史（`Record`）”** 与 **“未定稿活跃流（`InFlight`）”** 的生命周期与淘汰策略严格分离：

1. **未定稿活跃流层（`inFlights map[string]*inFlightState` —— 绝不被历史窗口淘汰）**：
   - **多路并发流与头元数据**：将 Phase 1 的单指针扩展为有界字典（默认单会话最多 `MaxConcurrentStreams = 8` 条并发 `stream_id`），并在首帧保留可选 `Meta json.RawMessage`（如 `{"tool_name":"exec_sql","call_id":"call_1"}`），原生承载多路并行工具调用（Parallel Tool Calls）的半截 JSON 参数流与思维链块。
   - **原地聚合免截断**：同一 `stream_id` 的流式 `delta` 在内存 `bytes.Buffer` 中原地拼接（例如 10,000 个 Token 碎片仅占约 `40 KiB` 内存，而非 10,000 条独立事件），在 `fin=true` 定稿前**绝不参与历史滑动窗口淘汰**。只要任务仍在生成，中途任意时刻重连均可通过 `event: inflight_snapshot` 完整拿到主库尚未落库的半截内容。
   - **并发上限**：已有 `MaxConcurrentStreams` 条流在途时，新 `stream_id` 的非 `fin` 首帧返回 `409`（`ErrTooManyStreams`）；单帧流（`seq=1, fin=true`）不占名额，照常接受。
   - **僵尸流封存（数据面最小版本，控制面租约上线前）**：每个 chunk 都会续期该流的空闲期限。超过 `InFlightIdleTimeout`（默认 `2m`）没有新 chunk 的流，由 broker 清扫器把已拼接内容封存成一条 `Record`（`"sealed":"idle_timeout"`，保留 `Meta`），扇出给订阅者并释放名额与配额；之后同一 `stream_id` 的新 `seq` 返回 `409`（`ErrStreamFinalized`），重发已接受过的 `seq` 按幂等返回原 `committed_offset`。生产者可以发空 `delta` 作为心跳。控制面的 Worker 租约到期时复用同一路径，标记 `worker_lost`。
2. **已定稿记录层（Committed `Record`，单调递增 `Offset = 1, 2, ...`）**：
   - 离散事件（`stream_id == ""`）或流完结帧（`fin == true`）归并为 1 条定稿 `Record`（`Offset + 1`），仅对已定稿 `Record` 执行 `HistoryTTL` 与字节水位滑动淘汰（维护当前缓冲区最早可用偏移 `earliest_offset`）。
3. **三层硬字节配额（封死单事件大包与内存膨胀）**：
   - `MaxEventBytes`（默认 `256 KiB`）：**只限制单次发布请求**（`delta + payload + meta`），在请求到达时检查，超限返回 `413`（收紧 Phase 1 中过宽的 `4 MiB` 上限）。流定稿后的 `Record` 由下一层 `MaxInFlightBytes` 约束，不受 `256 KiB` 限制；
   - `MaxInFlightBytes`（默认 `2 MiB` / 会话）：限制单会话所有未完结 `inFlights` 的拼接文本 + `Meta` 总字节数（足以容纳数十万字符的单轮长输出，同时防止异常 Worker 无限流式写入不发 `fin` 导致 OOM）。在途内容不能淘汰最旧部分（否则会从中间截断正在生成的文字），所以超限时**拒绝该 chunk**，返回 `413`（`inflight_quota_exceeded`），流保持原样；生产者仍可发空 `delta` 的 `fin` 把已有内容定稿。携带显式 `payload` 的 `fin` 不增长拼接缓冲区，不受此限；
   - `MaxHistoryBytes`（默认 `8 MiB` / 会话）+ `HistoryTTL`（默认 `15m`）：仅淘汰最老的**已定稿 `Record`**（推进 `earliest_offset`），不影响活跃 `inFlights`。字节规则永远保留最新一条 `Record`；字节计数 = `payload + meta + type + stream_id` + 每条固定 `64B` 开销。

### 1.3 会话纪元与偏移计数器：流的身份比数据活得久

游标是 `<epoch>:<offset>`。**epoch 属于会话流本身（每个会话一份），不属于 broker 进程**，由 Storage 在会话流创建时生成：
- **淘汰数据不重置身份**：`HistoryTTL` / `MaxHistoryBytes` 只删除 `Record`，会话的 `epoch` 与 `latest_offset`（已提交过的最大 offset）保留。所以拿旧游标来的客户端得到的是 `cursor_expired`，而不是误判成纪元变化，offset 也永远不会在同一个 epoch 里从 1 重来。
- **元数据单独保留更久**：会话流（epoch、offset 计数器、元数据）闲置超过 `MetaTTL`（默认 `24h`）才整个删除；发布、订阅都会续期，broker 清扫器还会给所有仍在内存里的会话（包括只有订阅者、长时间没有发布的会话）续期。删除后再创建会生成**新 epoch**，offset 从 1 开始。
- **内存版进程重启**：所有会话都是新 epoch。
- `/healthz` 返回的是 broker 的 `instance_id`，不是游标 epoch。
- 对照：Centrifugo 按 channel 生成 epoch，`history_ttl` 到期只清数据，epoch 与 offset 存在 `history_meta_ttl`（默认 30 天）更长的 meta 里；ElectricSQL 的 shape handle、Kafka 的 topic ID 也都在重建时换新。

### 1.4 发布重试幂等

- **流式 chunk**：按 `(stream_id, seq)` 去重。在途流内 `seq <= last_seq` 返回 `deduplicated: true`；流定稿（`fin` 或封存）后 `5m` 内，重发已接受过的 `seq` 返回原 `committed_offset`。
- **离散事件**：可选字段 `idempotency_key`，同一会话内 `5m` 窗口（与 `HistoryTTL` 无关）。重复请求返回原 `committed_offset` + `epoch` + `deduplicated: true`，不再扇出。key 只在 `Record` 提交成功之后记录，所以存储失败后的重试会正常提交。**不比对请求内容**。
- 对照：NATS JetStream `Nats-Msg-Id`（默认窗口 2 分钟，重复返回原 seq）、Centrifugo `idempotency_key`（5 分钟，重复返回原 offset + epoch、不再广播）；调研的项目都不比对内容。

---

## 2. 冷热衔接契约：业务主库快照与 `seamline_cursor` 因果对齐

当客户端首次打开历史会话，或收到断层通知需要回源业务主库同步历史时，必须有明确的水位线约定以消除“漏读一段”或“重复一段”的竞态窗口。

### 2.1 写入侧顺序约定（Worker $\rightarrow$ Seamline $\rightarrow$ 业务主库）
1. **先定稿发布至 Seamline**：当一轮消息或工具步骤完成时，Worker 先调用 Seamline `Publish`（原子记录或 `fin=true`），获取返回的权威游标 `seamline_cursor = "<epoch>:<committed_offset>"`（例如 `"9f8e7d6c5b4a:12"`；`epoch` 是 `Publish` 响应里的会话 epoch）。
2. **携带游标落业务主库**：Worker（或业务后端）将该轮业务数据写入主数据库时，在**同一数据库事务**中更新该会话的水位线字段 `last_seamline_cursor = "9f8e7d6c5b4a:12"`（同时建议保留业务幂等键 `stream_id` / `message_id`）。

### 2.2 读取与订阅衔接顺序（Client $\rightarrow$ 业务主库 $\rightarrow$ Seamline）
1. 客户端调用业务后端接口加载历史快照，响应体同时返回 `{ messages: [...], seamline_cursor: "<epoch>:<K_db>" }`（全新会话返回空串 `""`）。
2. 客户端携带 `?after_offset=<seamline_cursor>`（或 `Last-Event-ID: <seamline_cursor>`）订阅 Seamline `GET /v1/sessions/{id}/stream`。

### 2.3 并发交错下的无损性验证
假设 Worker 刚刚完成第 `K` 条记录的生成：
- **情形 A（客户端读主库发生在主库事务提交之后）**：主库返回已包含第 `K` 条消息的快照及游标 `<epoch>:K`。客户端以 `after_offset=<epoch>:K` 订阅 Seamline，Seamline 跳过 `1..K`，直接下发后续第 `K+1` 轮的 `inflight_snapshot` 与实时增量——**零漏读、零重复**。
- **情形 B（客户端读主库发生在 Seamline `Publish` 之后、主库事务提交之前）**：主库返回截至 `K-1` 的历史快照及游标 `<epoch>:(K-1)`。客户端以 `after_offset=<epoch>:(K-1)` 订阅 Seamline，Seamline 从热窗口精确补发 `Record K` 及后续实时流——**同样零漏读、零重复**。
- *（注：若业务架构采用“先写主库、后推 Seamline”的相反顺序，则需在 `Record.Payload` 中携带主库主键/消息 ID，由客户端对边界上的 1 条重叠记录做幂等去重。）*

---

## 3. 显式断层控制协议：`event: cursor_reset`（消除静默缺口）

在短期热窗口与内存实例重启场景下，Phase 1 中“遇到 `epoch` 不一致就静默重置 `afterOffset = 0` 重放”的做法会产生**静默数据缺口（Silent Gap）**：例如内存版重启后 `epoch` 改变且缓冲区为空，客户端重连收到 0 条历史重放，会误以为断线期间没有任何消息产生。

因此，Phase 2 引入显式断层通知帧 **`event: cursor_reset`**：

### 3.1 触发条件（仅针对携带了非空游标的续传请求）
- **初始无游标订阅（`after_epoch == "" && after_offset == 0`）**：属于全新连接而非断线续传，不发送 `cursor_reset`，直接发送当前热窗口内 `[earliest_offset .. latest_offset]` + 活跃 `inflight_snapshot` + 实时流。
- **断层场景 1 —— 纪元变更（`reason: "epoch_mismatch"`）**：
  客户端游标的 epoch 与该会话当前 epoch 不同（内存节点重启、会话闲置超过 `MetaTTL` 后重建）。**只带 offset、不带 epoch 的游标**无法判断属于哪个纪元，也按 `epoch_mismatch` 处理。即使新纪元当前 `latest_offset == 0`（空缓冲），服务端也**必须在首帧发送 `event: cursor_reset`**，明确告知客户端旧纪元上下文已不可连续重放。
- **断层场景 2 —— 游标已滑出热窗口（`reason: "cursor_expired"`）**：
  epoch 相同，但 `after_offset + 1 < earliest_offset`（客户端落后的某段已定稿 `Record` 已被 `HistoryTTL` 或 `MaxHistoryBytes` 淘汰）。服务端首帧发送 `event: cursor_reset`。
- **断层场景 3 —— 游标比服务端还新（`reason: "cursor_ahead"`）**：
  epoch 相同，但 `after_offset > latest_offset`。按 §1.3 的规则正常情况下不会出现，只来自伪造或出错的游标。服务端首帧发送 `event: cursor_reset`。
- **补发途中发现缺口**：`cursor_reset` **只出现在首帧**。如果补发过程中发现历史中间缺了一段（例如分页读取期间被淘汰），服务端发送 `event: stream_error`（`{"error":"insufficient_state"}`）并关闭连接；浏览器带 `Last-Event-ID` 自动重连后，游标已早于窗口，自然在首帧拿到 `cursor_reset`。做法与 Centrifugo 的 `insufficient state` 断开一致。

### 3.2 `event: cursor_reset` 帧格式与客户端恢复动作
```text
event: cursor_reset
data: {"reason":"epoch_mismatch","requested_epoch":"old123","requested_offset":42,"current_epoch":"new456","earliest_offset":1,"latest_offset":0}
```
`cursor_reset` 帧不带 `id:`。发完后连接不断开，服务端接着补发当前热窗口内的 `Record`、各条在途流的 `inflight_snapshot` 和实时流。这一点有别于 Centrifugo（`recovered:false` 时不补发窗口剩余部分），原因是正在生成的内容只在 Seamline 里、主库还没有；而且补发的 `record` 带 `id:`，能把浏览器的 `Last-Event-ID` 推进到新纪元，下次重连不会再触发一次 reset。

客户端收到 `event: cursor_reset` 后的标准动作：
1. 保持当前 SSE 连接不断开，暂存随后到达的当前纪元 `record`（`offset >= earliest_offset`）、`inflight_snapshot`（**正在生成的未落库半截内容**）与实时 `delta`；
2. 异步向业务主库拉取一次最新已落库历史快照（带回最新 `seamline_cursor`），用主库快照补齐已定稿历史缺口，并以 `offset` 对齐合并步骤 1 暂存的热流事件——从而同时保证**历史已落库记录不漏、当前未落库生成流不断**。

---

## 4. 核心生态位：Seamline 与 Centrifugo 的本质区别

在明确了下行热窗口边界后，Seamline 与通用 Go 实时消息网关 `Centrifugo` 的定位差异如下：

- **下行传输层**：两者均提供基于 `epoch + offset` 的短期热窗口断线恢复（Seamline 保持纯标准 HTTP POST + 原生 W3C SSE 零私有 SDK 优势，并通过内置 `inflight_snapshot` 消除流式 Token 对内存/Redis 的百倍写放大）。**如果业务只需要单向广播推流，直接使用 Centrifugo 即可。**
- **核心护城河 —— 有状态 Agent 会话控制面（Stateful Session Control Plane）**：
  Centrifugo 的抽象是**无状态广播频道（Channel）**；Seamline 的抽象是**有生命周期状态的 Agent 任务会话（Session + Run）**，专门内建业务使用 Centrifugo 时仍需自行手写的四项分布式协调原语：

| 对比维度 | **Centrifugo（无状态实时广播网关）** | **Seamline（有状态 Agent 会话总线）** |
| :--- | :--- | :--- |
| **1. 后台 Worker 暴毙监管** | ❌ 只管前端订阅者在线态，不感知后台发布者死活；Worker OOM 崩溃后前端永久卡在“运行中”。 | ✅ **内置 Worker 心跳租约（Lease）**：Worker 失联超时后自动封存半截 `inFlights`、推进状态至 `failed (worker_lost)` 并广播终态。 |
| **2. 人工审批防重门禁** | ❌ 多端同时点“批准”仅被当作两条普通消息转发，业务需自建分布式锁。 | ✅ **一等公民 `WaitingApproval` 原子锁**：按 `interrupt_id` 执行 CAS 幂等解锁，多端并发连点全局严格仅放行首次。 |
| **3. 反向运行干预信道** | ❌ 单向下行推送为主，停止与插话需业务后端自建信令队列并处理竞态。 | ✅ **双向非对称控制流**：内置面向后台 Worker 的控制订阅信道，原生支持 `Cancel`（立即停机封存）与 `Steer`（插话排队）。 |
| **4. 同会话并发防撞** | ❌ 同一频道允许任意多发布者并发混推。 | ✅ **内置 Run 互斥策略**：同一 `session_id` 并发触发时支持 `reject` / `interrupt` / `enqueue`。 |
| **5. 流式碎片写放大** | ⚠️ 每个 Token chunk 占用一条独立历史槽位与一次 Redis 写入。 | ✅ **内存多路 `InFlight` 原地聚合**：百条碎片 0 存储写入，完结归并为 1 条 `Record` 写入内存/Redis。 |

---

## 5. 演进路线图（Phase 2 $\rightarrow$ Phase 3）

### Phase 2：热窗口加固 + 多路流扩展 + 有状态反向控制面（下一步开发）
1. **热窗口与协议加固**：
   - 扩展 `inFlights` 支持有界多路并发流（Parallel Tool Calls）与首帧 `Meta` 头信息；
   - 落地三层字节配额（`MaxEventBytes` / `MaxInFlightBytes` / `MaxHistoryBytes`）与 `HistoryTTL` 已定稿记录淘汰；
   - 实现 `event: cursor_reset`（`epoch_mismatch` | `cursor_expired`）显式断层通知。
2. **有状态反向控制面与 Worker 租约**：
   - Run 并发互斥策略（`reject` / `interrupt` / `enqueue`）；
   - 面向后台 Worker 的控制流接口（实时下发 `Cancel`、`Steer`、`ApprovalResolved`）；
   - `WaitingApproval` + `interrupt_id` 原子 CAS 幂等门禁；
   - Worker 租约心跳与超时自动判死收尾（封存未完结 `inFlights` 并广播 `worker_lost`）。

### Phase 3：可插拔 Redis 热存储后端与生产配套
1. **`RedisStorage` 驱动**：将归并后的定稿 `Record`、会话状态与租约锁映射至 Redis（利用原生 `EXPIRE` 管理热窗口，支持多实例水平扩展，无需自研本地磁盘 WAL）。
2. **轻量安全与可观测性**：分离 Worker 发布令牌与下游订阅/控制令牌，暴露 `/metrics` 核心指标。
