# Seamline v2 架构演进与核心边界规范

**纯事件游标（`<epoch>:<seq>`） · 分段水位热窗口 · Agent 有状态会话控制面**

本文档是 Seamline v2（Phase 2 / Phase 3）的权威设计规范，确立四大核心决策：
1. **数据面彻底收敛为载荷透明的“纯单调事件游标（`<epoch>:<seq>`）”**（移除服务端字符串拼接与双游标归并）；
2. **分段水位字节环形缓冲（Pinned-Run Byte RingBuffer）+ 三层硬字节配额**（在不解析业务 Payload 的前提下，保证未落入业务主库的活跃长任务绝不被中途截断）；
3. **业务主库因果水位线（`seamline_cursor`）与显式断层协议（`event: cursor_reset`）**；
4. **聚焦与通用推送网关（如 Centrifugo）的真正分水岭——有状态 Agent 会话控制面**。

---

## 1. 存储边界与纯事件游标数据面

### 1.1 存储职责边界：只管“短期热恢复窗口”，不做永久数据库
在生产级 Agent 系统中，**长期历史持久化（System of Record）归属客户端本地存储（SQLite / IndexedDB / JSONL）或业务后端主数据库（PostgreSQL / MySQL）**。Seamline 不自研本地磁盘 WAL，仅负责活跃运行期及短期恢复窗口：
- **默认内置 `MemoryStorage`**（`internal/hotwindow`）：零外部依赖单二进制启动，按字节配额与 `TTL` 自动淘汰过期历史事件；
- **可选插件 `RedisStorage`（Phase 3）**：基于 Redis 原生 `Streams (XADD)` + `EXPIRE` 承载跨重启热窗口与多实例共享状态，并在驱动层通过 **透明 I/O 微批管道（5~10ms Pipeline 批量刷写）** 吸收高频 Token 事件的网络往返开销，无需破坏上层逐事件游标模型。

### 1.2 统一载荷透明事件模型（Payload-Opaque Event Stream）
放弃 Phase 1 将流拆分为 `Record` 与 `InFlight` 并在 Broker 内做字符串 `+` 拼接的做法，全链路统一为单一不可变结构 **`Event`**：

- **单一事件原语**：无论是用户输入、单步文本 `delta`、多路并行工具调用（Parallel Tool Calls）交错输出的半截 JSON 参数碎片、思维链块，还是 `run_started` / `run_finished` 状态事件，进入会话即分配严格递增的 `seq = 1, 2, 3...`，Broker **不解析、不拼接 `Payload json.RawMessage`**，100% 原样缓存与广播。
- **单一游标格式 `<epoch>:<seq>`**：每一条业务事件帧在 SSE 上均携带 `id: <epoch>:<seq>`，且**不写 `event:` 行**（浏览器 `EventSource` 按默认 `message` 派发，生产者的 `type` 永远不会与控制帧重名）；不带 `id:` 的帧仅限 `cursor_reset` 与 `stream_error` 两类控制帧。发布请求为 `{"type","payload","idempotency_key"}`（`type` 必填、`payload` 可选且须为合法 JSON），响应为 `{"epoch","seq","deduplicated"}`。客户端在 `seq = 42` 断线重连时，服务端精确从 `seq = 43` 起补发，零重传浪费，客户端只需维护一套纯粹的增量事件消费逻辑。
- **下游完全中立**：对外仅为标准 HTTP POST + W3C SSE（同时支持 `Last-Event-ID` 请求头与 `?after_seq=<epoch>:<seq>` 查询参数），任何自建后端服务（Go / Python / Rust / Node）、CLI/TUI、桌面/移动端、协作子 Agent 或原生浏览器 `EventSource` 均可直连。

### 1.3 分段水位锁定与三层硬字节配额（解决未落库长任务防截断）
业务主库通常只在**一轮任务（Run）结束时**才执行落库，**正在运行途中的事件尚未写入主库**。若使用无差别 FIFO 环形缓冲，单轮长任务产生数千个 Token 事件时会把本轮前半段挤出窗口，导致中途刷新页面时主库与 Seamline 均无法恢复前半段。

为此，Seamline 采用 **分段水位字节环形缓冲（Pinned-Run Byte RingBuffer）**：

```text
  已淘汰区间              可淘汰历史区 (已落业务主库)           锁定活跃区 (正在生成，尚未落主库，禁止截断！)
 ───────────────┬──────────────────────────────────────────┬──────────────────────────────────────────────► seq
 1 .. earliest-1│ earliest_seq  ...  active_run_start_seq-1│ active_run_start_seq  ...  latest_seq
                └────── 受 HistoryTTL + MaxHistoryBytes ───┘└────────── 受 MaxActiveRunBytes 保护 ─────────┘
```

1. **活跃轮起始水位 `active_run_start_seq`（免淘汰锁定，显式开轮）**：
   - **只有显式 `run_started` 才开新轮**：会话处于非运行态（`idle` / `completed` / `failed` / `interrupted`）且收到 `type == "run_started"` 时，记录 `active_run_start_seq = event.Seq`，会话状态置为 `running`，并开始活跃轮锁定与 `ActiveRunIdleTimeout` 空闲计时。运行中再次收到 `run_started` 时保持原起点不变。生产者在一轮开始时必须先发 `run_started`。
   - 非运行态下收到的其他事件（包括轮外的终态事件）直接分配 `seq` 进入历史区，不开新轮，也不会触发 `idle_timeout` 封存。
   - 当运行中收到终态事件（`type == "run_finished"` 或 `"run_failed"`，以及 Phase 2 控制面的 `"run_interrupted"`）提交成功时，重置 `active_run_start_seq = 0`，本轮全部事件转入历史区；
   - **淘汰铁律**：`HistoryTTL` 与 `MaxHistoryBytes` **仅允许淘汰 `seq < active_run_start_seq` 的已完结轮次事件**（当 `active_run_start_seq == 0` 时可淘汰全部历史事件）。因此只要当前轮仍在运行，始终满足 **`earliest_seq <= active_run_start_seq`**——中途任意时刻刷新或重连，未落主库的当前轮事件 100% 完整在位。
   - **最近一轮保护**：最近一个已完结轮次（`[last_run_start_seq .. last_run_end_seq]`）**不参与 `MaxHistoryBytes` 字节淘汰**，且它的 `HistoryTTL` **从该轮结束时刻开始计算**；直到下一轮完结、它不再是"最近一轮"后，才按普通历史事件处理。由于保留窗口必须连续，排在最近一轮之后的轮外历史也要等该轮按 `HistoryTTL` 过期后才能按字节淘汰，在此之前保留的历史字节可以超过 `MaxHistoryBytes`（仍受 `HistoryTTL` 约束）。否则一轮超过 `MaxHistoryBytes` 或运行超过 `HistoryTTL` 时，会在 `run_finished` 提交的同一刻被淘汰大半，破坏 §2.3 情形 B（客户端在主库事务提交前读主库，需要从热窗口补发 `(S_prev, S]`）。
2. **静默僵尸轮自动封存（`ActiveRunIdleTimeout`，默认 `2m`）**：
   - 每次成功发布事件均续期活跃轮空闲期限。若生产者崩溃或静默超过 `ActiveRunIdleTimeout` 仍未发终态事件（控制面 Worker Lease 上线前的最小兜底，上线后与租约机制复用同一路径），后台清扫器自动追加一条终态事件（`type: "run_failed"`, `payload: {"reason":"idle_timeout"}`），扇出给订阅者并将 `active_run_start_seq` 归零，防止死流永久锁死内存。生产者空闲等待下游慢工具时可发送轻量心跳或状态事件续期。
3. **三层硬字节配额（封死大包与内存膨胀）**：
   - `MaxEventBytes`（默认 `256 KiB`）：限制单次 `PublishRequest` 的载荷字节数，入口处拦截，超限返回 `413`（`ErrEventTooLarge`）；
   - `MaxActiveRunBytes`（默认 `16 MiB` / 会话）：限制当前锁定活跃区（`seq >= active_run_start_seq`）累计保留字节数（足以容纳 **50,000+ 条**流式 Token / 工具 JSON 碎片事件，同时在异常 Worker 死循环狂吐事件时以 `413 ErrActiveRunQuotaExceeded` 熔断防 OOM）。**终态事件（`run_finished` / `run_failed` / `run_interrupted`）豁免此配额检查**（自身仍受 `MaxEventBytes` 限制），保证超限后生产者仍能正常结束本轮；
   - `MaxHistoryBytes`（默认 `8 MiB` / 会话）+ `HistoryTTL`（默认 `15m`）：仅对已完结历史区事件执行 FIFO 淘汰，推进 `earliest_seq`。

### 1.4 每会话独立 `epoch` 与 `MetaTTL`
游标为 `<epoch>:<seq>`。**`epoch` 属于每个会话流自身（由 `Storage` 在会话创建时生成），不属于 Broker 进程**（`/healthz` 返回的是进程 `instance_id`）：
- **淘汰事件不重置身份**：`HistoryTTL` / `MaxHistoryBytes` 只回收旧 `Event`，会话的 `epoch` 与 `latest_seq` 计数器保留。因此持旧游标重连的客户端收到的是明确的 `cursor_expired`，且同一 `epoch` 内 `seq` 绝不回退或从 1 重来。
- **元数据长周期保留（`MetaTTL`，默认 `24h`）**：会话元数据（`epoch`、`latest_seq`、状态）闲置超过 `MetaTTL` 才整体回收（发布、订阅，以及清扫器对内存中每个驻留 Actor 的 `Touch` 均会续期）。回收后若再次创建同名会话，生成**全新 `epoch`** 且 `seq` 从 1 开始；内存版进程重启后所有会话自然均为新 `epoch`。

### 1.5 发布重试幂等（`idempotency_key`）
- `PublishRequest` 提供可选字段 `idempotency_key`（同一会话内保留 `5m` 时间窗，独立于 `HistoryTTL`；索引存放在会话 Actor 中，Actor 被空闲驱逐时随之丢弃，因此 `IdleSessionTTL`（默认 `30m`）不应小于该时间窗）。
- 重复 `idempotency_key` 直接返回首次提交的 `{seq, epoch, deduplicated: true}` 且不再扇出（不比对请求体内容，与 NATS `Nats-Msg-Id` 及 Centrifugo `idempotency_key` 语义一致）。
- **原子性红线**：`idempotency_key` 与 `sessionActor` 内存状态（`latest_seq`、`active_run_start_seq`、会话状态）必须在 `Storage.Append` **成功之后**才更新；存储写入失败时内存零变更，调用方使用相同 `idempotency_key` 重试可正常提交。
- **没有独立的元数据写入**：会话状态（`state`、`active_run_start_seq`、最近一轮）由 `Storage.Append` 与事件在同一次提交里原子写入（Redis 中为同一个 Lua 脚本），不再有 best-effort 的 `SaveMeta`；`Inspect` / `ListSessions` 直接读 Storage。

---

## 2. 冷热衔接契约：业务主库快照与 `seamline_cursor` 因果对齐

当客户端首次打开历史会话，或收到断层通知回源业务主库同步历史时，通过因果水位线消除漏读与重复：

### 2.1 写入侧顺序约定（Worker $\rightarrow$ Seamline $\rightarrow$ 业务主库）
0. **一轮开始时先发 `run_started`**：这一轮之后的事件才会进入活跃区锁定（§1.3）。
1. **先发布定稿终态至 Seamline**：一轮 Run 完成时，Worker 发布 `run_finished` 事件至 Seamline，获取返回的会话水位线 `seamline_cursor = "<epoch>:<seq>"`（例如 `"9f8e7d6c5b4a:520"`）。
2. **携带游标同事务落业务主库**：Worker（或业务后端）将该轮完整消息/工具记录写入主数据库时，在**同一数据库事务**中更新会话的 `last_seamline_cursor = "9f8e7d6c5b4a:520"`。

### 2.2 读取与订阅衔接顺序（Client $\rightarrow$ 业务主库 $\rightarrow$ Seamline）
1. 客户端请求业务主库加载历史已完结轮次，响应同时返回 `{ messages: [...], seamline_cursor: "<epoch>:<S_db>" }`（全新会话返回 `""`）。
2. 客户端携带 `?after_seq=<seamline_cursor>`（或 `Last-Event-ID: <seamline_cursor>`）连接 `GET /v1/sessions/{id}/stream`。

### 2.3 并发交错无损证明
假设第 $R$ 轮刚刚在 `seq = S` 处完结：
- **情形 A（客户端读主库发生在主库事务提交之后）**：主库返回包含第 $R$ 轮的快照及 `<epoch>:S`。客户端以 `after_seq=<epoch>:S` 订阅 Seamline，Seamline 精确推送 `seq > S` 的第 $R+1$ 轮实时事件——**零漏读、零重复**。
- **情形 B（客户端读主库发生在 Seamline `run_finished` 之后、主库事务提交之前）**：主库返回截至第 $R-1$ 轮的快照及 `<epoch>:S_{prev}`。客户端以 `after_seq=<epoch>:S_{prev}` 订阅 Seamline，Seamline 从热窗口精确补发 `(S_{prev}, S]` 的第 $R$ 轮事件流——**零漏读、零重复**。

---

## 3. 显式断层控制协议：`event: cursor_reset`

为彻底消除内存版重启或历史淘汰时的静默数据缺口，对所有携带非空游标（`after_epoch != ""` 或 `after_seq > 0`）的续传请求执行严格游标分类：

### 3.1 游标分类与首帧行为
- **全新无游标订阅（`after_epoch == "" && after_seq == 0`）**：不发 `cursor_reset`，直接重放当前热窗口 `[earliest_seq .. snap_seq]` 并无缝切入实时流 `seq > snap_seq`。
- **断层 1 —— 纪元不匹配（`reason: "epoch_mismatch"`）**：
  `after_epoch != session_epoch`（包括内存节点重启、`MetaTTL` 到期重建、或仅传裸 `after_seq > 0` 未传 `epoch`）。即使新纪元当前 `latest_seq == 0`（空缓冲），也**必须在首帧下发 `event: cursor_reset`**。
- **断层 2 —— 游标已滑出热窗口（`reason: "cursor_expired"`）**：
  `after_epoch == session_epoch` 且 `after_seq + 1 < earliest_seq`（请求的旧已完结轮次事件已被淘汰）。首帧下发 `event: cursor_reset`。
- **断层 3 —— 游标超前于服务端（`reason: "cursor_ahead"`）**：
  `after_epoch == session_epoch` 且 `after_seq > latest_seq`（非法或错乱游标）。首帧下发 `event: cursor_reset`。
- **补发中途缺口（`insufficient_state`）**：`cursor_reset` 仅出现在首帧；若重放途中底层记录因并发淘汰产生空洞，发送 `event: stream_error`（`{"error":"insufficient_state"}`）并关闭连接，客户端重连后将在首帧收到 `cursor_reset`。

### 3.2 `cursor_reset` 帧格式与“未入库活跃轮”零缺口恢复
```text
event: cursor_reset
data: {"reason":"cursor_expired","requested_epoch":"9f8e7d6c5b4a","requested_seq":42,"current_epoch":"9f8e7d6c5b4a","earliest_seq":301,"latest_seq":850,"active_run_start_seq":600}
```
`cursor_reset` **不携带 `id:`**，且发完后**不断开连接**，服务端紧接着补发当前热窗口内 `[earliest_seq .. snap_seq]` 的全部事件（每帧带 `id: <epoch>:<seq>`，自动把浏览器 `Last-Event-ID` 推进至最新位置）并转入实时流。

**为什么这能实现 100% 零缺口恢复？**
由 §1.3 的淘汰铁律可知，若当前有未完结长任务正在生成，必有 `earliest_seq <= active_run_start_seq`（上例中 `301 <= 600`）：
1. 客户端收到 `cursor_reset` 后保持连接暂存后续事件，异步向业务主库拉取最新已落库快照（主库已存妥 `< 600` 的所有已完成轮次，返回 `seamline_cursor = "...:599"`）；
2. 客户端丢弃暂存流中 `seq <= 599` 的重叠历史事件，保留 `seq >= 600` 的全部当前活跃轮事件继续渲染——**已入库历史由主库补齐，尚未入库的正在生成内容由 Seamline 锁定区完整补齐，严丝合缝！**

---

## 4. 核心生态位：Seamline 与 Centrifugo 的本质区别

当数据面采用正统的纯事件游标后，Seamline 与通用 Go 实时推送网关 `Centrifugo` 的边界非常清晰：

- **数据平面（同构部分）**：两者均采用 `epoch + seq/offset` 热窗口恢复模型（Seamline 保持纯标准 HTTP POST + 原生 W3C SSE 零私有 SDK 优势，外加针对未落库活跃 Run 的 `active_run_start_seq` 分段锁定保护）。**若仅需单向消息广播，直接使用 Centrifugo 即可。**
- **核心分水岭 —— 有状态 Agent 会话控制面（Stateful Session Control Plane）**：
  Centrifugo 的抽象是**无状态广播频道（Channel）**；Seamline 的抽象是**有生命周期状态的 Agent 任务会话（Session + Run）**，内建生产级 Background Agent 必需的四项分布式协调原语：

| 对比维度 | **Centrifugo（无状态实时广播网关）** | **Seamline（有状态 Agent 会话总线）** |
| :--- | :--- | :--- |
| **1. 后台 Worker 暴毙监管** | ❌ 不感知发布者死活；Worker OOM 崩溃后前端永久卡在“思考中”。 | ✅ **内置 Worker 心跳租约（Lease）**：失联超时自动封存当前活跃轮、置为 `failed (worker_lost)` 并广播终态。 |
| **2. 人工审批防重门禁** | ❌ 多端同时点“批准”仅被当作两条普通消息转发，业务需自建分布式锁。 | ✅ **一等公民 `WaitingApproval` 原子锁**：按 `interrupt_id` 执行 CAS 幂等解锁，多端连点全局严格仅放行首次。 |
| **3. 反向运行干预信道** | ❌ 单向下行推送为主，停止与插话需业务后端自建信令队列处理竞态。 | ✅ **双向非对称控制流**：内置面向后台 Worker 的控制订阅信道，原生支持 `Cancel`（立即停机）与 `Steer`（插话排队）。 |
| **4. 同会话并发防撞** | ❌ 同一频道允许任意多发布者并发混推。 | ✅ **内置 Run 互斥策略**：同一 `session_id` 并发触发时原生支持 `reject` / `interrupt` / `enqueue`。 |
| **5. 未落库长任务防截断** | ⚠️ 纯固定大小/TTL 滑窗，单轮海量 Token 易把正在生成的头部挤丢。 | ✅ **分段水位锁定（`active_run_start_seq`）**：滑动淘汰只清已完结轮次，运行中未落库事件全程锁定免截断。 |

---

## 5. 演进路线图（Phase 2 $\rightarrow$ Phase 3）

### Phase 2a：纯事件游标热窗口底座重构（数据面）
- 将双速字符串归并精简为统一载荷透明 `Event`（`<epoch>:<seq>`，每帧业务事件携带 `id:`）；
- 落地每会话独立 `epoch` + `MetaTTL`、分段水位锁定（`active_run_start_seq` + `ActiveRunIdleTimeout`）、三层硬字节配额（`MaxEventBytes` / `MaxActiveRunBytes` / `MaxHistoryBytes` + `HistoryTTL`）、`idempotency_key` 去重及 `event: cursor_reset` 断层协议。

### Phase 2b：有状态 Agent 会话控制面（核心壁垒）
- 同会话 Run 并发互斥策略（`reject` / `interrupt` / `enqueue`）；
- 面向后台 Worker 的反向控制流接口（实时下发 `Cancel`、`Steer`、`ApprovalResolved`）；
- `WaitingApproval` + `interrupt_id` 原子 CAS 幂等审批门禁；
- Worker 心跳租约（Lease Timeout）与失联自动判死收尾。

### Phase 3：可插拔 Redis 热存储后端与生产配套
- `RedisStorage` 驱动（Redis Streams + 原生 `EXPIRE` + 透明 5~10ms Pipeline 微批刷写，支持多实例无状态水平扩展）；
- 分离 Worker 发布令牌与下游订阅/控制令牌，暴露 `/metrics` 核心运行指标。
