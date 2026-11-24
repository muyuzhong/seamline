# Seamline v2 架构演进与核心边界规范

**单调事件游标 · TTL 热恢复窗口 · Agent 有状态会话控制面**

本文档记录 Seamline 在完成 Phase 1 验证后确立的核心架构收敛决策：明确中间件的存储职责边界、从“双游标归并”向“纯事件游标 + TTL 热窗口”的数据面简化，以及 Seamline 区别于通用实时推送网关（如 Centrifugo）的核心护城河。

---

## 1. 职责边界：不做永久数据库，只管“短期热恢复窗口（Hot Window + TTL）”

在真实的生产级 Agent 系统中，**长期历史持久化（System of Record）不属于流式中间件的职责**：
- **Local-first 客户端（CLI / TUI / 桌面端 / 移动端）**：完整对话与工具轨迹直接持久化在客户端本地（SQLite / IndexedDB / JSONL）。
- **服务端 SaaS 应用**：业务后端自身拥有主数据库（PostgreSQL / MySQL / MongoDB），用于会话列表、账单核算与后续轮次 Prompt 组装。

若流式中间件自行维护一套本地磁盘 WAL 作为永久存储，不仅重复造数据库轮子，还会造成“业务主库一份、中间件磁盘一份”的双写一致性负担。因此，Seamline 严守以下存储边界：

1. **只存短期热窗口（TTL + 容量上限）**：仅缓存活跃运行期及结束后短期窗口内（例如 `TTL = 10m ~ 30m`、单会话环形缓冲上限 `max_events = 4096`）的事件流与控制状态，到期自动释放。
2. **双引擎可插拔，零自研磁盘 WAL**：
   - **默认内置 `MemoryEngine`**：零外部依赖单二进制启动，满足本地开发、单机部署与嵌入式场景。
   - **生产可选 `RedisEngine`**：基于 Redis 原生 `Streams (XADD MAXLEN ~)` + `Hash` + `EXPIRE` 实现跨重启热恢复与多实例无状态水平扩容。
3. **冷热衔接清晰**：秒级/分钟级断网、切网、页面刷新直接命中 Seamline 热窗口毫秒级追平；跨天重新打开历史会话时，下游应用先从自身主库或本地缓存加载历史快照，再接入 Seamline 订阅实时增量。

---

## 2. 数据面收敛：单调事件游标（Single Monotonic Event Cursor）

确立“短期 TTL 热窗口”前提后，无需再为节省永久磁盘空间而在 Broker 内做流式字符串拼接与归并。Phase 2 起数据面统一收敛为 **会话内单调递增事件游标（`<epoch>:<seq>`）**：

### 2.1 核心优势
- **100% 载荷透明（Payload-Opaque）**：Broker 不再假设流内容是可拼接纯文本。无论是文本 `delta`、多路并行原生工具调用（Parallel Tool Calls）交错输出的半截 JSON 参数流、推理思维链块，还是自定义业务状态补丁，进入会话即分配严格递增的 `seq = 1, 2, 3...` 原样缓存与扇出。
- **下游完全中立（Downstream-Agnostic）**：协议层仅为标准 HTTP + JSON + `text/event-stream`（同时支持 `Last-Event-ID` 请求头与 `?after_seq=<epoch>:<seq>` 查询参数）。任何自建后端服务（Go / Python / Rust / Node）、CLI 终端、桌面/移动客户端、协作子 Agent 或浏览器原生 `EventSource` 均可零 SDK 直连。
- **单套增量状态机 & 零重传浪费**：每一帧事件均携带唯一游标 `id: <epoch>:<seq>`。下游在 `seq = 42` 闪断重连时，Broker 精确从 `seq = 43` 补发，无需重传已收半截文本，下游也无需编写额外的“快照覆盖草稿”分支。
- **天然映射 Redis Streams**：内存环形切片与 Redis `XADD / XRANGE` 模型 1:1 同构，消除多存储后端适配复杂度。

### 2.2 窗口截断保护（Cursor Expiration）
当极端长任务的事件数超出环形缓冲上限（或已过 `TTL`），导致重连客户端携带的 `after_seq` 早于当前缓冲区首条事件 `earliest_seq` 时，Broker 显式返回 `event: cursor_expired`（携带 `earliest_seq` 与 `latest_seq`），明确告知下游需先向业务主库同步最新状态快照，绝不静默漏帧。

---

## 3. 核心生态位：Seamline 与 Centrifugo 的本质区别

当数据面收敛为“单调事件游标 + TTL 热缓冲 + 可选 Redis”后，必须客观回答一个核心架构问题：**Seamline 与成熟的 Go 实时推送网关 `Centrifugo` 到底有什么区别？**

### 3.1 相同点（不重复发明的部分）
在 **“单向下行事件扇出 + `epoch:seq` 短期缓存断线恢复”** 这一半数据平面上，两者底层机制高度同构。Seamline 在这一层的差异仅在于**纯标准 HTTP/SSE 零私有 SDK 直连**（无需引入 Centrifugo 专用客户端协议库）。**如果只需要单向消息推送与断线补帧，直接使用 Centrifugo 即可。**

### 3.2 本质分水岭：无状态广播管道 vs. 有状态 Agent 会话控制面

`Centrifugo` 的核心抽象是 **无状态广播频道（Stateless Channel）**——它只负责把消息从发布者搬运给订阅者，完全不感知“任务生命周期”、“发布者进程死活”或“反向审批锁”。若用 Centrifugo 搭建生产级 Background Agent，业务团队仍需自行手写最易出错的分布式状态协调逻辑。

**Seamline 的真正核心价值，是内建了面向长耗时 Agent 的“运行态会话控制面（Stateful Session Control Plane）”**：

| 对比维度 | **Centrifugo（通用实时推送网关）** | **Seamline（Agent 有状态会话总线）** |
| :--- | :--- | :--- |
| **核心领域抽象** | **Channel（广播房间）**：无任务状态概念 | **Session + Run（有状态任务会话）**：内置 `idle / running / waiting_approval / interrupted / completed / failed` 状态机 |
| **同会话并发防撞** | ❌ 无（多个 Worker 往同一频道并发推流会互相污染） | ✅ **内置 Run 互斥门禁**：支持 `reject`（拒绝并发）、`interrupt`（打断旧轮开新轮）、`enqueue`（排队执行）策略 |
| **反向控制信道** | ❌ 单向为主（前端控制需绕回业务后端自建队列） | ✅ **双向非对称通道**：内置面向后台 Worker 的控制信令流（`Cancel` 实时停机穿透、`Steer` 运行中插话排队） |
| **人工审批门禁（HITL）** | ❌ 无（多端同时点“批准”会被当作两条普通消息转发） | ✅ **一等公民 `WaitingApproval` 原子锁**：基于 `interrupt_id` 的 CAS 幂等门禁，多端并发连点严格保证全局仅放行一次 |
| **后台 Worker 死活监管** | ❌ 只管前端订阅者在线态，不感知后台发布者死活 | ✅ **内置 Worker 心跳租约（Lease）**：后台 Worker 进程若 OOM 暴毙失联超时，Broker 自动接管收尸、置为 `failed (worker_lost)` 并广播通知所有下游停止等待 |
| **下游接入协议** | 推荐使用私有协议客户端 SDK (`centrifuge-*`) | 纯标准 HTTP POST + W3C SSE (`Last-Event-ID` / `?after_seq=`)，任意自建应用 10 行代码直连 |

> **一句话定位边界**：Centrifugo 解决的是 **“怎么把实时消息可靠推给前端”**；Seamline 解决的是 **“怎么托管一个在后台长跑的 Agent 会话——既能断线续流，又能多端幂等批权限、随时踩刹车/插话、且后台 Worker 暴毙时不留僵尸会话”**。

---

## 4. 演进路线图（Phase 2 $\rightarrow$ Phase 3）

### Phase 2：单调事件游标收敛 + 有状态反向控制面（下一步核心）
1. **数据面精简升级**：
   - 将双速字符串归并简化为会话级 **单调事件游标（`epoch:seq`）+ 定长/TTL 环形热缓冲**，原生无损透传并行工具调用 JSON 流与任意自定义事件结构；
   - 支持 `cursor_expired` 截断通知与 TTL 自动回收。
2. **反向控制状态机与 Worker 租约**：
   - **Run 并发互斥与生命周期管理**；
   - **Worker 控制通道**（支持后台 Worker 实时接收 `Cancel` 中断、`Steer` 插话与审批结果）；
   - **幂等人工审批门禁（`WaitingApproval` + `interrupt_id` 原子 CAS）**；
   - **Worker 租约心跳（Lease Timeout）**：超时失联自动推进至 `failed (worker_lost)` 并向全量下游广播终态。

### Phase 3：可插拔 Redis 热存储后端与生产配套
1. **`RedisEngine` 驱动**：基于 Redis Streams + Hash + 原生 TTL 实现跨实例/跨重启的热窗口与会话锁共享，支持多节点水平扩展。
2. **轻量安全与可观测性**：区分 Worker 发布令牌与下游订阅/控制令牌，暴露 `/metrics` 核心运行指标与极简接入示例。
