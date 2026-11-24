# Seamline

**Resumable Dual-Speed AI Stream Broker in Pure Go**  
**面向 AI 生成式流的双速可恢复流式代理（零外部依赖 · Go 标准库实现）**

[![CI](https://github.com/muyuzhong/seamline/actions/workflows/ci.yml/badge.svg)](https://github.com/muyuzhong/seamline/actions/workflows/ci.yml)
[![Go 1.23+](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-emerald.svg)](LICENSE)

---

## Why Seamline? / 为什么需要 Seamline？

Traditional LLM and AI agent backends face a painful architectural tradeoff when streaming tokens over Server-Sent Events (SSE):

1. **Fire-and-Forget SSE (No Mid-Stream Persistence):** Tokens stream directly from the worker to the browser. If the user hits **F5**, switches mobile networks, or opens the conversation on a second tab mid-generation, the half-finished response disappears or breaks.
2. **Naive Per-Token Persistence (100x Write Amplification):** Writing every 3-character token delta into Postgres, Kafka, or Redis Streams turns a single 500-token paragraph into 500 durable database writes, bloating storage and slowing catch-up reads.

**Seamline** resolves this tradeoff with a **Dual-Speed Stream Architecture** (**双速流式架构**):
- **Fast Path (RAM Coalescing):** High-frequency token deltas (`seq=1..N`) are coalesced in memory inside a per-session actor and fanned out non-blockingly to live SSE subscribers with **zero storage I/O**.
- **Slow Path (Single-Record Compaction):** When the stream completes (`fin=true`), all 100+ token deltas compact into **one durable `Record`**, advancing the session offset by `+1`.
- **Zero-Gap Mid-Stream Recovery (`Last-Event-ID` + `inflight_snapshot`):** Only finalized `record` frames carry an SSE `id: <epoch>:<offset>` cursor. When a browser reconnects mid-stream (or reloads from `offset=0`), Seamline atomically replays committed records after the cursor, emits a single coalesced `inflight_snapshot` containing all tokens generated up to `last_seq`, and seamlessly resumes live `delta` tailing—with **zero lost or duplicated characters**.

> **一句话总结：** Seamline 将高频临时的 Token 增量（内存聚合 + 无阻塞广播）与低频持久化的完整消息记录（`fin=true` 单条归并落盘）彻底解耦；配合 `Last-Event-ID` 与 `inflight_snapshot` 原子快照交接，原生浏览器 `EventSource` 断线重连或中途按 **F5 刷新页面** 都能做到不丢一字、不重一字。

---

## Architecture / 系统架构

```
  AI Worker / Agent                        Seamline Broker (Per-Session Actor)                      Browser / Mobile Clients
 ┌─────────────────┐                    ┌───────────────────────────────────────┐                 ┌──────────────────────────┐
 │ POST /events    │  atomic / fin=true │  1. Storage Engine (Committed Log)    │  event: record  │ Native EventSource / SDK │
 │  - user_message ├───────────────────►│     Offset 1: user_message            ├────────────────►│ id: a1b2c3:1             │
 │  - delta seq=1  │                    │     Offset 2: compacted assistant msg │                 │                          │
 │  - delta seq=2  │  in-flight deltas  ├───────────────────────────────────────┤  inflight_snap  │ Mid-stream F5 / Reconnect│
 │  - ...          ├───────────────────►│  2. Ephemeral InFlight Buffer (RAM)   ├────────────────►│ Last-Event-ID: a1b2c3:1  │
 │  - delta fin=1  │  (0 DB writes!)    │     mergedBuf: "Hello world..."       │   + live delta  │  -> 1x inflight_snapshot │
 └─────────────────┘                    │     lastSeq:   48                     │                 │  -> live deltas 49..N    │
                                        └───────────────────────────────────────┘                 └──────────────────────────┘
```

### Zero-Gap Catch-Up & Live Handoff Invariant

Inside `Broker.Subscribe(ctx, sessionID, afterOffset, afterEpoch)`:
1. **Epoch Validation:** If `afterEpoch != ""` and `afterEpoch != broker.Epoch()` (e.g., an in-memory node restarted with fresh state), `afterOffset` automatically resets to `0` (Centrifugo-style epoch safety).
2. **Atomic Registration & Snapshot:** Under `actor.mu`, the subscriber registers its buffered `liveCh` **at the exact instant** it snapshots `snapOffset := actor.lastOffset` and `inFlightSnap` (`merged`, `last_seq`), then immediately releases the lock.
3. **Deduplicated Pump:**
   - Reads committed records `(afterOffset, snapOffset]` from `Storage` and emits `event: record` (`id: <epoch>:<offset>`).
   - If `inFlightSnap != nil`, emits one `event: inflight_snapshot` (`seq = inFlightSnap.LastSeq`, `delta = inFlightSnap.Merged`).
   - Drains `liveCh`, skipping any `record` with `offset <= snapOffset` and any `delta` with `seq <= inFlightSnap.LastSeq`, forwarding every subsequent event in strict order.
4. **Slow Subscriber Backpressure:** `Publish` never blocks on slow readers (`select` with `default`). If a subscriber's buffer fills up, Seamline evicts only that subscriber and emits `event: stream_error` (`{"error":"slow_subscriber"}`) before closing its stream.

---

## SSE Wire Protocol / 传输协议规范

`GET /v1/sessions/{id}/stream` speaks standard W3C Server-Sent Events:

```text
: connected epoch=9f8e7d6c5b4a

id: 9f8e7d6c5b4a:1
event: record
data: {"kind":"record","offset":1,"epoch":"9f8e7d6c5b4a","type":"user_message","payload":{"role":"user","content":"Explain Seamline"},"created_at":"2026-11-21T12:00:00Z"}

event: inflight_snapshot
data: {"kind":"inflight_snapshot","epoch":"9f8e7d6c5b4a","stream_id":"turn-1","seq":42,"type":"assistant_message","delta":"Seamline decouples high-frequency token deltas...","created_at":"2026-11-21T12:00:01Z"}

event: delta
data: {"kind":"delta","epoch":"9f8e7d6c5b4a","stream_id":"turn-1","seq":43,"type":"assistant_message","delta":" from durable storage.","created_at":"2026-11-21T12:00:01Z"}

id: 9f8e7d6c5b4a:2
event: record
data: {"kind":"record","offset":2,"epoch":"9f8e7d6c5b4a","stream_id":"turn-1","type":"assistant_message","payload":{"content":"Seamline decouples high-frequency token deltas from durable storage."},"created_at":"2026-11-21T12:00:02Z"}

event: stream_error
data: {"error":"slow_subscriber"}
```

### Why `id:` Is Only Set on `event: record`
Browser `EventSource` automatically remembers the most recent `id:` header and sends it as `Last-Event-ID` on reconnect. Because `delta` frames intentionally omit `id:`, a client that drops at token `#42` reconnects with `Last-Event-ID: <epoch>:1` (the last committed record), receives the coalesced `inflight_snapshot` for tokens `1..55` generated so far, and resumes live `delta` frames from `#56` onward.

### Why `event: stream_error` Instead of `event: error`
Per the HTML `EventSource` specification, a server-sent `event: error` dispatches on the same `"error"` event channel as built-in browser network connection errors (`es.onerror`). Naming application-level terminal stream notifications `event: stream_error` keeps protocol backpressure signals (`slow_subscriber`) cleanly separated from transport disconnects.

---

## Quickstart / 快速上手

Requirements: **Go 1.23+** (100% Go standard library, zero external dependencies).

### 1. Start the Seamline Server & Live F5 Playground

```bash
go run ./cmd/seamline --addr :8080 --demo=true
```

Open **http://localhost:8080** in your browser to launch the interactive **Seamline Live Playground**:
- Click **Stream 100+ AI Tokens** to trigger a background token stream.
- Click **Drop 1.2s & Resume (`Last-Event-ID`)** on Client B—or press **F5** mid-stream—and watch `inflight_snapshot` + live `delta` recovery and single-record WAL compaction in real time.

### 2. Stream from the Standalone CLI Worker (`simworker`)

In a second terminal:

```bash
go run ./cmd/simworker \
  --server http://localhost:8080 \
  --session demo-1 \
  --delay 30ms \
  --text "Hello from simworker! Every token delta streams live in memory and compacts into one record on fin=true."
```

### 3. Inspect & Resume via `curl`

```bash
# Health & Broker Epoch
curl -s http://localhost:8080/healthz | jq .

# Create a session (body optional)
curl -s -X POST http://localhost:8080/v1/sessions \
  -H 'Content-Type: application/json' \
  -d '{"session_id":"chat-42"}' | jq .

# Publish an atomic user message (commits Offset 1)
curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"user_message","payload":{"role":"user","content":"Hi!"}}' | jq .

# Publish in-flight token deltas (0 DB writes until fin=true)
curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"msg-1","seq":1,"type":"assistant_message","delta":"Hello ","fin":false}' | jq .

curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' \
  -d '{"stream_id":"msg-1","seq":2,"type":"assistant_message","delta":"world!","fin":true}' | jq .

# Inspect session state, LastOffset, and active subscribers
curl -s http://localhost:8080/v1/sessions/chat-42 | jq .

# Subscribe / Resume SSE stream from a Last-Event-ID cursor
curl -N -H 'Last-Event-ID: <epoch>:1' \
  http://localhost:8080/v1/sessions/chat-42/stream
```

---

## HTTP API Summary

| Method & Path | Description |
| :--- | :--- |
| `GET /healthz` | Health status and active broker `epoch`. |
| `POST /v1/sessions` | Create or load a session (`201 Created`). Optional `{"session_id":"..."}`. |
| `GET /v1/sessions` | List active/persisted session snapshots (`200 OK`). |
| `GET /v1/sessions/{id}` | Inspect session metadata, `in_flight` buffer, and `subscriber_count` (`200` / `404`). |
| `POST /v1/sessions/{id}/events` | Publish an atomic record or streaming delta (`200`, `400` on invalid/gap, `409` on stream conflict). |
| `GET /v1/sessions/{id}/stream` | Resumable SSE endpoint (`Last-Event-ID` header or `?after_offset=<n>&epoch=<e>`). |
| `GET /` | Embedded **Seamline Live Playground** (when `--demo=true`). |
| `POST /v1/demo/simulate` | Trigger background ~100-chunk token stream for zero-setup browser evaluation (`--demo=true`). |

---

## Testing / 测试验证

```bash
gofmt -l .
go vet ./...
go test -race -v ./...
```

---

## Roadmap / 演进路线图

- **Phase 1 (Current):** Core dual-speed broker (`SessionActor`), in-memory storage engine, monotonic sequence idempotency, atomic `inflight_snapshot` + live `delta` handoff, slow-subscriber eviction, idle actor eviction, zero-dependency HTTP/SSE server, CLI worker simulator, and embedded F5 Live Playground.
- **Phase 2:** Append-only disk Write-Ahead Log (WAL) + CRC32 framing & segment compaction implementing `storage.Storage`, crash recovery with persistent epoch continuity, and Prometheus `/metrics` observability.
- **Phase 3:** Multi-stream concurrent tool-call multiplexing per session, consistent-hash horizontal session routing, and lightweight TypeScript / Go client SDKs.

---

## License

[MIT License](LICENSE) © 2026 muyuzhong
