# Seamline

**Resumable AI Stream Broker in Pure Go — one event cursor, a pinned active run, plain SSE**  
**面向 AI Agent 的可恢复流式代理：纯事件游标 · 活跃轮锁定 · 标准 SSE（零外部依赖 · Go 标准库实现）**

[![CI](https://github.com/muyuzhong/seamline/actions/workflows/ci.yml/badge.svg)](https://github.com/muyuzhong/seamline/actions/workflows/ci.yml)
[![Go 1.23+](https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white)](go.mod)
[![License: MIT](https://img.shields.io/badge/License-MIT-emerald.svg)](LICENSE)

---

## Why Seamline? / 为什么需要 Seamline？

Streaming an AI agent's output over Server-Sent Events breaks the moment the user hits **F5**, switches networks or opens a second tab: the half-finished answer is gone, and the application database only stores a turn after it finished.

**Seamline** keeps a short, resumable hot window for every session:
- **One cursor for every event:** text deltas, half-finished tool-call JSON, status events — every published event is payload-opaque and gets the next `<epoch>:<seq>`. Every SSE frame carries `id: <epoch>:<seq>`, so a reconnect with `Last-Event-ID` (or `?after_seq=`) continues exactly at the next event.
- **The active run is pinned:** between `run_started` and `run_finished` nothing of the run is evicted, so a page load in the middle of a long answer replays the whole run — the part that is not in your database yet.
- **Explicit gaps:** a cursor from an older epoch, one that fell out of the window, or one ahead of the stream receives a `cursor_reset` first; nothing is skipped silently.
- **Production guards:** byte quotas per event, per active run and per history, idle runs sealed with `run_failed`, `idempotency_key` deduplication for retries.

> **一句话总结：** 每个事件进入会话就拿到单调递增的游标 `<epoch>:<seq>`，每帧 SSE 都带 `id:`；从 `run_started` 到终态事件之间的活跃轮绝不被淘汰，所以中途刷新也能补齐主库里还没有的半截内容；游标无法续接时显式发 `cursor_reset`，绝不静默丢帧。

---

## Architecture / 系统架构

```text
  AI Worker / Agent                  Seamline (per-session actor + hot window)                 Browser / Mobile / Services
 ┌────────────────────┐            ┌──────────────────────────────────────────────┐          ┌─────────────────────────────┐
 │ POST /events       │            │ seq 1  user_message           history         │          │ EventSource / any SSE client│
 │  run_started       ├───────────►│ seq 2  run_started   ◄── active_run_start_seq │─────────►│ id: 9f8e7d6c5b4a:57         │
 │  assistant_delta   │  {epoch,   │ seq 3  assistant_delta        pinned          │  every   │ data: {"seq":57,…}          │
 │  tool_call_delta   │   seq}     │ …                             (never evicted) │  event   │                             │
 │  run_finished      │            │ seq 57 assistant_delta                         │          │ F5 / reconnect:             │
 └────────────────────┘            └──────────────────────────────────────────────┘          │ Last-Event-ID → seq 58 …    │
                                                                                              └─────────────────────────────┘
```

---

## SSE Wire Protocol / 传输协议

```text
: connected epoch=9f8e7d6c5b4a

id: 9f8e7d6c5b4a:2
data: {"seq":2,"epoch":"9f8e7d6c5b4a","type":"run_started","payload":{"run_id":"run_1"},"created_at":"2026-11-24T12:00:00Z"}

id: 9f8e7d6c5b4a:3
data: {"seq":3,"epoch":"9f8e7d6c5b4a","type":"assistant_delta","payload":{"text":"Seamline "},"created_at":"2026-11-24T12:00:01Z"}

event: cursor_reset
data: {"reason":"cursor_expired","requested_epoch":"9f8e7d6c5b4a","requested_seq":1,"current_epoch":"9f8e7d6c5b4a","earliest_seq":2,"latest_seq":57,"active_run_start_seq":2}

event: stream_error
data: {"error":"slow_subscriber"}
```

- Business events have **no `event:` line** (`EventSource` delivers them as `message`) and always carry `id: <epoch>:<seq>`.
- `cursor_reset` (first frame only) and `stream_error` carry no `id:`. Control frames are never named `error`, which would collide with `EventSource`'s network error event.
- The epoch belongs to the session stream; `/healthz` reports the process `instance_id`.

---

## Quickstart / 快速上手

Requirements: **Go 1.23+** (standard library only).

### 1. Start the server and the F5 Live Playground

```bash
go run ./cmd/seamline --addr :8080 --demo=true    # or: make run-demo
```

Open **http://localhost:8080**, click **Start a Simulated Run**, then press **F5**, use **Drop 1.2s & Resume** on Client B, or **Resume with Stale Epoch** to see a `cursor_reset`.

### 2. Stream a run from the CLI worker

```bash
go run ./cmd/simworker --server http://localhost:8080 --session demo-1 --delay 30ms --tools 2
```

It publishes `run_started`, `user_message`, interleaved `assistant_delta` / `tool_call_delta` events and `run_finished`, with an `idempotency_key` per event and retries on transport errors, and prints the final `seamline_cursor`.

### 3. Use the API with `curl`

```bash
curl -s http://localhost:8080/healthz

# Start a run, publish events, end the run
curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' -d '{"type":"run_started"}'
curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"assistant_delta","payload":{"text":"Hello "},"idempotency_key":"run-1:1"}'
curl -s -X POST http://localhost:8080/v1/sessions/chat-42/events \
  -H 'Content-Type: application/json' -d '{"type":"run_finished"}'

# Session state, window and active run
curl -s http://localhost:8080/v1/sessions/chat-42

# Resume after a cursor
curl -N -H 'Last-Event-ID: <epoch>:2' http://localhost:8080/v1/sessions/chat-42/stream
```

---

## HTTP API Summary

| Method & Path | Description |
| :--- | :--- |
| `GET /healthz` | Status and process `instance_id`. |
| `POST /v1/sessions` | Create a session stream (`201`). Optional `{"session_id":"..."}`. |
| `GET /v1/sessions` | List sessions (`meta` with `epoch`, `state`, `earliest_seq`, `latest_seq`, `active_run_start_seq`; `subscriber_count`). |
| `GET /v1/sessions/{id}` | Inspect one session (`200` / `404`). |
| `POST /v1/sessions/{id}/events` | Publish `{"type","payload","idempotency_key"}` → `{"epoch","seq","deduplicated"}`. `400` invalid, `413` `event_too_large` / `active_run_quota_exceeded`. |
| `GET /v1/sessions/{id}/stream` | Resumable SSE (`Last-Event-ID` header or `?after_seq=<epoch>:<seq>`). |
| `GET /` | Embedded Playground (`--demo=true`). |
| `POST /v1/demo/simulate` | Start a simulated run for the Playground (`--demo=true`). |

Server flags for the hot window: `--max-event-bytes`, `--max-active-run-bytes`, `--max-history-bytes`, `--history-ttl`, `--run-idle-timeout`, `--meta-ttl`.

---

## Testing / 测试验证

```bash
make check   # gofmt + go vet + go test -race -count=2
make lint    # golangci-lint (includes gocognit <= 15)
```

---

## Documentation & Engineering Specs / 架构与工程规范

- [Design decisions & roadmap (`docs/DESIGN_V2.md`)](docs/DESIGN_V2.md) — pure event cursor, pinned-run hot window and quotas, per-session epochs, idempotency, `seamline_cursor` database alignment, `cursor_reset`, and the stateful control plane that separates Seamline from Centrifugo.
- [Architecture specification (`docs/ARCHITECTURE.md`)](docs/ARCHITECTURE.md) — event model, commit consistency, zero-gap handoff proof sketch, lock hierarchy, SSE wire protocol.
- [Engineering constraints (`AGENTS.md`)](AGENTS.md) — hard invariants and verification gates.

---

## Roadmap / 演进路线图

- **Phase 1 (done):** Per-session actor broker, atomic catch-up to live handoff, slow-subscriber eviction (`stream_error`), idle actor eviction, HTTP/SSE server, CLI worker simulator, `gocognit <= 15` CI gates.
- **Phase 2a (implemented — Pure Event Cursor & Pinned-Run Hot Window):** Unify data events around a payload-opaque **monotonic event cursor (`<epoch>:<seq>`)** with per-session epochs (`MetaTTL`), **pinned active-run byte ring buffer** (`active_run_start_seq` exempting uncommitted runs from `HistoryTTL` / `MaxHistoryBytes` truncation, guarded by `MaxEventBytes` & `MaxActiveRunBytes`), `idempotency_key` deduplication, and explicit `event: cursor_reset` (`epoch_mismatch` / `cursor_expired` / `cursor_ahead`). See [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md).
- **Phase 2b (Stateful Agent Session Control Plane):** Run concurrency guards (`reject` / `interrupt` / `enqueue`), bidirectional Worker control channel (`Cancel` / `Steer` queue), atomic CAS `WaitingApproval` human-in-the-loop gate, and Worker heartbeat lease (zombie-session reaper).
- **Phase 3 (Production Scale-Out):** Pluggable **Redis hot-window backend** (`Streams + EXPIRE` with transparent pipeline micro-batching; permanent history stays in client storage or business primary DBs), publisher/subscriber token auth, and `/metrics` observability.

---

## License

[MIT License](LICENSE) © 2026 muyuzhong
