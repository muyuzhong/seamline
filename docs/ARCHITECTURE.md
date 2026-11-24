# Seamline Architecture Specification

**架构设计规范 · Resumable AI Stream Broker · Pure Event Cursor + Pinned-Run Hot Window**

This document specifies the implemented data plane of **Seamline**: the payload-opaque event model, the short hot window with a pinned active run, the commit and failure consistency model, the zero-gap subscription handoff, the concurrency hierarchy, and the W3C Server-Sent Events (SSE) wire protocol. The reasoning behind each decision, the database alignment contract and the roadmap live in [`DESIGN_V2.md`](DESIGN_V2.md).

---

## 1. Overview & Design Goals

An AI agent session produces a single ordered stream of heterogeneous events: user messages, text deltas, interleaved argument fragments of parallel tool calls, status events, and the run boundaries `run_started` / `run_finished`. Seamline does not interpret payloads; it orders, retains and fans out events.

Seamline is a **short hot window**, not a system of record: long-term history belongs to client-local storage or the application's own database, aligned through the `seamline_cursor` watermark (see [`DESIGN_V2.md` §2](DESIGN_V2.md)).

### Design Goals
- **One cursor for everything:** every event of a session gets the next `seq`; a client resumes from `<epoch>:<seq>` and receives exactly the events after it.
- **Payload opacity:** payloads (`json.RawMessage`) are validated as JSON and stored and delivered byte-for-byte; the broker never concatenates or re-encodes them.
- **No truncation of the active run:** between `run_started` and its terminal event nothing of the run is evicted, so a page load in the middle of a long run replays the whole run.
- **Explicit discontinuity:** a cursor that cannot be continued gaplessly receives a `cursor_reset`; history is never skipped silently.
- **Bounded memory, idempotent retries, cross-session isolation:** byte quotas per event, per active run and per history; `idempotency_key` deduplication; slow consumers and slow storage on one session never block others.

---

## 2. Event Dataflow

```text
  Producer (AI Worker)                  Seamline Broker (sessionActor)                       Subscribers (EventSource / SDK)
 ┌──────────────────────┐            ┌─────────────────────────────────────────┐           ┌──────────────────────────────┐
 │ POST /events         │            │ 1. validate type / JSON / MaxEventBytes │           │ id: <epoch>:<seq>            │
 │ {type, payload,      ├───────────►│ 2. idempotency_key lookup               ├──────────►│ data: {"seq":…,"type":…,     │
 │  idempotency_key}    │            │ 3. nextCommit: seq+1, run state machine │  every    │        "payload":…}          │
 └──────────────────────┘            │ 4. store.Append(event + session state)  │  event    │                              │
                                     │ 5. apply commit, fan out                │           │ Reconnect / F5:              │
          ╳ producer goes silent     ├─────────────────────────────────────────┤           │  [event: cursor_reset]       │
 ───────────────────────────────────►│ sweeper: run_failed {"reason":          ├──────────►│  retained window             │
                                     │          "idle_timeout"}                │           │  live events                 │
                                     └────────────────────┬────────────────────┘           └──────────────────────────────┘
                                                          ▼
                     hotwindow (per session): epoch, seq counter, state, active_run_start_seq, events
       evicted history │ history (TTL, MaxHistoryBytes) │ latest finished run (TTL from its end) │ active run (pinned)
```

### Event Paths

| Event | Run state before | Run state after | Retention of this event |
| :--- | :--- | :--- | :--- |
| any non-run event | no run | no run | history |
| `run_started` | no run | running, `active_run_start_seq = seq` | pinned |
| `run_started` | running | unchanged (start kept) | pinned |
| any non-terminal event | running | running | pinned |
| `run_finished` / `run_failed` / `run_interrupted` | running | completed / failed / interrupted, pin = 0 | becomes the latest finished run |
| terminal event | no run | unchanged | history |
| sweeper `run_failed {"reason":"idle_timeout"}` | running, silent for `ActiveRunIdleTimeout` | failed, pin = 0 | latest finished run |

---

## 3. Domain Model & Session Lifecycle

### 3.1 Event and Session State
- `model.Event{Seq, Epoch, Type, Payload, CreatedAt}` is the only stored unit.
- Session states: `idle` (no run yet), `running` (an active run), `completed` / `failed` / `interrupted` (state of the last run). Events outside a run never change the state.

### 3.2 Session Stream Identity: Epoch & Seq
- **The epoch belongs to the session stream, not to the broker process.** `hotwindow` generates a random 12-hex epoch when it creates a stream (`EnsureStream`). `Broker.InstanceID()` (`/healthz` `instance_id`) only identifies the process.
- Storage owns the seq counter (`LatestSeq`) and requires `Append` to use exactly `LatestSeq + 1` with the stream's epoch.
- Retention evicts events but **never** resets the epoch or the seq counter; an evicted cursor is reported as `cursor_expired`.
- A stream is dropped only after `MetaTTL` without activity (publishes, subscriptions, and the sweeper's `Touch` of every live actor keep it alive). Recreating it yields a **new epoch** and seq restarts at 1. An in-memory restart gives every session a new epoch.

### 3.3 Limits

| Limit | Default | Enforced in | On overflow |
| :--- | :--- | :--- | :--- |
| `MaxEventBytes` (payload of one publish) | 256 KiB | `broker.validatePublishRequest`; body limit = `MaxEventBytes` + 16 KiB | `413 event_too_large` |
| `MaxActiveRunBytes` (retained bytes of the active run) | 16 MiB | `hotwindow` `stream.check` | `413 active_run_quota_exceeded`; terminal events exempt |
| `ActiveRunIdleTimeout` | 2m | sweeper `sealIdleRuns` | `run_failed {"reason":"idle_timeout"}` |
| `MaxHistoryBytes` (events outside the active run; `payload + type + 64B` each) | 8 MiB | `hotwindow` `stream.evict` | oldest history evicted; latest finished run and newest event kept |
| `HistoryTTL` | 15m | `Append` / `Sweep` | history evicted; the latest finished run counts from its end |
| `MetaTTL` | 24h | `MemoryStorage.Sweep` | stream dropped (new epoch on recreation) |
| `IdempotencyWindow` | 5m | `windowIndex` in the actor | key forgotten |

### 3.4 Idempotent Retries
A publish may carry `idempotency_key`. Within `IdempotencyWindow` a repeat returns the original `{epoch, seq, deduplicated: true}` and is not fanned out. The key is recorded only after the append succeeds. Content is not compared.

---

## 4. Commit & Failure Consistency Model

`Publish` runs under `actor.mu`, serializing one session while sessions run in parallel.

### 4.1 Pre-Commit Immutability
Validation, `ctx.Err()`, the idempotency lookup and `actor.nextCommit` (next seq, run state machine, normalized payload copy) only build local values. The actor (`latestSeq`, `activeRunStartSeq`, `state`, `runActiveAt`, `idempotent`) is not touched.

### 4.2 One Atomic Append
`store.Append(hotwindow.Commit{Event, State, ActiveRunStartSeq})` validates the epoch, `seq == LatestSeq + 1`, the run transition (a run starts only from "no run", at this event's seq, and ends only from the current run) and the active-run quota, then writes the event and the session state together and applies retention. There is no second metadata write, so storage can never hold an event without its run bookkeeping; `Inspect`, `ListSessions` and `CreateSession` read storage directly.

### 4.3 Point of No Return
After `Append` returns `nil`: `applyCommitLocked`, record the `idempotency_key`, `fanOutLocked`; `Publish` then returns success. Idle sealing uses the same path.

### 4.4 Failure Matrix

| Failure | Storage | Actor | Subscribers | Retry |
| :--- | :--- | :--- | :--- | :--- |
| validation / quota rejects | unchanged | unchanged | nothing | fix the request or end the run |
| `Append` fails | unchanged | unchanged (seq not consumed, run pin unchanged, key not recorded) | nothing | commits normally with the same seq |
| `Append` succeeds, client times out | event committed | applied | event delivered | same `idempotency_key` → original seq, no duplicate |
| sealing `Append` fails | unchanged | run still active | nothing | sweeper retries next tick |

---

## 5. Catch-Up to Live Tailing Zero-Gap Proof Sketch

**Theorem:** for a session stream with epoch $E$, a subscriber that is not evicted (slow consumer or insufficient state) receives (a) when its cursor is `<E>:S_0` with `EarliestSeq - 1 <= S_0 <= LatestSeq`: every event with seq $> S_0$ exactly once and in order; (b) otherwise: one `cursor_reset` followed by every retained event and all later events exactly once and in order.

1. **Linearization point:** `registerSubscriber` inserts `sub` and captures `(epoch, snapSeq = latestSeq, activeRunStartSeq)` in one `actor.mu` critical section $T_{sub}$. `Publish` and sealing hold `actor.mu` across `Append` and `fanOutLocked`, so every event with seq $\le$ `snapSeq` is in storage and every later event goes to `liveCh` in order.
2. **Cursor classification** (`planReplay`) uses only the cursor and the snapshot; one atomic `ReadAfter` returns the events and the window, so the `cursor_expired` test (`S_0 + 1 < EarliestSeq`) and the replayed events agree.
3. **Replay** sends events up to `snapSeq` (later ones arrive through `liveCh`) and checks contiguity: for a resumable cursor the first event must be `S_0 + 1`; after a reset or on a fresh subscription it may start anywhere in the window. A hole ends the subscription with `ErrInsufficientState`.
4. **Live:** every `liveCh` event has seq $>$ `snapSeq`; the `Seq <= snapSeq` filter is defense in depth. $\blacksquare$

Because the active run is pinned, `EarliestSeq <= active_run_start_seq` holds whenever a run is active: a reset client that reloads committed history from its database still receives the whole unfinished run from Seamline.

---

## 6. Concurrency, Lock Hierarchy & Actor Pinning Model

### 6.1 Lock Order

```text
  Broker.mu (b.mu)
         │
         ▼
  sessionActor.mu (actor.mu)      handler.demoMu (server layer only)
         │
         ▼
  subState.errMu (sub.errMu)      MemoryStorage.mu (leaf inside Storage calls)
```

### 6.2 `b.mu` Rules
`b.mu` protects `b.closed` and `b.actors`; it is never held while blocking on `actor.mu` or doing storage I/O.
- **`acquireActor`:** cached lookup under `b.mu.RLock()` with atomic pinning; on a miss `store.EnsureStream` outside `b.mu`, then double-checked insertion under `b.mu.Lock()`.
- **`evictIdleActors`:** under `b.mu.Lock()`, skip pinned actors and probe others with `actor.mu.TryLock()`.
- **`sealIdleRuns`:** pin all actors under `b.mu.RLock()`, release it, `TryLock` each actor.
- **`maintainStorage`:** copy live session IDs under `b.mu.RLock()`, then `Touch` and `Sweep` outside `b.mu`.
- **`Inspect` / `ListSessions`:** read storage; `subscriberCount` looks up the actor under `b.mu.RLock()` and locks it after releasing `b.mu`.

### 6.3 Pinning & Idle Eviction
API calls pin their actor and release it with `defer actor.unpin()` (which refreshes `lastActiveNano`). **Sweeper pins are released with `actor.pins.Add(-1)` and never touch the actor.** An actor is evicted iff, while `actor.mu` is held via `TryLock()`: no pins, no subscribers, **no active run** (the actor owns the run's idle timer), and idle longer than `IdleSessionTTL`. Evicting an actor only drops the broker-side cache; the stream stays in storage.

### 6.4 Sweeper Pass (every `SweepInterval`)
1. `sealIdleRuns`: end runs silent for `ActiveRunIdleTimeout`.
2. `evictIdleActors`.
3. `maintainStorage`: `Touch` every remaining actor's stream, then `store.Sweep(now)` (history TTL, `MetaTTL`).

---

## 7. SSE Wire Protocol Specification

`GET /v1/sessions/{id}/stream` (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`).

### 7.1 Cursor Resolution
1. `Last-Event-ID: <epoch>:<seq>` (sent by `EventSource` on automatic reconnects).
2. `?after_seq=<epoch>:<seq>` or `?after_seq=<seq>&epoch=<epoch>` (page load, database watermark, SDKs).
3. Default: fresh subscription (retained window, then live events; no reset).

A cursor with a seq but no epoch cannot be validated and is answered with `cursor_reset` `epoch_mismatch`.

### 7.2 Frame Catalog

| Frame | Wire format | `id:` | Semantics |
| :--- | :--- | :---: | :--- |
| Connected | `: connected epoch=<session epoch>\n\n` | No | First bytes after `200 OK`. |
| Keepalive | `: keepalive\n\n` | No | Every `KeepaliveInterval` (default `15s`). |
| Event | `id: <epoch>:<seq>\ndata: {"seq":…,"epoch":…,"type":…,"payload":…,"created_at":…}\n\n` | **Yes** | Every business event. No `event:` line: `EventSource` dispatches it as `message`, so producer types never collide with control frame names. |
| Cursor reset | `event: cursor_reset\ndata: {"reason":…,"requested_epoch":…,"requested_seq":…,"current_epoch":…,"earliest_seq":…,"latest_seq":…,"active_run_start_seq":…}\n\n` | No | At most once, first frame only; `reason` is `epoch_mismatch`, `cursor_expired` or `cursor_ahead`. The window and live events follow on the same connection. |
| Stream error | `event: stream_error\ndata: {"error":"slow_subscriber"}\n\n` or `{"error":"insufficient_state"}` | No | Sent right before the server closes the stream. Never named `error`. |

### 7.3 Reference Client
1. Event: if `seq` is the next one, apply it to the client's own model (for example append `assistant_delta` text to the current run bubble, group `tool_call_delta` by `call_id`) and remember `<epoch>:<seq>` as the cursor (native `EventSource` does this automatically).
2. `cursor_reset`: keep the connection and buffer the following events, reload committed history from the application's database (with its `seamline_cursor`), drop buffered events with `seq <=` that watermark in the same epoch, and apply the rest ([`DESIGN_V2.md` §2–§3](DESIGN_V2.md)).
3. `stream_error`: native `EventSource` clients must **not** call `es.close()`; the browser reconnects with `Last-Event-ID`. A new `EventSource` passes `?after_seq=<epoch>:<seq>`.

### 7.4 HTTP Status Codes
Errors are `{"error": "...", "code": "..."}`.

| Status | `code` | Cause |
| :--- | :--- | :--- |
| `400` | `invalid_request` | empty `type`, invalid JSON, malformed cursor |
| `404` | `not_found` | unknown session on `GET /v1/sessions/{id}` |
| `409` | `conflict` | demo simulation already running |
| `413` | `event_too_large` | payload over `MaxEventBytes` or body over the request limit |
| `413` | `active_run_quota_exceeded` | active run over `MaxActiveRunBytes` |
| `503` | `broker_closed` | broker shutting down |

---

## 8. Evolution

- **Phase 2b — Stateful Agent Session Control Plane:** Run concurrency policies, `Cancel` / `Steer` worker channel, idempotent `WaitingApproval` gate, worker heartbeat leases (a lease expiry reuses the sealing path with `worker_lost`).
- **Phase 3 — `RedisStorage`:** the `hotwindow.Storage` interface is shaped for Redis (meta hash with epoch, seq, state and pin plus `EXPIRE`; `XADD` with explicit `<seq>-0` IDs; `XRANGE`; `XTRIM MINID`), see `internal/hotwindow/hotwindow.go`. The idempotency index and run idle timers live in the owning broker instance, so multi-instance deployments need session affinity or moving them into Redis.
