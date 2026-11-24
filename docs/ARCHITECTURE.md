# Seamline Architecture Specification

**架构设计规范 · Resumable Dual-Speed AI Stream Broker**

This document specifies the internal architecture, state invariants, failure consistency model, concurrency hierarchy, zero-gap subscription handoff proof, and W3C Server-Sent Events (SSE) wire protocol of **Seamline**.

---

## 1. Overview & Design Goals

AI agent and LLM backends produce two fundamentally different classes of events within a single conversation session:

1. **Low-frequency, high-value state transitions** (`user_message`, `tool_call`, `tool_result`, finalized `assistant_message`, `run_finished`): Must be durably persisted in strict monotonic order.
2. **High-frequency, ephemeral token deltas** (`seq = 1..N`, 20–150 chunks/sec): Valuable for low-latency UI rendering while generation is in flight, but once `fin=true` arrives, only the compacted full message matters for historical replay.

Traditional architectures fail in one of two ways:
- **Fire-and-forget SSE:** Zero storage amplification, but pressing **F5**, switching Wi-Fi/cellular networks, or opening a second browser tab mid-generation loses or truncates the in-flight response.
- **Per-token log persistence:** Writing every 3-byte token chunk to Postgres, Kafka, or Redis Streams causes **100x+ write amplification** and bloats catch-up reads.

### Design Goals
- **Zero Write Amplification Mid-Stream:** Coalesce `1..N-1` streaming deltas in RAM inside a per-session actor with **zero `Record` storage writes** until `fin=true` compacts the entire turn into **one `Record`** (`offset + 1`).
- **Exact Mid-Stream Resumability:** Reconnecting clients using standard browser `EventSource` (`Last-Event-ID: <epoch>:<offset>`) or cold-reloading from `offset=0` reconstruct the exact generated text with **zero missing characters and zero duplicated characters**.
- **Failure Atomicity & Retry Safety:** Storage errors or context cancellations must never corrupt in-memory sequence counters, wedge a session in `409 Conflict`, or silently drop committed events from live subscribers.
- **Cross-Session Fault Isolation:** Slow consumers (`ErrSlowSubscriber`) and slow per-session storage writes must never block unrelated sessions or stall global broker operations.

---

## 2. Dual-Speed Engine Dataflow

```text
  Producer (AI Worker)                       Seamline Broker (sessionActor)                        Subscribers (EventSource / SDK)
 ┌────────────────────┐                   ┌──────────────────────────────────┐                   ┌─────────────────────────────────┐
 │ Atomic Event       │  StreamID == ""   │ 1. Validate & Marshal Locally    │                   │                                 │
 │ (user_message)     ├──────────────────►│ 2. store.Append(Offset = k+1)    ├──────────────────►│ id: <epoch>:k+1                 │
 └────────────────────┘                   │ 3. Advance lastOffset & Fan-Out  │   event: record   │ event: record                   │
                                          │ 4. Best-effort SaveMeta          │                   │                                 │
 ┌────────────────────┐                   ├──────────────────────────────────┤                   ├─────────────────────────────────┤
 │ Streaming Deltas   │  seq=1..N-1       │ Fast Path (RAM Only, 0 Append):  │                   │ (No SSE id: header!)            │
 │ (fin = false)      ├──────────────────►│ • Coalesce into inFlight.merged  ├──────────────────►│ event: delta (seq=1..N-1)       │
 └────────────────────┘                   │ • Advance inFlight.lastSeq       │   event: delta    │                                 │
                                          │ • Non-blocking fanOutLocked      │                   │ Mid-Stream Reconnect (F5):      │
 ┌────────────────────┐                   ├──────────────────────────────────┤                   │ Last-Event-ID: <epoch>:k+1      │
 │ Final Stream Chunk │  seq=N, fin=true  │ Slow Path (Compaction Commit):   │                   │  1) Catch-up records > k+1      │
 │ (fin = true)       ├──────────────────►│ 1. Build mergedText locally      ├──────────────────►│  2) 1x event: inflight_snapshot │
 └────────────────────┘                   │ 2. store.Append(Offset = k+2)    │   event: delta    │  3) Live event: delta (seq > S) │
                                          │ 3. Clear inFlight, record compact│ + event: record   │  4) id: <epoch>:k+2 record      │
                                          │ 4. Fan-out fin delta + Record    │                   └─────────────────────────────────┘
                                          │ 5. Best-effort SaveMeta          │
                                          └──────────────────────────────────┘
```

### Fast Path vs. Slow Path Summary

| Path | Trigger | `Storage.Append` Calls | `inFlight` Buffer Mutation | SSE Frames Emitted | SSE `id:` Header |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Atomic Record** | `stream_id == ""` | `1` (`offset + 1`) | Untouched | `1x event: record` | `<epoch>:<offset>` |
| **Fast Path (Delta)** | `stream_id != ""`, `fin == false` | **`0`** | Appends `delta`, sets `lastSeq = seq` | `1x event: delta` | *Omitted* |
| **Slow Path (Compact)** | `stream_id != ""`, `fin == true` | **`1`** (`offset + 1`) | Cleared (`nil`), sets `lastCompacted*` | `1x event: delta` (`fin:true`) + `1x event: record` | Only on `event: record` |

---

## 3. Domain Model & Session Lifecycle

### Session States (`model.SessionState`)
- `idle`: Session created, hydrated from cold storage, or finished compacting an assistant stream (`fin=true` on a standard message).
- `running`: An in-flight stream (`actor.inFlight != nil`) is actively coalescing token deltas in memory.
- `completed`: Terminal session marker triggered when an atomic event or compacted stream carries `type == "run_finished"`.
- `failed`: Terminal session marker triggered when an atomic event or compacted stream carries `type == "run_failed"`.

### Cold-Reload Normalization & Broker Epochs
- Each `Broker` instance owns a 12-hex-character `Epoch` (generated at startup in Phase 1, or persisted in WAL headers in Phase 2).
- Every SSE cursor embeds `<epoch>:<offset>`. If a client reconnects to a restarted in-memory node with `afterEpoch != "" && afterEpoch != broker.Epoch()`, `Broker.Subscribe` resets `afterOffset = 0` so the client never skips records due to a high offset from a prior process lifetime.
- When `acquireActor` or `ListSessions` loads metadata from cold storage where `meta.State == "running"` (e.g., after an unclean process restart or eviction), Seamline normalizes `meta.State` to `"idle"` because ephemeral RAM `inFlight` state does not survive process restarts.

---

## 4. State & Storage Failure Consistency Model

`Publish` executes under `actor.mu`, serializing all state transitions within a single `sessionID` while allowing full parallelism across distinct sessions. To guarantee retry idempotency and live-subscriber consistency in the presence of transient storage errors or caller context cancellation, `Publish` enforces a strict **Two-Phase Commit Boundary** around `store.Append`.

### 4.1 Pre-Commit Immutability (Before `store.Append`)
All request validation (`json.Valid`, sequence gap checks, stream conflict checks, `ctx.Err()`) and candidate computations (`seq`, `mergedText`, `recordPayload`) happen in **stack-local variables** before any storage write or `actor` field mutation:

1. **No premature `inFlight` allocation:** When `actor.inFlight == nil` and `seq == 1`, a candidate state is prepared locally rather than immediately assigning `actor.inFlight = &inFlightState{}`. If `fin=true` `store.Append` (or `ctx.Err()`) fails on a single-chunk stream, `actor.inFlight` remains `nil` instead of leaving behind an empty placeholder stream that blocks future `stream_id`s with `409 Conflict`.
2. **No premature `mergedBuf` / `lastSeq` mutation on `fin=true`:** On `fin=true`, `mergedText` is computed as `existingMerged + req.Delta` without mutating `actor.inFlight.mergedBuf` or `actor.inFlight.lastSeq` until `store.Append` returns `nil`.
   - If `store.Append` fails, `actor.inFlight` retains its exact pre-call `(mergedBuf, lastSeq)`. The worker can safely retry the **exact same `(stream_id, seq, delta, fin=true)`** request and commit cleanly—neither falsely deduplicating (`seq <= lastSeq`) nor duplicating the final chunk (`"Hellolo"`).

### 4.2 `store.Append` as the Point of No Return
`store.Append(commitCtx, sessionID, rec)` is the **sole durability commit point**:

- Once `store.Append` returns `nil`, the `Record` exists in `Storage` at `nextOffset`. Rolling back or returning an HTTP `500` after this point would cause non-idempotent atomic retries to write duplicate records and cause live subscribers to miss committed offsets.
- Therefore, immediately after `store.Append` succeeds:
  1. `actor` advances its in-memory state (`lastOffset = nextOffset`, `meta`, `lastCompactedStreamID/Seq/Offset`, and `actor.inFlight = nil` for `fin=true`).
  2. `actor.fanOutLocked` broadcasts the committed `OutEvent` (preceded by the final `fin=true` `EventDelta` on stream completion) to all active subscribers.
  3. `store.SaveMeta(context.WithoutCancel(ctx), actor.meta)` executes as **best-effort** metadata persistence:
     - Using `context.WithoutCancel(ctx)` prevents an HTTP client timeout/disconnect right after `Append` from aborting the metadata update.
     - Even if `SaveMeta` returns an error, `Publish` **still succeeds** (`CommittedOffset: rec.Offset`) because `SessionMeta.LastOffset` is reconstructible from the record log.

### 4.3 Cold-Load Tail Reconciliation
Whenever an idle-evicted or restarted session is hydrated in `acquireActor` (or listed via `ListSessions`):
1. `LoadMeta(ctx, sessionID)` loads the last saved `SessionMeta` (if any, giving `startOffset = meta.LastOffset`).
2. `ReadFrom(ctx, sessionID, startOffset)` scans any committed records beyond `startOffset`.
3. Because `commitFinStreamLocked` persists terminal `fin=true` types (`run_finished` / `run_failed`) directly on the compacted `Record.Type`, any newer tail records found by `ReadFrom` deterministically advance `meta.LastOffset`, `meta.UpdatedAt`, and `meta.State` via the single canonical helper `model.StateFromRecordType(lastRec.Type)`—even if prior best-effort `SaveMeta` calls failed.

### 4.4 Failure Consistency Matrix

| Failure Scenario | `Storage` State | `actor` Memory State | Live Subscribers | Worker Retry Behavior (`same req`) |
| :--- | :--- | :--- | :--- | :--- |
| `seq=1, fin=false` canceled before commit | Unchanged | `inFlight == nil` | Nothing emitted | Cleanly starts stream at `seq=1` |
| `seq=N, fin=true` `Append` fails | Offset unchanged | `inFlight` stays at `seq=N-1` | Nothing emitted for `seq=N` | Commits `Offset+1` with exact `mergedText` |
| Atomic `Append` succeeds, `SaveMeta` fails | `Offset+1` persisted | `lastOffset = Offset+1` | Receives `record` (`Offset+1`) | Caller gets `200 OK` (no duplicate retry); cold reload reconciles via `ReadFrom` |
| `fin=true` `Append` succeeds, `SaveMeta` fails | `Offset+1` persisted | `inFlight = nil`, `lastCompacted*` set | Receives `fin` `delta` + `record` | Caller gets `200 OK`; late duplicate retry returns `Deduplicated: true, CommittedOffset: Offset+1` |

---

## 5. Catch-Up to Live Tailing Zero-Gap Proof Sketch

**Theorem (Zero-Gap & Duplicate-Free Subscription Handoff):** For any session $S$ within a constant broker `Epoch`, a subscriber connecting with `afterOffset` $O_0$ that is not evicted for slow consumption observes every committed record with `Offset` $> O_0$, and for any stream active at or after subscription time, observes the exact sequence of character deltas without gaps or duplicates.

### Proof Sketch
1. **Linearization Point under `actor.mu`:**
   In `Broker.Subscribe`, while holding `actor.mu` (which mutually excludes `Broker.Publish`):
   - `sub` is inserted into `actor.subscribers` (with buffered channel `sub.liveCh`);
   - `snapOffset` $\leftarrow$ `actor.lastOffset`;
   - `inFlightSnap` $\leftarrow$ `actor.inFlightViewLocked()` (capturing `(StreamID: id*, LastSeq: Q_snap, Merged: M_snap)` if an active stream exists, else `nil`).
   Let $T_{\text{sub}}$ denote this atomic critical section.

2. **Partition of Events Relative to $T_{\text{sub}}$:**
   Because `Publish` also holds `actor.mu` across state updates and `fanOutLocked`:
   - **Pre-$T_{\text{sub}}$ committed records** have `Offset` $\in (O_0, \text{snapOffset}]$. Since `Storage` is append-only with strictly increasing offsets, `store.ReadFrom(ctx, sessionID, O_0)` filtered by `Offset <= snapOffset` delivers every committed record in $(O_0, \text{snapOffset}]$ exactly once in Step 1.
   - **Pre-$T_{\text{sub}}$ in-flight deltas** (if stream `id*` was active at $T_{\text{sub}}$) have sequences $1 \dots Q_{\text{snap}}$ already coalesced into $M_{\text{snap}}$. Step 2 emits a single `inflight_snapshot` carrying `(Seq: Q_snap, Delta: M_snap)`.
   - **Post-$T_{\text{sub}}$ publications** execute after `sub` is registered in `actor.subscribers`, so `fanOutLocked` enqueues every post-$T_{\text{sub}}$ `OutEvent` into `sub.liveCh` in exact publication order:
     - Every post-$T_{\text{sub}}$ `EventCommitted` has `Offset` $> \text{snapOffset}$, passing the `ev.Offset <= snapOffset` filter in Step 3. (Conversely, `ReadFrom` in Step 1 explicitly ignores `rec.Offset > snapOffset`, preventing double delivery if `Publish` commits while Step 1 is reading storage!)
     - Every post-$T_{\text{sub}}$ `EventDelta` for stream `id*` has `Seq` $> Q_{\text{snap}}$ (enforced by monotonic `Publish` sequence validation), passing the `ev.Seq <= inFlightSnap.LastSeq` filter in Step 3 and concatenating seamlessly onto $M_{\text{snap}}$.
     - When `id*` finishes (`fin=true`) and emits its `EventCommitted`, Step 3 clears `inFlightSnap = nil`, ensuring future streams are unaffected. $\blacksquare$

---

## 6. Concurrency, Lock Hierarchy & Actor Pinning Model

### 6.1 Strict Lock Partial Order
All mutex acquisitions obey a strict DAG:

```text
  Broker.mu (b.mu)
         │
         ▼
  sessionActor.mu (actor.mu)      handler.demoMu (independent, server layer only)
         │
         ▼
  subState.errMu (sub.errMu)      MemoryStorage.mu (leaf RWMutex inside Storage calls)
```

### 6.2 Non-Blocking Global RWMutex (`b.mu`) Rules
`b.mu` (`sync.RWMutex`) protects `b.closed` and the `b.actors` map (`map[string]*sessionActor`). To guarantee $O(1)$ contention-free session lookup even when individual sessions perform slow `Storage` writes under `actor.mu`:
- **`acquireActor`:** Checks `b.actors[sessionID]` under `b.mu.RLock()`, atomically incrementing `actor.pins` and updating `actor.lastActiveNano` without acquiring `actor.mu`. On a cache miss, it releases `b.mu.RUnlock()` **before** calling `store.LoadMeta` / `store.ReadFrom`, then acquires `b.mu.Lock()` using double-checked locking to insert the hydrated actor.
- **`evictIdleActors` (Background Sweeper):** Iterates `b.actors` under `b.mu.Lock()`, skipping pinned actors (`actor.pins.Load() > 0`) and probing unpinned actors via **non-blocking `actor.mu.TryLock()`**. If `TryLock()` returns `false`, another goroutine is actively using the actor, so the sweeper immediately skips it without stalling `b.mu`. Dirty metadata flushes (`!actor.metaPersisted`) are pinned and executed after releasing `b.mu`.
- **`ListSessions` & `Close`:** Snapshot or swap `*sessionActor` pointers under `b.mu.RLock()` / `b.mu.Lock()`, release `b.mu` immediately, and then acquire `actor.mu` on each pointer outside `b.mu`.

### 6.3 Atomic Actor Pinning & Idle Eviction
`acquireActor` atomically increments `actor.pins` (`atomic.Int32`) before returning an actor, and every caller (`CreateSession`, `Publish`, `Subscribe`, `Inspect`) executes `defer actor.unpin()`. An actor is eligible for idle eviction in `evictIdleActors` if and only if all four conditions hold simultaneously while `actor.mu` is held via `TryLock()`:
1. `actor.pins.Load() == 0` (no in-progress API call holds a reference)
2. `len(actor.subscribers) == 0` (no active SSE stream is tailing the actor)
3. `actor.inFlight == nil` (no unfinished token stream is buffered exclusively in RAM)
4. `now.Sub(time.Unix(0, actor.lastActiveNano.Load())) > b.cfg.IdleSessionTTL`

---

## 7. SSE Wire Protocol Specification

`GET /v1/sessions/{id}/stream` implements W3C Server-Sent Events (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`).

### 7.1 Cursor Resolution Precedence
1. **`Last-Event-ID` HTTP Header** (sent automatically by browser `EventSource` on reconnect): `<epoch>:<offset>` (or bare `<offset>`).
2. **Query Parameters** (used on initial page load or SDK bootstrap): `?after_offset=<offset>&epoch=<epoch>` (or `?after_offset=<epoch>:<offset>`).
3. **Default:** `afterOffset = 0, afterEpoch = ""` (replays full session history + active `inflight_snapshot` + live stream).

### 7.2 Frame Catalog

| Frame Type | Wire Format | Carries `id:`? | Semantics |
| :--- | :--- | :---: | :--- |
| **Connected Handshake** | `: connected epoch=<epoch>\n\n` | No | Emitted immediately upon HTTP `200 OK` flush to announce active broker epoch. |
| **Keepalive Heartbeat** | `: keepalive\n\n` | No | Emitted every `KeepaliveInterval` (default `15s`) to prevent reverse-proxy idle timeouts. |
| **Committed Record** | `id: <epoch>:<offset>\nevent: record\ndata: {...}\n\n` | **Yes** | Durable session record (`user_message`, compacted `assistant_message`, etc.). Advances client `Last-Event-ID`. |
| **In-Flight Snapshot** | `event: inflight_snapshot\ndata: {...}\n\n` | **No** | Coalesced text (`delta`) up to `seq` for a stream already running when the client subscribed. |
| **Live Token Delta** | `event: delta\ndata: {...}\n\n` | **No** | Incremental token slice (`seq`, `delta`, `fin`). |
| **Terminal Stream Error** | `event: stream_error\ndata: {"error":"slow_subscriber"}\n\n` | **No** | Sent right before closing a slow subscriber's HTTP response. Named `stream_error` so it never collides with browser `EventSource`'s built-in network `error` event. |

### 7.3 Reference Client Reconstruction State Machine
Clients maintain a map of committed messages by `offset` plus at most one active `inFlight` buffer keyed by `stream_id`:
1. On `event: record`: If `stream_id` matches the active `inFlight` buffer, replace the temporary streaming bubble with the authoritative `payload` at `offset`; otherwise append `record` at `offset`. Browser `EventSource` automatically updates `Last-Event-ID` to `<epoch>:<offset>`.
2. On `event: inflight_snapshot`: Initialize/replace the temporary `inFlight` buffer for `stream_id` with `text = ev.delta` and `lastSeq = ev.seq`.
3. On `event: delta`: If `ev.seq > lastSeq`, append `ev.delta` to the `inFlight` buffer and advance `lastSeq = ev.seq`.
4. On `event: stream_error`: Inspect `data.error` (e.g., `"slow_subscriber"`). Note that browsers send the `Last-Event-ID` HTTP header **only** during automatic `EventSource` reconnection—never when instantiating a brand-new `new EventSource(url)`:
   - **Native `EventSource` client:** Do **not** call `es.close()`; once the server closes the HTTP stream after `stream_error`, the browser automatically reconnects with `Last-Event-ID: <epoch>:<offset>` and recovers via catch-up + `inflight_snapshot`.
   - **New `EventSource` instance or manual reconnect:** Pass the saved cursor in the query string (`?after_offset=<epoch>:<offset>`, supported by `extractStreamCursor`), or send an explicit `Last-Event-ID` header in non-browser HTTP SDKs.

---

## 8. Architectural Evolution (Phase 2 -> Phase 3)

> **Design Evolution Notice:** Sections 1–7 above document the currently implemented **Phase 1** baseline on `main`. Next-stage architecture decisions are specified in **[`docs/DESIGN_V2.md`](DESIGN_V2.md)** and supersede Phase 1's dual-speed string coalescing when Phase 2 is implemented:
> - **Payload-Opaque Pure Event Cursor (`<epoch>:<seq>`) + Pinned-Run Hot Window (No Custom Disk WAL):** Long-term history belongs to client local storage or the application's primary DB, synchronized via the `seamline_cursor = "<epoch>:<seq>"` causal watermark contract. Every published event receives a per-session `<epoch>:<seq>` cursor (`id:` on every business event frame), while a **Pinned-Run Byte RingBuffer (`active_run_start_seq`)** ensures `HistoryTTL` / `MaxHistoryBytes` evict **only completed-run events (`seq < active_run_start_seq`)** and never truncate an uncommitted active Run (`MaxActiveRunBytes`, `MaxEventBytes`).
> - **Explicit Gap Signaling (`event: cursor_reset`):** Superseding Phase 1's silent `afterOffset = 0` reset whenever a non-empty cursor hits `epoch_mismatch`, `cursor_expired`, or `cursor_ahead` (with mid-replay eviction gaps closing via `stream_error: insufficient_state`).
> - **Stateful Agent Session Control Plane & Pluggable Redis:** Differentiating Seamline from stateless pub/sub gateways (such as Centrifugo) via Run concurrency guards, bidirectional `Cancel` / `Steer` channels, idempotent CAS `WaitingApproval` gates, Worker heartbeat leases, and a Phase 3 `RedisStorage` backend with pipeline micro-batching.
