# Seamline Architecture Specification

**架构设计规范 · Resumable Dual-Speed AI Stream Broker · Hot-Window Data Plane**

This document specifies the implemented data plane of **Seamline**: the dual-speed engine (committed records + concurrent in-flight streams), the short TTL hot window, the storage failure consistency model, the zero-gap subscription handoff, the concurrency hierarchy, and the W3C Server-Sent Events (SSE) wire protocol. The reasoning behind each decision, the database alignment contract and the Phase 2/3 roadmap live in [`DESIGN_V2.md`](DESIGN_V2.md).

---

## 1. Overview & Design Goals

AI agent and LLM backends produce two fundamentally different classes of events within a single conversation session:

1. **Low-frequency, high-value state transitions** (`user_message`, `tool_result`, finalized `assistant_message`, `run_finished`): committed in strict monotonic order.
2. **High-frequency token deltas** (`seq = 1..N`, 20–150 chunks/sec, often several streams at once for parallel tool calls): valuable while generation is in flight, but once `fin=true` arrives only the compacted message matters for replay.

Seamline is a **short hot window**, not a system of record: long-term history belongs to client-local storage or the application's own database (see [`DESIGN_V2.md` §1–§2](DESIGN_V2.md)).

### Design Goals
- **Zero Write Amplification Mid-Stream:** Coalesce `1..N-1` deltas in RAM inside a per-session actor with **zero storage writes** until `fin=true` compacts the stream into **one `Record`** (`offset + 1`).
- **Exact Mid-Stream Resumability:** Reconnecting clients (`Last-Event-ID: <epoch>:<offset>`) or fresh page loads reconstruct every committed record in the window and the exact text of every in-flight stream, with **zero missing and zero duplicated characters**.
- **Explicit Discontinuity:** A cursor that cannot be continued gaplessly (different epoch, evicted from the window, or ahead of the stream) always receives an explicit `cursor_reset`; nothing is ever silently skipped.
- **Bounded Memory:** Per-event, per-session in-flight, and per-session history byte limits; idle streams and sessions are reclaimed.
- **Failure Atomicity & Retry Safety:** Storage errors or context cancellations never corrupt in-memory counters, wedge a stream, or drop committed events from live subscribers. Retries are idempotent.
- **Cross-Session Fault Isolation:** Slow consumers and slow per-session storage writes never block unrelated sessions or global broker operations.

---

## 2. Dual-Speed Engine Dataflow

```text
  Producer (AI Worker)                    Seamline Broker (sessionActor)                      Subscribers (EventSource / SDK)
 ┌────────────────────┐                ┌───────────────────────────────────────┐            ┌──────────────────────────────┐
 │ Atomic Event       │ stream_id=""   │ 1. Validate, idempotency_key lookup   │            │ id: <epoch>:k+1              │
 │ (user_message)     ├───────────────►│ 2. store.Append(offset k+1)           ├───────────►│ event: record                │
 └────────────────────┘                │ 3. Advance state, fan out, SaveMeta   │            │                              │
 ┌────────────────────┐                ├───────────────────────────────────────┤            ├──────────────────────────────┤
 │ Streaming Deltas   │ stream_id=A,B… │ Fast path (RAM only, 0 storage I/O):  │            │ event: delta (no id:)        │
 │ (fin = false)      ├───────────────►│ • inFlights[stream_id].merged += Δ    ├───────────►│ (meta on seq=1)              │
 └────────────────────┘                │ • ≤ MaxConcurrentStreams, ≤ quota     │            │                              │
 ┌────────────────────┐                ├───────────────────────────────────────┤            │ Reconnect / F5:              │
 │ Final Chunk        │ seq=N fin=true │ Slow path (compaction commit):        │            │  [cursor_reset]              │
 │ (fin = true)       ├───────────────►│ store.Append(offset k+2, merged text) ├───────────►│  records in window           │
 └────────────────────┘                │ remove stream, fan out delta + record │            │  1x inflight_snapshot/stream │
          ╳ producer goes silent       ├───────────────────────────────────────┤            │  live deltas & records       │
 ────────────────────────────────────► │ Sweeper: seal after InFlightIdleTimeout├──────────►│ event: record (sealed)       │
                                       └───────────────────┬───────────────────┘            └──────────────────────────────┘
                                                           ▼
                                       Storage hot window (per session): epoch, offset counter,
                                       committed records evicted by HistoryTTL / MaxHistoryBytes
```

### Path Summary

| Path | Trigger | `Storage.Append` | In-flight state | SSE frames | SSE `id:` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Atomic Record** | `stream_id == ""` | `1` | Untouched | `record` | `<epoch>:<offset>` |
| **Fast Path (Delta)** | `stream_id != ""`, `fin == false` | **`0`** | Appends `delta`, `lastSeq = seq` (stream created on first chunk) | `delta` | *Omitted* |
| **Slow Path (Compact)** | `stream_id != ""`, `fin == true` | **`1`** | Stream removed, quota released, recorded in `finalized` | `delta` (`fin:true`) + `record` | Only on `record` |
| **Idle Sealing** | no chunk for `InFlightIdleTimeout` | **`1`** | Same as compaction, `Record.Sealed = "idle_timeout"` | `record` | Only on `record` |

---

## 3. Domain Model & Session Lifecycle

### 3.1 Session States (`model.SessionState`)
- `idle`: No in-flight stream (fresh, hydrated from storage, or all streams compacted).
- `running`: At least one in-flight stream is coalescing deltas in memory.
- `completed` / `failed`: A committed record carries `type == "run_finished"` / `"run_failed"` (`model.StateFromRecordType`).

### 3.2 Session Stream Identity: Epoch & Offsets
- **The epoch belongs to the session stream, not to the broker process.** `Storage` generates a random 12-hex epoch when it creates a session stream (`EnsureStream`). Every record, cursor and `PublishResult.epoch` uses it. `Broker.InstanceID()` (reported by `/healthz` as `instance_id`) only identifies the process.
- `Storage` owns the offset counter (`StreamInfo.LatestOffset`, the highest offset ever appended) and requires `Append` to use exactly `LatestOffset + 1` with the stream's epoch.
- Retention evicts records but **never** resets the epoch or the offset counter. An evicted cursor is therefore reported as `cursor_expired`, never confused with a new incarnation.
- A stream (epoch, counter, metadata) is dropped only after `MetaTTL` of inactivity; recreating it yields a **new epoch** and offsets restart at `1`. An in-memory process restart gives every session a new epoch.
- Hydrated actors normalize a stored `running` state to `idle`: in-flight streams live only in actors and do not survive eviction or restarts (actors with in-flight streams are never evicted).

### 3.3 Hot-Window Limits

| Limit | Default | Enforced in | On overflow |
| :--- | :--- | :--- | :--- |
| `MaxEventBytes` (`delta + payload + meta` of one publish) | 256 KiB | `broker.validatePublishRequest` | `ErrEventTooLarge` → `413` |
| `MaxConcurrentStreams` per session | 8 | `checkStreamCapacityLocked` | `ErrTooManyStreams` → `409` (single-chunk `fin` streams take no slot) |
| `MaxInFlightBytes` (merged text + meta of all in-flight streams) | 2 MiB | `checkStreamCapacityLocked` | `ErrInFlightQuotaExceeded` → `413`; stream untouched; an empty `fin` always fits; `fin` with explicit `payload` is exempt |
| `InFlightIdleTimeout` | 2m | sweeper `sealIdleStreams` | stream sealed into a `Record` (`sealed: "idle_timeout"`); later new seqs → `ErrStreamFinalized` (`409`) |
| `MaxHistoryBytes` per session (`payload + meta + type + stream_id + 64B` per record) | 8 MiB | `MemoryStorage.Append` | oldest records evicted, newest always kept |
| `HistoryTTL` | 15m | `MemoryStorage.Append` / `Sweep` | records older than the TTL evicted |
| `MetaTTL` | 24h | `MemoryStorage.Sweep` | idle stream dropped entirely (new epoch on recreation) |
| `IdempotencyWindow` | 5m | `windowIndex` (`finalized`, `idempotent`) | entries forgotten |

### 3.4 Idempotent Retries
- **Stream chunks** deduplicate by `(stream_id, seq)`: `seq <= lastSeq` of an in-flight stream returns `deduplicated: true`; within `IdempotencyWindow` after compaction or sealing, `seq <= lastSeq` returns the original `committed_offset`, and any newer seq (or auto-seq `0`) returns `ErrStreamFinalized`.
- **Atomic events** may carry `idempotency_key`. Within `IdempotencyWindow` a repeat returns the original `committed_offset` + `epoch` with `deduplicated: true` and is not fanned out. The key is recorded only after the append succeeds. Content is not compared. An `idempotency_key` on a stream chunk is rejected (`400`).

---

## 4. State & Storage Failure Consistency Model

`Publish` executes under `actor.mu`, serializing all state transitions within a session while allowing full parallelism across sessions. `store.Append` is the commit boundary.

### 4.1 Pre-Commit Immutability (Before `store.Append`)
All validation (`MaxEventBytes`, `json.Valid` of `payload` / `meta`, sequence gaps, finalized-stream checks, `MaxConcurrentStreams`, `MaxInFlightBytes`, `ctx.Err()`) and candidate computations (`seq`, merged text, record payload, `meta`) happen in **local variables**:

1. **No premature stream allocation:** A new `inFlightState` is created only after every check passed. A failing single-chunk stream (`seq=1, fin=true`) leaves no stream behind, so it cannot hold a slot or quota.
2. **No premature buffer mutation on `fin=true`:** The record text is `existingMerged + delta` computed locally. If `store.Append` fails, the stream keeps its exact pre-call `(mergedBuf, lastSeq)` and the same request can be retried (no false dedupe, no `"Hellolo"`).
3. **No premature idempotency:** `idempotency_key` and `finalized` entries are written only after a successful append.

### 4.2 `store.Append` as the Point of No Return
Once `store.Append` returns `nil` (atomic, `fin=true` compaction, or idle sealing) the record exists in storage. Rolling back would let retries duplicate records and live subscribers miss offsets. Therefore, immediately:
1. Advance actor state: `lastOffset`, `meta`, remove the stream from `inFlights` and release its bytes, record `finalized` / `idempotent` entries.
2. `fanOutLocked` the committed event (preceded by the `fin=true` delta on compaction).
3. `store.SaveMeta(context.WithoutCancel(ctx), meta)` as **best-effort**. A failure never fails `Publish`.

### 4.3 Cold-Load Reconciliation
`Storage` is authoritative for the epoch, `LatestOffset` and `EarliestOffset`. `LoadMeta` and `ListSessions` merge the best-effort saved metadata with the stream: when the newest retained record is newer than the saved `LastOffset`, `State` and `UpdatedAt` come from that record (`model.StateFromRecordType`). A stream without saved metadata gets synthesized metadata. Hydration therefore needs no tail scan, and a lost `SaveMeta` never makes offsets go backwards.

### 4.4 Failure Consistency Matrix

| Failure Scenario | Storage | Actor Memory | Live Subscribers | Retry of the same request |
| :--- | :--- | :--- | :--- | :--- |
| `seq=1, fin=false` rejected (quota, slots, cancel) | Unchanged | No stream created | Nothing | Accepted once capacity allows |
| `seq=N, fin=true` `Append` fails | Unchanged | Stream stays at `seq=N-1` | Nothing for `seq=N` | Commits `offset+1` with exact text |
| Atomic with `idempotency_key`, `Append` fails | Unchanged | Key not recorded | Nothing | Commits normally |
| Atomic `Append` ok, `SaveMeta` fails | `offset+1` | `lastOffset = offset+1` | `record` | With key: deduplicated; reload reconciles from storage |
| `fin=true` `Append` ok, `SaveMeta` fails | `offset+1` | Stream removed, `finalized` set | `delta` + `record` | `deduplicated: true`, original `committed_offset` |
| Idle sealing `Append` fails | Unchanged | Stream stays in flight | Nothing | Sweeper retries on the next tick |

---

## 5. Catch-Up to Live Tailing Zero-Gap Proof Sketch

**Theorem (Zero-Gap & Duplicate-Free Handoff):** For a session stream with epoch $E$, a subscriber that is not evicted (slow consumer or insufficient state) observes either (a) when its cursor is `<E>:O_0` with `EarliestOffset - 1 <= O_0 <= LatestOffset`: every committed record with offset $> O_0$, or (b) otherwise: one `cursor_reset` followed by every record still retained; and in both cases the exact delta sequence of every stream in flight at or after subscription time.

1. **Linearization point under `actor.mu`:** `registerSubscriber` inserts `sub` into `actor.subscribers` and captures `(epoch, snapOffset = actor.lastOffset, views of all in-flight streams)` in one critical section $T_{sub}$. `Publish` and sealing hold `actor.mu` across state updates and `fanOutLocked`.
2. **Partition:**
   - Records committed before $T_{sub}$ have offsets $\le$ `snapOffset` and are read from storage.
   - Deltas published before $T_{sub}$ are contained in the captured views (`Merged`, `LastSeq`) and are emitted as one `inflight_snapshot` per stream.
   - Everything published after $T_{sub}$ is enqueued into `sub.liveCh` in publication order and is strictly newer than the snapshot (offset $>$ `snapOffset`, seq $>$ the stream's `LastSeq`). The `isCoveredBySnapshot` filter is therefore defense in depth only.
3. **Cursor classification (`planReplay`)** uses only the cursor and the snapshot, then one atomic `store.ReadFrom` returns both the records and the window, so the `cursor_expired` test (`O_0 + 1 < EarliestOffset`) and the replayed records are consistent. At most one `cursor_reset` is sent, always first.
4. **Contiguity check:** For a resumable cursor the first replayed record must be `O_0 + 1` and records must be contiguous up to `snapOffset`; after a reset or for a fresh subscription the replay may start anywhere in the window but must then be contiguous. Records above `snapOffset` are skipped (they arrive through `liveCh`). A hole ends the subscription with `ErrInsufficientState`, so a gap is never delivered silently; the reconnect gets a `cursor_reset`. $\blacksquare$

---

## 6. Concurrency, Lock Hierarchy & Actor Pinning Model

### 6.1 Strict Lock Partial Order

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
`b.mu` protects `b.closed` and the `b.actors` map. It is never held while blocking on `actor.mu` or doing storage I/O:
- **`acquireActor`:** Looks up `b.actors` under `b.mu.RLock()`, pinning atomically without `actor.mu`. On a miss it releases `b.mu` before `store.EnsureStream` / `store.Stream` and `store.LoadMeta`, then inserts with double-checked locking under `b.mu.Lock()`.
- **`evictIdleActors`:** Iterates under `b.mu.Lock()`, skipping pinned actors and probing others with `actor.mu.TryLock()`. Dirty metadata flushes are pinned and executed after releasing `b.mu`.
- **`sealIdleStreams`:** Pins all actors under `b.mu.RLock()`, releases it, then `TryLock`s each actor; busy actors are retried next tick.
- **`maintainStorage`:** Copies live session IDs under `b.mu.RLock()`, then calls `store.Touch` for each and `store.Sweep` outside `b.mu`.
- **`ListSessions` & `Close`:** Snapshot or swap actor pointers under `b.mu`, release it, then lock each actor individually.

### 6.3 Atomic Actor Pinning & Idle Eviction
Every API call pins its actor (`actor.pins.Add(1)`) and releases it with `defer actor.unpin()`, which also refreshes `lastActiveNano`. **Sweeper pins are released with `actor.pins.Add(-1)` and never touch the actor**, otherwise the sweeper itself would keep every session alive. An actor is evicted iff, while `actor.mu` is held via `TryLock()`:
1. `actor.pins.Load() == 0`
2. `len(actor.subscribers) == 0`
3. `len(actor.inFlights) == 0` (silent streams are sealed first, so dead producers cannot pin a session forever)
4. `now - lastActiveNano > IdleSessionTTL`

Evicting an actor only drops the broker-side cache: the stream, its epoch and its retained records stay in storage until retention or `MetaTTL` removes them.

### 6.4 Sweeper Pass (every `SweepInterval`)
1. `sealIdleStreams`: seal streams silent for `InFlightIdleTimeout`.
2. `evictIdleActors`: evict idle actors.
3. `maintainStorage`: `Touch` the stream of every remaining actor (so a session with attached subscribers never expires), then `store.Sweep(now)` (history TTL, `MetaTTL`).

---

## 7. SSE Wire Protocol Specification

`GET /v1/sessions/{id}/stream` implements W3C Server-Sent Events (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `X-Accel-Buffering: no`).

### 7.1 Cursor Resolution Precedence
1. **`Last-Event-ID` header** (sent automatically by browser `EventSource` on reconnect): `<epoch>:<offset>`.
2. **Query parameters** (initial page load, SDK bootstrap, DB watermark): `?after_offset=<epoch>:<offset>` or `?after_offset=<offset>&epoch=<epoch>`.
3. **Default:** fresh subscription (whole retained window, in-flight snapshots, live stream; no reset).

A cursor without an epoch but with a non-zero offset cannot be validated against the current incarnation and is answered with `cursor_reset` `epoch_mismatch`.

### 7.2 Frame Catalog

| Frame Type | Wire Format | Carries `id:`? | Semantics |
| :--- | :--- | :---: | :--- |
| **Connected Handshake** | `: connected epoch=<session epoch>\n\n` | No | First bytes after `200 OK`; announces the session stream epoch. |
| **Keepalive Heartbeat** | `: keepalive\n\n` | No | Every `KeepaliveInterval` (default `15s`). |
| **Cursor Reset** | `event: cursor_reset\ndata: {"reason":…,"requested_epoch":…,"requested_offset":…,"current_epoch":…,"earliest_offset":…,"latest_offset":…}\n\n` | **No** | At most once, first event only. `reason` is `epoch_mismatch`, `cursor_expired` or `cursor_ahead`. The retained window, snapshots and live stream follow on the same connection. |
| **Committed Record** | `id: <epoch>:<offset>\nevent: record\ndata: {...}\n\n` | **Yes** | Committed record (`meta` and `sealed` when present). Advances `Last-Event-ID`. |
| **In-Flight Snapshot** | `event: inflight_snapshot\ndata: {...}\n\n` | **No** | One per stream in flight at subscription time: merged text up to `seq`, plus `meta`. |
| **Live Token Delta** | `event: delta\ndata: {...}\n\n` | **No** | Incremental slice (`stream_id`, `seq`, `delta`, `fin`; `meta` on a stream's first delta). |
| **Terminal Stream Error** | `event: stream_error\ndata: {"error":"slow_subscriber"}\n\n` or `{"error":"insufficient_state"}` | **No** | Sent right before the server closes the stream. Never named `error` (collides with `EventSource`'s network `error` event). |

### 7.3 Reference Client Reconstruction State Machine
Clients keep committed records by `offset` and a map of in-flight buffers keyed by `stream_id`:
1. `record`: store at `offset`; if a buffer exists for its `stream_id`, replace that streaming bubble with the authoritative `payload` (a `sealed` record means the producer went silent and the text is partial).
2. `inflight_snapshot`: initialize/replace the buffer for `stream_id` with `text = delta`, `lastSeq = seq`, `meta`.
3. `delta`: if `seq == lastSeq + 1` append and advance.
4. `cursor_reset`: keep the connection, buffer the following events, reload committed history from the application's source of truth, then merge (see [`DESIGN_V2.md` §2–§3](DESIGN_V2.md)).
5. `stream_error`: native `EventSource` clients must **not** call `es.close()`; the browser reconnects with `Last-Event-ID` (for `insufficient_state` that reconnect receives a `cursor_reset`). A brand-new `EventSource` must pass the saved cursor as `?after_offset=<epoch>:<offset>`.

### 7.4 Publish Status Codes

| Status | Broker error |
| :--- | :--- |
| `400` | `ErrInvalidInput` (empty event, invalid JSON `payload`/`meta`, `idempotency_key` on a chunk), `ErrSequenceGap` |
| `409` | `ErrTooManyStreams`, `ErrStreamFinalized` |
| `413` | `ErrEventTooLarge`, `ErrInFlightQuotaExceeded` |
| `503` | `ErrBrokerClosed` |

---

## 8. Architectural Evolution (Phase 2 -> Phase 3)

The data plane above implements the hot-window part of [`DESIGN_V2.md`](DESIGN_V2.md). Still to come:
- **Stateful Agent Session Control Plane (Phase 2):** Run concurrency policies, `Cancel` / `Steer` worker channel, idempotent `WaitingApproval` gate, and worker heartbeat leases. A lease expiry reuses the sealing path with a `worker_lost` marker.
- **`RedisStorage` (Phase 3):** The `Storage` interface is shaped for Redis (meta hash with epoch + offset counter and `EXPIRE`, `XADD` with explicit `<offset>-0` IDs, `XRANGE`, `XTRIM MINID`); see the mapping in `internal/storage/storage.go`. In-flight streams and dedupe indexes stay in the owning broker instance, so multi-instance deployments need session affinity or moving those into Redis.
