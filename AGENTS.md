# Seamline — Repository Instructions (`AGENTS.md`)

**Seamline** is a zero-dependency, resumable dual-speed AI stream broker written in pure Go (`go 1.23`, standard library only). It decouples high-frequency token deltas (up to 8 concurrent in-memory streams per session with non-blocking SSE fan-out) from committed records (`fin=true` single-record compaction) kept in a short TTL hot window, enabling zero-gap mid-stream `Last-Event-ID` recovery, F5 page reloads, and explicit `cursor_reset` when a cursor can no longer be continued.

This file is the **single source of truth** for AI coding agents (Claude Code, Codex, Cursor, Gemini, Antigravity) and human contributors. Keep rules actionable, verifiable, and grounded in Seamline's invariants.

---

## Project Map

- `cmd/seamline/`: HTTP/SSE server entrypoint, CLI flags, and deterministic graceful shutdown (`brk.Close()` before `httpServer.Shutdown()`).
- `cmd/simworker/`: Standalone CLI worker simulator streaming realistic token deltas over HTTP.
- `internal/model/`: Canonical domain and wire structs (`Record`, `PublishRequest`, `PublishResult`, `OutEvent`, `CursorReset`, `InFlightView`, `SessionMeta`, `SessionSnapshot`).
- `internal/storage/`: Hot-window `Storage` interface (each method maps onto Redis primitives) and `MemoryStorage` (`memstore.go`: per-session epoch + offset counter that survive eviction, `HistoryTTL` / `MaxHistoryBytes` record eviction, `MetaTTL` stream expiry; `memstore_meta.go`: best-effort metadata reconciled with the stream).
- `internal/broker/`: Core `Broker` and config (`broker.go`), `sessionActor` and hydration (`actor.go`), background sweeper: idle-stream sealing, actor eviction, storage maintenance (`sweep.go`), atomic publish + `idempotency_key` (`publish.go`), multi-stream publish/compaction/sealing (`stream.go`), subscription registration and live pump (`subscribe.go`), cursor classification and catch-up replay (`replay.go`), time-bounded dedupe index (`window.go`).
- `internal/server/`: Standard-library `http.ServeMux` REST + W3C SSE transport (`server.go`) and embedded F5 Live Playground (`demo.go`, `demo.html`).
- `docs/ARCHITECTURE.md`: Formal specification of the implemented data plane: hot window, storage failure consistency model, zero-gap handoff proof sketch, lock hierarchy, and SSE wire protocol.
- `docs/DESIGN_V2.md`: Phase 2 / Phase 3 blueprint and the decisions behind the data plane (byte quotas, multi-stream `inFlights` with `Meta`, per-session epochs, idempotency, `seamline_cursor` DB alignment, `event: cursor_reset`) plus the stateful Agent session control plane still to be built.

---

## Read When Relevant

Consult the corresponding specification **before** modifying that subsystem:

| Area Being Changed | Required Reference |
| :--- | :--- |
| Retention, byte quotas, epochs, idempotency, `cursor_reset`, control plane, Redis | [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md) |
| `internal/broker/` state transitions, retries, `inFlights` lifecycle, sealing, or `Storage` calls | [State & Storage Failure Consistency](docs/ARCHITECTURE.md#4-state--storage-failure-consistency-model) |
| `Broker.Subscribe`, `runSubscriberPump`, `Last-Event-ID`, or epoch cursor handling | [Zero-Gap Catch-Up to Live Handoff](docs/ARCHITECTURE.md#5-catch-up-to-live-tailing-zero-gap-proof-sketch) |
| Locks (`b.mu`, `actor.mu`, `sub.errMu`), `pins`, `evictIdleActors`, or `ListSessions` | [Concurrency, Lock Hierarchy & Pinning](docs/ARCHITECTURE.md#6-concurrency-lock-hierarchy--actor-pinning-model) |
| `internal/server/`, SSE frame formatting, CORS, or `demo.html` Playground UI | [SSE Wire Protocol Specification](docs/ARCHITECTURE.md#7-sse-wire-protocol-specification) |
| Linter rules, Makefile targets, or GitHub Actions CI | [`Makefile`](Makefile), [`.golangci.yml`](.golangci.yml), [`.github/workflows/ci.yml`](.github/workflows/ci.yml) |

---

## Mandatory Verification Gate

Run these targets before marking any task complete or opening/updating a PR:

| Gate | Command | Requirement |
| :--- | :--- | :--- |
| Format check | `make fmt-check` | Zero unformatted `.go` files (`gofmt -l .`) |
| Static analysis | `make vet` | `go vet ./...` exits `0` |
| Linter suite | `make lint` | `golangci-lint run` exits `0` (includes `gocognit <= 15` gate) |
| Complexity gate | `make complexity` | Every non-test `.go` function has `gocognit <= 15` |
| Race detector tests | `make test-race` | `go test -race -count=2 ./...` passes with zero races |
| Static binary build | `make build` | `CGO_ENABLED=0 go build` succeeds for `./cmd/...` |
| **Full pre-PR gate** | **`make check`** | Runs `fmt-check` + `vet` + `test-race` |

Formatting rewrites files; always re-run `make check` and `make lint` **after** the final edit.

---

## Hard Engineering Invariants

Every change to Seamline **must** preserve the invariants below. When a change intentionally evolves one of them (for example a Phase 2/3 item from [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md)), update this file, [`CLAUDE.md`](CLAUDE.md), and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) in the same PR instead of treating the old rule as a blocker.

### 1. Dependency Discipline & No `cgo`
- **[Phase 1 & 2 Core]:** Core runtime packages (`internal/model`, `internal/broker`, `internal/server`, `MemoryStorage`) use **100% Go 1.23+ standard library** (`go 1.23` in `go.mod`). Optional external drivers (such as Phase 3 `RedisStorage`) require explicit approval before adding dependencies.
- Never run `go mod init` or `go mod tidy` with an experimental host toolchain that rewrites the `go 1.23` directive in `go.mod`.
- Production binaries (`./cmd/seamline`, `./cmd/simworker`) must build cleanly with `CGO_ENABLED=0`. (Note: `go test -race` requires `CGO_ENABLED=1` on the test runner; never set global `CGO_ENABLED=0` across race-test steps.)

### 2. Strict Lock Hierarchy & Cross-Session Isolation
- Lock acquisition order is strictly one-way:
  ```text
  Broker.mu (b.mu)  ──►  sessionActor.mu (actor.mu)  ──►  subState.errMu (sub.errMu)
  ```
  `MemoryStorage.mu` is a leaf lock taken only inside `Storage` calls.
- **Never** acquire `b.mu` while holding `actor.mu` or `sub.errMu`.
- **Never block waiting on `actor.mu` while holding `b.mu`:**
  - `Publish` performs `Storage` I/O while holding `actor.mu`. If any caller holds `b.mu` and calls blocking `actor.mu.Lock()`, a single slow disk/WAL write on Session A will freeze `acquireActor` for every other session in the process.
  - **Sweeper rule (`evictIdleActors`):** While holding `b.mu`, always probe actors with `if !actor.mu.TryLock() { continue }`. A locked actor is actively serving a request and is ineligible for idle eviction anyway.
  - **Sealing rule (`sealIdleStreams`):** Pin actors under `b.mu.RLock()`, release `b.mu`, then `TryLock` each actor (skip busy ones until the next tick). Release the sweeper's pin with `actor.pins.Add(-1)`, **never** `unpin()`: `unpin()` touches `lastActiveNano`, and a sweeper that touches actors keeps every session alive forever.
  - **Snapshot listing rule (`ListSessions`):** Copy actor pointers out of `b.actors` under `b.mu.RLock()`, release `b.mu.RUnlock()` **immediately**, and then lock/snapshot each `sessionActor` individually outside `b.mu`.
  - **Hydration rule (`acquireActor`):** Lookup cached actors under `b.mu.RLock()` using atomic pinning (`actor.pins.Add(1)` without locking `actor.mu`); perform cold `store.EnsureStream` / `store.Stream` and `store.LoadMeta` outside `b.mu`, using double-checked insertion under `b.mu.Lock()`.
- Prevent actor eviction during active operations exclusively via atomic reference counting (`actor.pins.Add(1)` before releasing `b.mu`, paired with `defer actor.unpin()`). Do not introduce dead/unread boolean eviction flags.

### 3. Storage Failure & State Mutation Invariant (`store.Append` Is the Point of No Return)
- **Pre-Commit Immutability:** Validate inputs (`MaxEventBytes`, JSON validity), check `ctx.Err()`, run capacity checks (`MaxConcurrentStreams`, `MaxInFlightBytes`), and compute candidate values (next sequence number, merged text, marshaled JSON payload, `Meta`) in **local variables** before touching storage or mutating `actor`.
  - **Never** mutate `actor.inFlights`, `inFlightBytes`, `mergedBuf`, `lastSeq`, `lastOffset`, `finalized`, or `idempotent` **before** `store.Append` succeeds (or before pre-flight checks pass on non-`fin` chunks).
  - Why: If `store.Append` fails on `fin=true` after `lastSeq` or `mergedBuf` was already mutated, a retry with the same `(stream_id, seq)` falsely hits `seq <= lastSeq` deduplication, wedging the stream. Recording an `idempotency_key` before the append makes the retry of a failed publish return a "duplicate" of a record that was never committed. Allocating an `inFlightState` before the first chunk is verified leaves a zombie stream holding a slot and quota.
- **Point of No Return (`store.Append`):** Once `store.Append` returns `nil` (atomic event, `fin=true` compaction, or idle sealing), the record is committed:
  1. Immediately advance `actor` in-memory state (`lastOffset`, `meta`, remove the stream from `inFlights` and release its `inFlightBytes`, record it in `finalized` / record the `idempotency_key`).
  2. Immediately call `actor.fanOutLocked(...)` so live subscribers never miss a committed record or terminal `fin=true` delta.
  3. Persist `store.SaveMeta` as **best-effort** using `context.WithoutCancel(ctx)`. **Never** return an error from `Publish` or skip `fanOutLocked` if `SaveMeta` fails after `store.Append` succeeded: `Storage` owns the authoritative epoch and `LatestOffset`, and `LoadMeta` / `ListSessions` reconcile `State` from the newest retained record via `model.StateFromRecordType`.
- **Non-`fin` Delta Consistency:** On `fin=false` chunks, ensure memory mutation (`inFlights`, `mergedBuf`, `lastSeq`, `inFlightBytes`) and `fanOutLocked` execute atomically without an intervening failing `SaveMeta` aborting between buffer mutation and fan-out.
- **Hot-Window Retention (`Storage`):** Retention (`HistoryTTL`, `MaxHistoryBytes`) only evicts the oldest committed records and always keeps the newest one; it never resets a stream's epoch or offset counter. A stream (epoch, offset counter, metadata) is dropped only after `MetaTTL` of inactivity; the sweeper `Touch`es every live actor's stream so attached subscribers never lose their stream. In-flight streams live only in actors and are never evicted by retention; a silent stream is sealed after `InFlightIdleTimeout` (`Record.Sealed = "idle_timeout"`).

### 4. Zero-Gap Subscription Handoff
- In `Broker.Subscribe(ctx, sessionID, afterOffset, afterEpoch)`, register `actor.subscribers[sub] = struct{}{}` and capture the snapshot (session `epoch`, `snapOffset := actor.lastOffset`, every active in-flight stream) inside the **exact same `actor.mu` critical section**. Every event in `liveCh` is then strictly newer than the snapshot.
- Cursors are classified before any frame is sent (`planReplay`):
  - `afterEpoch == "" && afterOffset == 0` → fresh subscription: whole retained window, **no** reset.
  - epoch differs from the session epoch (including a bare offset without epoch) → `cursor_reset` `epoch_mismatch`.
  - `afterOffset > snapOffset` → `cursor_reset` `cursor_ahead`.
  - `afterOffset + 1 < earliestOffset` (checked against the window returned by the same `ReadFrom`) → `cursor_reset` `cursor_expired`.
  - Never silently reset a cursor to `0`.
- In `runSubscriberPump`:
  1. Emit at most one `cursor_reset`, and only as the first event.
  2. Replay committed records up to `snapOffset` from a single `store.ReadFrom` (records + window read atomically), verifying contiguity. A hole ends the subscription with `ErrInsufficientState` (the server sends `stream_error` `insufficient_state` and closes); never emit `cursor_reset` mid-stream.
  3. Emit one `inflight_snapshot` per active stream with `LastSeq > 0` (ordered by start time).
  4. Drain `sub.liveCh`. The `isCoveredBySnapshot` filter (records `<= snapOffset`, deltas `<=` their stream's snapshot seq) is defense in depth: it cannot trigger while the critical-section rule above holds.

### 5. Non-Blocking Fan-Out, SSE Wire Protocol & Playground XSS Safety
- **[Permanent] Non-blocking `Publish`:** `fanOutLocked` must use non-blocking channel sends (`select { case sub.liveCh <- ev: default: ... }`). A stalled reader is evicted (`ErrSlowSubscriber`) without delaying producers or sibling subscribers.
- **[Permanent] Cursor placement (`id:`):** Emit SSE `id: <epoch>:<offset>` **exclusively** on finalized `event: record` frames, where `<epoch>` is the session stream epoch. Never emit `id:` on `inflight_snapshot`, `delta`, `cursor_reset`, or `stream_error` frames; otherwise browser `EventSource` overwrites its last committed offset cursor. `cursor_reset` carries the flat `model.CursorReset` JSON as its `data`.
- **Terminal error event naming & reconnection:** Emit `event: stream_error` (never `event: error`, which collides with `EventSource`'s native network `error` event) with `{"error":"slow_subscriber"}` or `{"error":"insufficient_state"}`. Browsers only send `Last-Event-ID` on automatic `EventSource` reconnects—never on `new EventSource(url)`—so native `EventSource` clients must leave the instance open on `stream_error` for automatic `Last-Event-ID` recovery, or pass `?after_offset=<epoch>:<offset>` when opening a new `EventSource`.
- **Single CORS & Shutdown Path:** Set CORS headers once in `withCORS` middleware rather than duplicating them inside handlers. During shutdown (`cmd/seamline/main.go`), call `brk.Close()` **once** and **before** `httpServer.Shutdown()` so open SSE handlers terminate immediately instead of waiting out the shutdown timeout.
- **DOM XSS Prevention:** In `internal/server/demo.html` (and any future web UI), render session IDs, payloads, deltas, and URL parameters exclusively via safe DOM APIs (`document.createElement`, `node.textContent`, `element.replaceChildren`)—**never** interpolate dynamic strings into `innerHTML`.

### 6. Readability, File Modularity & Cognitive Complexity (`gocognit <= 15`)
- **Hard Complexity Ceiling (`gocognit <= 15`):** Every non-test Go function across `cmd/` and `internal/` **must** keep cognitive complexity `<= 15` (strictly enforced in CI via `gocognit` in `.golangci.yml`).
- **Flat Guard Clauses & Table-of-Contents Top-Level Functions:** Prefer early returns and flat guard clauses over deeply nested `if / else / switch / select` blocks. Top-level entrypoints (`Publish`, `Subscribe`, `handleStream`, `acquireActor`) should read top-to-bottom like a concise table of contents delegating to single-purpose helpers (`publishAtomicLocked`, `publishStreamLocked`, `replayCatchUp`, `drainLiveStream`, `streamSSELoop`).
- **Focused ~250-Line Files Over 1,000-Line Monoliths:** Split multi-concern packages by cohesive responsibility (e.g., `broker.go`, `actor.go`, `publish.go`, `subscribe.go`) around ~200–300 lines per file so human reviewers can understand each file without scrolling a 1,000-line monolith. Never contort logic artificially just to game metrics—extract natural domain steps with clear names.

### 7. Testing & Linter Discipline
- Every bug fix or state-machine change in `internal/broker` or `internal/storage` must ship with deterministic **fault-injection regression tests** (`flakyStore` / `slowStore`) verifying:
  - `fin=true` `Append` failure followed by same-`seq` retry commits cleanly with exact content;
  - first-chunk failure leaves no in-flight stream and does not block subsequent streams;
  - post-`Append` `SaveMeta` failure still fans out to live subscribers, returns `CommittedOffset`, prevents duplicate records on retry, and recovers state after actor eviction + cold reload;
  - slow storage I/O on Session A does not block `Publish`, `ListSessions`, or idle sweeping on Session B;
  - an `idempotency_key` is only remembered after a successful append.
- Hot-window changes must keep regression tests for: each `cursor_reset` reason, retention keeping epoch + offsets, `MetaTTL` rotating the epoch, live sessions not expiring, idle sealing, `MaxConcurrentStreams` / `MaxInFlightBytes` limits, and replay gaps ending with `insufficient_state`.
- Key tests must be able to fail: verify new tests by deliberately breaking the code they guard (and record which mutations they kill). Tests must not hang on a regression; bound every wait with a timeout. No round-trip serialization tests.
- Always close `*http.Response.Body` in production and test code (`bodyclose` is enforced in CI).
- Check errors with `errors.Is` / `errors.As` and wrap with `%w` (`errorlint` is enforced in CI).
- Keep `Storage` implementations defensive: deep-copy `json.RawMessage` byte slices on both write (`Append`) and read (`ReadFrom`) so caller buffer reuse can never mutate stored history under `-race`.

---

## Maintaining These Instructions

- Keep `AGENTS.md` concise and authoritative. Deep protocol proofs, failure matrices, and wire-protocol state machines belong in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- Keep [`CLAUDE.md`](CLAUDE.md) synchronized as a lightweight pointer to `AGENTS.md` so every AI coding assistant follows identical engineering constraints.
