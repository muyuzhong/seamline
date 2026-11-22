# Seamline — Repository Instructions (`AGENTS.md`)

**Seamline** is a zero-dependency, resumable dual-speed AI stream broker written in pure Go (`go 1.23`, standard library only). It decouples high-frequency ephemeral LLM token deltas (in-memory coalescing + non-blocking SSE fan-out) from low-frequency durable state (`fin=true` single-record compaction), enabling zero-gap mid-stream `Last-Event-ID` recovery and F5 page reloads.

This file is the **single source of truth** for AI coding agents (Claude Code, Codex, Cursor, Gemini, Antigravity) and human contributors. Keep rules actionable, verifiable, and grounded in Seamline's invariants.

---

## Project Map

- `cmd/seamline/`: HTTP/SSE server entrypoint, CLI flags, and deterministic graceful shutdown (`brk.Close()` before `httpServer.Shutdown()`).
- `cmd/simworker/`: Standalone CLI worker simulator streaming realistic token deltas over HTTP.
- `internal/model/`: Canonical domain and wire structs (`Record`, `PublishRequest`, `PublishResult`, `OutEvent`, `InFlightView`, `SessionMeta`, `SessionSnapshot`).
- `internal/storage/`: `Storage` interface and `MemoryStorage` reference implementation (`sync.RWMutex`, deep-copied `Payload` slices, strictly monotonic `Offset >= 1`).
- `internal/broker/`: Core `Broker` (`broker.go`), per-session `sessionActor` & idle sweeper (`actor.go`), dual-speed `Publish` state machine (`publish.go`), and zero-gap `Subscribe` catch-up/live pump (`subscribe.go`).
- `internal/server/`: Standard-library `http.ServeMux` REST + W3C SSE transport (`server.go`) and embedded F5 Live Playground (`demo.go`, `demo.html`).
- `docs/ARCHITECTURE.md`: Formal specification of the Dual-Speed Engine, storage failure consistency model, zero-gap handoff proof sketch, lock hierarchy, and SSE wire protocol.

---

## Read When Relevant

Consult the corresponding specification **before** modifying that subsystem:

| Area Being Changed | Required Reference |
| :--- | :--- |
| `internal/broker/` state transitions, retries, `inFlight` lifecycle, or `Storage` calls | [State & Storage Failure Consistency](docs/ARCHITECTURE.md#4-state--storage-failure-consistency-model) |
| `Broker.Subscribe`, `runSubscriberPump`, `Last-Event-ID`, or epoch cursor handling | [Zero-Gap Catch-Up to Live Handoff](docs/ARCHITECTURE.md#5-catch-up-to-live-tailing-zero-gap-proof-sketch) |
| Locks (`b.mu`, `actor.mu`, `sub.errMu`), `pins`, `evictIdleActors`, or `ListSessions` | [Concurrency, Lock Hierarchy & Pinning](docs/ARCHITECTURE.md#6-concurrency-lock-hierarchy--actor-pinning-model) |
| `internal/server/`, SSE frame formatting, CORS, or `demo.html` Playground UI | [SSE Wire Protocol Specification](docs/ARCHITECTURE.md#7-sse-wire-protocol-specification) |
| Linter rules, Makefile targets, GitHub Actions CI, or PR workflow | [Contributing Guide](CONTRIBUTING.md), [`.golangci.yml`](.golangci.yml) |

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

Every change to Seamline **must** preserve the following seven invariants. Violations cause silent stream corruption, cross-session stalls, stuck sessions, browser security bugs, or unmaintainable monoliths.

### 1. Zero-Dependency Core & No `cgo`
- Phase 1 runtime packages use **100% Go 1.23+ standard library** (`go 1.23` in `go.mod`). Do not add third-party modules to `go.mod` without explicit architectural approval.
- Never run `go mod init` or `go mod tidy` with an experimental host toolchain that rewrites the `go 1.23` directive in `go.mod`.
- Production binaries (`./cmd/seamline`, `./cmd/simworker`) must build cleanly with `CGO_ENABLED=0`. (Note: `go test -race` requires `CGO_ENABLED=1` on the test runner; never set global `CGO_ENABLED=0` across race-test steps.)

### 2. Strict Lock Hierarchy & Cross-Session Isolation
- Lock acquisition order is strictly one-way:
  ```text
  Broker.mu (b.mu)  ──►  sessionActor.mu (actor.mu)  ──►  subState.errMu (sub.errMu)
  ```
- **Never** acquire `b.mu` while holding `actor.mu` or `sub.errMu`.
- **Never block waiting on `actor.mu` while holding `b.mu`:**
  - `Publish` performs `Storage` I/O while holding `actor.mu`. If any caller holds `b.mu` and calls blocking `actor.mu.Lock()`, a single slow disk/WAL write on Session A will freeze `acquireActor` for every other session in the process.
  - **Sweeper rule (`evictIdleActors`):** While holding `b.mu`, always probe actors with `if !actor.mu.TryLock() { continue }`. A locked actor is actively serving a request and is ineligible for idle eviction anyway.
  - **Snapshot listing rule (`ListSessions`):** Copy actor pointers out of `b.actors` under `b.mu.RLock()`, release `b.mu.RUnlock()` **immediately**, and then lock/snapshot each `sessionActor` individually outside `b.mu`.
  - **Hydration rule (`acquireActor`):** Lookup cached actors under `b.mu.RLock()` using atomic pinning (`actor.pins.Add(1)` without locking `actor.mu`); perform cold `store.LoadMeta` and `store.ReadFrom` outside `b.mu`, using double-checked insertion under `b.mu.Lock()`.
- Prevent actor eviction during active operations exclusively via atomic reference counting (`actor.pins.Add(1)` before releasing `b.mu`, paired with `defer actor.unpin()`). Do not introduce dead/unread boolean eviction flags.

### 3. Storage Failure & State Mutation Invariant (`store.Append` Is the Point of No Return)
- **Pre-Commit Immutability:** Validate inputs, check `ctx.Err()`, and compute candidate values (next sequence number, merged text, marshaled JSON payload, candidate `inFlightState`) in **local variables** before touching storage or mutating `actor`.
  - **Never** mutate `actor.inFlight`, `mergedBuf`, `lastSeq`, `lastOffset`, or `lastCompacted*` **before** `store.Append` succeeds (or before pre-flight checks pass on non-`fin` chunks).
  - Why: If `store.Append` fails on `fin=true` after `lastSeq` or `mergedBuf` was already mutated, a retry with the same `(stream_id, seq)` falsely hits `seq <= lastSeq` deduplication (`Deduplicated: true` with `CommittedOffset: 0`), permanently wedging the session in `409 Conflict` (`ErrStreamConflict`) and preventing idle eviction. Likewise, allocating `actor.inFlight = &inFlightState{}` before verifying the first chunk leaves an empty zombie stream on failure.
- **Point of No Return (`store.Append`):** Once `store.Append` returns `nil` (for either an atomic event or a `fin=true` stream compaction), the record is durably committed:
  1. Immediately advance `actor` in-memory state (`lastOffset`, `meta`, `lastCompacted*`, and `actor.inFlight = nil` on `fin=true`).
  2. Immediately call `actor.fanOutLocked(...)` so live subscribers never miss a committed record or terminal `fin=true` delta.
  3. Persist `store.SaveMeta` as **best-effort** using `context.WithoutCancel(ctx)` so caller cancellation mid-commit cannot abort metadata persistence. **Never** return an error from `Publish` or skip `fanOutLocked` if `SaveMeta` fails after `store.Append` succeeded—`acquireActor` automatically reconciles `LastOffset` and session state from `store.ReadFrom` tail records on cold load.
- **Non-`fin` Delta Consistency:** On `fin=false` chunks, ensure memory mutation (`inFlight`, `mergedBuf`, `lastSeq`) and `fanOutLocked` execute atomically without an intervening failing `SaveMeta` aborting between buffer mutation and fan-out.

### 4. Zero-Gap Subscription Handoff
- In `Broker.Subscribe(ctx, sessionID, afterOffset, afterEpoch)`:
  - If `afterEpoch != "" && afterEpoch != b.Epoch()`, reset `afterOffset = 0` before catch-up so stale cursors from prior broker epochs replay cleanly from the start of the session.
  - Register `actor.subscribers[sub] = struct{}{}`, read `snapOffset := actor.lastOffset`, and capture `inFlightSnap := actor.inFlightViewLocked()` inside the **exact same `actor.mu` critical section**.
- In `runSubscriberPump`:
  1. Replay committed records in `(afterOffset, snapOffset]` from `store.ReadFrom`.
  2. If `inFlightSnap != nil && inFlightSnap.LastSeq > 0`, emit one `inflight_snapshot` (`Seq = inFlightSnap.LastSeq`, `Delta = inFlightSnap.Merged`).
  3. Drain `sub.liveCh`, dropping any `EventCommitted` with `Offset <= snapOffset` and any `EventDelta` matching `inFlightSnap.StreamID` with `Seq <= inFlightSnap.LastSeq` (and clearing `inFlightSnap` once its compacted `EventCommitted` is observed).

### 5. Non-Blocking Fan-Out, SSE Wire Protocol & Playground XSS Safety
- **Non-blocking `Publish`:** `fanOutLocked` must use non-blocking channel sends (`select { case sub.liveCh <- ev: default: ... }`). A stalled reader is evicted (`ErrSlowSubscriber`) without delaying producers or sibling subscribers.
- **Cursor placement (`id:`):** Emit SSE `id: <epoch>:<offset>` **exclusively** on finalized `event: record` frames. Never emit `id:` on `inflight_snapshot`, `delta`, or `stream_error` frames; otherwise browser `EventSource` overwrites its last committed offset cursor.
- **Terminal error event naming:** Emit `event: stream_error` (never `event: error`). In browsers, `event: error` collides with `EventSource`'s native network `error` event and confuses reconnection handlers.
- **Single CORS & Shutdown Path:** Set CORS headers once in `withCORS` middleware rather than duplicating them inside handlers. During shutdown (`cmd/seamline/main.go`), call `brk.Close()` **once** and **before** `httpServer.Shutdown()` so open SSE handlers terminate immediately instead of waiting out the shutdown timeout.
- **DOM XSS Prevention:** In `internal/server/demo.html` (and any future web UI), render session IDs, payloads, deltas, and URL parameters exclusively via safe DOM APIs (`document.createElement`, `node.textContent`, `element.replaceChildren`)—**never** interpolate dynamic strings into `innerHTML`.

### 6. Readability, File Modularity & Cognitive Complexity (`gocognit <= 15`)
- **Hard Complexity Ceiling (`gocognit <= 15`):** Every non-test Go function across `cmd/` and `internal/` **must** keep cognitive complexity `<= 15` (strictly enforced in CI via `gocognit` in `.golangci.yml`).
- **Flat Guard Clauses & Table-of-Contents Top-Level Functions:** Prefer early returns and flat guard clauses over deeply nested `if / else / switch / select` blocks. Top-level entrypoints (`Publish`, `Subscribe`, `handleStream`, `acquireActor`) should read top-to-bottom like a concise table of contents delegating to single-purpose helpers (`publishAtomicLocked`, `publishStreamLocked`, `replayCatchUp`, `drainLiveStream`, `streamSSELoop`).
- **Focused ~250-Line Files Over 1,000-Line Monoliths:** Split multi-concern packages by cohesive responsibility (e.g., `broker.go`, `actor.go`, `publish.go`, `subscribe.go`) around ~200–300 lines per file so human reviewers can understand each file without scrolling a 1,000-line monolith. Never contort logic artificially just to game metrics—extract natural domain steps with clear names.

### 7. Testing & Linter Discipline
- Every bug fix or state-machine change in `internal/broker` or `internal/storage` must ship with deterministic **fault-injection regression tests** (`flakyStore` / `slowStore`) verifying:
  - `fin=true` `Append` failure followed by same-`seq` retry commits cleanly with exact content;
  - first-chunk failure leaves `InFlight == nil` and does not block subsequent streams;
  - post-`Append` `SaveMeta` failure still fans out to live subscribers, returns `CommittedOffset`, prevents duplicate records on retry, and recovers state after actor eviction + cold reload;
  - slow storage I/O on Session A does not block `Publish`, `ListSessions`, or idle sweeping on Session B.
- Always close `*http.Response.Body` in production and test code (`bodyclose` is enforced in CI).
- Check errors with `errors.Is` / `errors.As` and wrap with `%w` (`errorlint` is enforced in CI).
- Keep `Storage` implementations defensive: deep-copy `json.RawMessage` byte slices on both write (`Append`) and read (`ReadFrom`) so caller buffer reuse can never mutate stored history under `-race`.

---

## Maintaining These Instructions

- Keep `AGENTS.md` concise and authoritative. Deep protocol proofs and state tables belong in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); contributor workflow belongs in [`CONTRIBUTING.md`](CONTRIBUTING.md).
- Keep [`CLAUDE.md`](CLAUDE.md) synchronized as a lightweight pointer to `AGENTS.md` so every AI coding assistant follows identical engineering constraints.
