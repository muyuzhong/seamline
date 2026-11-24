# Seamline — Repository Instructions (`AGENTS.md`)

**Seamline** is a zero-dependency, resumable AI stream broker written in pure Go (`go 1.23`, standard library only). Every published event is payload-opaque and gets the next per-session cursor `<epoch>:<seq>`; events live in a short TTL hot window where the active run (`run_started` … terminal event) is never evicted, and every SSE event frame carries `id: <epoch>:<seq>`, enabling exact `Last-Event-ID` recovery, F5 reloads in the middle of a run, and an explicit `cursor_reset` whenever a cursor cannot be continued.

This file is the **single source of truth** for AI coding agents (Claude Code, Codex, Cursor, Gemini, Antigravity) and human contributors. Keep rules actionable, verifiable, and grounded in Seamline's invariants.

---

## Project Map

- `cmd/seamline/`: HTTP/SSE server entrypoint, CLI flags, and deterministic graceful shutdown (`brk.Close()` before `httpServer.Shutdown()`).
- `cmd/simworker/`: CLI worker simulator: one run of `run_started`, `user_message`, interleaved `assistant_delta` / `tool_call_delta` events and `run_finished`, with idempotency keys and retries (`main.go`, `publisher.go`).
- `internal/model/`: Domain and wire structs (`Event`, `PublishRequest`, `PublishResult`, `Delivery`, `CursorReset`, `SessionMeta`, `SessionSnapshot`) and the run event types (`TerminalState`).
- `internal/hotwindow/`: Hot-window `Storage` interface (each method maps onto Redis primitives, `hotwindow.go`), `MemoryStorage` (`memstore.go`) and per-stream retention / run bookkeeping (`stream.go`).
- `internal/broker/`: `Broker`, config, Inspect/List (`broker.go`), `sessionActor` and hydration (`actor.go`), `Publish` (`publish.go`), run state machine and idle sealing (`run.go`), subscription registration and live pump (`subscribe.go`), cursor classification and catch-up replay (`replay.go`), sweeper (`sweep.go`), time-bounded idempotency index (`window.go`).
- `internal/server/`: `http.ServeMux` REST + W3C SSE transport (`server.go`), error codes (`errors.go`) and the embedded F5 Live Playground (`demo.go`, `demo.html`).
- `docs/ARCHITECTURE.md`: Specification of the implemented data plane: event model, hot window, commit consistency, zero-gap handoff proof sketch, lock hierarchy, and SSE wire protocol.
- `docs/DESIGN_V2.md`: **Authoritative Phase 2 / Phase 3 blueprint** — payload-opaque **pure monotonic event cursor (`<epoch>:<seq>`)**, per-session `epoch` + `MetaTTL`, pinned active-run byte ring buffer (`active_run_start_seq` + three-tier byte quotas), `idempotency_key`, `seamline_cursor` DB alignment, `event: cursor_reset`, and stateful Agent session control plane.

---

## Read When Relevant

Consult the corresponding specification **before** modifying that subsystem:

| Area Being Changed | Required Reference |
| :--- | :--- |
| Retention, runs, quotas, epochs, idempotency, `cursor_reset`, DB alignment, control plane, Redis | [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md) |
| `internal/broker/` publish path, run state machine, sealing, or `Storage` calls | [Commit & Failure Consistency](docs/ARCHITECTURE.md#4-commit--failure-consistency-model) |
| `Broker.Subscribe`, `runSubscriberPump`, `Last-Event-ID`, or epoch cursor handling | [Zero-Gap Catch-Up to Live Handoff](docs/ARCHITECTURE.md#5-catch-up-to-live-tailing-zero-gap-proof-sketch) |
| Locks (`b.mu`, `actor.mu`, `sub.errMu`), `pins`, the sweeper, or `ListSessions` | [Concurrency, Lock Hierarchy & Pinning](docs/ARCHITECTURE.md#6-concurrency-lock-hierarchy--actor-pinning-model) |
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

Every change to Seamline **must** preserve the invariants below. When a change intentionally evolves one of them (for example a Phase 2b/3 item from [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md)), update this file, [`CLAUDE.md`](CLAUDE.md) and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) in the same PR instead of treating the old rule as a blocker.

### 1. Dependency Discipline & No `cgo`
- Core runtime packages (`internal/model`, `internal/hotwindow`, `internal/broker`, `internal/server`) use **100% Go 1.23+ standard library** (`go 1.23` in `go.mod`). Optional external drivers (such as a Phase 3 `RedisStorage`) require explicit approval before adding dependencies.
- Never run `go mod init` or `go mod tidy` with an experimental host toolchain that rewrites the `go 1.23` directive in `go.mod`.
- Production binaries (`./cmd/seamline`, `./cmd/simworker`) must build cleanly with `CGO_ENABLED=0`. (Note: `go test -race` requires `CGO_ENABLED=1` on the test runner; never set global `CGO_ENABLED=0` across race-test steps.)

### 2. Strict Lock Hierarchy & Cross-Session Isolation
- Lock acquisition order is strictly one-way; `MemoryStorage.mu` is a leaf taken only inside `Storage` calls:
  ```text
  Broker.mu (b.mu)  ──►  sessionActor.mu (actor.mu)  ──►  subState.errMu (sub.errMu)
  ```
- **Never** acquire `b.mu` while holding `actor.mu` or `sub.errMu`.
- **Never block waiting on `actor.mu` while holding `b.mu`:** `Publish` performs `Storage` I/O under `actor.mu`, so a slow write on session A must not freeze other sessions.
  - **Hydration (`acquireActor`):** look up cached actors under `b.mu.RLock()` with atomic pinning; call `store.EnsureStream` outside `b.mu`; insert with double-checked locking under `b.mu.Lock()`.
  - **Eviction (`evictIdleActors`):** under `b.mu` probe actors only with `actor.mu.TryLock()`. An actor with an active run (`activeRunStartSeq != 0`) is never evicted: it owns the run's idle timer.
  - **Sealing (`sealIdleRuns`):** pin actors under `b.mu.RLock()`, release `b.mu`, then `TryLock` each actor. Release the sweeper's pin with `actor.pins.Add(-1)`, **never** `unpin()`: `unpin()` refreshes `lastActiveNano`, and a sweeper that touches actors keeps every session alive.
  - **Reads (`Inspect`, `ListSessions`, `CreateSession`):** read session state from `Storage`, which is authoritative; lock an actor only to count subscribers, after releasing `b.mu`.
- Prevent actor eviction during active operations only via atomic pins (`actor.pins.Add(1)` before releasing `b.mu`, paired with `defer actor.unpin()`). Do not introduce boolean eviction flags.

### 3. Commit & State Mutation Invariant (`store.Append` Is the Point of No Return)
- **One atomic commit:** `store.Append` writes the event **together with** the session state it leaves behind (`state`, `active_run_start_seq`, latest finished run) as one `hotwindow.Commit`. There is no separate metadata write (no `SaveMeta`).
- **Pre-commit immutability:** validate (`type`, JSON payload, `MaxEventBytes`), check `ctx.Err()`, look up the `idempotency_key`, and build the commit with `actor.nextCommit` in local variables. **Never** change `latestSeq`, `activeRunStartSeq`, `state`, `runActiveAt` or the `idempotent` index before `store.Append` succeeds. Why: a failed append must not consume a seq, move the run pin, or remember a key (the retry would be falsely deduplicated).
- **Point of no return:** once `store.Append` returns `nil`, immediately `applyCommitLocked`, record the `idempotency_key`, and `fanOutLocked`; never return an error from `Publish` after that. Idle sealing (`run_failed {"reason":"idle_timeout"}`) goes through the same path.
- **Runs are explicit:** only `run_started` while no run is active opens a run; `run_finished` / `run_failed` / `run_interrupted` while running end it; every other event (including terminal types outside a run) leaves the run as is.
- **Retention (`hotwindow`):** never evict the active run (`seq >= active_run_start_seq`); the latest finished run ignores `MaxHistoryBytes` and expires `HistoryTTL` after its end; other history expires by `HistoryTTL` or `MaxHistoryBytes`, always keeping the newest event and a contiguous window. Retention never resets a stream's epoch or seq counter; only `MetaTTL` drops a stream, and the sweeper `Touch`es every live actor's stream. `MaxActiveRunBytes` never rejects a terminal event.

### 4. Zero-Gap Subscription Handoff
- In `Broker.Subscribe`, register `actor.subscribers[sub]` and capture the snapshot `(epoch, latestSeq, activeRunStartSeq)` inside the **exact same `actor.mu` critical section**. Every event in `liveCh` is then newer than the snapshot; the `Seq <= snapSeq` filter in `drainLiveStream` is defense in depth.
- Cursors are classified before any frame is sent (`planReplay`): no cursor → fresh subscription (whole window, no reset); other epoch or a bare seq → `epoch_mismatch`; seq beyond the snapshot → `cursor_ahead`; next seq evicted (checked against the window from the same `ReadAfter`) → `cursor_expired`. Never silently reset a cursor.
- `cursor_reset` is sent at most once and only as the first frame; the retained window and live events follow on the same connection. Replay checks contiguity (`contiguity`); a hole ends the subscription with `ErrInsufficientState` (`stream_error insufficient_state`), never a silent skip.

### 5. Non-Blocking Fan-Out, SSE Wire Protocol & Playground XSS Safety
- **Non-blocking `Publish`:** `fanOutLocked` uses non-blocking channel sends. A stalled reader is evicted (`ErrSlowSubscriber`) without delaying producers or sibling subscribers.
- **Cursor placement:** every business event frame is `id: <epoch>:<seq>` + `data: <Event JSON>` with **no `event:` line** (EventSource `message`), so producer event types can never collide with control frame names. Control frames `event: cursor_reset` (flat `model.CursorReset` JSON) and `event: stream_error` carry **no** `id:`.
- **Terminal errors:** emit `event: stream_error` (never `event: error`, which collides with `EventSource`'s network `error` event) with `{"error":"slow_subscriber"}` or `{"error":"insufficient_state"}`. Native `EventSource` clients keep the instance open so the browser reconnects with `Last-Event-ID`; a new `EventSource` passes `?after_seq=<epoch>:<seq>`.
- **Single CORS & shutdown path:** set CORS headers once in `withCORS`. During shutdown (`cmd/seamline/main.go`), call `brk.Close()` **once** and **before** `httpServer.Shutdown()`.
- **DOM XSS prevention:** in `demo.html` (and any future web UI) render session IDs, payloads and URL parameters only via `document.createElement`, `textContent` and `replaceChildren`—**never** `innerHTML`.

### 6. Readability, File Modularity & Cognitive Complexity (`gocognit <= 15`)
- **Hard complexity ceiling (`gocognit <= 15`):** every non-test Go function across `cmd/` and `internal/` keeps cognitive complexity `<= 15` (enforced in CI via `.golangci.yml`).
- **Flat guard clauses & table-of-contents entrypoints:** prefer early returns. Entrypoints (`Publish`, `Subscribe`, `handleStream`, `acquireActor`) read top-to-bottom and delegate to single-purpose helpers (`nextCommit`, `replayCatchUp`, `replayEvents`, `drainLiveStream`, `streamSSELoop`).
- **Focused ~250-line files:** split packages by responsibility (~200–300 lines per file). Never contort logic just to game metrics.

### 7. Testing & Linter Discipline
- Changes to `internal/broker` or `internal/hotwindow` ship with deterministic regression tests, including fault injection (`flakyStore`, `gappyStore`) where storage behaviour matters. Keep coverage for: a failed append leaving no trace; the explicit run state machine; active run never evicted and latest finished run protected; terminal events exempt from the active quota; idle sealing; each `cursor_reset` reason; replay gaps ending with `insufficient_state`; epoch kept by retention and rotated by `MetaTTL`; live sessions not expiring; idempotency keys recorded only after commit; slow storage on one session not blocking others.
- Key tests must be able to fail: verify new tests by deliberately breaking the code they guard. Tests must not hang on a regression; bound every wait with a timeout. No round-trip serialization tests.
- Always close `*http.Response.Body` (`bodyclose`); use `errors.Is` / `errors.As` and wrap with `%w` (`errorlint`).
- Storage implementations deep-copy `json.RawMessage` payloads on write and read.

---

## Maintaining These Instructions

- Keep `AGENTS.md` concise and authoritative. Deep protocol proofs, failure matrices, and wire-protocol state machines belong in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
- Keep [`CLAUDE.md`](CLAUDE.md) synchronized as a lightweight pointer to `AGENTS.md` so every AI coding assistant follows identical engineering constraints.
