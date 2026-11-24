# Seamline — Repository Instructions (`AGENTS.md`)

**Seamline** is a zero-dependency, resumable AI stream broker written in pure Go (`go 1.23`, standard library only). Every published event is payload-opaque and gets the next per-session cursor `<epoch>:<seq>`; events live in a short TTL hot window where the active run (`run_started` … terminal event) is never evicted, and every SSE event frame carries `id: <epoch>:<seq>`, enabling exact `Last-Event-ID` recovery, F5 reloads in the middle of a run, and an explicit `cursor_reset` whenever a cursor cannot be continued.

This file is the **single source of truth** for AI coding agents (Claude Code, Codex, Cursor, Gemini, Antigravity) and human contributors. Keep rules actionable, verifiable, and grounded in Seamline's invariants.

---

## Project Map

- `cmd/seamline/`: HTTP/SSE server entrypoint, CLI flags and graceful shutdown.
- `cmd/simworker/`: CLI worker that publishes one simulated run with idempotency keys and retries.
- `internal/model/`: Domain and wire types shared by every layer.
- `internal/hotwindow/`: The hot-window `Storage` interface (shaped for Redis) and the in-memory `MemoryStorage`: seq allocation, the run state machine, retention and byte quotas.
- `internal/broker/`: Per-session actors: publish and fan-out, subscriptions with cursor classification and catch-up replay, idle run sealing, idempotency keys and the sweeper.
- `internal/server/`: REST + W3C SSE transport, error codes and the embedded F5 Live Playground.
- `docs/ARCHITECTURE.md`: Specification of the implemented data plane, down to files and internal functions: event model, hot window, commit consistency, zero-gap handoff proof sketch, lock hierarchy and SSE wire protocol.
- `docs/DESIGN_V2.md`: **Authoritative Phase 2 / Phase 3 blueprint** — payload-opaque **pure monotonic event cursor (`<epoch>:<seq>`)**, per-session `epoch` + `MetaTTL`, pinned active-run byte ring buffer (`active_run_start_seq` + three-tier byte quotas), `idempotency_key`, `seamline_cursor` DB alignment, `event: cursor_reset`, and stateful Agent session control plane.

---

## Read When Relevant

Consult the corresponding specification **before** modifying that subsystem:

| Area Being Changed | Required Reference |
| :--- | :--- |
| Retention, runs, quotas, epochs, idempotency, `cursor_reset`, DB alignment, control plane, Redis | [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md) |
| The publish path, run state, idle sealing, or `Storage` | [Commit & Failure Consistency](docs/ARCHITECTURE.md#4-commit--failure-consistency-model) |
| Subscriptions, `Last-Event-ID`, or cursor handling | [Zero-Gap Catch-Up to Live Handoff](docs/ARCHITECTURE.md#5-catch-up-to-live-tailing-zero-gap-proof-sketch) |
| Locks, actor lifetime, or the sweeper | [Concurrency, Lock Hierarchy & Pinning](docs/ARCHITECTURE.md#6-concurrency-lock-hierarchy--actor-pinning-model) |
| `internal/server/`, SSE frames, CORS, or the Playground UI | [SSE Wire Protocol Specification](docs/ARCHITECTURE.md#7-sse-wire-protocol-specification) |
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

Every change to Seamline **must** preserve the invariants below. When a change intentionally evolves one of them (for example a Phase 2b/3 item from [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md)), update this file and [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) in the same PR instead of treating the old rule as a blocker.

### 1. Dependency Discipline & No `cgo`
- Core runtime packages (`internal/model`, `internal/hotwindow`, `internal/broker`, `internal/server`) use **100% Go 1.23+ standard library** (`go 1.23` in `go.mod`). Optional external drivers (such as a Phase 3 `RedisStorage`) require explicit approval before adding dependencies.
- Never run `go mod init` or `go mod tidy` with an experimental host toolchain that rewrites the `go 1.23` directive in `go.mod`.
- Production binaries (`./cmd/seamline`, `./cmd/simworker`) must build cleanly with `CGO_ENABLED=0`. (Note: `go test -race` requires `CGO_ENABLED=1` on the test runner; never set global `CGO_ENABLED=0` across race-test steps.)

### 2. Strict Lock Hierarchy & Cross-Session Isolation
- Lock order is strictly one-way; the storage lock is a leaf taken only inside `Storage` calls:
  ```text
  broker lock  ──►  session actor lock  ──►  subscription error lock
  ```
- **Never** take the broker lock while holding a session lock, and **never** block on a session lock while holding the broker lock: storage I/O runs under the session lock, so a slow write on one session must not freeze the others. Background maintenance only try-locks sessions.
- Storage is authoritative for session state: reads (`Inspect`, `ListSessions`, `CreateSession`) go to storage, not to in-memory actors.
- An API call pins its session actor so idle eviction cannot drop it; an actor with an active run or with subscribers is never evicted. Background maintenance must never count as session activity.

### 3. Commit & State Mutation Invariant (`Storage.Append` Is the Point of No Return)
- **One atomic commit:** `Storage.Append` assigns the next seq, applies the run state machine and the active-run quota, and writes the event **together with** the session state it leaves behind (`state`, `active_run_start_seq`, latest finished run). There is no separate metadata write.
- **Nothing changes before the commit:** validation and the idempotency lookup change nothing; the broker's view of the session (latest seq, run pin, run idle timer, idempotency keys) changes only after `Append` succeeds. Why: a failed append must not consume a seq, move the run pin, or remember a key (the retry would be falsely deduplicated).
- **Point of no return:** once `Append` succeeds, mirror its result, fan the event out and record the `idempotency_key`; never return an error from `Publish` after that. Idle sealing (`run_failed {"reason":"idle_timeout"}`) goes through the same path.
- **Runs are explicit:** only `run_started` while no run is active opens a run; `run_finished` / `run_failed` / `run_interrupted` while running end it; every other event (including terminal types outside a run) leaves the run as is.
- **Retention (`hotwindow`):** never evict the active run (`seq >= active_run_start_seq`); the latest finished run ignores `MaxHistoryBytes` and expires `HistoryTTL` after its end; other history expires by `HistoryTTL`, or by `MaxHistoryBytes` while always keeping the newest event; the window stays contiguous. Retention never resets a stream's epoch or seq counter; only `MetaTTL` drops a stream, and the broker keeps the streams of its live sessions alive. `MaxActiveRunBytes` never rejects a terminal event.

### 4. Zero-Gap Subscription Handoff
- Register the subscriber and capture the snapshot `(epoch, latest seq, active_run_start_seq)` in **one** session critical section, so every live event is newer than the snapshot.
- Classify the cursor before any frame is sent: no cursor → fresh subscription (whole window, no reset); other epoch or a bare seq → `epoch_mismatch`; seq beyond the snapshot → `cursor_ahead`; next seq already evicted → `cursor_expired`. Never silently reset a cursor.
- `cursor_reset` is sent at most once and only as the first frame; the retained window and live events follow on the same connection. A hole in the replay ends the subscription with `stream_error` `insufficient_state`, never a silent skip.

### 5. Non-Blocking Fan-Out, SSE Wire Protocol & Playground XSS Safety
- **Non-blocking publish:** fan-out never blocks; a stalled reader is evicted (`slow_subscriber`) without delaying producers or other subscribers.
- **Cursor placement:** every business event frame is `id: <epoch>:<seq>` + `data: <Event JSON>` with **no `event:` line** (EventSource `message`), so producer event types can never collide with control frame names. Control frames `event: cursor_reset` (flat `model.CursorReset` JSON) and `event: stream_error` carry **no** `id:`.
- **Terminal errors:** emit `event: stream_error` (never `event: error`, which collides with `EventSource`'s network `error` event) with `{"error":"slow_subscriber"}` or `{"error":"insufficient_state"}`. Native `EventSource` clients keep the instance open so the browser reconnects with `Last-Event-ID`; a new `EventSource` passes `?after_seq=<epoch>:<seq>`.
- **Single CORS & shutdown path:** set CORS headers in one place. On shutdown close the broker **once** and **before** the HTTP server.
- **DOM XSS prevention:** in `demo.html` (and any future web UI) render session IDs, payloads and URL parameters only via `document.createElement`, `textContent` and `replaceChildren`—**never** `innerHTML`.

### 6. Readability & Cognitive Complexity (`gocognit <= 15`)
- **Hard complexity ceiling (`gocognit <= 15`):** every non-test Go function across `cmd/` and `internal/` keeps cognitive complexity `<= 15` (enforced in CI via `.golangci.yml`).
- **Flat guard clauses:** prefer early returns; entrypoints read top-to-bottom and delegate to single-purpose helpers.
- **Focused files:** split packages by responsibility (~200–300 lines per file). Never contort logic just to game metrics.

### 7. Testing
- Test externally visible contracts and core invariants, not implementation details; do not duplicate coverage another package already has, and write no serialization round-trip tests. Bound every wait with a timeout. Use fault injection only when a change touches the storage commit path.
- `Storage` isolates `Payload` at its write entry (`Append` keeps a private copy); stored events are immutable and readers share them.

---

## Maintaining These Instructions

- Keep `AGENTS.md` to the package map and the invariants. Files, internal functions, proofs, failure matrices and wire-protocol details belong in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and code comments.
- Keep [`CLAUDE.md`](CLAUDE.md) a lightweight pointer to `AGENTS.md`; do not copy rules into it.
