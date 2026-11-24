# Seamline — Claude & AI Assistant Instructions

Read **[`AGENTS.md`](AGENTS.md)** first — it is the canonical repository instruction file shared across Claude Code, Codex, Cursor, Gemini, and human contributors.

## Quick Commands

| Task | Command |
| :--- | :--- |
| Full pre-commit / pre-PR gate (`gofmt` + `go vet` + `-race`) | `make check` |
| Run `golangci-lint` (matches CI `.golangci.yml`) | `make lint` |
| Format Go files | `make fmt` |
| Run race-enabled test suite (`-count=2`) | `make test-race` |
| Build static CLI binaries (`CGO_ENABLED=0`) | `make build` |
| Launch Seamline server + F5 Live Playground on `:8080` | `make run-demo` |

## Non-Negotiable Rules Summary

> **Evolving rules:** When a change intentionally evolves one of these rules (for example a Phase 2b/3 item from [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md)), update `AGENTS.md`, `CLAUDE.md`, and `docs/ARCHITECTURE.md` in the same PR.

1. **[Phase 1 & 2 Core] Zero external Go dependencies** (`go 1.23` stdlib only; `CGO_ENABLED=0` binaries; external drivers like Phase 3 `RedisStorage` require explicit approval).
2. **[Permanent] Lock hierarchy & complexity:** `b.mu -> actor.mu -> sub.errMu` (never block on `actor.mu` under `b.mu`; use `TryLock` in sweepers and release sweeper pins with `pins.Add(-1)`, never `unpin()`); keep every non-test Go function at **`gocognit <= 15`**.
3. **[Permanent] Commit consistency:** `store.Append` writes the event and the session state (`state`, `active_run_start_seq`) atomically; there is no separate `SaveMeta`. Never mutate `sessionActor` state (`latestSeq`, `activeRunStartSeq`, `state`, idempotency keys) before it succeeds; once it succeeds, apply the commit and `fanOutLocked` immediately. Only explicit `run_started` opens a run; retention never evicts the active run or resets epoch/seq.
4. **[Permanent] Zero-gap SSE handoff:** Register `liveCh` and snapshot `(epoch, latestSeq, activeRunStartSeq)` in one `actor.mu` critical section; every business event frame carries `id: <epoch>:<seq>` and no `event:` line; classify the cursor (fresh / `epoch_mismatch` / `cursor_ahead` / `cursor_expired`) and send `event: cursor_reset` only as the first frame (no `id:`), never a silent reset; a replay gap ends the stream with `stream_error` `insufficient_state`; terminal stream errors are `event: stream_error` (never `event: error`).
5. **[Permanent] Playground safety:** Never write untrusted strings (such as `session_id` from URL params) via `innerHTML`; use `document.createElement` + `textContent` + `replaceChildren`.

For subsystem contracts read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); for design decisions and Phase 2b/3 evolution read [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md).
