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

> **Phase Scope Note:** Rules below Separate **Permanent Invariants** (all phases) from **Phase 1 Baselines** that evolve in Phase 2 per [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md). When implementing Phase 2/3 tasks, update `AGENTS.md`, `CLAUDE.md`, and `docs/ARCHITECTURE.md` in the same PR.

1. **[Phase 1 & 2 Core] Zero external Go dependencies** (`go 1.23` stdlib only; `CGO_ENABLED=0` binaries; external drivers like Phase 3 `RedisStorage` require explicit approval).
2. **[Permanent] Lock hierarchy & complexity:** `b.mu -> actor.mu -> sub.errMu` (never block on `actor.mu` under `b.mu`; use `TryLock` in sweepers); keep every non-test Go function at **`gocognit <= 15`**.
3. **[Permanent] Storage consistency:** Never mutate `sessionActor` memory state before `store.Append` succeeds. Once `store.Append` succeeds, treat it as the Point of No Return: update memory state and `fanOutLocked` immediately, and run `SaveMeta` as best-effort with `context.WithoutCancel(ctx)`.
4. **[Permanent + Phase 2 Evolution] Zero-gap SSE handoff:** Always register `liveCh` and capture the catch-up watermark inside one `actor.mu` critical section, and emit terminal stream errors as `event: stream_error` (never `event: error`). *(Phase 1 on `main` uses dual-speed `Record` + `InFlight` with `id:` only on `record`; **Phase 2 supersedes this per [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md)** with a payload-opaque **Pure Event Cursor `<epoch>:<seq>`** where every business event carries `id: <epoch>:<seq>`, uncommitted active runs are pinned via `active_run_start_seq`, and control frames `cursor_reset` / `stream_error` omit `id:`.)*
5. **[Permanent] Playground safety:** Never write untrusted strings (such as `session_id` from URL params) via `innerHTML`; use `document.createElement` + `textContent` + `replaceChildren`.

For Phase 1 subsystem contracts read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); for Phase 2/3 evolution read [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md).
