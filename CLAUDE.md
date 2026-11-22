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

1. **Zero external Go dependencies** (`go 1.23` stdlib only; `CGO_ENABLED=0` binaries).
2. **Lock hierarchy:** `b.mu -> actor.mu -> sub.errMu`. Never acquire `b.mu` under `actor.mu`, and never block on `actor.mu.Lock()` while holding `b.mu` (use `TryLock` in sweepers; copy actor pointers and unlock `b.mu` first in `ListSessions`).
3. **Storage consistency:** Never mutate `sessionActor` memory state (`inFlight`, `mergedBuf`, `lastSeq`, `lastCompacted*`) before `store.Append` succeeds. Once `store.Append` succeeds, treat it as the Point of No Return: update memory state and `fanOutLocked` immediately, and run `SaveMeta` as best-effort with `context.WithoutCancel(ctx)`.
4. **Zero-gap SSE handoff:** Register `liveCh` and snapshot `(snapOffset, inFlightSnap)` in one `actor.mu` critical section; emit `id: <epoch>:<offset>` **only** on `event: record`; emit terminal stream errors as `event: stream_error` (never `event: error`).
5. **Playground safety:** Never write untrusted strings (such as `session_id` from URL params) via `innerHTML`; use `document.createElement` + `textContent` + `replaceChildren`.

For deep subsystem contracts, read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) and [`CONTRIBUTING.md`](CONTRIBUTING.md).
