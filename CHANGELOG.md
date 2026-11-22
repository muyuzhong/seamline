# Changelog

All notable changes to **Seamline** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- **Core Dual-Speed Stream Engine (`internal/model`, `internal/storage`, `internal/broker`):**
  - Per-session `sessionActor` coalescing high-frequency streaming token deltas in RAM with zero storage I/O until `fin=true` single-record compaction.
  - Zero-gap catch-up to live SSE subscription handoff (`Last-Event-ID` cursor replay + atomic `inflight_snapshot` + deduplicated live `delta` pump).
  - Epoch-aware cursor validation (`<epoch>:<offset>`), monotonic sequence idempotency, late-retry deduplication after compaction, slow-subscriber eviction (`ErrSlowSubscriber`), and reference-counted idle actor eviction.
  - Thread-safe `MemoryStorage` with defensive `json.RawMessage` deep-copy isolation.
- **HTTP/SSE Transport & Live Playground (`internal/server`, `cmd/seamline`, `cmd/simworker`):**
  - Zero-dependency Go 1.23 `http.ServeMux` REST & W3C Server-Sent Events server (`GET /healthz`, `POST/GET /v1/sessions`, `GET /v1/sessions/{id}`, `POST /v1/sessions/{id}/events`, `GET /v1/sessions/{id}/stream`).
  - Standalone `simworker` CLI token stream simulator.
  - Embedded browser **Seamline Live Playground** (`--demo=true`) demonstrating mid-stream `Last-Event-ID` disconnect recovery and F5 page reloads.
- **Engineering Governance, Formal Architecture Spec & CI Tooling:**
  - Single-source AI & contributor invariants in `AGENTS.md`, `CLAUDE.md`, and `docs/ARCHITECTURE.md`.
  - Pinned `golangci-lint` v2 configuration (`.golangci.yml`, `.golangci-lint-version`), `Makefile` workflow automation, `.gitattributes`, Dependabot, and GitHub issue/PR templates.
  - Upgraded GitHub Actions CI (`gofmt`, `go vet`, `golangci-lint`, `go test -race -count=2`, and `CGO_ENABLED=0` binary build checks).

### Fixed
- Preserved pre-commit `sessionActor` immutability when `store.Append` or caller context fails so `fin=true` retries commit cleanly without wedging sessions in `409 Conflict`.
- Treated `store.Append` as the commit Point of No Return: memory state updates and live subscriber fan-out now always execute immediately upon `Append` success while `SaveMeta` runs best-effort via `context.WithoutCancel`.
- Decoupled global `Broker.mu` from per-session `actor.mu` in `evictIdleActors` (`TryLock`) and `ListSessions` so slow storage writes on one session never stall other sessions.
- Replaced unescaped `innerHTML` in `internal/server/demo.html` with safe DOM APIs (`createElement`, `textContent`, `replaceChildren`) and renamed terminal SSE error frames from `event: error` to `event: stream_error` to avoid colliding with native `EventSource` network error events.
