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

For subsystem contracts read [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md); for design decisions and Phase 2b/3 evolution read [`docs/DESIGN_V2.md`](docs/DESIGN_V2.md).
