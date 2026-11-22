# Contributing to Seamline / 参与贡献指南

Thank you for your interest in contributing to **Seamline**! We welcome bug reports, architectural improvements, storage adapters, and protocol refinements.  
感谢关注并参与 **Seamline** 开源项目！在开始编码前，请先阅读本指南以及我们的[行为准则 (`CODE_OF_CONDUCT.md`)](CODE_OF_CONDUCT.md)。

---

## 1. Before You Code / 开始编码前

For non-trivial behavioral changes, new HTTP/SSE wire fields, or new `storage.Storage` backends, please **open an issue first** using our [GitHub Issue Templates](https://github.com/muyuzhong/seamline/issues/new/choose) so maintainers can align on design invariants before implementation.

Please review:
- [`AGENTS.md`](AGENTS.md) — Hard engineering constraints, lock hierarchy, storage consistency rules, and AI coding assistant guidelines.
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Dual-Speed Engine dataflow, failure consistency matrix, and zero-gap handoff proof sketch.

> **Security Vulnerabilities / 安全漏洞报告：** Do **not** open a public issue for security bugs. Follow [`SECURITY.md`](SECURITY.md) to submit a private GitHub Security Advisory.

---

## 2. Development Setup / 本地开发环境

### Prerequisites
- **Go 1.23+** (Phase 1 uses 100% Go standard library; `CGO_ENABLED=0` for binaries, `CGO_ENABLED=1` with a C compiler on host only for `go test -race`).
- **`golangci-lint`** (pinned version in [`.golangci-lint-version`](.golangci-lint-version); install via `make install-lint`).

### Common `Makefile` Workflows

```bash
make help        # List all available workflow targets
make fmt         # Format Go files with gofmt
make check       # Run mandatory pre-PR gate: fmt-check + vet + test-race (-count=2)
make lint        # Run golangci-lint with .golangci.yml
make build       # Build static ./bin/seamline and ./bin/simworker binaries (CGO_ENABLED=0)
make run-demo    # Launch Seamline server + embedded F5 Live Playground at http://localhost:8080
```

---

## 3. Pull Request & Stacked PR Workflow / PR 规范

1. **Focused Changes (支持 Stacked PRs):** Keep each pull request scoped to a single concern (e.g., core broker/storage engine -> HTTP/SSE transport -> governance/tooling). When submitting stacked PRs, clearly note the base branch in the PR description and merge bottom-up without rewriting reviewed history unnecessarily.
2. **Preserve Core Invariants (严守核心架构不变量):**
   - **Zero runtime dependencies** in Phase 1 (`go.mod` must remain pure Go 1.23 stdlib).
   - **Strict Lock Hierarchy:** `b.mu -> actor.mu -> sub.errMu`. Never block on `actor.mu.Lock()` while holding `b.mu` (`TryLock` in sweepers; copy actor pointers and release `b.mu` before inspecting actors in `ListSessions`).
   - **Commit Boundary (`store.Append`):** Never mutate `sessionActor` memory state (`inFlight`, `mergedBuf`, `lastSeq`, `lastCompacted*`) before `store.Append` succeeds. Once `store.Append` succeeds, update memory state and `fanOutLocked` immediately, running `SaveMeta` as best-effort with `context.WithoutCancel(ctx)`.
   - **SSE Wire Safety:** Emit `id: <epoch>:<offset>` **only** on finalized `event: record` frames; use `event: stream_error` (never `event: error`) for terminal stream errors; never use unescaped `innerHTML` in `internal/server/demo.html`.
3. **Fault-Injection & Race Testing (故障注入与并发竞态测试):**
   - Any change to `internal/broker` or `internal/storage` must include deterministic fault-injection tests (`flakyStore` / `slowStore`) exercising `Append` failures, post-`Append` `SaveMeta` failures, and cross-session concurrency under `go test -race -count=2 ./...`.
4. **Conventional Commit Messages:** Use clear prefixes such as `feat(broker): ...`, `fix(server): ...`, `docs(arch): ...`, `chore(ci): ...`.

---

## 4. License / 开源协议

By contributing to Seamline, you agree that your contributions will be licensed under the [MIT License](LICENSE).
