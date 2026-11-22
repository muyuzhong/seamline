## Summary / 变更摘要

<!-- Describe what this PR changes and why. Link any related issue or note the stacked PR base branch if applicable. -->

- **Related Issue / 关联议题:** 
- **Stacked Base Branch (if applicable):** `main`

---

## Key Changes / 核心改动

- 

---

## Engineering & Concurrency Checklist / 架构与并发安全自检清单

Please verify all applicable items against [`AGENTS.md`](../AGENTS.md) and [`docs/ARCHITECTURE.md`](../docs/ARCHITECTURE.md):

- [ ] **Verification Gate:** `make check` (`gofmt`, `go vet`, `go test -race -count=2 ./...`) and `make lint` (`golangci-lint`) pass cleanly.
- [ ] **Zero-Dependency Core:** `go.mod` retains pure Go 1.23 stdlib with zero unapproved third-party dependencies, and `make build` (`CGO_ENABLED=0`) succeeds.
- [ ] **Lock Hierarchy (`b.mu -> actor.mu -> sub.errMu`):** `b.mu` is never acquired while holding `actor.mu`, and never blocks waiting on `actor.mu.Lock()` (uses `TryLock` in sweepers; copies actor pointers before unlocking `b.mu` in `ListSessions`).
- [ ] **Storage & Retry Consistency:** No `sessionActor` memory state (`inFlight`, `mergedBuf`, `lastSeq`, `lastCompacted*`) is mutated prior to `store.Append` succeeding; once `store.Append` succeeds, state advances and `fanOutLocked` runs even if best-effort `SaveMeta` fails.
- [ ] **Fault-Injection Coverage:** Added/updated deterministic fault-injection (`flakyStore` / `slowStore`) and `-race` tests (required for any `internal/broker` or `internal/storage` change).
- [ ] **SSE Wire & UI Safety:** SSE `id:` is emitted only on finalized `event: record` frames, terminal stream errors use `event: stream_error`, all `*http.Response.Body` handles are closed, and `demo.html` uses safe DOM APIs (`textContent` / `replaceChildren`, never unescaped `innerHTML`).

---

## Verification Output / 测试验证记录

```text
$ make check
```
