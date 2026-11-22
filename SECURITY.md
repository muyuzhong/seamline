# Security Policy / 安全策略

## Supported Versions / 受支持版本

| Version | Supported | Notes |
| :--- | :---: | :--- |
| `0.1.x` / `main` | ✅ | Active development & security fixes |

---

## Reporting a Vulnerability / 报告安全漏洞

If you discover a security vulnerability in **Seamline**, please report it **privately** using GitHub's **"Report a vulnerability"** button on the repository's [Security Advisories](https://github.com/muyuzhong/seamline/security/advisories/new) page.

Please **do not** open a public GitHub Issue or Pull Request for suspected security vulnerabilities.

### What to Include
- Description of the vulnerability and affected component (`internal/broker`, `internal/server`, `demo.html`, etc.).
- Minimal reproduction steps (`curl` commands, SSE frame payload, or Go test snippet).
- Potential impact (e.g., denial of service, cross-session data leak, DOM XSS in Playground).

We aim to acknowledge reports within **72 hours**, validate impact, and coordinate a patched release and advisory disclosure.

---

## Security Architecture & Deployment Guidance / 安全设计说明

Seamline incorporates several defense-in-depth measures by default:

1. **Zero Third-Party Runtime Supply Chain Surface:** Phase 1 compiles exclusively against the Go 1.23+ standard library (`CGO_ENABLED=0`), eliminating transitive third-party runtime dependency vulnerabilities.
2. **Bounded HTTP Request Bodies:** JSON API endpoints wrap request bodies in `http.MaxBytesReader` (`1 MiB` for control/session endpoints, `4 MiB` for event publishing) to prevent unbounded memory allocation.
3. **Slow-Consumer Isolation:** `Broker.Publish` fans out non-blockingly (`select` with `default`); a stalled or maliciously slow SSE reader is evicted (`ErrSlowSubscriber` -> `event: stream_error`) without exhausting memory or stalling producers.
4. **Playground DOM Hardening & Production Flag:**
   - The embedded Playground (`internal/server/demo.html`) renders dynamic session IDs and event content using safe DOM APIs (`createElement`, `textContent`, `replaceChildren`) rather than unescaped `innerHTML`.
   - For internet-facing production deployments, pass `--demo=false` to disable `/` and `/v1/demo/simulate`, and place Seamline behind TLS termination and authentication/authorization middleware appropriate to your application boundary.
