# LLM Gateway — Portfolio Project Plan

## Context

Greenfield side project for an SDE2 backend engineer. The goal is a **portfolio-grade LLM gateway** — a unified HTTP API that sits in front of multiple LLM providers (OpenAI, Anthropic, Gemini) and adds the cross-cutting concerns every real LLM application needs: routing, failover, rate limiting, caching, cost tracking, and observability.

Built in **Go**, deployed to **Fly.io** (or Render). Working directory is empty; nothing to migrate.

The plan is structured as **incremental phases**, each independently shippable, each demoable, each adds a resume bullet. Phase 0 lands a working MVP fast; phases after that add depth.

---

## Tech Stack

| Concern | Choice | Rationale |
|---|---|---|
| Language | Go 1.22+ | I/O-bound proxy, native streaming, single binary |
| HTTP | `net/http` + `chi` router | Stdlib-first; `chi` for clean middleware composition |
| Config | `viper` or plain YAML + envconfig | YAML for provider/route config, env for secrets |
| Cache / RL store | Redis (`go-redis/v9`) | Token-bucket RL and response cache in one dep |
| Vector store (Phase 2b) | Redis Stack (HNSW) or Qdrant | Redis Stack keeps deps minimal |
| Metrics | Prometheus client | `/metrics` endpoint |
| Tracing | OpenTelemetry SDK | OTLP → Grafana Tempo / Honeycomb free tier |
| Logging | `slog` (stdlib) | Structured JSON, no extra dep |
| Tests | `testify` + `httptest` | Standard |
| Container | Multi-stage Dockerfile, distroless | Tiny image for Fly.io |
| Deploy | Fly.io (`fly.toml`) | Free tier, global edge, easy Redis via Upstash |

---

## Architecture (target end-state)

```
            ┌─────────────────────────────────────────────┐
client ───► │  HTTP server (chi)                          │
            │  ├─ auth middleware (API key → tenant)      │
            │  ├─ rate limit middleware (Redis bucket)    │
            │  ├─ cache middleware (exact → semantic)     │
            │  ├─ request normalizer  ─┐                  │
            │  └─ router  ─────────────┼─► provider       │
            │     ├─ OpenAI adapter    │   selection      │
            │     ├─ Anthropic adapter │   + failover     │
            │     └─ Gemini adapter    │                  │
            │  ├─ cost accountant (post-response)         │
            │  ├─ metrics (Prometheus)                    │
            │  └─ traces (OTel)                           │
            └─────────────────────────────────────────────┘
                            │
                ┌───────────┴───────────┐
            Redis (RL + cache)    Postgres (usage ledger, Phase 3)
```

**Unified request schema**: model the API after OpenAI's `/v1/chat/completions` since it's the de-facto standard; translate to Anthropic/Gemini shapes inside adapters. Streaming uses SSE passthrough.

---

## Phased Roadmap

### Phase 0 — MVP (weekend 1)
**Goal:** unified `/v1/chat/completions` endpoint, 2 providers, deployed to Fly.io.

- `cmd/gateway/main.go` — entrypoint, signal handling, graceful shutdown
- `internal/server` — chi router, middleware chain skeleton
- `internal/provider` — `Provider` interface: `Complete(ctx, Request) (Response, error)` and `Stream(ctx, Request) (<-chan Chunk, error)`
- `internal/provider/openai`, `internal/provider/anthropic` — two adapters
- `internal/router` — pick provider from `model` field (`openai/gpt-4o`, `anthropic/claude-sonnet-4-6`)
- API-key auth middleware (single static key from env, multi-tenant comes in Phase 3)
- Streaming SSE passthrough
- Dockerfile + `fly.toml`, deploy to Fly.io
- README with curl examples
- **Demo bullet:** "Unified OpenAI-compatible API fronting Anthropic + OpenAI, deployed at <url>"

### Phase 1 — Reliability (weekend 2)
**Goal:** failover, retries, timeouts — make it production-shaped.

- Per-provider timeouts + context cancellation
- Retry policy with exponential backoff + jitter (`cenkalti/backoff` or hand-rolled)
- **Failover chain**: per-route config (`primary: openai, fallback: [anthropic]`) — on 5xx/timeouts
- Circuit breaker per provider (`sony/gobreaker`)
- Structured slog with request IDs propagated via context
- Health endpoint `/healthz`, readiness `/ready` (Redis ping)
- **Demo bullet:** "Configurable failover chains with circuit breakers — kill OpenAI, traffic shifts to Anthropic in <1s"

### Phase 2a — Rate limiting (weekend 3, first half)
**Goal:** Redis-backed token-bucket per API key.

- `internal/ratelimit` — token-bucket via Redis Lua script (atomic refill+consume)
- Limits config: requests/min AND tokens/min (LLM-specific — bill by tokens, not just reqs)
- 429 responses with `Retry-After` + `X-RateLimit-*` headers
- Per-route + per-key + global tiers

### Phase 2b — Caching (weekend 3, second half)
**Goal:** exact-match cache, then semantic.

- **Exact cache**: hash `(model, messages, temp, top_p, …)` → Redis GET/SETEX
- **Semantic cache** (stretch): embed user message, ANN search in Redis Stack HNSW, cosine threshold
- Cache-control headers: `X-Cache: HIT|MISS|SEMANTIC-HIT`
- **Demo bullet:** "Semantic cache: 40% hit rate on a benchmark corpus, ~80ms vs 2s cold path"

### Phase 3 — Multi-tenancy + cost tracking (weekend 4)
**Goal:** turn it into a SaaS-shaped service.

- Postgres for: tenants, API keys, usage ledger (one row per request: tenant, model, prompt+completion tokens, cost USD, latency)
- Migrations via `goose` or `golang-migrate`
- `/admin/keys` CRUD (basic auth or magic-link)
- `/admin/usage` — per-tenant rollups by day/model
- Cost computation: load pricing table per model (cents/1k tokens, in/out separately)
- **Demo bullet:** "Per-tenant cost analytics with idle-time materialized views"

### Phase 4 — Observability (weekend 5)
**Goal:** real dashboards.

- Prometheus metrics: RED (rate/errors/duration) per provider, per route, per tenant; cache hit ratio; token throughput
- OpenTelemetry traces: HTTP span → provider HTTP span → Redis span; export OTLP → Grafana Cloud free tier or Honeycomb
- Grafana dashboard JSON checked into `deploy/grafana/`
- k6 or `vegeta` load-test scripts in `loadtest/`, results in README (p50/p95/p99 charts)
- **Demo bullet:** "End-to-end traces from request → provider → cache, 5k RPS sustained in load test"

### Phase 5 (optional stretch)
- Embeddings + moderation endpoints
- Prompt template management (`/v1/prompts/:id`)
- Budget alerts (Slack webhook when tenant exceeds $X/day)
- WASM-based request transformers
- gRPC streaming variant

---

## Proposed Repo Structure

```
llm-gateway/
├── cmd/gateway/main.go
├── internal/
│   ├── server/          # chi router, middleware chain
│   ├── middleware/      # auth, ratelimit, cache, logging, otel
│   ├── provider/
│   │   ├── provider.go  # interface + shared types
│   │   ├── openai/
│   │   ├── anthropic/
│   │   └── gemini/
│   ├── router/          # provider selection + failover chain
│   ├── ratelimit/       # token bucket (Phase 2a)
│   ├── cache/           # exact + semantic (Phase 2b)
│   ├── tenant/          # keys, quotas (Phase 3)
│   ├── usage/           # ledger writer + rollups (Phase 3)
│   ├── cost/            # pricing tables
│   └── obs/             # metrics + tracing setup
├── config/gateway.yaml  # routes, models, failover chains
├── deploy/
│   ├── Dockerfile
│   ├── fly.toml
│   ├── docker-compose.yaml   # local: gateway + redis + postgres
│   └── grafana/dashboards/
├── loadtest/
├── migrations/
└── README.md
```

---

## Phase 0 Verification (end-to-end)

After Phase 0:
1. `docker compose up` — gateway + Redis come up locally
2. `curl localhost:8080/v1/chat/completions -H "Authorization: Bearer $KEY" -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'` → OpenAI response
3. Same curl with `"model":"anthropic/claude-haiku-4-5"` → Anthropic response, identical envelope
4. Add `"stream":true` → SSE chunks stream through
5. `fly deploy` → live URL; rerun step 2 against it
6. Unit tests: `go test ./...` covers provider adapters with `httptest` fakes

Subsequent phases each add a verification step (kill-provider failover drill, k6 load test, etc.) — defined when their phase starts.

---

## Open Questions (defer to implementation time, not blockers now)

- API envelope: strict OpenAI compat vs. a slightly cleaner own schema? — leaning OpenAI-compat for client-SDK reuse.
- Semantic cache embedding model: paid (OpenAI `text-embedding-3-small`) vs local (`fastembed` via ONNX)? Defer to Phase 2b.
- Admin UI: skip (curl + README) vs. minimal HTMX dashboard? Decide before Phase 3.
