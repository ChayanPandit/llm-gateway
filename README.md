# llm-gateway

A unified, OpenAI-compatible HTTP gateway in front of multiple LLM providers.
One canonical request/response shape; the gateway translates to and from each
upstream. Built in Go, deployed to Fly.io.

## What problem this solves

If your app talks to more than one LLM provider — for cost, redundancy, or
because different models are better at different jobs — you end up with N
client SDKs, N request shapes, N response shapes, and N streaming formats
sprinkled through your code. Every place that calls an LLM gets a switch
statement.

This gateway collapses that. Clients always send an OpenAI-shaped request
to `POST /v1/chat/completions`. Only one field changes between providers:

```
"model": "openai/gpt-4o-mini"          → routed to OpenAI
"model": "anthropic/claude-haiku-4-5"  → routed to Anthropic
```

The response envelope is identical regardless of who actually served the
request. Streaming chunks are normalized to the same SSE shape. Adding a new
provider means writing one adapter; no client code changes.

## Features

- Single OpenAI-compatible endpoint: `POST /v1/chat/completions`
- Two upstream providers shipped: OpenAI, Anthropic
- Prefix-based model routing (`openai/...`, `anthropic/...`)
- Streaming passthrough as Server-Sent Events
- **Reliability:** retries with exponential backoff + jitter, per-provider
  circuit breakers, configurable failover chain, per-request timeouts
- **Caching:** Redis-backed exact-match response cache with TTL, namespacing,
  `X-Cache` headers, per-request `Cache-Control: no-store` opt-out
- **Health:** `/healthz` for liveness, `/ready` reflecting breaker state
- Bearer-token authentication on a single static gateway key
- Structured JSON logging via `slog`, request IDs propagated through context
- Graceful shutdown on SIGINT/SIGTERM with a 30s drain window
- Distroless container (~11 MB final image), non-root runtime
- Deploy-ready Fly.io config

## How it works

```
[client]
   │ POST /v1/chat/completions
   │ Authorization: Bearer <GATEWAY_API_KEY>
   │ {"model":"anthropic/claude-haiku-4-5", "messages":[...]}
   ▼
[chi router] ────────────────────────────────────────── internal/server/server.go
   ├─ middleware: request-id  (random ID, threaded via context)
   ├─ middleware: recoverer   (panic → 500)
   ├─ middleware: logger      (method/path/status/duration JSON line)
   └─ middleware: auth        (Bearer token check)
   ▼
[handler: chatCompletionsHandler]
   ├─ decode JSON → provider.Request
   ├─ router.Resolve("anthropic/claude-haiku-4-5")  ── internal/router/router.go
   │     → (anthropicAdapter, "claude-haiku-4-5")
   ├─ req.Model = "claude-haiku-4-5"
   └─ branch on req.Stream:
        ├─ false → adapter.Complete(ctx, req) → JSON
        └─ true  → adapter.Stream(ctx, req)   → SSE pump
   ▼
[adapter] ───────────────────────────────────────────── internal/provider/{openai,anthropic}
   ├─ translate request to upstream shape
   ├─ call upstream API
   └─ translate response back to OpenAI envelope
   ▼
[client] ← identical envelope regardless of upstream
```

**Why the OpenAI envelope is canonical.** OpenAI's chat-completions shape is
the de-facto standard — most LLM SDKs and client libraries speak it natively.
Picking it as the gateway's external contract means the OpenAI adapter is
nearly empty (forward the body, swap the auth header) and every other adapter
does the translation work. Clients can keep using the OpenAI SDK pointed at
the gateway URL.

**Why streaming uses channels.** Each adapter's `Stream` returns
`(<-chan StreamDelta, <-chan error)`. The handler drains both inside a single
`select` that also watches `ctx.Done()`. If the client disconnects mid-stream,
context cancellation propagates into the adapter goroutine and aborts the
upstream call cleanly — no leaked goroutines, no orphaned upstream sockets.

## Getting started

### Prerequisites

- Go 1.22 or newer
- An OpenAI and/or Anthropic API key
- Docker (optional, for the compose path)

### Configuration

The gateway is configured entirely through environment variables.

**Required:**

| Env var             | Required           | Default  | Description                                                   |
|---------------------|--------------------|----------|---------------------------------------------------------------|
| `GATEWAY_API_KEY`   | yes                | —        | Bearer token clients must present to use the gateway          |
| `OPENAI_API_KEY`    | one provider req'd | —        | Enables the `openai/` route prefix                            |
| `ANTHROPIC_API_KEY` | one provider req'd | —        | Enables the `anthropic/` route prefix                         |
| `ADDR`              | no                 | `:8080`  | Listen address                                                |

**Reliability (optional, all have sensible defaults):**

| Env var               | Default | Description                                                       |
|-----------------------|---------|-------------------------------------------------------------------|
| `REQUEST_TIMEOUT`     | `120s`  | Per-request deadline; honored end-to-end through retries and upstream calls |
| `RETRY_MAX_ATTEMPTS`  | `3`     | Total attempts including the initial call                          |
| `RETRY_BASE_DELAY`    | `100ms` | First retry delay (doubles each attempt, with ±25% jitter)         |
| `RETRY_MAX_DELAY`     | `5s`    | Backoff cap                                                        |
| `BREAKER_THRESHOLD`   | `5`     | Consecutive failures before tripping a provider's breaker          |
| `BREAKER_RESET`       | `30s`   | How long Open before allowing a single half-open probe             |

**Failover (optional):**

| Env var               | Default | Description                                                       |
|-----------------------|---------|-------------------------------------------------------------------|
| `FAILOVER_OPENAI`     | (none)  | Fallback model (`<provider>/<model>`) when any `openai/*` request fails  |
| `FAILOVER_ANTHROPIC`  | (none)  | Fallback model when any `anthropic/*` request fails                |

**Cache (optional, opt-in):**

| Env var            | Default              | Description                                                  |
|--------------------|----------------------|--------------------------------------------------------------|
| `CACHE_ENABLED`    | `false`              | Set to `true`/`1`/`yes` to enable the response cache         |
| `REDIS_URL`        | (none)               | e.g. `redis://localhost:6379`, `rediss://...` for TLS         |
| `CACHE_TTL`        | `24h`                | How long each cached response lives                          |
| `CACHE_NAMESPACE`  | `llmcache:`          | Prefix on every Redis key; allows sharing Redis with other apps |

The gateway refuses to start if `GATEWAY_API_KEY` is unset or if no provider
keys are configured. Misconfigured failover (unknown fallback provider, bad
syntax) logs a warning and starts with primary-only routing for that prefix.

### Running locally

```sh
cp .env.example .env       # fill in your provider keys + a gateway key
```

Either run the binary directly:

```sh
go run ./cmd/gateway
```

Or via Docker Compose (same image as production):

```sh
docker compose -f deploy/docker-compose.yaml --env-file .env up --build
```

## Caching

When `CACHE_ENABLED=true` and Redis is reachable, non-streaming
`/v1/chat/completions` requests are looked up in Redis before reaching the
upstream provider. A hit returns the cached response in ~5 ms and skips
both the upstream call (cost) and the failover chain (latency).

### How the cache key is computed

A SHA-256 hex digest of the canonical request fields that determine the
model's output:

- `model` — fully qualified (`openai/gpt-4o-mini`); different providers'
  models never collide even with the same short name
- `messages` — order-significant
- `temperature`, `top_p`, `max_tokens` — when present

`stream`, request ID, and gateway API key are **not** part of the key.

### Response headers

| Header        | Meaning                                                            |
|---------------|--------------------------------------------------------------------|
| `X-Cache: HIT`     | Response served from Redis; no upstream call                  |
| `X-Cache: MISS`    | Response came from upstream and was just written to the cache |
| `X-Cache: BYPASS`  | Cache skipped (streaming request, or `Cache-Control: no-store`) |
| `X-Cache-Key`      | The full cache key, useful for debugging                      |

### What is NOT cached

- **Streaming requests.** Streams pass through to the upstream every time
  (`X-Cache: BYPASS`). Once an SSE chunk has hit the wire, replaying a
  cached response would produce a confusing dual response. See the
  "Why streaming skips caching" design note below.
- **Requests with `Cache-Control: no-store`.** Per-request opt-out so
  callers can force a fresh upstream call for one-off needs.
- **Failed responses.** Only successful 2xx responses get cached.

### What about non-deterministic sampling?

The cache stores responses regardless of `temperature`. If you cached a
response generated at `temperature=0.9`, subsequent hits return the
*same* response, not a freshly-sampled one. This matches industry
convention (LiteLLM, Portkey, Helicone). If true freshness matters for a
specific request, send `Cache-Control: no-store`.

### When Redis goes down

The gateway **fails open**: cache errors (Redis unreachable, decode
failure, connection refused) are logged at `WARN` and the request falls
through to the upstream as if no cache were configured. Caching is an
optimization, never a correctness primitive — a Redis outage degrades
performance but never blocks user traffic.

---

## Usage

### Routing

Models are addressed as `<provider>/<model>`:

| Prefix       | Examples                                                    |
|--------------|-------------------------------------------------------------|
| `openai/`    | `openai/gpt-4o-mini`, `openai/gpt-4o`                       |
| `anthropic/` | `anthropic/claude-haiku-4-5`, `anthropic/claude-sonnet-4-6` |

### Non-streaming request

```sh
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "anthropic/claude-haiku-4-5",
    "messages": [{"role":"user","content":"Say hi in one word."}]
  }'
```

### Streaming (SSE)

```sh
curl -N http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "openai/gpt-4o-mini",
    "messages": [{"role":"user","content":"Count to five."}],
    "stream": true
  }'
```

Swap the `model` prefix between `openai/` and `anthropic/` — the response
envelope is identical.

### Error responses

Errors are returned as JSON with this shape:

```json
{ "error": { "message": "invalid or missing API key", "type": "Unauthorized" } }
```

Status code matrix:

| Status | When                                                                |
|--------|---------------------------------------------------------------------|
| `400`  | Malformed JSON, missing `model`/`messages`, unknown provider prefix |
| `4xx`  | Upstream client errors (e.g. invalid model name) passed through as-is |
| `401`  | Missing or wrong `Authorization: Bearer ...` header                 |
| `502`  | Upstream provider 5xx, all retries + failover exhausted             |
| `503`  | Provider not configured **or** all providers' circuit breakers are open |
| `504`  | Request exceeded `REQUEST_TIMEOUT`                                  |
| `500`  | Unexpected internal error                                           |

## API reference

| Method | Path                   | Auth       | Description                                              |
|--------|------------------------|------------|----------------------------------------------------------|
| GET    | `/healthz`             | none       | Liveness probe; always returns `ok`                      |
| GET    | `/ready`               | none       | Readiness probe; 200 if any provider breaker is non-Open, 503 if all upstreams are unreachable |
| POST   | `/v1/chat/completions` | Bearer key | OpenAI-shaped chat completion; SSE if `stream:true`      |

The request and response shapes follow OpenAI's chat-completions schema. See
[`internal/provider/provider.go`](internal/provider/provider.go) for the exact
Go types.

## Project layout

```
cmd/gateway/             entrypoint, env config, reliability + cache wiring
internal/
  provider/              Provider interface + OpenAI-shaped types + UpstreamError
    openai/              OpenAI adapter (near-passthrough)
    anthropic/           Anthropic adapter (translates to/from Messages API)
  reliability/           hand-rolled retry, circuit breaker, decorator combining both
  cache/                 Cache interface + canonical key derivation + Redis impl
  router/                "<provider>/<model>" → primary + ordered failover chain
  server/                chi router, auth + timeout middleware, handler, SSE, cache lookup
deploy/                  Dockerfile, fly.toml, docker-compose.yaml (with redis service)
```

## Tech stack

| Concern    | Choice                                | Why                                                            |
|------------|---------------------------------------|----------------------------------------------------------------|
| Language   | Go 1.22+                              | I/O-bound proxying with native streaming; single static binary |
| HTTP       | stdlib `net/http` + `chi`             | Stdlib `http.Handler` everywhere; chi adds middleware groups   |
| Logging    | `log/slog` (stdlib)                   | Structured JSON without an extra dependency                    |
| Container  | Multi-stage build → `distroless/static` | ~11 MB final image, no shell, non-root user                  |
| Deploy     | Fly.io (`fly.toml`)                   | Free tier, global edge, autoscale-to-zero, easy secrets        |

## Design notes

**Why the OpenAI envelope is the canonical shape.** It's the de-facto
standard across the LLM ecosystem. Choosing it as the gateway's external
contract means the OpenAI adapter is nearly empty and the translation work
lives entirely inside non-OpenAI adapters. Clients can keep using the OpenAI
SDK by changing only the base URL.

**Why channel-based streaming.** Each adapter's `Stream` returns paired
channels for deltas and a terminal error. The server handler drains both
inside a single `select` that also listens for `ctx.Done()`. Backpressure is
free (the channel is buffered, slow consumers pause the producer), and a
client disconnect cancels the upstream call cleanly via context propagation.

**Why `chi` over gin/echo/fiber.** chi handlers are plain `http.Handler` —
nothing custom. That means `httptest` works unchanged for unit tests, any
stdlib-compatible middleware plugs in, and if the project ever needs to
switch routers the rest of the code doesn't move. The competing frameworks
all introduce their own `Context` type.

**Why distroless.** No shell, no package manager, no userland to compromise.
The `distroless/static` base is ~2 MB; the gateway binary is ~9 MB; total
image is ~11 MB. CVE surface is essentially the Go binary itself.

**Why prefix-based model routing.** The gateway strips the `<provider>/`
prefix in the router and hands adapters the upstream-native model name.
Adapters never need to know about gateway-flavored naming, and adding a new
provider is a single map entry plus an adapter implementation.

**Why hand-roll retry + circuit breaker.** Both are 30-60 line state
machines that benefit from being read and owned by the codebase: the retry
loop is just `for attempt < N { op(); sleep(backoff) }` plus jitter, and
the breaker is a three-state machine that fits on one screen. Pulling in
`cenkalti/backoff` and `sony/gobreaker` would have meant ~5x the surface
area for tests to cover for the same behavior.

**Why streaming skips retries and failover.** Once an SSE chunk has been
written to the client, the response is committed — retrying or failing
over would produce a confusing dual response. Streams get the circuit
breaker check up-front (fast-fail when an upstream is known to be down)
and are then one-shot. Non-streaming `Complete` calls get the full
retry-loop + failover-chain treatment.

**Why 4xx errors are passed through.** Gateway-level 502 hides the
distinction between "your request was malformed" and "the upstream is
broken." Forwarding 4xx upstream status codes verbatim lets clients see
genuine client errors (`400 invalid model`, `404 model not found`) while
still mapping 5xx and transport errors to `502 Bad Gateway`.

**Why exact-match caching first, semantic later.** Exact-match is ~10x
less code than semantic and lets us validate the cache plumbing (Redis
client, key derivation, headers, fail-open semantics, TTL) without also
debugging an embedding service and a vector index. Semantic builds on the
same `Cache` interface in a follow-up — same Redis dep, same handler
hook, just a different `Get` implementation.

**Why streaming skips caching.** Same reasoning as why streams skip
retries and failover: once the first SSE chunk has been written, the
response is committed. Replaying a cached response as synthetic chunks
is possible but error-prone (timing, finish_reason emission, edge cases
around tool calls). Production gateways like LiteLLM and Portkey skip
streams too. Non-streaming workloads are where caching usually matters
anyway — batch jobs, agent loops, RAG queries.

**Why fail-open on cache errors.** Caching is an optimization, never a
correctness primitive. A Redis outage shouldn't take down the gateway;
it should degrade silently to "no cache" mode and let traffic continue.
The handler logs cache failures at `WARN` so they're visible in
observability but they never block a request.

## Testing

### Unit tests

```sh
go test ./...
```

Test coverage by package:

| Package                              | What it tests                                                                       |
|--------------------------------------|-------------------------------------------------------------------------------------|
| `internal/provider/openai`           | Headers, body marshaling, response decoding, SSE chunk parsing — using `httptest`   |
| `internal/provider/anthropic`        | System-message lifting, `max_tokens` default, `stop_reason` mapping, stream folding |
| `internal/reliability`               | Retry exhaustion + non-retryable short-circuit, jitter ranges, breaker state machine, decorator combines them correctly |
| `internal/cache`                     | Canonical key derivation (determinism, model/message/temperature changes, order significance), Redis impl round-trip, TTL expiry, namespacing, corrupt-entry handling |
| `internal/router`                    | Prefix parsing, unknown-provider error, fallback resolution, fallback validation     |

All upstream calls are faked via `httptest.NewServer`, an injectable
clock for the breaker, and an in-process `miniredis` for the cache —
no real API keys, no real Redis, no sleep waits required to run the
suite.

### Local smoke (requires real provider keys)

After `docker compose up` or `go run`, exercise both providers and the
streaming path:

```sh
# health check
curl http://localhost:8080/healthz

# OpenAI route
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"Say hi."}]}'

# Anthropic route — same envelope
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"anthropic/claude-haiku-4-5","messages":[{"role":"user","content":"Say hi."}]}'

# Streaming
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"Count to five."}],"stream":true}'
```

### Error-path probes

```sh
# 401 — wrong gateway key
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer wrong" -d '{"model":"openai/x","messages":[{"role":"user","content":"hi"}]}'

# 400 — unknown provider prefix
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"gemini/foo","messages":[{"role":"user","content":"hi"}]}'

# 400 — missing prefix
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
```

### Cache drills (requires Redis running locally)

```sh
# Enable the cache: set CACHE_ENABLED=true + REDIS_URL=redis://localhost:6379
# (docker compose does this for you; for go run, export the vars yourself)

# First request — MISS, hits upstream, gets cached
curl -i -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
# → X-Cache: MISS, X-Cache-Key: <hash>

# Same request again — HIT, returns from Redis in ~5ms
curl -i -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
# → X-Cache: HIT, same X-Cache-Key

# Opt out per-request — BYPASS, fresh call every time
curl -i -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Cache-Control: no-store" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'
# → X-Cache: BYPASS

# Stream — also BYPASS, streams always pass through
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}],"stream":true}'
# → X-Cache: BYPASS
```

### Reliability drills (requires real provider keys)

```sh
# Failover drill: set OPENAI_API_KEY=sk-bogus and FAILOVER_OPENAI=anthropic/claude-haiku-4-5,
# then call an openai/* model — the response should come from Anthropic.
# Look for `fallback succeeded` in the logs.
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -d '{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}'

# Circuit-breaker drill: set OPENAI_API_KEY=sk-bogus and leave FAILOVER_OPENAI empty.
# Send 5 requests in a row → all fail with 502. The 6th should fail-fast
# with 503 (ErrCircuitOpen) without contacting OpenAI at all.

# /ready
curl http://localhost:8080/ready
# → "ready" (200) if any provider's breaker is non-Open
# → "no providers ready" (503) if every upstream is wedged
```

## Deploy to Fly.io

```sh
fly launch --no-deploy --config deploy/fly.toml --dockerfile deploy/Dockerfile
fly secrets set GATEWAY_API_KEY=$(openssl rand -hex 32)
fly secrets set OPENAI_API_KEY=sk-...
fly secrets set ANTHROPIC_API_KEY=sk-ant-...
fly deploy --config deploy/fly.toml --dockerfile deploy/Dockerfile
```

The Fly config provisions a shared-CPU 256 MB VM in `bom`, autoscales to zero
when idle, and runs `/healthz` checks every 30s.

## License

MIT (to be added).
