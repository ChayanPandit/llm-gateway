# llm-gateway

A unified HTTP gateway in front of multiple LLM providers (OpenAI, Anthropic,
and more). Clients speak one OpenAI-compatible schema —
`POST /v1/chat/completions` — and the gateway translates to/from each upstream
and folds responses back into the same envelope. Streaming responses are
delivered as Server-Sent Events.

Built in Go. Deployed to Fly.io.

## Routing

Models are addressed as `<provider>/<model>`:

| Prefix       | Examples                                                    |
|--------------|-------------------------------------------------------------|
| `openai/`    | `openai/gpt-4o-mini`, `openai/gpt-4o`                       |
| `anthropic/` | `anthropic/claude-haiku-4-5`, `anthropic/claude-sonnet-4-6` |

## Run locally

```sh
cp .env.example .env       # fill in your provider keys
go run ./cmd/gateway       # or: docker compose -f deploy/docker-compose.yaml --env-file .env up --build
```

Non-streaming request:

```sh
curl http://localhost:8080/v1/chat/completions \
  -H "Authorization: Bearer $GATEWAY_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "anthropic/claude-haiku-4-5",
    "messages": [{"role":"user","content":"Say hi in one word."}]
  }'
```

Streaming (SSE):

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

## Deploy to Fly.io

```sh
fly launch --no-deploy --config deploy/fly.toml --dockerfile deploy/Dockerfile
fly secrets set GATEWAY_API_KEY=$(openssl rand -hex 32)
fly secrets set OPENAI_API_KEY=sk-...
fly secrets set ANTHROPIC_API_KEY=sk-ant-...
fly deploy --config deploy/fly.toml --dockerfile deploy/Dockerfile
```

## Layout

```
cmd/gateway/         entrypoint, signal handling
internal/
  provider/          Provider interface + OpenAI-shaped types
    openai/          adapter (near-passthrough)
    anthropic/       adapter (translates to/from Messages API)
  router/            "<provider>/<model>" → Provider + upstream model name
  server/            chi router, auth middleware, handler, SSE streaming
deploy/              Dockerfile, fly.toml, docker-compose.yaml
```

## Endpoints

| Method | Path                   | Auth       | Notes                                    |
|--------|------------------------|------------|------------------------------------------|
| GET    | `/healthz`             | none       | liveness                                 |
| POST   | `/v1/chat/completions` | Bearer key | OpenAI-shaped; `stream:true` returns SSE |

## Test

```sh
go test ./...
```

## License

MIT (to be added).
