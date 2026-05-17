# llm-gateway

A unified HTTP gateway in front of multiple LLM providers (OpenAI, Anthropic,
and more). Clients speak one OpenAI-compatible schema —
`POST /v1/chat/completions` — and the gateway translates to/from each upstream
and folds responses back into the same envelope. Streaming responses are
delivered as Server-Sent Events.

Built in Go. Deployed to Fly.io.

## Status

🚧 Early development. Phase 0 (MVP: unified API + OpenAI & Anthropic adapters
+ SSE streaming) is in progress on the `phase-0/mvp` branch.

## Routing

Models are addressed as `<provider>/<model>`:

| Prefix       | Examples                                                 |
|--------------|----------------------------------------------------------|
| `openai/`    | `openai/gpt-4o-mini`, `openai/gpt-4o`                    |
| `anthropic/` | `anthropic/claude-haiku-4-5`, `anthropic/claude-sonnet-4-6` |

## Running locally

_Filled in as Phase 0 lands._

## License

MIT (to be added).
