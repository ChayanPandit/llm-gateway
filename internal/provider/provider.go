// Package provider defines the contract every upstream LLM adapter implements,
// plus the OpenAI-compatible request/response shapes the gateway speaks externally.
package provider

import (
	"context"
	"errors"
	"fmt"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Request struct {
	// Model is the upstream model name with the provider prefix stripped
	// (e.g. "gpt-4o-mini", "claude-haiku-4-5"). The router strips the prefix
	// before handing the request to an adapter.
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type Choice struct {
	Index        int     `json:"index"`
	Message      Message `json:"message"`
	FinishReason string  `json:"finish_reason"`
}

type Response struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
}

// StreamDelta is one incremental chunk in an SSE stream, shaped like an
// OpenAI chat-completion chunk so the gateway can serialize it directly.
type StreamDelta struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []DeltaChoice `json:"choices"`
}

type DeltaChoice struct {
	Index        int     `json:"index"`
	Delta        Message `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

// Provider is the interface every LLM upstream adapter implements.
// Complete returns a single response; Stream returns a channel of incremental
// deltas plus a channel that yields a single terminal error (or nil) once
// the delta channel closes.
type Provider interface {
	Name() string
	Complete(ctx context.Context, req *Request) (*Response, error)
	Stream(ctx context.Context, req *Request) (<-chan StreamDelta, <-chan error)
}

var (
	ErrUnknownModel  = errors.New("unknown model")
	ErrNotConfigured = errors.New("provider not configured (missing API key)")
)

// UpstreamError represents a failure talking to an upstream LLM provider —
// either an HTTP error response or a network/transport error. The reliability
// layer inspects Status (and Err) to decide whether to retry: 5xx and 429 are
// retryable, 4xx is not, and transport errors (Status == 0) are retryable.
type UpstreamError struct {
	Provider string // adapter name, e.g. "openai"
	Status   int    // HTTP status from upstream; 0 if the request didn't complete
	Body     string // raw response body (truncated) for debugging
	Err      error  // underlying transport error if any (timeout, conn refused, parse, etc.)
}

func (e *UpstreamError) Error() string {
	if e.Status == 0 {
		return fmt.Sprintf("upstream %s: %v", e.Provider, e.Err)
	}
	return fmt.Sprintf("upstream %s: status=%d body=%s", e.Provider, e.Status, e.Body)
}

func (e *UpstreamError) Unwrap() error { return e.Err }

// Retryable reports whether the failure is worth another attempt.
//   - status == 0 (transport-level): retry (timeout, conn refused, dropped conn)
//   - 429: retry (rate limited)
//   - 5xx: retry (server error)
//   - everything else (4xx): permanent client error, don't retry
func (e *UpstreamError) Retryable() bool {
	if e.Status == 0 {
		return true
	}
	if e.Status == 429 {
		return true
	}
	return e.Status >= 500 && e.Status < 600
}
