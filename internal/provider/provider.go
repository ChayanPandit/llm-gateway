// Package provider defines the contract every upstream LLM adapter implements,
// plus the OpenAI-compatible request/response shapes the gateway speaks externally.
package provider

import (
	"context"
	"errors"
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
	ErrUpstream      = errors.New("upstream provider error")
	ErrUnknownModel  = errors.New("unknown model")
	ErrNotConfigured = errors.New("provider not configured (missing API key)")
)
