package cache

import (
	"testing"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

func ptrF(v float64) *float64 { return &v }
func ptrI(v int) *int         { return &v }

func TestKey_Deterministic(t *testing.T) {
	req := &provider.Request{
		Model:       "openai/gpt-4o-mini",
		Messages:    []provider.Message{{Role: "user", Content: "hi"}},
		Temperature: ptrF(0.7),
	}
	k1 := Key(req)
	k2 := Key(req)
	if k1 != k2 {
		t.Fatalf("identical input produced different keys:\n  %s\n  %s", k1, k2)
	}
	// Hex sha256 is always 64 chars.
	if len(k1) != 64 {
		t.Fatalf("key length = %d, want 64", len(k1))
	}
}

func TestKey_DiffersOnMessageChange(t *testing.T) {
	base := &provider.Request{
		Model:    "openai/gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
	}
	changed := *base
	changed.Messages = []provider.Message{{Role: "user", Content: "hello"}}
	if Key(base) == Key(&changed) {
		t.Fatal("different messages produced the same key")
	}
}

func TestKey_DiffersOnModelChange(t *testing.T) {
	a := &provider.Request{Model: "openai/gpt-4o-mini", Messages: []provider.Message{{Role: "user", Content: "hi"}}}
	b := &provider.Request{Model: "anthropic/claude-haiku-4-5", Messages: []provider.Message{{Role: "user", Content: "hi"}}}
	if Key(a) == Key(b) {
		t.Fatal("different models produced the same key")
	}
}

func TestKey_DiffersOnTemperatureChange(t *testing.T) {
	a := &provider.Request{
		Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}},
		Temperature: ptrF(0.0),
	}
	b := &provider.Request{
		Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}},
		Temperature: ptrF(0.7),
	}
	if Key(a) == Key(b) {
		t.Fatal("different temperatures produced the same key")
	}
}

func TestKey_OmitNilOptionals(t *testing.T) {
	// Two requests, neither specifying temperature/top_p/max_tokens,
	// should produce the same key (omitempty handling).
	a := &provider.Request{Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}}}
	b := &provider.Request{Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}}}
	if Key(a) != Key(b) {
		t.Fatal("identical inputs with nil optionals produced different keys")
	}
}

func TestKey_IgnoresStreamField(t *testing.T) {
	// Stream is not part of keyInput, so otherwise-identical requests with
	// different Stream values should share a cache slot.
	a := &provider.Request{Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}}, Stream: true}
	b := &provider.Request{Model: "x", Messages: []provider.Message{{Role: "user", Content: "hi"}}, Stream: false}
	if Key(a) != Key(b) {
		t.Fatal("stream field should not affect cache key")
	}
}

func TestKey_MessageOrderMatters(t *testing.T) {
	// Conversation history order is meaningful — reordering should produce
	// different keys.
	a := &provider.Request{
		Model: "x",
		Messages: []provider.Message{
			{Role: "user", Content: "first"},
			{Role: "user", Content: "second"},
		},
	}
	b := &provider.Request{
		Model: "x",
		Messages: []provider.Message{
			{Role: "user", Content: "second"},
			{Role: "user", Content: "first"},
		},
	}
	if Key(a) == Key(b) {
		t.Fatal("reordered messages produced the same key")
	}
}
