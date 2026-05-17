package router

import (
	"context"
	"errors"
	"testing"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

type fakeProvider struct{ name string }

func (f *fakeProvider) Name() string { return f.name }
func (f *fakeProvider) Complete(context.Context, *provider.Request) (*provider.Response, error) {
	return nil, nil
}
func (f *fakeProvider) Stream(context.Context, *provider.Request) (<-chan provider.StreamDelta, <-chan error) {
	return nil, nil
}

func TestResolve_PrimaryOnly(t *testing.T) {
	openai := &fakeProvider{name: "openai"}
	r := New(map[string]provider.Provider{"openai": openai})

	chain, err := r.Resolve("openai/gpt-4o-mini")
	if err != nil {
		t.Fatal(err)
	}
	if chain.Primary.Provider != openai || chain.Primary.Model != "gpt-4o-mini" {
		t.Fatalf("primary = %+v", chain.Primary)
	}
	if len(chain.Fallback) != 0 {
		t.Fatalf("fallback len = %d, want 0", len(chain.Fallback))
	}
}

func TestResolve_WithFallback(t *testing.T) {
	openai := &fakeProvider{name: "openai"}
	anthropic := &fakeProvider{name: "anthropic"}
	r := New(map[string]provider.Provider{"openai": openai, "anthropic": anthropic})

	if err := r.SetFallback("openai", "anthropic/claude-haiku-4-5"); err != nil {
		t.Fatal(err)
	}

	chain, err := r.Resolve("openai/gpt-4o-mini")
	if err != nil {
		t.Fatal(err)
	}
	if chain.Primary.Provider != openai {
		t.Fatal("primary should be openai")
	}
	if len(chain.Fallback) != 1 {
		t.Fatalf("fallback len = %d, want 1", len(chain.Fallback))
	}
	if chain.Fallback[0].Provider != anthropic || chain.Fallback[0].Model != "claude-haiku-4-5" {
		t.Fatalf("fallback = %+v", chain.Fallback[0])
	}
}

func TestResolve_UnknownProvider(t *testing.T) {
	r := New(map[string]provider.Provider{"openai": &fakeProvider{name: "openai"}})
	if _, err := r.Resolve("anthropic/claude"); !errors.Is(err, provider.ErrUnknownModel) {
		t.Fatalf("expected ErrUnknownModel, got %v", err)
	}
	if _, err := r.Resolve("gpt-4o-mini"); !errors.Is(err, provider.ErrUnknownModel) {
		t.Fatalf("expected ErrUnknownModel for missing prefix, got %v", err)
	}
}

func TestSetFallback_RejectsUnknownProvider(t *testing.T) {
	r := New(map[string]provider.Provider{"openai": &fakeProvider{name: "openai"}})
	if err := r.SetFallback("openai", "anthropic/claude"); err == nil {
		t.Fatal("expected error: anthropic not registered")
	}
	if err := r.SetFallback("openai", "no-slash"); err == nil {
		t.Fatal("expected error: missing prefix")
	}
}

func TestSetFallback_EmptyStringClears(t *testing.T) {
	openai := &fakeProvider{name: "openai"}
	anthropic := &fakeProvider{name: "anthropic"}
	r := New(map[string]provider.Provider{"openai": openai, "anthropic": anthropic})
	_ = r.SetFallback("openai", "anthropic/claude-haiku-4-5")
	_ = r.SetFallback("openai", "")

	chain, _ := r.Resolve("openai/gpt-4o-mini")
	if len(chain.Fallback) != 0 {
		t.Fatalf("fallback should be cleared, got %d entries", len(chain.Fallback))
	}
}
