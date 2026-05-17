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

func TestResolve(t *testing.T) {
	openai := &fakeProvider{name: "openai"}
	r := New(map[string]provider.Provider{"openai": openai})

	got, model, err := r.Resolve("openai/gpt-4o-mini")
	if err != nil {
		t.Fatal(err)
	}
	if got != openai || model != "gpt-4o-mini" {
		t.Fatalf("got provider=%v model=%q", got, model)
	}

	if _, _, err := r.Resolve("anthropic/claude"); !errors.Is(err, provider.ErrUnknownModel) {
		t.Fatalf("expected ErrUnknownModel, got %v", err)
	}
	if _, _, err := r.Resolve("gpt-4o-mini"); !errors.Is(err, provider.ErrUnknownModel) {
		t.Fatalf("expected ErrUnknownModel for missing prefix, got %v", err)
	}
}
