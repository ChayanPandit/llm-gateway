// Package router maps a fully-qualified model name like "openai/gpt-4o-mini"
// to a chain of provider targets: a primary plus an ordered list of
// fallbacks the handler should try if the primary fails.
package router

import (
	"fmt"
	"strings"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

// Target is a (provider, upstream-model) pair the handler can call directly.
type Target struct {
	Provider provider.Provider
	Model    string // upstream-native model name (no provider prefix)
}

// Chain is a primary target plus ordered fallbacks. The handler tries
// Primary first; on a failure that's eligible for failover (retries
// exhausted, breaker open, or transport-level error) it walks Fallback
// left-to-right.
type Chain struct {
	Primary  Target
	Fallback []Target
}

type Router struct {
	providers map[string]provider.Provider
	// fallback maps a primary provider prefix to a single fully-qualified
	// fallback model (e.g. "openai" -> "anthropic/claude-haiku-4-5").
	// Empty string means no fallback for that provider.
	fallback map[string]string
}

// New constructs a router with no fallbacks configured.
func New(providers map[string]provider.Provider) *Router {
	return &Router{providers: providers, fallback: map[string]string{}}
}

// SetFallback registers a fallback model for the given primary provider
// prefix. If fallbackModel is "" the entry is removed. Returns an error if
// fallbackModel cannot be parsed or its provider isn't registered.
func (r *Router) SetFallback(primaryProvider, fallbackModel string) error {
	if fallbackModel == "" {
		delete(r.fallback, primaryProvider)
		return nil
	}
	prefix, _, ok := strings.Cut(fallbackModel, "/")
	if !ok {
		return fmt.Errorf("fallback model %q missing \"<provider>/\" prefix", fallbackModel)
	}
	if _, exists := r.providers[prefix]; !exists {
		return fmt.Errorf("fallback provider %q not registered", prefix)
	}
	r.fallback[primaryProvider] = fallbackModel
	return nil
}

// Resolve looks up the primary target and any configured fallback chain.
func (r *Router) Resolve(model string) (*Chain, error) {
	primary, err := r.resolveOne(model)
	if err != nil {
		return nil, err
	}
	chain := &Chain{Primary: primary}

	if fb, ok := r.fallback[providerPrefix(model)]; ok && fb != "" {
		t, err := r.resolveOne(fb)
		if err != nil {
			// Misconfigured fallback shouldn't sink the primary call; log
			// would be nice here but the router is dep-free. Skip silently;
			// SetFallback already validates at startup time.
			return chain, nil
		}
		chain.Fallback = append(chain.Fallback, t)
	}
	return chain, nil
}

func (r *Router) resolveOne(model string) (Target, error) {
	prefix, rest, ok := strings.Cut(model, "/")
	if !ok {
		return Target{}, fmt.Errorf("%w: %q (expected \"<provider>/<model>\")", provider.ErrUnknownModel, model)
	}
	p, found := r.providers[prefix]
	if !found {
		return Target{}, fmt.Errorf("%w: provider %q", provider.ErrUnknownModel, prefix)
	}
	return Target{Provider: p, Model: rest}, nil
}

func providerPrefix(model string) string {
	prefix, _, _ := strings.Cut(model, "/")
	return prefix
}
