// Package router maps a fully-qualified model name like "openai/gpt-4o-mini"
// to the provider adapter that should serve it, and strips the prefix so the
// adapter sees the upstream-native model name.
package router

import (
	"fmt"
	"strings"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

type Router struct {
	providers map[string]provider.Provider
}

func New(providers map[string]provider.Provider) *Router {
	return &Router{providers: providers}
}

// Resolve splits "provider/model" into the matching Provider plus the
// unprefixed model name. Returns provider.ErrUnknownModel if the prefix is
// unrecognized or absent.
func (r *Router) Resolve(model string) (provider.Provider, string, error) {
	prefix, rest, ok := strings.Cut(model, "/")
	if !ok {
		return nil, "", fmt.Errorf("%w: %q (expected \"<provider>/<model>\")", provider.ErrUnknownModel, model)
	}
	p, found := r.providers[prefix]
	if !found {
		return nil, "", fmt.Errorf("%w: provider %q", provider.ErrUnknownModel, prefix)
	}
	return p, rest, nil
}
