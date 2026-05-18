// Package cache provides response caching for LLM Complete calls. The
// gateway looks up an incoming request by canonical key before invoking
// the provider; on a hit it returns the cached response and skips both
// the upstream call (cost) and the failover chain (latency).
//
// Phase 2b-1 ships an exact-match cache: identical inputs produce identical
// keys. Semantic caching (vector similarity over message embeddings) is a
// follow-up that plugs into the same Cache interface.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

// Cache is the contract every cache implementation satisfies. The exact-
// match Redis backing is one impl; future semantic-match is another.
//
// Get returns (response, hit, err). A miss is (nil, false, nil) — distinct
// from a Redis-down error (nil, false, err) so callers can fail-open on
// errors without confusing them with misses.
type Cache interface {
	Get(ctx context.Context, key string) (*provider.Response, bool, error)
	Put(ctx context.Context, key string, resp *provider.Response, ttl time.Duration) error
}

// keyInput is the projection of provider.Request that determines the
// model's output. Fields not in this struct (like Stream) do NOT influence
// the cache key — a request with stream=true and one with stream=false
// against otherwise-identical inputs would produce the same response and
// should share a cache entry (though Phase 2b-1 only caches non-streaming).
type keyInput struct {
	// Model is the FULLY QUALIFIED model name including provider prefix
	// (e.g. "openai/gpt-4o-mini"). Two providers' models with the same
	// short name must not share a cache slot.
	Model       string             `json:"model"`
	Messages    []provider.Message `json:"messages"`
	Temperature *float64           `json:"temperature,omitempty"`
	TopP        *float64           `json:"top_p,omitempty"`
	MaxTokens   *int               `json:"max_tokens,omitempty"`
}

// Key derives a stable cache key from a request. Identical inputs yield
// identical keys across process restarts and machines.
//
// The key is hex(sha256(canonical_json)). Go's encoding/json emits struct
// fields in declaration order and pointer-omitempty fields drop cleanly,
// so the canonical JSON is deterministic for our struct shape.
func Key(req *provider.Request) string {
	input := keyInput{
		Model:       req.Model,
		Messages:    req.Messages,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   req.MaxTokens,
	}
	b, _ := json.Marshal(&input)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
