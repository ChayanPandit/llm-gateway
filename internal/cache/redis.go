package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/redis/go-redis/v9"
)

// RedisCache is the exact-match cache backed by a single Redis instance.
// Keys are namespaced (default "llmcache:<key>") so the cache can share
// a Redis with other applications without collisions.
type RedisCache struct {
	rdb       *redis.Client
	namespace string
}

// NewRedisCache wraps an existing *redis.Client. The caller is responsible
// for connection setup, auth, TLS, etc — Redis URL parsing belongs in main.go.
//
// namespace is prepended to every key; pass "" to use the default.
func NewRedisCache(rdb *redis.Client, namespace string) *RedisCache {
	if namespace == "" {
		namespace = "llmcache:"
	}
	return &RedisCache{rdb: rdb, namespace: namespace}
}

// Get returns (response, hit, err). Note the three-state return:
//   - hit  : (resp, true, nil)
//   - miss : (nil, false, nil)
//   - down : (nil, false, err)   — caller should fail-open, log and continue
func (c *RedisCache) Get(ctx context.Context, key string) (*provider.Response, bool, error) {
	b, err := c.rdb.Get(ctx, c.namespace+key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("redis get: %w", err)
	}
	var resp provider.Response
	if err := json.Unmarshal(b, &resp); err != nil {
		// Treat a corrupt cache entry as a miss; the upstream call will
		// repopulate the key on success.
		return nil, false, fmt.Errorf("cache decode: %w", err)
	}
	return &resp, true, nil
}

// Put writes a response into Redis with the given TTL. A TTL of 0 means
// "never expire" — usually undesirable for LLM responses since model
// quality / prompts evolve, so callers should pass a positive duration.
func (c *RedisCache) Put(ctx context.Context, key string, resp *provider.Response, ttl time.Duration) error {
	b, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("cache encode: %w", err)
	}
	if err := c.rdb.Set(ctx, c.namespace+key, b, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
