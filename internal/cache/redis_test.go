package cache

import (
	"context"
	"testing"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestCache(t *testing.T) (*RedisCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return NewRedisCache(rdb, "test:"), mr
}

func sampleResponse() *provider.Response {
	return &provider.Response{
		ID:      "chatcmpl-1",
		Object:  "chat.completion",
		Created: 1700000000,
		Model:   "gpt-4o-mini",
		Choices: []provider.Choice{{
			Index:        0,
			Message:      provider.Message{Role: "assistant", Content: "hi"},
			FinishReason: "stop",
		}},
		Usage: provider.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}
}

func TestRedis_PutThenGet(t *testing.T) {
	c, _ := newTestCache(t)
	ctx := context.Background()
	want := sampleResponse()

	if err := c.Put(ctx, "k1", want, 1*time.Minute); err != nil {
		t.Fatal(err)
	}
	got, hit, err := c.Get(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if !hit {
		t.Fatal("expected hit, got miss")
	}
	if got.Choices[0].Message.Content != "hi" {
		t.Fatalf("content = %q", got.Choices[0].Message.Content)
	}
}

func TestRedis_Miss(t *testing.T) {
	c, _ := newTestCache(t)
	resp, hit, err := c.Get(context.Background(), "no-such-key")
	if err != nil {
		t.Fatalf("miss should not be an error: %v", err)
	}
	if hit {
		t.Fatal("expected miss")
	}
	if resp != nil {
		t.Fatal("miss should return nil response")
	}
}

func TestRedis_TTLExpires(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()
	if err := c.Put(ctx, "k1", sampleResponse(), 1*time.Second); err != nil {
		t.Fatal(err)
	}
	// miniredis lets tests fast-forward its clock.
	mr.FastForward(2 * time.Second)
	_, hit, err := c.Get(ctx, "k1")
	if err != nil {
		t.Fatal(err)
	}
	if hit {
		t.Fatal("entry should have expired")
	}
}

func TestRedis_Namespaced(t *testing.T) {
	c, mr := newTestCache(t)
	ctx := context.Background()
	_ = c.Put(ctx, "k1", sampleResponse(), 1*time.Minute)

	// The raw Redis key includes the namespace prefix.
	if !mr.Exists("test:k1") {
		t.Fatal("expected namespaced key to exist")
	}
	if mr.Exists("k1") {
		t.Fatal("unprefixed key should not exist")
	}
}

func TestRedis_CorruptEntryReturnsError(t *testing.T) {
	c, mr := newTestCache(t)
	// Inject garbage at the raw key.
	mr.Set("test:bad", "not valid json")

	_, hit, err := c.Get(context.Background(), "bad")
	if err == nil {
		t.Fatal("expected decode error")
	}
	if hit {
		t.Fatal("corrupt entry should not count as hit")
	}
}
