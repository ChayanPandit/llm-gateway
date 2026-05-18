package cache

import (
	"context"
	"testing"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

func TestLastUserMessage_FindsMostRecent(t *testing.T) {
	req := &provider.Request{
		Messages: []provider.Message{
			{Role: "system", Content: "you are helpful"},
			{Role: "user", Content: "hi"},
			{Role: "assistant", Content: "hello"},
			{Role: "user", Content: "how are you"},
		},
	}
	if got := lastUserMessage(req); got != "how are you" {
		t.Fatalf("got %q, want %q", got, "how are you")
	}
}

func TestLastUserMessage_NoUserMessage(t *testing.T) {
	req := &provider.Request{
		Messages: []provider.Message{
			{Role: "system", Content: "you are helpful"},
		},
	}
	if got := lastUserMessage(req); got != "" {
		t.Fatalf("got %q, want empty", got)
	}
}

func TestVectorToBytes_RoundTripsViaFloat32Bits(t *testing.T) {
	// Encoding is little-endian Float32 — sanity check a tiny vector.
	v := []float32{1.0, -2.5, 0.0}
	b := vectorToBytes(v)
	if len(b) != 12 {
		t.Fatalf("byte length = %d, want 12 (3 floats * 4 bytes)", len(b))
	}
	// First float (1.0) → 0x3F800000 little-endian → 00 00 80 3F
	want := []byte{0x00, 0x00, 0x80, 0x3F}
	for i, w := range want {
		if b[i] != w {
			t.Fatalf("byte %d = %#x, want %#x", i, b[i], w)
		}
	}
}

func TestParseSearchResult_EmptyResults(t *testing.T) {
	// FT.SEARCH returns [0] when no docs match.
	_, _, ok := parseSearchResult([]interface{}{int64(0)})
	if ok {
		t.Fatal("empty result should return ok=false")
	}
}

func TestParseSearchResult_OneHit(t *testing.T) {
	// Shape: [1, "doc_key", [field, value, ...]]
	raw := []interface{}{
		int64(1),
		"llmcache:semantic:abc",
		[]interface{}{
			"response_json", `{"id":"x"}`,
			"score", "0.04",
		},
	}
	rj, dist, ok := parseSearchResult(raw)
	if !ok {
		t.Fatal("expected hit")
	}
	if rj != `{"id":"x"}` {
		t.Fatalf("response_json = %q", rj)
	}
	if dist < 0.039 || dist > 0.041 {
		t.Fatalf("distance = %v, want ~0.04", dist)
	}
}

func TestParseSearchResult_Malformed(t *testing.T) {
	// Non-slice — corrupt response.
	_, _, ok := parseSearchResult("not a slice")
	if ok {
		t.Fatal("expected ok=false for non-slice")
	}
	// Slice too short.
	_, _, ok = parseSearchResult([]interface{}{int64(1)})
	if ok {
		t.Fatal("expected ok=false for too-short slice")
	}
}

// --- Interface-level handler tests ---
//
// The handler logic in internal/server uses the SemanticCache interface,
// not the concrete RedisSemanticCache. We exercise the contract here with
// a fake that scripts hit/miss/error scenarios. The real RediSearch impl
// is exercised manually via the README drill — miniredis doesn't speak
// the FT.* commands.

type fakeSemantic struct {
	resp       *provider.Response
	hit        bool
	similarity float64
	err        error
	indexErr   error
}

func (f *fakeSemantic) LookupSimilar(_ context.Context, _ *provider.Request, _ float64) (
	*provider.Response, bool, float64, error,
) {
	return f.resp, f.hit, f.similarity, f.err
}

func (f *fakeSemantic) Index(_ context.Context, _ *provider.Request, _ *provider.Response, _ time.Duration) error {
	return f.indexErr
}

// Compile-time check that fakeSemantic satisfies SemanticCache.
var _ SemanticCache = (*fakeSemantic)(nil)

func TestFakeSatisfiesSemanticCache(t *testing.T) {
	// Smoke test that the fake works through the interface.
	f := &fakeSemantic{hit: true, similarity: 0.97}
	resp, hit, sim, err := f.LookupSimilar(context.Background(), &provider.Request{}, 0.95)
	if err != nil || !hit || sim != 0.97 {
		t.Fatalf("got resp=%v hit=%v sim=%v err=%v", resp, hit, sim, err)
	}
	if err := f.Index(context.Background(), &provider.Request{}, &provider.Response{}, time.Minute); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}
