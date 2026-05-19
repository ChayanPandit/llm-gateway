package cache

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/embedder"
	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/redis/go-redis/v9"
)

// SemanticCache matches requests by *meaning* rather than bytes. The
// implementation embeds the last user message into a vector, ANN-searches
// a Redis Stack (RediSearch) HNSW index for the closest stored entry, and
// returns the cached response if cosine similarity meets the threshold.
//
// Two operations are separate (not bundled into the existing Cache
// interface) because LookupSimilar takes a similarity threshold and
// returns the matched score — neither makes sense for exact-match.
type SemanticCache interface {
	// LookupSimilar embeds req's last user message and returns the nearest
	// stored response if its cosine similarity to the query >= threshold.
	// On miss, returns (nil, false, similarity, nil) — similarity reports
	// the best candidate even when it failed the threshold, useful for
	// debugging and tuning.
	LookupSimilar(ctx context.Context, req *provider.Request, threshold float64) (
		resp *provider.Response,
		hit bool,
		similarity float64,
		err error,
	)

	// Index embeds req's last user message and stores (vector → response)
	// in the search index with the given TTL.
	Index(ctx context.Context, req *provider.Request, resp *provider.Response, ttl time.Duration) error
}

// RedisSemanticCache implements SemanticCache against Redis Stack's
// RediSearch module. Vectors are stored as FLOAT32 BLOBs inside hashes;
// the HNSW index supports cosine-distance KNN queries.
type RedisSemanticCache struct {
	rdb       *redis.Client
	emb       embedder.Embedder
	namespace string // key prefix, e.g. "llmcache:semantic:"
	indexName string // e.g. "llmcache:semantic:idx"
}

// NewRedisSemanticCache constructs the cache. Caller must invoke
// EnsureIndex once at startup to create the HNSW index on the running
// Redis Stack instance (idempotent).
func NewRedisSemanticCache(rdb *redis.Client, emb embedder.Embedder, namespace string) *RedisSemanticCache {
	if namespace == "" {
		namespace = "llmcache:semantic:"
	}
	return &RedisSemanticCache{
		rdb:       rdb,
		emb:       emb,
		namespace: namespace,
		indexName: strings.TrimSuffix(namespace, ":") + ":idx",
	}
}

// EnsureIndex creates the RediSearch HNSW index if it doesn't already
// exist. Errors from a pre-existing index are swallowed (idempotent).
// Other failures (Redis Stack not running, missing RediSearch module)
// are returned so main.go can decide whether to disable the semantic
// cache and log a warning.
func (c *RedisSemanticCache) EnsureIndex(ctx context.Context) error {
	args := []interface{}{
		"FT.CREATE", c.indexName,
		"ON", "HASH",
		"PREFIX", "1", c.namespace,
		"SCHEMA",
		"embedding", "VECTOR", "HNSW", "6",
		"TYPE", "FLOAT32",
		"DIM", c.emb.Dim(),
		"DISTANCE_METRIC", "COSINE",
		"response_json", "TEXT",
	}
	err := c.rdb.Do(ctx, args...).Err()
	if err == nil {
		return nil
	}
	// "Index already exists" is fine — we're idempotent.
	if strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return nil
	}
	return fmt.Errorf("FT.CREATE: %w", err)
}

func (c *RedisSemanticCache) LookupSimilar(
	ctx context.Context,
	req *provider.Request,
	threshold float64,
) (*provider.Response, bool, float64, error) {
	text := lastUserMessage(req)
	if text == "" {
		return nil, false, 0, nil
	}

	vec, err := c.emb.Embed(ctx, text)
	if err != nil {
		return nil, false, 0, fmt.Errorf("embed: %w", err)
	}
	blob := vectorToBytes(vec)

	// "*=>[KNN 1 @embedding $vec AS score]" returns the single nearest doc.
	// COSINE distance: smaller = more similar; similarity = 1 - distance.
	args := []interface{}{
		"FT.SEARCH", c.indexName,
		"*=>[KNN 1 @embedding $vec AS score]",
		"PARAMS", "2", "vec", blob,
		"RETURN", "2", "response_json", "score",
		"SORTBY", "score",
		"DIALECT", "2",
	}
	raw, err := c.rdb.Do(ctx, args...).Result()
	if err != nil {
		return nil, false, 0, fmt.Errorf("FT.SEARCH: %w", err)
	}

	respJSON, score, ok := parseSearchResult(raw)
	if !ok {
		// Empty result set — index has no entries yet, or no neighbors.
		return nil, false, 0, nil
	}
	similarity := 1.0 - score
	if similarity < threshold {
		return nil, false, similarity, nil
	}

	var resp provider.Response
	if err := json.Unmarshal([]byte(respJSON), &resp); err != nil {
		// Corrupt cache entry — treat as a miss; the upstream call will
		// repopulate clean data on success.
		return nil, false, similarity, fmt.Errorf("decode cached response: %w", err)
	}
	return &resp, true, similarity, nil
}

func (c *RedisSemanticCache) Index(
	ctx context.Context,
	req *provider.Request,
	resp *provider.Response,
	ttl time.Duration,
) error {
	text := lastUserMessage(req)
	if text == "" {
		return nil
	}
	vec, err := c.emb.Embed(ctx, text)
	if err != nil {
		return fmt.Errorf("embed: %w", err)
	}
	respJSON, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("marshal response: %w", err)
	}

	// Reuse the exact-match key as the document ID so identical inputs
	// overwrite rather than duplicate.
	docKey := c.namespace + Key(req)

	if err := c.rdb.HSet(ctx, docKey,
		"embedding", vectorToBytes(vec),
		"response_json", string(respJSON),
	).Err(); err != nil {
		return fmt.Errorf("HSET: %w", err)
	}
	if ttl > 0 {
		if err := c.rdb.Expire(ctx, docKey, ttl).Err(); err != nil {
			return fmt.Errorf("EXPIRE: %w", err)
		}
	}
	return nil
}

// lastUserMessage extracts the most recent "user" role message's content.
// Returns "" if no user message is present (the caller should treat that
// as a miss / no-op).
func lastUserMessage(req *provider.Request) string {
	for i := len(req.Messages) - 1; i >= 0; i-- {
		if req.Messages[i].Role == "user" {
			return req.Messages[i].Content
		}
	}
	return ""
}

// vectorToBytes encodes []float32 as little-endian raw bytes for
// RediSearch. Redis Stack expects this exact wire shape for FLOAT32
// vector fields.
func vectorToBytes(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}

// parseSearchResult walks RediSearch's FT.SEARCH response shape:
//
//	[total_results, doc_key_1, [field_1, value_1, field_2, value_2, ...], ...]
//
// We requested two fields per doc (response_json + score), one result, so
// the field array should have four elements.
func parseSearchResult(raw interface{}) (responseJSON string, distance float64, ok bool) {
	outer, isSlice := raw.([]interface{})
	if !isSlice || len(outer) < 3 {
		return "", 0, false
	}
	// outer[0] is the total result count; non-zero means we have a doc.
	if total, _ := outer[0].(int64); total == 0 {
		return "", 0, false
	}
	// outer[1] is the doc key; outer[2] is the fields array.
	fields, ok := outer[2].([]interface{})
	if !ok {
		return "", 0, false
	}
	var rj, sc string
	for i := 0; i+1 < len(fields); i += 2 {
		name, _ := fields[i].(string)
		val, _ := fields[i+1].(string)
		switch name {
		case "response_json":
			rj = val
		case "score":
			sc = val
		}
	}
	if rj == "" || sc == "" {
		return "", 0, false
	}
	var d float64
	if _, err := fmt.Sscanf(sc, "%f", &d); err != nil {
		return "", 0, false
	}
	return rj, d, true
}

// Compile-time check: RedisSemanticCache satisfies SemanticCache.
var _ SemanticCache = (*RedisSemanticCache)(nil)
