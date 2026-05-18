// Package embedder defines the contract for turning text into a vector.
// Used by the semantic cache to look up similar prior requests via vector
// similarity search in Redis Stack.
//
// The interface is deliberately tiny so it stays trivial to add new
// backends (local ONNX models, alternative cloud providers) without
// touching the cache or handler code.
package embedder

import "context"

// Embedder turns text into a fixed-dimension float32 vector suitable for
// cosine similarity comparison.
type Embedder interface {
	// Embed returns the embedding for text. The returned vector should be
	// the model's native shape (no manual L2-normalization required;
	// OpenAI's embeddings are already unit-length, and cosine similarity
	// is scale-invariant anyway).
	Embed(ctx context.Context, text string) ([]float32, error)

	// Dim is the vector dimension the implementation produces. Used by
	// callers to size buffers and validate the Redis vector index schema
	// at startup time.
	Dim() int

	// Name identifies the embedder for logging.
	Name() string
}
