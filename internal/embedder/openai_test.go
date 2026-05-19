package embedder

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

func TestOpenAIEmbed_HappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("auth header = %q", got)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "text-embedding-3-small" {
			t.Fatalf("model = %v", body["model"])
		}
		if body["input"] != "hello world" {
			t.Fatalf("input = %v", body["input"])
		}

		// Build a fake 1536-dim vector.
		vec := make([]float32, 1536)
		for i := range vec {
			vec[i] = 0.001 * float32(i)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResponse{
			Model: "text-embedding-3-small",
			Data:  []openAIData{{Embedding: vec, Index: 0}},
		})
	}))
	defer srv.Close()

	e := NewOpenAI("test-key", WithBaseURL(srv.URL))
	v, err := e.Embed(context.Background(), "hello world")
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 1536 {
		t.Fatalf("vector length = %d, want 1536", len(v))
	}
	if e.Dim() != 1536 {
		t.Fatalf("Dim() = %d, want 1536", e.Dim())
	}
}

func TestOpenAIEmbed_NotConfigured(t *testing.T) {
	e := NewOpenAI("") // empty key
	_, err := e.Embed(context.Background(), "x")
	if !errors.Is(err, provider.ErrNotConfigured) {
		t.Fatalf("expected ErrNotConfigured, got %v", err)
	}
}

func TestOpenAIEmbed_UpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "rate limit", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	e := NewOpenAI("test-key", WithBaseURL(srv.URL))
	_, err := e.Embed(context.Background(), "x")
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UpstreamError, got %T", err)
	}
	if ue.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d", ue.Status)
	}
	if !ue.Retryable() {
		t.Fatal("429 should be retryable")
	}
}

func TestOpenAIEmbed_NetworkError(t *testing.T) {
	// Point at a closed server to simulate a network failure.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close()

	e := NewOpenAI("test-key", WithBaseURL(srv.URL))
	_, err := e.Embed(context.Background(), "x")
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UpstreamError, got %T: %v", err, err)
	}
	if ue.Status != 0 {
		t.Fatalf("status = %d, want 0 (transport error)", ue.Status)
	}
	if !ue.Retryable() {
		t.Fatal("transport errors should be retryable")
	}
}

func TestOpenAIEmbed_DimensionMismatch(t *testing.T) {
	// Server returns a 100-dim vector but we expect 1536.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		vec := make([]float32, 100)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openAIResponse{
			Data: []openAIData{{Embedding: vec, Index: 0}},
		})
	}))
	defer srv.Close()

	e := NewOpenAI("test-key", WithBaseURL(srv.URL))
	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("expected dimension mismatch error")
	}
}

func TestOpenAIEmbed_EmptyData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer srv.Close()

	e := NewOpenAI("test-key", WithBaseURL(srv.URL))
	_, err := e.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error for empty data array")
	}
}
