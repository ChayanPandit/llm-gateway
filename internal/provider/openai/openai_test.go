package openai

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

func TestComplete_Passthrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Fatalf("auth header = %q", got)
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "gpt-4o-mini" {
			t.Fatalf("model = %v", body["model"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
            "id":"x","object":"chat.completion","created":1,"model":"gpt-4o-mini",
            "choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
        }`)
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	resp, err := a.Complete(context.Background(), &provider.Request{
		Model:    "gpt-4o-mini",
		Messages: []provider.Message{{Role: "user", Content: "hello"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.Content != "hi" {
		t.Fatalf("content = %q", resp.Choices[0].Message.Content)
	}
}

func TestComplete_UpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	_, err := a.Complete(context.Background(), &provider.Request{
		Model: "gpt-4o-mini", Messages: []provider.Message{{Role: "user", Content: "x"}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UpstreamError, got %T: %v", err, err)
	}
	if ue.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d (want 500)", ue.Status)
	}
	if !ue.Retryable() {
		t.Fatal("500 should be retryable")
	}
}

func TestComplete_ClientErrorNotRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	_, err := a.Complete(context.Background(), &provider.Request{
		Model: "gpt-4o-mini", Messages: []provider.Message{{Role: "user", Content: "x"}},
	})
	var ue *provider.UpstreamError
	if !errors.As(err, &ue) {
		t.Fatalf("expected *UpstreamError, got %T", err)
	}
	if ue.Retryable() {
		t.Fatal("400 should NOT be retryable")
	}
}

func TestStream_ParsesSSE(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		_, _ = io.WriteString(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`+"\n\n")
		flusher.Flush()
		_, _ = io.WriteString(w, `data: {"id":"x","object":"chat.completion.chunk","created":1,"model":"gpt-4o-mini","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":null}]}`+"\n\n")
		flusher.Flush()
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	deltas, errs := a.Stream(context.Background(), &provider.Request{
		Model: "gpt-4o-mini", Messages: []provider.Message{{Role: "user", Content: "x"}}, Stream: true,
	})
	var got []string
	for d := range deltas {
		if len(d.Choices) > 0 {
			got = append(got, d.Choices[0].Delta.Content)
		}
	}
	if err, ok := <-errs; ok && err != nil {
		t.Fatalf("stream err: %v", err)
	}
	if len(got) != 2 || got[1] != "hi" {
		t.Fatalf("chunks = %v", got)
	}
}
