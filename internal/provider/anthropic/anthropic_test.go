package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

func TestComplete_TranslatesEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("x-api-key"); got != "test-key" {
			t.Fatalf("api-key header = %q", got)
		}
		if r.Header.Get("anthropic-version") == "" {
			t.Fatal("missing anthropic-version header")
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["system"] != "you are helpful" {
			t.Fatalf("system = %v", body["system"])
		}
		if _, ok := body["max_tokens"]; !ok {
			t.Fatal("max_tokens missing (Anthropic requires it)")
		}
		msgs, _ := body["messages"].([]any)
		if len(msgs) != 1 {
			t.Fatalf("messages len = %d", len(msgs))
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
            "id":"msg_1","model":"claude-haiku-4-5","stop_reason":"end_turn",
            "content":[{"type":"text","text":"hi there"}],
            "usage":{"input_tokens":5,"output_tokens":3}
        }`)
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	resp, err := a.Complete(context.Background(), &provider.Request{
		Model: "claude-haiku-4-5",
		Messages: []provider.Message{
			{Role: "system", Content: "you are helpful"},
			{Role: "user", Content: "hi"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.Content != "hi there" {
		t.Fatalf("content = %q", resp.Choices[0].Message.Content)
	}
	if resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("finish_reason = %q (want \"stop\")", resp.Choices[0].FinishReason)
	}
	if resp.Usage.TotalTokens != 8 {
		t.Fatalf("total tokens = %d (want 8)", resp.Usage.TotalTokens)
	}
}

func TestStream_FoldsAnthropicEvents(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		write := func(s string) { _, _ = io.WriteString(w, s); flusher.Flush() }
		write(`event: message_start` + "\n" + `data: {"type":"message_start","message":{"id":"msg_1","model":"claude-haiku-4-5"}}` + "\n\n")
		write(`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello "}}` + "\n\n")
		write(`event: content_block_delta` + "\n" + `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"world"}}` + "\n\n")
		write(`event: message_delta` + "\n" + `data: {"type":"message_delta","delta":{"stop_reason":"end_turn"}}` + "\n\n")
		write(`event: message_stop` + "\n" + `data: {"type":"message_stop"}` + "\n\n")
	}))
	defer srv.Close()

	a := New("test-key", WithBaseURL(srv.URL))
	deltas, errs := a.Stream(context.Background(), &provider.Request{
		Model:    "claude-haiku-4-5",
		Messages: []provider.Message{{Role: "user", Content: "hi"}},
		Stream:   true,
	})
	var content string
	var finishReason string
	for d := range deltas {
		if len(d.Choices) == 0 {
			continue
		}
		content += d.Choices[0].Delta.Content
		if d.Choices[0].FinishReason != nil {
			finishReason = *d.Choices[0].FinishReason
		}
	}
	if err, ok := <-errs; ok && err != nil {
		t.Fatalf("stream err: %v", err)
	}
	if content != "hello world" {
		t.Fatalf("content = %q", content)
	}
	if finishReason != "stop" {
		t.Fatalf("finish_reason = %q", finishReason)
	}
}
