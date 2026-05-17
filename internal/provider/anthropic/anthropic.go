// Package anthropic adapts the gateway's OpenAI-shaped request/response
// envelope onto Anthropic's Messages API (POST /v1/messages).
//
// Translation rules:
//   - "system" role messages are concatenated and lifted into the top-level
//     `system` field; everything else maps 1:1 to the messages array.
//   - max_tokens is required by Anthropic; we default to 4096 if absent.
//   - Streaming events (message_start, content_block_delta, message_delta,
//     message_stop) are folded back into OpenAI-shaped StreamDelta chunks
//     so downstream clients see one consistent SSE shape.
package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

const (
	defaultBaseURL   = "https://api.anthropic.com"
	apiVersion       = "2023-06-01"
	defaultMaxTokens = 4096
)

type Adapter struct {
	apiKey  string
	baseURL string
	client  *http.Client
}

type Option func(*Adapter)

func WithBaseURL(u string) Option          { return func(a *Adapter) { a.baseURL = u } }
func WithHTTPClient(c *http.Client) Option { return func(a *Adapter) { a.client = c } }

func New(apiKey string, opts ...Option) *Adapter {
	a := &Adapter{
		apiKey:  apiKey,
		baseURL: defaultBaseURL,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
	for _, o := range opts {
		o(a)
	}
	return a
}

func (a *Adapter) Name() string { return "anthropic" }

type anthMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthRequest struct {
	Model       string        `json:"model"`
	MaxTokens   int           `json:"max_tokens"`
	System      string        `json:"system,omitempty"`
	Messages    []anthMessage `json:"messages"`
	Temperature *float64      `json:"temperature,omitempty"`
	TopP        *float64      `json:"top_p,omitempty"`
	Stream      bool          `json:"stream,omitempty"`
}

type anthContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type anthResponse struct {
	ID         string             `json:"id"`
	Model      string             `json:"model"`
	Content    []anthContentBlock `json:"content"`
	StopReason string             `json:"stop_reason"`
	Usage      anthUsage          `json:"usage"`
}

func translateRequest(req *provider.Request) *anthRequest {
	var systemParts []string
	msgs := make([]anthMessage, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == "system" {
			systemParts = append(systemParts, m.Content)
			continue
		}
		msgs = append(msgs, anthMessage{Role: m.Role, Content: m.Content})
	}
	maxTokens := defaultMaxTokens
	if req.MaxTokens != nil {
		maxTokens = *req.MaxTokens
	}
	return &anthRequest{
		Model:       req.Model,
		MaxTokens:   maxTokens,
		System:      strings.Join(systemParts, "\n\n"),
		Messages:    msgs,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		Stream:      req.Stream,
	}
}

func mapStopReason(anth string) string {
	switch anth {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	default:
		return anth
	}
}

func (a *Adapter) Complete(ctx context.Context, req *provider.Request) (*provider.Response, error) {
	if a.apiKey == "" {
		return nil, provider.ErrNotConfigured
	}
	upstreamReq := translateRequest(req)
	upstreamReq.Stream = false

	body, err := json.Marshal(upstreamReq)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", apiVersion)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", provider.ErrUpstream, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%w: status=%d body=%s", provider.ErrUpstream, resp.StatusCode, string(b))
	}

	var ar anthResponse
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return nil, fmt.Errorf("%w: decode: %v", provider.ErrUpstream, err)
	}

	var text strings.Builder
	for _, c := range ar.Content {
		if c.Type == "text" {
			text.WriteString(c.Text)
		}
	}

	return &provider.Response{
		ID:      ar.ID,
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   ar.Model,
		Choices: []provider.Choice{{
			Index:        0,
			Message:      provider.Message{Role: "assistant", Content: text.String()},
			FinishReason: mapStopReason(ar.StopReason),
		}},
		Usage: provider.Usage{
			PromptTokens:     ar.Usage.InputTokens,
			CompletionTokens: ar.Usage.OutputTokens,
			TotalTokens:      ar.Usage.InputTokens + ar.Usage.OutputTokens,
		},
	}, nil
}

// Anthropic SSE event shapes we care about.
type anthStreamEvent struct {
	Type    string          `json:"type"`
	Index   int             `json:"index"`
	Delta   json.RawMessage `json:"delta"`
	Message json.RawMessage `json:"message"`
}

type anthTextDelta struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type anthMessageDeltaPayload struct {
	StopReason string    `json:"stop_reason"`
	Usage      anthUsage `json:"usage"`
}

type anthMessageStartPayload struct {
	ID    string `json:"id"`
	Model string `json:"model"`
}

func (a *Adapter) Stream(ctx context.Context, req *provider.Request) (<-chan provider.StreamDelta, <-chan error) {
	deltas := make(chan provider.StreamDelta, 16)
	errs := make(chan error, 1)

	go func() {
		defer close(deltas)
		defer close(errs)

		if a.apiKey == "" {
			errs <- provider.ErrNotConfigured
			return
		}
		upstreamReq := translateRequest(req)
		upstreamReq.Stream = true

		body, err := json.Marshal(upstreamReq)
		if err != nil {
			errs <- err
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/messages", bytes.NewReader(body))
		if err != nil {
			errs <- err
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("x-api-key", a.apiKey)
		httpReq.Header.Set("anthropic-version", apiVersion)
		httpReq.Header.Set("Accept", "text/event-stream")

		resp, err := a.client.Do(httpReq)
		if err != nil {
			errs <- fmt.Errorf("%w: %v", provider.ErrUpstream, err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			errs <- fmt.Errorf("%w: status=%d body=%s", provider.ErrUpstream, resp.StatusCode, string(b))
			return
		}

		var (
			id      string
			model   = req.Model
			created = time.Now().Unix()
		)

		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					errs <- err
				}
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			payload := strings.TrimPrefix(line, "data: ")

			var ev anthStreamEvent
			if err := json.Unmarshal([]byte(payload), &ev); err != nil {
				errs <- fmt.Errorf("%w: parse event: %v", provider.ErrUpstream, err)
				return
			}

			switch ev.Type {
			case "message_start":
				var ms struct {
					Message anthMessageStartPayload `json:"message"`
				}
				_ = json.Unmarshal([]byte(payload), &ms)
				id = ms.Message.ID
				if ms.Message.Model != "" {
					model = ms.Message.Model
				}
				// Emit an initial role chunk to match OpenAI conventions.
				deltas <- provider.StreamDelta{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []provider.DeltaChoice{{
						Index: 0,
						Delta: provider.Message{Role: "assistant"},
					}},
				}
			case "content_block_delta":
				var td anthTextDelta
				_ = json.Unmarshal(ev.Delta, &td)
				if td.Type != "text_delta" || td.Text == "" {
					continue
				}
				deltas <- provider.StreamDelta{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []provider.DeltaChoice{{
						Index: 0,
						Delta: provider.Message{Content: td.Text},
					}},
				}
			case "message_delta":
				var md anthMessageDeltaPayload
				_ = json.Unmarshal(ev.Delta, &md)
				if md.StopReason == "" {
					continue
				}
				reason := mapStopReason(md.StopReason)
				deltas <- provider.StreamDelta{
					ID: id, Object: "chat.completion.chunk", Created: created, Model: model,
					Choices: []provider.DeltaChoice{{
						Index:        0,
						Delta:        provider.Message{},
						FinishReason: &reason,
					}},
				}
			case "message_stop":
				return
			}
		}
	}()

	return deltas, errs
}
