// Package embedder — OpenAI implementation.
//
// Calls POST https://api.openai.com/v1/embeddings with the
// text-embedding-3-small model by default. Errors are returned as
// *provider.UpstreamError so the semantic cache's fail-open logic
// reuses the same error-classification primitives the chat-completions
// path already understands.
package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

const (
	defaultOpenAIBaseURL = "https://api.openai.com"
	defaultOpenAIModel   = "text-embedding-3-small"
	defaultOpenAIDim     = 1536
	providerName         = "openai-embeddings"
)

// OpenAIEmbedder calls OpenAI's /v1/embeddings endpoint.
type OpenAIEmbedder struct {
	apiKey  string
	model   string
	dim     int
	baseURL string
	client  *http.Client
}

type Option func(*OpenAIEmbedder)

func WithModel(model string, dim int) Option {
	return func(o *OpenAIEmbedder) { o.model = model; o.dim = dim }
}
func WithBaseURL(u string) Option          { return func(o *OpenAIEmbedder) { o.baseURL = u } }
func WithHTTPClient(c *http.Client) Option { return func(o *OpenAIEmbedder) { o.client = c } }

// NewOpenAI constructs an embedder. apiKey is required; sensible defaults
// for everything else (model = text-embedding-3-small, dim = 1536,
// 60s HTTP timeout).
func NewOpenAI(apiKey string, opts ...Option) *OpenAIEmbedder {
	e := &OpenAIEmbedder{
		apiKey:  apiKey,
		model:   defaultOpenAIModel,
		dim:     defaultOpenAIDim,
		baseURL: defaultOpenAIBaseURL,
		client:  &http.Client{Timeout: 60 * time.Second},
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *OpenAIEmbedder) Dim() int     { return e.dim }
func (e *OpenAIEmbedder) Name() string { return providerName }

type openAIRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

type openAIData struct {
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type openAIResponse struct {
	Data  []openAIData `json:"data"`
	Model string       `json:"model"`
}

func (e *OpenAIEmbedder) Embed(ctx context.Context, text string) ([]float32, error) {
	if e.apiKey == "" {
		return nil, provider.ErrNotConfigured
	}
	body, err := json.Marshal(openAIRequest{Model: e.model, Input: text})
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/v1/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+e.apiKey)

	resp, err := e.client.Do(httpReq)
	if err != nil {
		return nil, &provider.UpstreamError{Provider: providerName, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, &provider.UpstreamError{Provider: providerName, Status: resp.StatusCode, Body: string(b)}
	}

	var out openAIResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, &provider.UpstreamError{Provider: providerName, Err: fmt.Errorf("decode embeddings response: %w", err)}
	}
	if len(out.Data) == 0 {
		return nil, &provider.UpstreamError{Provider: providerName, Err: fmt.Errorf("empty embeddings response")}
	}
	if len(out.Data[0].Embedding) != e.dim {
		return nil, &provider.UpstreamError{Provider: providerName, Err: fmt.Errorf("unexpected vector dimension: got %d, want %d", len(out.Data[0].Embedding), e.dim)}
	}
	return out.Data[0].Embedding, nil
}
