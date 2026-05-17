// Package openai is a thin adapter over the OpenAI chat-completions API.
// Because the gateway's external schema mirrors OpenAI's, this adapter is
// almost a straight passthrough — it only swaps the auth header and trusts
// the caller to have stripped the "openai/" prefix from the model name.
package openai

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

const providerName = "openai"

const defaultBaseURL = "https://api.openai.com"

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

func (a *Adapter) Name() string { return providerName }

func (a *Adapter) Complete(ctx context.Context, req *provider.Request) (*provider.Response, error) {
	if a.apiKey == "" {
		return nil, provider.ErrNotConfigured
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)

	resp, err := a.client.Do(httpReq)
	if err != nil {
		return nil, &provider.UpstreamError{Provider: providerName, Err: err}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return nil, &provider.UpstreamError{Provider: providerName, Status: resp.StatusCode, Body: string(b)}
	}

	var out provider.Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, &provider.UpstreamError{Provider: providerName, Err: fmt.Errorf("decode response: %w", err)}
	}
	return &out, nil
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
		// Force streaming on the upstream request.
		clone := *req
		clone.Stream = true

		body, err := json.Marshal(&clone)
		if err != nil {
			errs <- err
			return
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			errs <- err
			return
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)
		httpReq.Header.Set("Accept", "text/event-stream")

		resp, err := a.client.Do(httpReq)
		if err != nil {
			errs <- &provider.UpstreamError{Provider: providerName, Err: err}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			errs <- &provider.UpstreamError{Provider: providerName, Status: resp.StatusCode, Body: string(b)}
			return
		}

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
			if payload == "[DONE]" {
				return
			}
			var d provider.StreamDelta
			if err := json.Unmarshal([]byte(payload), &d); err != nil {
				errs <- &provider.UpstreamError{Provider: providerName, Err: fmt.Errorf("parse chunk: %w", err)}
				return
			}
			select {
			case deltas <- d:
			case <-ctx.Done():
				return
			}
		}
	}()

	return deltas, errs
}
