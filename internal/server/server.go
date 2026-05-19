// Package server wires the chi router, middleware chain, and the
// /v1/chat/completions handler that fronts every upstream provider.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/cache"
	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/ChayanPandit/llm-gateway/internal/reliability"
	"github.com/ChayanPandit/llm-gateway/internal/router"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Config struct {
	GatewayAPIKey       string
	Logger              *slog.Logger
	RequestTimeout      time.Duration          // applied to /v1/* requests
	Breakers            []*reliability.Breaker // for /ready to inspect
	Cache               cache.Cache            // optional; nil disables exact-match caching
	CacheTTL            time.Duration          // ignored if Cache is nil
	SemanticCache       cache.SemanticCache    // optional; nil disables semantic
	SimilarityThreshold float64                // ignored if SemanticCache is nil
}

func New(cfg Config, r *router.Router) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 120 * time.Second
	}

	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.Recoverer)
	mux.Use(requestLogger(cfg.Logger))

	mux.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.Get("/ready", readinessHandler(cfg.Breakers))

	mux.Group(func(g chi.Router) {
		g.Use(authMiddleware(cfg.GatewayAPIKey))
		g.Use(timeoutMiddleware(cfg.RequestTimeout))
		g.Post("/v1/chat/completions", chatCompletionsHandler(r, cfg.Cache, cfg.CacheTTL, cfg.SemanticCache, cfg.SimilarityThreshold, cfg.Logger))
	})

	return mux
}

// timeoutMiddleware bounds how long any single request can hold a goroutine.
// The deadline propagates through context.Context into upstream HTTP calls
// and retry sleeps, so cancellation is honored end-to-end.
func timeoutMiddleware(d time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// readinessHandler returns 200 if at least one provider's breaker is not
// fully Open. Returns 503 only when every upstream is unreachable, signalling
// load balancers / orchestrators to pull this instance out of rotation.
func readinessHandler(breakers []*reliability.Breaker) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if len(breakers) == 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		for _, b := range breakers {
			if b.State() != reliability.StateOpen {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte("ready"))
				return
			}
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no providers ready"))
	}
}

func authMiddleware(expected string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if expected == "" {
				writeError(w, http.StatusInternalServerError, "gateway misconfigured: no API key set")
				return
			}
			auth := r.Header.Get("Authorization")
			const prefix = "Bearer "
			if len(auth) <= len(prefix) || auth[:len(prefix)] != prefix || auth[len(prefix):] != expected {
				writeError(w, http.StatusUnauthorized, "invalid or missing API key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"bytes", ww.BytesWritten(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}

func chatCompletionsHandler(
	rt *router.Router,
	c cache.Cache,
	ttl time.Duration,
	sc cache.SemanticCache,
	threshold float64,
	log *slog.Logger,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req provider.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
			return
		}
		if req.Model == "" || len(req.Messages) == 0 {
			writeError(w, http.StatusBadRequest, "`model` and `messages` are required")
			return
		}

		chain, err := rt.Resolve(req.Model)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		ctx := r.Context()
		if req.Stream {
			// Streams bypass the cache — see internal/cache/cache.go.
			w.Header().Set("X-Cache", "BYPASS")
			streamReq := req
			streamReq.Model = chain.Primary.Model
			streamResponse(ctx, w, chain.Primary.Provider, &streamReq, log)
			return
		}

		// Determine cache eligibility:
		// - Cache must be configured
		// - Request must not opt out via Cache-Control: no-store
		cacheEligible := (c != nil || sc != nil) && !cacheOptOut(r)

		// Tier 1: exact-match cache lookup.
		var cacheKey string
		if cacheEligible && c != nil {
			cacheKey = cache.Key(&req)
			if resp, hit, err := c.Get(ctx, cacheKey); err != nil {
				log.Warn("cache get failed; falling through",
					"err", err.Error(),
					"request_id", middleware.GetReqID(ctx),
				)
			} else if hit {
				w.Header().Set("X-Cache", "HIT")
				w.Header().Set("X-Cache-Key", cacheKey)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
		}

		// Tier 2: semantic-match cache lookup on exact miss.
		if cacheEligible && sc != nil {
			if resp, hit, sim, err := sc.LookupSimilar(ctx, &req, threshold); err != nil {
				log.Warn("semantic cache lookup failed; falling through",
					"err", err.Error(),
					"request_id", middleware.GetReqID(ctx),
				)
			} else if hit {
				w.Header().Set("X-Cache", "SEMANTIC-HIT")
				w.Header().Set("X-Cache-Similarity", fmt.Sprintf("%.4f", sim))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
				return
			} else if sim > 0 {
				// Useful for tuning — log the best candidate even on a miss.
				log.Debug("semantic miss",
					"best_similarity", sim,
					"threshold", threshold,
					"request_id", middleware.GetReqID(ctx),
				)
			}
		}

		// Tier 3: upstream call via failover chain.
		resp, lastErr := tryChain(ctx, chain, &req, log)
		if lastErr != nil {
			handleUpstreamError(w, lastErr)
			return
		}

		// Populate BOTH caches on success. Failures are non-fatal.
		if cacheEligible {
			if c != nil && cacheKey != "" {
				if err := c.Put(ctx, cacheKey, resp, ttl); err != nil {
					log.Warn("cache put failed",
						"err", err.Error(),
						"request_id", middleware.GetReqID(ctx),
					)
				}
				w.Header().Set("X-Cache-Key", cacheKey)
			}
			if sc != nil {
				if err := sc.Index(ctx, &req, resp, ttl); err != nil {
					log.Warn("semantic cache index failed",
						"err", err.Error(),
						"request_id", middleware.GetReqID(ctx),
					)
				}
			}
			w.Header().Set("X-Cache", "MISS")
		} else if c != nil || sc != nil {
			w.Header().Set("X-Cache", "BYPASS")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}

// cacheOptOut reports whether the request asks the gateway to skip the
// cache layer via the standard Cache-Control directive.
func cacheOptOut(r *http.Request) bool {
	cc := r.Header.Get("Cache-Control")
	if cc == "" {
		return false
	}
	for _, tok := range strings.Split(cc, ",") {
		if strings.EqualFold(strings.TrimSpace(tok), "no-store") {
			return true
		}
	}
	return false
}

// tryChain attempts the primary target, then walks fallbacks on failover-
// eligible errors. Returns the last error encountered if every target fails.
func tryChain(ctx context.Context, chain *router.Chain, baseReq *provider.Request, log *slog.Logger) (*provider.Response, error) {
	targets := append([]router.Target{chain.Primary}, chain.Fallback...)
	var lastErr error
	for i, t := range targets {
		// Hand the adapter the unprefixed, upstream-native model name.
		req := *baseReq
		req.Model = t.Model

		resp, err := t.Provider.Complete(ctx, &req)
		if err == nil {
			if i > 0 {
				log.Info("fallback succeeded",
					"provider", t.Provider.Name(),
					"model", t.Model,
					"request_id", middleware.GetReqID(ctx),
				)
			}
			return resp, nil
		}
		lastErr = err

		if !isFailoverEligible(err) || i == len(targets)-1 {
			return nil, err
		}
		log.Warn("primary failed, attempting fallback",
			"provider", t.Provider.Name(),
			"err", err.Error(),
			"request_id", middleware.GetReqID(ctx),
		)
	}
	return nil, lastErr
}

// isFailoverEligible decides whether a Complete error should trigger a
// fallback attempt. Eligible:
//   - ErrCircuitOpen (provider's breaker is rejecting)
//   - *provider.UpstreamError (5xx, 429, transport — already exhausted retries)
//
// NOT eligible:
//   - ErrUnknownModel, ErrNotConfigured (configuration problems)
//   - context cancellation
//   - 4xx client errors
func isFailoverEligible(err error) bool {
	if errors.Is(err, reliability.ErrCircuitOpen) {
		return true
	}
	var ue *provider.UpstreamError
	if errors.As(err, &ue) {
		return ue.Retryable() || ue.Status == 0
	}
	return false
}

func streamResponse(ctx context.Context, w http.ResponseWriter, p provider.Provider, req *provider.Request, log *slog.Logger) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported by transport")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)

	deltas, errs := p.Stream(ctx, req)
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-deltas:
			if !ok {
				// Drain any terminal error, then send [DONE].
				if err, ok := <-errs; ok && err != nil {
					log.Error("stream error", "err", err)
					_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", jsonString(err.Error()))
					flusher.Flush()
					return
				}
				_, _ = w.Write([]byte("data: [DONE]\n\n"))
				flusher.Flush()
				return
			}
			b, err := json.Marshal(d)
			if err != nil {
				log.Error("marshal chunk", "err", err)
				continue
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

type errorEnvelope struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	env := errorEnvelope{}
	env.Error.Message = msg
	env.Error.Type = http.StatusText(status)
	_ = json.NewEncoder(w).Encode(env)
}

func handleUpstreamError(w http.ResponseWriter, err error) {
	var ue *provider.UpstreamError
	switch {
	case errors.Is(err, provider.ErrNotConfigured):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, reliability.ErrCircuitOpen):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusGatewayTimeout, "request timed out")
	case errors.Is(err, provider.ErrUnknownModel):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.As(err, &ue):
		// Map 4xx upstream → 502 from gateway? No — pass-through 4xx for
		// client visibility, treat 5xx as 502 (bad gateway).
		if ue.Status >= 400 && ue.Status < 500 {
			writeError(w, ue.Status, err.Error())
		} else {
			writeError(w, http.StatusBadGateway, err.Error())
		}
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
