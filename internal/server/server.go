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
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/ChayanPandit/llm-gateway/internal/router"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

type Config struct {
	GatewayAPIKey string
	Logger        *slog.Logger
}

func New(cfg Config, r *router.Router) http.Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	mux := chi.NewRouter()
	mux.Use(middleware.RequestID)
	mux.Use(middleware.Recoverer)
	mux.Use(requestLogger(cfg.Logger))

	mux.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.Group(func(g chi.Router) {
		g.Use(authMiddleware(cfg.GatewayAPIKey))
		g.Post("/v1/chat/completions", chatCompletionsHandler(r, cfg.Logger))
	})

	return mux
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

func chatCompletionsHandler(rt *router.Router, log *slog.Logger) http.HandlerFunc {
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

		p, upstreamModel, err := rt.Resolve(req.Model)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// Hand the adapter the unprefixed model name.
		req.Model = upstreamModel

		ctx := r.Context()
		if req.Stream {
			streamResponse(ctx, w, p, &req, log)
			return
		}

		resp, err := p.Complete(ctx, &req)
		if err != nil {
			handleUpstreamError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
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
	switch {
	case errors.Is(err, provider.ErrNotConfigured):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, provider.ErrUnknownModel):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, provider.ErrUpstream):
		writeError(w, http.StatusBadGateway, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
