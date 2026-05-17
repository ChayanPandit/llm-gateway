// Command gateway is the LLM gateway entrypoint: it reads env-driven
// configuration, builds providers wrapped with reliability decorators,
// wires the HTTP server, and runs with graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/ChayanPandit/llm-gateway/internal/provider/anthropic"
	"github.com/ChayanPandit/llm-gateway/internal/provider/openai"
	"github.com/ChayanPandit/llm-gateway/internal/reliability"
	"github.com/ChayanPandit/llm-gateway/internal/router"
	"github.com/ChayanPandit/llm-gateway/internal/server"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	gatewayKey := os.Getenv("GATEWAY_API_KEY")
	if gatewayKey == "" {
		logger.Error("GATEWAY_API_KEY is required")
		os.Exit(1)
	}

	retryCfg := reliability.RetryConfig{
		MaxAttempts: getEnvInt("RETRY_MAX_ATTEMPTS", 3),
		BaseDelay:   getEnvDuration("RETRY_BASE_DELAY", 100*time.Millisecond),
		MaxDelay:    getEnvDuration("RETRY_MAX_DELAY", 5*time.Second),
	}
	breakerCfg := reliability.BreakerConfig{
		Threshold:  getEnvInt("BREAKER_THRESHOLD", 5),
		ResetAfter: getEnvDuration("BREAKER_RESET", 30*time.Second),
	}
	requestTimeout := getEnvDuration("REQUEST_TIMEOUT", 120*time.Second)

	// Build the raw provider map first; wrap each with reliability decorators
	// next. We keep the breakers list to feed into /ready.
	rawProviders := map[string]provider.Provider{}
	if k := os.Getenv("OPENAI_API_KEY"); k != "" {
		rawProviders["openai"] = openai.New(k)
		logger.Info("registered provider", "name", "openai")
	}
	if k := os.Getenv("ANTHROPIC_API_KEY"); k != "" {
		rawProviders["anthropic"] = anthropic.New(k)
		logger.Info("registered provider", "name", "anthropic")
	}
	if len(rawProviders) == 0 {
		logger.Error("no providers configured (set OPENAI_API_KEY and/or ANTHROPIC_API_KEY)")
		os.Exit(1)
	}

	providers := map[string]provider.Provider{}
	var breakers []*reliability.Breaker
	for name, raw := range rawProviders {
		b := reliability.NewBreaker(name, breakerCfg)
		providers[name] = reliability.Wrap(raw, b, retryCfg, logger)
		breakers = append(breakers, b)
	}

	rt := router.New(providers)
	configureFailover(rt, logger)

	handler := server.New(server.Config{
		GatewayAPIKey:  gatewayKey,
		Logger:         logger,
		RequestTimeout: requestTimeout,
		Breakers:       breakers,
	}, rt)

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	idleClosed := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		logger.Info("shutdown signal received")
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error("shutdown error", "err", err)
		}
		close(idleClosed)
	}()

	logger.Info("listening",
		"addr", addr,
		"retry_max_attempts", retryCfg.MaxAttempts,
		"breaker_threshold", breakerCfg.Threshold,
		"request_timeout", requestTimeout.String(),
	)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-idleClosed
	logger.Info("bye")
}

// configureFailover reads FAILOVER_<PROVIDER> env vars and registers each
// configured fallback. Logs a warning (not fatal) on misconfiguration so
// the gateway still starts with primary-only routing.
func configureFailover(rt *router.Router, log *slog.Logger) {
	for _, e := range os.Environ() {
		const prefix = "FAILOVER_"
		if !strings.HasPrefix(e, prefix) {
			continue
		}
		eq := strings.IndexByte(e, '=')
		if eq < 0 {
			continue
		}
		primary := strings.ToLower(e[len(prefix):eq])
		fallback := e[eq+1:]
		if fallback == "" {
			continue
		}
		if err := rt.SetFallback(primary, fallback); err != nil {
			log.Warn("failover misconfigured, ignoring",
				"primary", primary,
				"fallback", fallback,
				"err", err.Error(),
			)
			continue
		}
		log.Info("failover configured", "primary", primary, "fallback", fallback)
	}
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
