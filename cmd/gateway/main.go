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

	"github.com/ChayanPandit/llm-gateway/internal/cache"
	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/ChayanPandit/llm-gateway/internal/provider/anthropic"
	"github.com/ChayanPandit/llm-gateway/internal/provider/openai"
	"github.com/ChayanPandit/llm-gateway/internal/reliability"
	"github.com/ChayanPandit/llm-gateway/internal/router"
	"github.com/ChayanPandit/llm-gateway/internal/server"
	"github.com/redis/go-redis/v9"
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

	// Cache is optional. If CACHE_ENABLED=true (and REDIS_URL is set), we
	// connect; otherwise the handler runs without caching. Connect failures
	// don't fatal the gateway — log a warning and proceed cache-less so a
	// flaky Redis can't prevent a fresh deploy from coming up.
	var c cache.Cache
	cacheTTL := getEnvDuration("CACHE_TTL", 24*time.Hour)
	if isTruthy(os.Getenv("CACHE_ENABLED")) {
		c = buildRedisCache(logger)
	}

	handler := server.New(server.Config{
		GatewayAPIKey:  gatewayKey,
		Logger:         logger,
		RequestTimeout: requestTimeout,
		Breakers:       breakers,
		Cache:          c,
		CacheTTL:       cacheTTL,
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
		"cache_enabled", c != nil,
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

// buildRedisCache parses REDIS_URL, dials Redis, and returns a Cache.
// Returns nil (with a warning log) on any failure so the gateway can still
// serve traffic without caching when Redis is unavailable.
func buildRedisCache(log *slog.Logger) cache.Cache {
	url := os.Getenv("REDIS_URL")
	if url == "" {
		log.Warn("CACHE_ENABLED is set but REDIS_URL is empty; cache disabled")
		return nil
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		log.Warn("invalid REDIS_URL; cache disabled", "err", err.Error())
		return nil
	}
	rdb := redis.NewClient(opts)

	// Sanity ping with a short deadline so we don't block startup on a
	// wedged Redis. A failure is not fatal — the handler fail-opens on
	// runtime Redis errors anyway.
	pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Warn("Redis ping failed at startup; cache enabled but degraded",
			"redis_url", url, "err", err.Error())
	} else {
		log.Info("Redis cache connected", "redis_url", url)
	}

	ns := os.Getenv("CACHE_NAMESPACE")
	return cache.NewRedisCache(rdb, ns)
}

// isTruthy interprets common boolean env strings.
func isTruthy(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "t", "yes", "y", "on":
		return true
	}
	return false
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
