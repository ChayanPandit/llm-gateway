// Command gateway is the LLM gateway entrypoint: it builds providers from
// environment configuration, wires the HTTP server, and runs with graceful
// shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
	"github.com/ChayanPandit/llm-gateway/internal/provider/anthropic"
	"github.com/ChayanPandit/llm-gateway/internal/provider/openai"
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

	providers := map[string]provider.Provider{}
	if k := os.Getenv("OPENAI_API_KEY"); k != "" {
		providers["openai"] = openai.New(k)
		logger.Info("registered provider", "name", "openai")
	}
	if k := os.Getenv("ANTHROPIC_API_KEY"); k != "" {
		providers["anthropic"] = anthropic.New(k)
		logger.Info("registered provider", "name", "anthropic")
	}
	if len(providers) == 0 {
		logger.Error("no providers configured (set OPENAI_API_KEY and/or ANTHROPIC_API_KEY)")
		os.Exit(1)
	}

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	rt := router.New(providers)
	handler := server.New(server.Config{GatewayAPIKey: gatewayKey, Logger: logger}, rt)

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

	logger.Info("listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("server failed", "err", err)
		os.Exit(1)
	}
	<-idleClosed
	logger.Info("bye")
}
