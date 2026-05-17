// Package reliability provides retry, circuit-breaker, and decorator
// primitives that wrap provider.Provider instances to add resilience to
// upstream LLM calls. All primitives are hand-rolled with no external deps.
package reliability

import (
	"context"
	"log/slog"
	"math/rand"
	"time"
)

// RetryConfig describes the exponential-backoff schedule used by Do.
//
// Attempt N (zero-indexed) waits min(MaxDelay, BaseDelay * 2^N) plus ±25%
// random jitter before firing. MaxAttempts is the total attempt count
// including the initial call, so MaxAttempts=3 means: attempt 0 immediately,
// attempt 1 after one backoff, attempt 2 after a second backoff.
type RetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetry is a sensible starting policy: 3 attempts, 100ms base, 5s cap.
var DefaultRetry = RetryConfig{
	MaxAttempts: 3,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    5 * time.Second,
}

// Retryable lets callers signal whether an error is worth another attempt.
// Errors that don't implement this interface are treated as permanent.
type Retryable interface {
	Retryable() bool
}

// Do executes op up to cfg.MaxAttempts times with exponential backoff and
// jitter between attempts. It returns immediately on:
//   - success (op returns nil)
//   - a non-retryable error (one whose Retryable() returns false, or any
//     error that doesn't implement Retryable)
//   - context cancellation
//
// Each retry is logged with attempt number, delay, and any request_id
// already present on the context's slog handler.
func Do(ctx context.Context, cfg RetryConfig, log *slog.Logger, op func() error) error {
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 1
	}
	if log == nil {
		log = slog.Default()
	}

	var lastErr error
	for attempt := 0; attempt < cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			delay := backoffDelay(cfg, attempt)
			log.Info("retry sleeping",
				"attempt", attempt+1,
				"delay_ms", delay.Milliseconds(),
				"prev_err", lastErr.Error(),
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := op()
		if err == nil {
			return nil
		}
		lastErr = err

		// Non-retryable: bail immediately.
		r, ok := err.(Retryable)
		if !ok || !r.Retryable() {
			return err
		}
	}
	return lastErr
}

// backoffDelay computes BaseDelay * 2^(attempt-1) with ±25% jitter,
// capped at MaxDelay. attempt is 1-indexed (the first retry).
func backoffDelay(cfg RetryConfig, attempt int) time.Duration {
	base := cfg.BaseDelay * (1 << (attempt - 1))
	if cfg.MaxDelay > 0 && base > cfg.MaxDelay {
		base = cfg.MaxDelay
	}
	// ±25% jitter — half above, half below the base.
	jitterRange := int64(base / 2)
	if jitterRange <= 0 {
		return base
	}
	jitter := time.Duration(rand.Int63n(jitterRange)) - time.Duration(jitterRange/2)
	return base + jitter
}
