package reliability

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

// ErrCircuitOpen is returned by a wrapped provider when the breaker is open
// and rejects the call without contacting the upstream. The server handler
// maps this to a 503 response or a failover attempt.
var ErrCircuitOpen = errors.New("circuit breaker open")

// reliableProvider wraps another Provider and adds:
//   - circuit-breaker gating on every call
//   - retry-with-backoff on Complete for retryable errors
//
// Streams are NOT retried: once an SSE chunk has been written to the client
// you can't un-send it, and starting a second upstream call mid-stream would
// confuse the consumer. Streams get only the breaker check up-front and a
// success/failure record at the end.
type reliableProvider struct {
	inner   provider.Provider
	breaker *Breaker
	retry   RetryConfig
	log     *slog.Logger
}

// Wrap decorates p with retry + circuit-breaker semantics. The returned
// Provider satisfies the same interface, so it slots into the router map
// in cmd/gateway/main.go without further changes.
func Wrap(p provider.Provider, b *Breaker, r RetryConfig, log *slog.Logger) provider.Provider {
	if log == nil {
		log = slog.Default()
	}
	return &reliableProvider{inner: p, breaker: b, retry: r, log: log}
}

func (r *reliableProvider) Name() string { return r.inner.Name() }

func (r *reliableProvider) Complete(ctx context.Context, req *provider.Request) (*provider.Response, error) {
	if !r.breaker.Allow() {
		r.log.Warn("circuit open, rejecting",
			"provider", r.inner.Name(),
			"breaker_state", r.breaker.State().String(),
		)
		return nil, ErrCircuitOpen
	}

	var resp *provider.Response
	err := Do(ctx, r.retry, r.log, func() error {
		var callErr error
		resp, callErr = r.inner.Complete(ctx, req)
		return callErr
	})

	if err != nil {
		r.breaker.RecordFailure()
		r.log.Warn("provider call failed",
			"provider", r.inner.Name(),
			"err", err.Error(),
			"breaker_state", r.breaker.State().String(),
		)
		return nil, err
	}
	r.breaker.RecordSuccess()
	return resp, nil
}

func (r *reliableProvider) Stream(ctx context.Context, req *provider.Request) (<-chan provider.StreamDelta, <-chan error) {
	if !r.breaker.Allow() {
		r.log.Warn("circuit open, rejecting stream",
			"provider", r.inner.Name(),
			"breaker_state", r.breaker.State().String(),
		)
		deltas := make(chan provider.StreamDelta)
		errs := make(chan error, 1)
		close(deltas)
		errs <- ErrCircuitOpen
		close(errs)
		return deltas, errs
	}

	innerDeltas, innerErrs := r.inner.Stream(ctx, req)

	// Wrap the inner channels so we can record success/failure when the
	// stream terminates. The outer channels mirror the inner ones 1:1.
	deltas := make(chan provider.StreamDelta, 16)
	errs := make(chan error, 1)

	go func() {
		defer close(deltas)
		defer close(errs)

		for d := range innerDeltas {
			select {
			case deltas <- d:
			case <-ctx.Done():
				r.breaker.RecordFailure()
				return
			}
		}

		// Inner deltas channel closed; check for a terminal error.
		var terminal error
		select {
		case e, ok := <-innerErrs:
			if ok {
				terminal = e
			}
		default:
		}

		if terminal != nil {
			r.breaker.RecordFailure()
			errs <- terminal
		} else {
			r.breaker.RecordSuccess()
		}
	}()

	return deltas, errs
}
