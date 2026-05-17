package reliability

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ChayanPandit/llm-gateway/internal/provider"
)

// fakeProvider lets tests script the responses Complete/Stream return.
type fakeProvider struct {
	name       string
	completeFn func() (*provider.Response, error)
	streamFn   func() (<-chan provider.StreamDelta, <-chan error)
	calls      int32
}

func (f *fakeProvider) Name() string { return f.name }
func (f *fakeProvider) Complete(_ context.Context, _ *provider.Request) (*provider.Response, error) {
	atomic.AddInt32(&f.calls, 1)
	return f.completeFn()
}
func (f *fakeProvider) Stream(_ context.Context, _ *provider.Request) (<-chan provider.StreamDelta, <-chan error) {
	atomic.AddInt32(&f.calls, 1)
	return f.streamFn()
}

func fastRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}
}

func TestComplete_HappyPath(t *testing.T) {
	fp := &fakeProvider{
		name: "fake",
		completeFn: func() (*provider.Response, error) {
			return &provider.Response{ID: "x"}, nil
		},
	}
	b := NewBreaker("fake", BreakerConfig{})
	p := Wrap(fp, b, fastRetry(), nil)

	resp, err := p.Complete(context.Background(), &provider.Request{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.ID != "x" {
		t.Fatalf("resp.ID = %q", resp.ID)
	}
	if atomic.LoadInt32(&fp.calls) != 1 {
		t.Fatalf("calls = %d, want 1", fp.calls)
	}
}

func TestComplete_RetriesTransientErrors(t *testing.T) {
	var n int32
	fp := &fakeProvider{
		name: "fake",
		completeFn: func() (*provider.Response, error) {
			if atomic.AddInt32(&n, 1) < 3 {
				return nil, &provider.UpstreamError{Provider: "fake", Status: 503}
			}
			return &provider.Response{ID: "ok"}, nil
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 10})
	p := Wrap(fp, b, fastRetry(), nil)

	resp, err := p.Complete(context.Background(), &provider.Request{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resp.ID != "ok" {
		t.Fatalf("resp.ID = %q", resp.ID)
	}
	// Three attempts in retry.Do registers as ONE breaker record (success).
	if b.State() != StateClosed {
		t.Fatalf("breaker state = %s, want closed", b.State())
	}
}

func TestComplete_DoesNotRetry4xx(t *testing.T) {
	var n int32
	fp := &fakeProvider{
		name: "fake",
		completeFn: func() (*provider.Response, error) {
			atomic.AddInt32(&n, 1)
			return nil, &provider.UpstreamError{Provider: "fake", Status: 400, Body: "bad request"}
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 10})
	p := Wrap(fp, b, fastRetry(), nil)

	_, err := p.Complete(context.Background(), &provider.Request{})
	if err == nil {
		t.Fatal("expected error")
	}
	if got := atomic.LoadInt32(&n); got != 1 {
		t.Fatalf("4xx should be called exactly once, got %d", got)
	}
}

func TestComplete_TripsBreakerAfterThreshold(t *testing.T) {
	fp := &fakeProvider{
		name: "fake",
		completeFn: func() (*provider.Response, error) {
			// Non-retryable error so each Complete call records exactly one failure.
			return nil, &provider.UpstreamError{Provider: "fake", Status: 400}
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 3})
	p := Wrap(fp, b, fastRetry(), nil)

	for i := 0; i < 3; i++ {
		_, _ = p.Complete(context.Background(), &provider.Request{})
	}
	if b.State() != StateOpen {
		t.Fatalf("breaker state after 3 failures = %s, want open", b.State())
	}

	// 4th call should be rejected by the breaker without hitting the inner provider.
	before := atomic.LoadInt32(&fp.calls)
	_, err := p.Complete(context.Background(), &provider.Request{})
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
	if atomic.LoadInt32(&fp.calls) != before {
		t.Fatal("inner provider should not have been called when breaker is open")
	}
}

func TestStream_RejectsWhenBreakerOpen(t *testing.T) {
	fp := &fakeProvider{
		name: "fake",
		streamFn: func() (<-chan provider.StreamDelta, <-chan error) {
			t.Fatal("inner Stream should not be called when breaker is open")
			return nil, nil
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 1})
	b.RecordFailure() // trip
	p := Wrap(fp, b, fastRetry(), nil)

	_, errs := p.Stream(context.Background(), &provider.Request{})
	err := <-errs
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}
}

func TestStream_RecordsSuccessOnCleanFinish(t *testing.T) {
	fp := &fakeProvider{
		name: "fake",
		streamFn: func() (<-chan provider.StreamDelta, <-chan error) {
			deltas := make(chan provider.StreamDelta, 2)
			errs := make(chan error, 1)
			deltas <- provider.StreamDelta{ID: "1"}
			deltas <- provider.StreamDelta{ID: "2"}
			close(deltas)
			close(errs)
			return deltas, errs
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 2})
	b.RecordFailure() // bring closer to threshold; success should reset
	p := Wrap(fp, b, fastRetry(), nil)

	deltas, errs := p.Stream(context.Background(), &provider.Request{})
	count := 0
	for range deltas {
		count++
	}
	if err := <-errs; err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 deltas, got %d", count)
	}
	if b.State() != StateClosed {
		t.Fatalf("breaker state = %s, want closed", b.State())
	}
}

func TestStream_RecordsFailureOnTerminalError(t *testing.T) {
	fp := &fakeProvider{
		name: "fake",
		streamFn: func() (<-chan provider.StreamDelta, <-chan error) {
			deltas := make(chan provider.StreamDelta)
			errs := make(chan error, 1)
			close(deltas)
			errs <- &provider.UpstreamError{Provider: "fake", Status: 502}
			close(errs)
			return deltas, errs
		},
	}
	b := NewBreaker("fake", BreakerConfig{Threshold: 1})
	p := Wrap(fp, b, fastRetry(), nil)

	_, errs := p.Stream(context.Background(), &provider.Request{})
	if err := <-errs; err == nil {
		t.Fatal("expected upstream error to propagate")
	}
	if b.State() != StateOpen {
		t.Fatalf("breaker state = %s, want open", b.State())
	}
}
