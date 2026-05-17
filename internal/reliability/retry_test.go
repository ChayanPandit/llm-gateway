package reliability

import (
	"context"
	"errors"
	"testing"
	"time"
)

type retryableErr struct {
	msg       string
	retryable bool
}

func (e *retryableErr) Error() string   { return e.msg }
func (e *retryableErr) Retryable() bool { return e.retryable }

func TestDo_SucceedsImmediately(t *testing.T) {
	calls := 0
	err := Do(context.Background(), DefaultRetry, nil, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestDo_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	cfg := RetryConfig{MaxAttempts: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}
	err := Do(context.Background(), cfg, nil, func() error {
		calls++
		if calls < 3 {
			return &retryableErr{msg: "transient", retryable: true}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDo_ExhaustsAttempts(t *testing.T) {
	calls := 0
	cfg := RetryConfig{MaxAttempts: 3, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}
	err := Do(context.Background(), cfg, nil, func() error {
		calls++
		return &retryableErr{msg: "always fails", retryable: true}
	})
	if err == nil {
		t.Fatal("expected error after exhausting attempts")
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestDo_NonRetryableStopsImmediately(t *testing.T) {
	calls := 0
	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}
	err := Do(context.Background(), cfg, nil, func() error {
		calls++
		return &retryableErr{msg: "permanent", retryable: false}
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("non-retryable should stop after 1 call, got %d", calls)
	}
}

func TestDo_UnknownErrorTypeStopsImmediately(t *testing.T) {
	// An error that doesn't implement Retryable is treated as permanent.
	calls := 0
	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: 1 * time.Millisecond, MaxDelay: 5 * time.Millisecond}
	err := Do(context.Background(), cfg, nil, func() error {
		calls++
		return errors.New("plain old error")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("plain error should stop after 1 call, got %d", calls)
	}
}

func TestDo_ContextCancelStopsMidSleep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := RetryConfig{MaxAttempts: 5, BaseDelay: 100 * time.Millisecond, MaxDelay: 500 * time.Millisecond}

	calls := 0
	// Cancel after the first attempt fails, while we're sleeping.
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	err := Do(ctx, cfg, nil, func() error {
		calls++
		return &retryableErr{msg: "transient", retryable: true}
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call before cancel, got %d", calls)
	}
}

func TestBackoffDelay_GrowsExponentiallyWithJitter(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 100 * time.Millisecond, MaxDelay: 10 * time.Second}

	// Attempt 1: base ~= 100ms, jitter window ±50ms → [50, 150]
	for i := 0; i < 20; i++ {
		d := backoffDelay(cfg, 1)
		if d < 50*time.Millisecond || d > 150*time.Millisecond {
			t.Fatalf("attempt 1 delay %v outside [50ms, 150ms]", d)
		}
	}
	// Attempt 4: base = 100ms * 8 = 800ms; jitter ±400ms → [400, 1200]
	for i := 0; i < 20; i++ {
		d := backoffDelay(cfg, 4)
		if d < 400*time.Millisecond || d > 1200*time.Millisecond {
			t.Fatalf("attempt 4 delay %v outside [400ms, 1200ms]", d)
		}
	}
}

func TestBackoffDelay_CapsAtMaxDelay(t *testing.T) {
	cfg := RetryConfig{BaseDelay: 100 * time.Millisecond, MaxDelay: 500 * time.Millisecond}
	// Attempt 10 raw base would be 51.2s; cap is 500ms with ±250ms jitter.
	for i := 0; i < 20; i++ {
		d := backoffDelay(cfg, 10)
		if d > 750*time.Millisecond {
			t.Fatalf("attempt 10 delay %v exceeded cap+jitter", d)
		}
	}
}
