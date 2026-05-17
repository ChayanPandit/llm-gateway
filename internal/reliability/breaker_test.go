package reliability

import (
	"sync"
	"testing"
	"time"
)

// fakeClock lets tests advance time deterministically.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestBreaker(threshold int, resetAfter time.Duration) (*Breaker, *fakeClock) {
	clock := &fakeClock{now: time.Unix(0, 0)}
	b := NewBreaker("test", BreakerConfig{
		Threshold:  threshold,
		ResetAfter: resetAfter,
		Now:        clock.Now,
	})
	return b, clock
}

func TestBreaker_StartsClosed(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
}

func TestBreaker_TripsAfterThreshold(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	for i := 0; i < 2; i++ {
		b.RecordFailure()
		if b.State() != StateClosed {
			t.Fatalf("state after %d failures = %s, want closed", i+1, b.State())
		}
	}
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatalf("state after 3 failures = %s, want open", b.State())
	}
	if b.Allow() {
		t.Fatal("open breaker should reject")
	}
}

func TestBreaker_SuccessResetsCounter(t *testing.T) {
	b, _ := newTestBreaker(3, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	b.RecordSuccess() // resets counter
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateClosed {
		t.Fatalf("state = %s, want closed (success should have reset counter)", b.State())
	}
}

func TestBreaker_OpenTransitionsToHalfOpenAfterReset(t *testing.T) {
	b, clock := newTestBreaker(2, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatalf("state = %s, want open", b.State())
	}

	// Before reset window elapses → still rejects.
	clock.Advance(500 * time.Millisecond)
	if b.Allow() {
		t.Fatal("should reject before reset window")
	}

	// After reset window → first Allow transitions to HalfOpen and returns true.
	clock.Advance(600 * time.Millisecond)
	if !b.Allow() {
		t.Fatal("should allow probe after reset window")
	}
	if b.State() != StateHalfOpen {
		t.Fatalf("state = %s, want half-open", b.State())
	}

	// Concurrent second Allow in HalfOpen should be rejected — only one probe.
	if b.Allow() {
		t.Fatal("second concurrent HalfOpen probe should be rejected")
	}
}

func TestBreaker_HalfOpenSuccessCloses(t *testing.T) {
	b, clock := newTestBreaker(2, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	clock.Advance(2 * time.Second)
	b.Allow() // → HalfOpen probe

	b.RecordSuccess()
	if b.State() != StateClosed {
		t.Fatalf("state after HalfOpen success = %s, want closed", b.State())
	}
	if !b.Allow() {
		t.Fatal("closed breaker should allow")
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	b, clock := newTestBreaker(2, time.Second)
	b.RecordFailure()
	b.RecordFailure()
	clock.Advance(2 * time.Second)
	b.Allow() // → HalfOpen probe

	b.RecordFailure()
	if b.State() != StateOpen {
		t.Fatalf("state after HalfOpen failure = %s, want open", b.State())
	}

	// Reset window restarts — should still reject just after.
	clock.Advance(500 * time.Millisecond)
	if b.Allow() {
		t.Fatal("should still reject within new reset window")
	}
}
