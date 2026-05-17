package reliability

import (
	"sync"
	"time"
)

// State represents the circuit breaker's current condition.
//
//   - StateClosed:   normal operation; all requests pass.
//   - StateOpen:     all requests rejected; we're letting the upstream rest.
//   - StateHalfOpen: a single probe is allowed; success closes the breaker,
//                    failure re-opens it.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	}
	return "unknown"
}

// BreakerConfig parameterizes the state machine.
type BreakerConfig struct {
	Threshold  int           // consecutive failures before tripping; default 5
	ResetAfter time.Duration // how long Open before allowing a probe; default 30s
	Now        func() time.Time
}

// Breaker is a hand-rolled circuit breaker — one independent state machine
// per upstream. Safe for concurrent use.
type Breaker struct {
	name       string
	threshold  int
	resetAfter time.Duration
	now        func() time.Time

	mu            sync.Mutex
	state         State
	failures      int
	openedAt      time.Time
	halfOpenInUse bool // limit HalfOpen to a single concurrent probe
}

// NewBreaker constructs a closed breaker. Zero-valued config fields fall
// back to sensible defaults (threshold 5, reset 30s, real wall-clock now).
func NewBreaker(name string, cfg BreakerConfig) *Breaker {
	if cfg.Threshold <= 0 {
		cfg.Threshold = 5
	}
	if cfg.ResetAfter <= 0 {
		cfg.ResetAfter = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Breaker{
		name:       name,
		threshold:  cfg.Threshold,
		resetAfter: cfg.ResetAfter,
		now:        cfg.Now,
		state:      StateClosed,
	}
}

// Name returns the breaker's identifier (typically the provider name).
func (b *Breaker) Name() string { return b.name }

// State returns the current state. Thread-safe.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Allow checks whether a request should be permitted through. It also
// advances the state machine from Open to HalfOpen when the reset window
// has elapsed, and reserves the single HalfOpen probe slot.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		return true

	case StateOpen:
		if b.now().Sub(b.openedAt) < b.resetAfter {
			return false
		}
		// Reset window elapsed — transition to HalfOpen and let this caller
		// be the probe.
		b.state = StateHalfOpen
		b.halfOpenInUse = true
		return true

	case StateHalfOpen:
		if b.halfOpenInUse {
			return false
		}
		b.halfOpenInUse = true
		return true
	}
	return false
}

// RecordSuccess marks the most recent call as successful. From HalfOpen this
// closes the breaker; from Closed it resets the failure counter.
func (b *Breaker) RecordSuccess() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.state = StateClosed
	b.halfOpenInUse = false
}

// RecordFailure increments the failure counter. From Closed, hitting the
// threshold trips the breaker to Open. From HalfOpen, any failure trips
// straight back to Open and restarts the reset window.
func (b *Breaker) RecordFailure() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateHalfOpen {
		b.state = StateOpen
		b.openedAt = b.now()
		b.halfOpenInUse = false
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.state = StateOpen
		b.openedAt = b.now()
	}
}
