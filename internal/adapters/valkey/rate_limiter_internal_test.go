package valkey

import (
	"container/list"
	"context"
	"testing"

	"github.com/crownroutes/payment-service/config"
)

func TestRateLimiter_RequiresConsecutiveFailuresToFlip(t *testing.T) {
	r := &RateLimiter{available: true, failureThreshold: 3}

	r.recordValkeyFailure()
	r.recordValkeyFailure()
	if !r.IsAvailable() {
		t.Fatal("below the failure threshold the limiter must stay on Valkey")
	}

	r.recordValkeyFailure()
	if r.IsAvailable() {
		t.Fatal("reaching the failure threshold should flip to local fallback")
	}
}

func TestRateLimiter_SuccessResetsFailureCount(t *testing.T) {
	r := &RateLimiter{available: true, failureThreshold: 2}

	r.recordValkeyFailure()
	r.recordValkeySuccess()
	r.recordValkeyFailure()

	if !r.IsAvailable() {
		t.Fatal("an interleaved success should reset the consecutive-failure count")
	}
}

func TestRateLimiter_MarkAvailableClearsFailureStreak(t *testing.T) {
	r := &RateLimiter{available: false, failureThreshold: 2, consecutiveFailures: 5}
	r.MarkAvailable()
	if r.consecutiveFailures != 0 {
		t.Errorf("recovery should reset the failure counter, got %d", r.consecutiveFailures)
	}
}

// Zero health-check interval must not panic in NewTicker, and Close must be
// safe to call twice. valkey-go connects eagerly, so we skip New() and
// construct the RateLimiter directly — the test only exercises the timer path.
func TestNewRateLimiter_ZeroIntervalDoesNotPanicAndCloses(t *testing.T) {
	rl := &RateLimiter{
		stopHealth:       make(chan struct{}),
		failureThreshold: defaultValkeyFailureThreshold,
	}
	rl.Close()
	rl.Close() // idempotent
}

func TestRateLimiter_LocalBucketPicksUpNewLimits(t *testing.T) {
	r := &RateLimiter{
		available: false,
		lruOrder:  list.New(),
		cfg:       config.RateLimitConfig{FallbackMultiplier: 1, LocalMaxBuckets: 100},
	}
	ctx := context.Background()

	r.Allow(ctx, "u", "m", "ip", 10, 5)
	r.Allow(ctx, "u", "m", "ip", 2, 1)

	entry, ok := r.localBuckets.Load("u")
	if !ok {
		t.Fatal("expected a local bucket for the user dimension")
	}
	if got := entry.(*lruBucketEntry).bucket.capacity; got != 2 {
		t.Errorf("local bucket should adopt the new capacity (2), got %v", got)
	}
}
