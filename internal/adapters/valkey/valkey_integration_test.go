//go:build integration

package valkey_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/valkey"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func rateLimiter(c *valkey.Client) *valkey.RateLimiter {
	return valkey.NewRateLimiter(c, config.RateLimitConfig{
		FallbackMultiplier:  0.5,
		LocalMaxBuckets:     1000,
		HealthCheckInterval: time.Second,
	}, nil, nil)
}

func TestRateLimiter_AdmitsUpToCapacityThenDenies(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 5
	for i := 0; i < capacity; i++ {
		if res := rl.Allow(ctx, "u1", "m1", "ip1", capacity, 0.0001); !res.Allowed {
			t.Fatalf("request %d should be allowed within capacity", i+1)
		}
	}
	res := rl.Allow(ctx, "u1", "m1", "ip1", capacity, 0.0001)
	if res.Allowed {
		t.Fatal("request beyond capacity should be denied")
	}
	if res.RetryAfter <= 0 {
		t.Error("denied request should report a positive RetryAfter")
	}
}

func TestRateLimiter_ConcurrentNoOverAdmit(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 10
	const goroutines = 50

	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow(ctx, "uc", "mc", "ipc", capacity, 0.0001).Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()

	if allowed != capacity {
		t.Errorf("expected exactly %d admitted under contention, got %d", capacity, allowed)
	}
}

func TestRateLimiter_RefillsOverTime(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	if !rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Fatal("second immediate request should be denied")
	}
	time.Sleep(50 * time.Millisecond)
	if !rl.Allow(ctx, "ur", "mr", "ipr", 1, 100).Allowed {
		t.Error("request after refill window should be allowed")
	}
}

func TestCircuitBreaker_ValidTransitionChain(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	store := valkey.NewCircuitBreakerStore(c)
	ctx := context.Background()
	const gw = "stripe"

	cb, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if cb.State != gateway.StateClosed {
		t.Fatalf("fresh breaker should be CLOSED, got %s", cb.State)
	}

	if err := store.Transition(ctx, cb, gateway.StateOpen); err != nil {
		t.Fatalf("CLOSED -> OPEN: %v", err)
	}
	if cb.State != gateway.StateOpen || cb.ConsecutiveFailures != 1 || cb.CooldownUntil.IsZero() {
		t.Errorf("after OPEN: state=%s fails=%d cooldownZero=%v", cb.State, cb.ConsecutiveFailures, cb.CooldownUntil.IsZero())
	}

	reloaded, err := store.Get(ctx, gw)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.State != gateway.StateOpen {
		t.Errorf("OPEN state should persist, got %s", reloaded.State)
	}

	if err := store.Transition(ctx, cb, gateway.StateHalfOpen); err != nil {
		t.Fatalf("OPEN -> HALF_OPEN: %v", err)
	}
	if err := store.Transition(ctx, cb, gateway.StateClosed); err != nil {
		t.Fatalf("HALF_OPEN -> CLOSED: %v", err)
	}
	if cb.ConsecutiveFailures != 0 {
		t.Errorf("CLOSED should reset failures, got %d", cb.ConsecutiveFailures)
	}
}

func TestCircuitBreaker_InvalidTransitionRejected(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	store := valkey.NewCircuitBreakerStore(c)
	ctx := context.Background()

	cb, err := store.Get(ctx, "razorpay")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Transition(ctx, cb, gateway.StateHalfOpen); err == nil {
		t.Fatal("CLOSED -> HALF_OPEN is illegal and must be rejected")
	}
}

func TestCircuitBreaker_AcquireProbeSingleFlight(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	store := valkey.NewCircuitBreakerStore(c)
	ctx := context.Background()

	first, err := store.AcquireProbe(ctx, "stripe", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AcquireProbe(ctx, "stripe", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !first || second {
		t.Errorf("probe should be single-flight: first=%v second=%v", first, second)
	}
}

func TestRateLimiter_DeniedDimensionDoesNotDrainOthers(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	const capacity = 1
	const rate = 0.0001

	if !rl.Allow(ctx, "u1", "m1", "shared-ip", capacity, rate).Allowed {
		t.Fatal("first request should be allowed")
	}
	if rl.Allow(ctx, "u2", "m2", "shared-ip", capacity, rate).Allowed {
		t.Fatal("second request on exhausted IP should be denied")
	}
	if !rl.Allow(ctx, "u2", "m2", "fresh-ip", capacity, rate).Allowed {
		t.Error("denial on one dimension must not drain other dimensions")
	}
}

func TestRateLimiter_NonPositiveRateDoesNotCrash(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	rl := rateLimiter(c)
	ctx := context.Background()

	if !rl.Allow(ctx, "u", "m", "ip", 2, 0).Allowed {
		t.Fatal("first request against a full bucket should be allowed even at rate 0")
	}
}

func TestResponseCache_GetPutRoundTrip(t *testing.T) {
	c := testsupport.RequireValkey(t)
	testsupport.FlushValkey(t, c)
	store := valkey.NewResponseCacheStore(c)
	ctx := context.Background()

	if hit, data, err := store.Get(ctx, "txn:abc"); err != nil || hit {
		t.Fatalf("fresh cache key should miss, hit=%v data=%q err=%v", hit, data, err)
	}

	body := []byte(`{"transaction_id":"txn:abc"}`)
	if err := store.Put(ctx, "txn:abc", body); err != nil {
		t.Fatalf("put: %v", err)
	}

	hit, data, err := store.Get(ctx, "txn:abc")
	if err != nil {
		t.Fatalf("get after put: %v", err)
	}
	if !hit {
		t.Fatal("expected a cache hit after put")
	}
	if string(data) != string(body) {
		t.Errorf("cached body mismatch:\n  want: %s\n  got:  %s", body, data)
	}
}
