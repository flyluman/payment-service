package valkey

import (
	"container/list"
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/ports"
)

const (
	defaultHealthCheckInterval    = 5 * time.Second
	defaultValkeyFailureThreshold = 3
)

type RateLimiter struct {
	client              valkey.Client
	cfg                 config.RateLimitConfig
	available           bool
	mu                  sync.RWMutex
	localBuckets        sync.Map
	lruOrder            *list.List
	lruMu               sync.Mutex
	lruCount            int
	logger              ports.Logger
	metrics             ports.MetricRecorder
	fallbackStartedAt   time.Time
	failureThreshold    int
	consecutiveFailures int32
	stopHealth          chan struct{}
	stopOnce            sync.Once
}

type lruBucketEntry struct {
	bucket  *localBucket
	lruNode *list.Element
}

type localBucket struct {
	mu         sync.Mutex
	tokens     float64
	capacity   float64
	lastRefill time.Time
	refillRate float64
}

type RateLimitResult struct {
	Allowed    bool
	RetryAfter time.Duration
}

func NewRateLimiter(c *Client, cfg config.RateLimitConfig, logger ports.Logger, metrics ports.MetricRecorder) *RateLimiter {
	r := &RateLimiter{
		client:           c.RateLimit,
		cfg:              cfg,
		available:        true,
		lruOrder:         list.New(),
		logger:           logger,
		metrics:          metrics,
		failureThreshold: defaultValkeyFailureThreshold,
		stopHealth:       make(chan struct{}),
	}

	interval := time.Duration(cfg.HealthCheckIntervalMS) * time.Millisecond
	if interval <= 0 {
		interval = defaultHealthCheckInterval
	}
	go r.runHealthCheck(interval)

	return r
}

func (r *RateLimiter) Close() {
	r.stopOnce.Do(func() { close(r.stopHealth) })
}

func (r *RateLimiter) runHealthCheck(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopHealth:
			return
		case <-ticker.C:
			if err := r.client.Do(context.Background(), r.client.B().Ping().Build()).Error(); err == nil {
				r.MarkAvailable()
			} else {
				r.markUnavailable()
			}
		}
	}
}

func bucketKey(namespace, entityID string) string {
	h := sha256.Sum256([]byte(entityID))
	return fmt.Sprintf("rate_limit:rl:%s:%x", namespace, h[:8])
}

const minRefillRate = 1e-9

func (r *RateLimiter) Allow(ctx context.Context, userID, tenantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	if ratePerSec <= 0 {
		ratePerSec = minRefillRate
	}

	r.mu.RLock()
	avail := r.available
	r.mu.RUnlock()

	if avail {
		return r.allowValkey(ctx, userID, tenantID, ip, capacity, ratePerSec)
	}
	return r.allowLocal(userID, tenantID, ip, capacity, ratePerSec)
}

func (r *RateLimiter) allowValkey(ctx context.Context, userID, tenantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	nowMs := time.Now().UnixMilli()

	keys := []string{
		bucketKey("user", userID),
		bucketKey("tenant", tenantID),
		bucketKey("ip", ip),
	}
	args := []string{
		strconv.FormatInt(capacity, 10),
		strconv.FormatFloat(ratePerSec, 'f', -1, 64),
		strconv.FormatInt(nowMs, 10),
		"1",
	}

	res := tokenBucketScript.Exec(ctx, r.client, keys, args)
	vals, err := res.AsIntSlice()
	if err != nil || len(vals) != 2 {
		r.recordValkeyFailure()
		return r.allowLocal(userID, tenantID, ip, capacity, ratePerSec)
	}
	r.recordValkeySuccess()

	if vals[0] == 0 {
		return RateLimitResult{Allowed: false, RetryAfter: time.Duration(vals[1]) * time.Millisecond}
	}
	return RateLimitResult{Allowed: true}
}

func (r *RateLimiter) recordValkeyFailure() {
	if int(atomic.AddInt32(&r.consecutiveFailures, 1)) >= r.failureThreshold {
		r.markUnavailable()
	}
}

func (r *RateLimiter) recordValkeySuccess() {
	atomic.StoreInt32(&r.consecutiveFailures, 0)
}

func (r *RateLimiter) allowLocal(userID, tenantID, ip string, capacity int64, ratePerSec float64) RateLimitResult {
	localCap := float64(capacity) * r.cfg.FallbackMultiplier

	ids := []string{userID, tenantID, ip}
	buckets := make([]*localBucket, len(ids))
	for i, id := range ids {
		buckets[i] = r.getOrCreateBucket(id, localCap, ratePerSec)
	}

	allowed := true
	var maxWait time.Duration
	now := time.Now()
	for _, b := range buckets {
		b.mu.Lock()
		b.capacity = localCap
		b.refillRate = ratePerSec
		elapsed := now.Sub(b.lastRefill).Seconds()
		b.tokens = min(b.capacity, b.tokens+elapsed*b.refillRate)
		b.lastRefill = now
		if b.tokens < 1 {
			allowed = false
			if b.refillRate > 0 {
				if wait := time.Duration((1-b.tokens)/b.refillRate*1000) * time.Millisecond; wait > maxWait {
					maxWait = wait
				}
			}
		}
		b.mu.Unlock()
	}

	if !allowed {
		return RateLimitResult{Allowed: false, RetryAfter: maxWait}
	}

	for _, b := range buckets {
		b.mu.Lock()
		if b.tokens >= 1 {
			b.tokens--
		}
		b.mu.Unlock()
	}
	return RateLimitResult{Allowed: true}
}

func (r *RateLimiter) getOrCreateBucket(id string, capacity, rate float64) *localBucket {
	if _, ok := r.localBuckets.Load(id); ok {
		r.lruMu.Lock()
		if cur, ok := r.localBuckets.Load(id); ok {
			entry := cur.(*lruBucketEntry)
			r.lruOrder.MoveToFront(entry.lruNode)
			r.lruMu.Unlock()
			return entry.bucket
		}
		r.lruMu.Unlock()
	}

	r.lruMu.Lock()
	defer r.lruMu.Unlock()

	if val, ok := r.localBuckets.Load(id); ok {
		entry := val.(*lruBucketEntry)
		r.lruOrder.MoveToFront(entry.lruNode)
		return entry.bucket
	}

	if r.lruCount >= r.cfg.LocalMaxBuckets {
		r.evictOldestLocked()
	}

	b := &localBucket{
		tokens:     capacity,
		capacity:   capacity,
		lastRefill: time.Now(),
		refillRate: rate,
	}

	node := r.lruOrder.PushFront(id)
	entry := &lruBucketEntry{bucket: b, lruNode: node}
	r.localBuckets.Store(id, entry)
	r.lruCount++
	return b
}

func (r *RateLimiter) evictOldestLocked() {
	if oldest := r.lruOrder.Back(); oldest != nil {
		id := oldest.Value.(string)
		r.localBuckets.Delete(id)
		r.lruOrder.Remove(oldest)
		r.lruCount--
	}
}

func (r *RateLimiter) markUnavailable() {
	r.mu.Lock()
	if !r.available {
		r.mu.Unlock()
		return
	}
	r.available = false
	r.fallbackStartedAt = time.Now().UTC()
	r.mu.Unlock()
	r.recordFallbackActivated()
}

func (r *RateLimiter) MarkAvailable() {
	var startedAt time.Time
	r.mu.Lock()
	if r.available {
		r.mu.Unlock()
		return
	}
	r.available = true
	startedAt = r.fallbackStartedAt
	r.fallbackStartedAt = time.Time{}
	r.mu.Unlock()

	atomic.StoreInt32(&r.consecutiveFailures, 0)
	r.recordFallbackRestored(startedAt)
}

func (r *RateLimiter) recordFallbackActivated() {
	if r.logger != nil {
		r.logger.Warn(ports.LogEventRateLimitFallback, map[string]any{"valkey_available": false})
	}
	if r.metrics != nil {
		r.metrics.Gauge(ports.MetricRateLimitValkeyAvailable, 0, nil)
		r.metrics.Increment(ports.MetricRateLimitFallbackActivationsTotal, nil)
	}
}

func (r *RateLimiter) recordFallbackRestored(startedAt time.Time) {
	durationSec := 0.0
	if !startedAt.IsZero() {
		durationSec = time.Since(startedAt).Seconds()
	}
	if r.logger != nil {
		fields := map[string]any{"valkey_available": true}
		if durationSec > 0 {
			fields["fallback_duration_sec"] = durationSec
		}
		r.logger.Info(ports.LogEventRateLimitRestored, fields)
	}
	if r.metrics != nil {
		r.metrics.Gauge(ports.MetricRateLimitValkeyAvailable, 1, nil)
		if durationSec > 0 {
			r.metrics.Histogram(ports.MetricRateLimitFallbackDurationSeconds, durationSec, nil)
		}
	}
}

func (r *RateLimiter) IsAvailable() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.available
}
