package testsupport

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/adapters/valkey"
)

var (
	valkeyOnce   sync.Once
	sharedValkey *valkey.Client
	valkeyErr    error
)

func RequireValkey(t *testing.T) *valkey.Client {
	t.Helper()
	valkeyOnce.Do(func() { sharedValkey, valkeyErr = setupValkey() })
	if valkeyErr != nil {
		t.Skipf("integration valkey unavailable: %v\n  start it with: docker compose up -d postgres valkey", valkeyErr)
	}
	return sharedValkey
}

func FlushValkey(t *testing.T, c *valkey.Client) {
	t.Helper()
	ctx := context.Background()
	if err := c.RateLimit.Do(ctx, c.RateLimit.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("testsupport: flush rate-limit db: %v", err)
	}
	if err := c.Cache.Do(ctx, c.Cache.B().Flushdb().Build()).Error(); err != nil {
		t.Fatalf("testsupport: flush cache db: %v", err)
	}
}

func setupValkey() (*valkey.Client, error) {
	c, err := valkey.New(valkeyConfigFromEnv())
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func valkeyConfigFromEnv() config.ValkeyConfig {
	return config.ValkeyConfig{
		Addrs:        []string{getEnv("PAYMENT_TEST_VALKEY_ADDR", "localhost:6379")},
		RateLimitDB:  0,
		CacheDB:      1,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	}
}
