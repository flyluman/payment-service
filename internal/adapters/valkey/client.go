package valkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/crownroutes/payment-service/config"
	"github.com/crownroutes/payment-service/internal/domain/transaction"
	"github.com/crownroutes/payment-service/internal/ports"
)

const (
	txnCacheTTL    = 5 * time.Second
	configCacheTTL = 30 * time.Second
)

// Client wraps two valkey-go clients: one for the rate-limit DB and one for the
// cache DB. The split mirrors the old go-redis UniversalClient setup.
type Client struct {
	RateLimit valkey.Client
	Cache     valkey.Client
}

func New(cfg config.ValkeyConfig) (*Client, error) {
	if len(cfg.Addrs) == 0 {
		return nil, fmt.Errorf("valkey: at least one address is required")
	}

	rl, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:      cfg.Addrs,
		SelectDB:         cfg.RateLimitDB,
		ConnWriteTimeout: cfg.WriteTimeout,
		ForceSingleClient: true,
		DisableCache:     true,
	})
	if err != nil {
		return nil, fmt.Errorf("valkey: rate_limit client: %w", err)
	}

	ca, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:      cfg.Addrs,
		SelectDB:         cfg.CacheDB,
		ConnWriteTimeout: cfg.WriteTimeout,
		ForceSingleClient: true,
		DisableCache:     true,
	})
	if err != nil {
		rl.Close()
		return nil, fmt.Errorf("valkey: cache client: %w", err)
	}

	return &Client{RateLimit: rl, Cache: ca}, nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.RateLimit.Do(ctx, c.RateLimit.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("valkey: rate_limit ping: %w", err)
	}
	if err := c.Cache.Do(ctx, c.Cache.B().Ping().Build()).Error(); err != nil {
		return fmt.Errorf("valkey: cache ping: %w", err)
	}
	return nil
}

func (c *Client) Close() error {
	c.RateLimit.Close()
	c.Cache.Close()
	return nil
}

func txnCacheKey(id uuid.UUID) string { return "txn:" + id.String() }

func (c *Client) Get(ctx context.Context, id uuid.UUID) (*transaction.Txn, error) {
	msg, err := c.Cache.Do(ctx, c.Cache.B().Get().Key(txnCacheKey(id)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return nil, nil
		}
		return nil, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	var txn transaction.Txn
	if err := json.Unmarshal(raw, &txn); err != nil {
		return nil, err
	}
	return &txn, nil
}

func (c *Client) Set(ctx context.Context, txn *transaction.Txn) error {
	raw, err := json.Marshal(txn)
	if err != nil {
		return err
	}
	return c.Cache.Do(ctx, c.Cache.B().Set().Key(txnCacheKey(txn.ID)).Value(string(raw)).ExSeconds(int64(txnCacheTTL.Seconds())).Build()).Error()
}

func (c *Client) Del(ctx context.Context, id uuid.UUID) error {
	return c.Cache.Do(ctx, c.Cache.B().Del().Key(txnCacheKey(id)).Build()).Error()
}

func configCacheKey(gatewayID string) string  { return "cfg:gw:" + gatewayID }
func timeoutCacheKey(gatewayID, method string) string {
	return "cfg:to:" + gatewayID + ":" + method
}

func (c *Client) GetGatewayConfig(ctx context.Context, gatewayID string) (*ports.GatewayConfig, error) {
	msg, err := c.Cache.Do(ctx, c.Cache.B().Get().Key(configCacheKey(gatewayID)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return nil, nil
		}
		return nil, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}
	var cfg ports.GatewayConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Client) SetGatewayConfig(ctx context.Context, cfg *ports.GatewayConfig) error {
	raw, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return c.Cache.Do(ctx, c.Cache.B().Set().Key(configCacheKey(cfg.GatewayID)).Value(string(raw)).ExSeconds(int64(configCacheTTL.Seconds())).Build()).Error()
}

func (c *Client) DelGatewayConfig(ctx context.Context, gatewayID string) error {
	return c.Cache.Do(ctx, c.Cache.B().Del().Key(configCacheKey(gatewayID)).Build()).Error()
}

func (c *Client) GetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) (time.Duration, error) {
	msg, err := c.Cache.Do(ctx, c.Cache.B().Get().Key(timeoutCacheKey(gatewayID, paymentMethod)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return 0, nil
		}
		return 0, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return 0, err
	}
	var sec int
	if err := json.Unmarshal(raw, &sec); err != nil {
		return 0, err
	}
	return time.Duration(sec) * time.Second, nil
}

func (c *Client) SetProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string, timeout time.Duration) error {
	raw, err := json.Marshal(int(timeout.Seconds()))
	if err != nil {
		return err
	}
	return c.Cache.Do(ctx, c.Cache.B().Set().Key(timeoutCacheKey(gatewayID, paymentMethod)).Value(string(raw)).ExSeconds(int64(configCacheTTL.Seconds())).Build()).Error()
}

func (c *Client) DelProcessingTimeout(ctx context.Context, gatewayID, paymentMethod string) error {
	return c.Cache.Do(ctx, c.Cache.B().Del().Key(timeoutCacheKey(gatewayID, paymentMethod)).Build()).Error()
}

const gwRefCacheTTL = time.Hour

func gwRefCacheKey(gatewayID, reference string) string {
	return "gwref:" + gatewayID + ":" + reference
}

func (c *Client) GetGatewayRef(ctx context.Context, gatewayID, reference string) (uuid.UUID, error) {
	msg, err := c.Cache.Do(ctx, c.Cache.B().Get().Key(gwRefCacheKey(gatewayID, reference)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return uuid.Nil, nil
		}
		return uuid.Nil, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return uuid.Nil, err
	}
	return uuid.ParseBytes(raw)
}

func (c *Client) SetGatewayRef(ctx context.Context, gatewayID, reference string, txnID uuid.UUID) error {
	return c.Cache.Do(ctx, c.Cache.B().Set().Key(gwRefCacheKey(gatewayID, reference)).Value(txnID.String()).ExSeconds(int64(gwRefCacheTTL.Seconds())).Build()).Error()
}
