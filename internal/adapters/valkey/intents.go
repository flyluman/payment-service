package valkey

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"
)

const (
	defaultIntentTTL = 60 * time.Second
	intentTTLBuffer  = 30 * time.Second
)

type IntentStore struct {
	client valkey.Client
}

func NewIntentStore(c *Client) *IntentStore {
	return &IntentStore{client: c.Cache}
}

func intentsKey(gatewayID string) string { return "gateway:intents:" + gatewayID }

func (s *IntentStore) EnterProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = defaultIntentTTL
	}
	key := intentsKey(gatewayID)
	expiry := time.Now().UTC().Add(ttl).Unix()

	cmds := make(valkey.Commands, 0, 2)
	cmds = append(cmds, s.client.B().Zadd().Key(key).ScoreMember().ScoreMember(float64(expiry), txnID.String()).Build())
	cmds = append(cmds, s.client.B().Expire().Key(key).Seconds(int64((ttl + intentTTLBuffer).Seconds())).Build())
	for _, r := range s.client.DoMulti(ctx, cmds...) {
		if err := r.Error(); err != nil {
			return fmt.Errorf("intents: enter %s: %w", gatewayID, err)
		}
	}
	return nil
}

func (s *IntentStore) ExitProcessing(ctx context.Context, gatewayID string, txnID uuid.UUID) error {
	if err := s.client.Do(ctx, s.client.B().Zrem().Key(intentsKey(gatewayID)).Member(txnID.String()).Build()).Error(); err != nil {
		return fmt.Errorf("intents: exit %s: %w", gatewayID, err)
	}
	return nil
}

func (s *IntentStore) ActiveIntents(ctx context.Context, gatewayIDs []string) (map[string]int, error) {
	out := make(map[string]int, len(gatewayIDs))
	if len(gatewayIDs) == 0 {
		return out, nil
	}

	now := strconv.FormatInt(time.Now().UTC().Unix(), 10)

	// Build a pipeline: ZREMRANGEBYSCORE + ZCARD for each gateway.
	cmds := make(valkey.Commands, 0, len(gatewayIDs)*2)
	for _, g := range gatewayIDs {
		key := intentsKey(g)
		cmds = append(cmds, s.client.B().Zremrangebyscore().Key(key).Min("0").Max(now).Build())
		cmds = append(cmds, s.client.B().Zcard().Key(key).Build())
	}

	results := s.client.DoMulti(ctx, cmds...)
	for i, g := range gatewayIDs {
		// ZCARD result is at index 2*i+1 (the second command in each pair).
		card, err := results[2*i+1].AsInt64()
		if err != nil {
			return nil, fmt.Errorf("intents: active count %s: %w", g, err)
		}
		out[g] = int(card)
	}
	return out, nil
}
