package valkey

import (
	"context"
	"errors"
	"time"

	"github.com/valkey-io/valkey-go"
)

const respCacheTTL = 5 * time.Minute

func respCacheKey(key string) string { return "resp_cache:" + key }

type ResponseCacheStore struct {
	client *Client
}

func NewResponseCacheStore(client *Client) *ResponseCacheStore {
	return &ResponseCacheStore{client: client}
}

func (s *ResponseCacheStore) Get(ctx context.Context, key string) (bool, []byte, error) {
	msg, err := s.client.Cache.Do(ctx, s.client.Cache.B().Get().Key(respCacheKey(key)).Build()).ToMessage()
	if err != nil {
		if errors.Is(err, valkey.Nil) {
			return false, nil, nil
		}
		return false, nil, err
	}
	raw, err := msg.AsBytes()
	if err != nil {
		return false, nil, err
	}
	return true, raw, nil
}

func (s *ResponseCacheStore) Put(ctx context.Context, key string, response []byte) error {
	return s.client.Cache.Do(ctx, s.client.Cache.B().Set().Key(respCacheKey(key)).Value(string(response)).ExSeconds(int64(respCacheTTL.Seconds())).Build()).Error()
}
