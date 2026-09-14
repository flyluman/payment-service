package gateways

import (
	"context"
	"fmt"
	"sync"

	"github.com/crownroutes/payment-service/internal/ports"
)

type Registry struct {
	mu       sync.RWMutex
	adapters map[string]ports.GatewayAdapter
	fetchers map[string]ports.SettlementReportFetcher
}

func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[string]ports.GatewayAdapter),
		fetchers: make(map[string]ports.SettlementReportFetcher),
	}
}

func (r *Registry) Register(gatewayID string, adapter ports.GatewayAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[gatewayID] = adapter
}

func (r *Registry) RegisterSettlementFetcher(gatewayID string, fetcher ports.SettlementReportFetcher) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fetchers[gatewayID] = fetcher
}

func (r *Registry) Get(gatewayID string) (ports.GatewayAdapter, error) {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("gateways: no adapter registered for %q", gatewayID)
	}
	return adapter, nil
}

func (r *Registry) SettlementFetcher(gatewayID string) (ports.SettlementReportFetcher, bool) {
	r.mu.RLock()
	fetcher, ok := r.fetchers[gatewayID]
	r.mu.RUnlock()
	return fetcher, ok
}

func (r *Registry) WebhookParser(gatewayID string) (ports.GatewayWebhookParser, bool) {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	parser, ok := adapter.(ports.GatewayWebhookParser)
	return parser, ok
}

// WebhookRequiresRefetch reports whether a gateway's webhook payloads are
// unsigned and must therefore be re-verified via CheckStatus.
func (r *Registry) WebhookRequiresRefetch(gatewayID string) bool {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	if u, ok := adapter.(ports.UnsignedWebhookGateway); ok {
		return u.UnsignedWebhooks()
	}
	return false
}

// CheckStatus delegates to the gateway adapter's status check, resolving
// per-tenant credentials from the request.
func (r *Registry) CheckStatus(ctx context.Context, gatewayID string, req ports.GatewayStatusRequest) (*ports.GatewayPaymentResponse, error) {
	r.mu.RLock()
	adapter, ok := r.adapters[gatewayID]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("gateways: no adapter registered for %q", gatewayID)
	}
	return adapter.CheckStatus(ctx, req)
}

func (r *Registry) IDs() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.adapters))
	for id := range r.adapters {
		ids = append(ids, id)
	}
	return ids
}
