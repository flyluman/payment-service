package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/ports"
)

type Encryptor interface {
	Encrypt(ctx context.Context, plaintext, aad []byte) ([]byte, error)
	Decrypt(ctx context.Context, blob, aad []byte) ([]byte, error)
}

type TenantConfigStore struct {
	db  *DB
	enc Encryptor
}

func NewTenantConfigStore(db *DB, enc Encryptor) *TenantConfigStore {
	return &TenantConfigStore{db: db, enc: enc}
}

var _ ports.TenantConfigStore = (*TenantConfigStore)(nil)

func (s *TenantConfigStore) Get(ctx context.Context, tenantID uuid.UUID, gatewayID string) (*gateway.TenantGatewayConfig, error) {
	row := s.db.Pool().QueryRow(ctx, `SELECT tenant_id, gateway_id, provider, encrypted_config, config_version, is_active, created_at, updated_at
FROM tenant_gateway_configs
WHERE tenant_id = $1 AND gateway_id = $2`, tenantID, gatewayID)
	cfg, err := scanTenantGatewayConfig(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrGatewayNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("tenant_config: get %s/%s: %w", tenantID, gatewayID, err)
	}
	if err := s.decryptConfig(ctx, cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (s *TenantConfigStore) ListByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	return s.list(ctx, `SELECT tenant_id, gateway_id, provider, encrypted_config, config_version, is_active, created_at, updated_at
FROM tenant_gateway_configs
WHERE tenant_id = $1
ORDER BY gateway_id`, tenantID)
}

func (s *TenantConfigStore) ListActiveByTenant(ctx context.Context, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	return s.list(ctx, `SELECT tenant_id, gateway_id, provider, encrypted_config, config_version, is_active, created_at, updated_at
FROM tenant_gateway_configs
WHERE tenant_id = $1 AND is_active = true
ORDER BY gateway_id`, tenantID)
}

func (s *TenantConfigStore) ListByGateway(ctx context.Context, gatewayID string) ([]*gateway.TenantGatewayConfig, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT tenant_id, gateway_id, provider, encrypted_config, config_version, is_active, created_at, updated_at
FROM tenant_gateway_configs
WHERE gateway_id = $1
ORDER BY tenant_id`, gatewayID)
	if err != nil {
		return nil, fmt.Errorf("tenant_config: list by gateway %s: %w", gatewayID, err)
	}
	defer rows.Close()

	var configs []*gateway.TenantGatewayConfig
	for rows.Next() {
		cfg, err := scanTenantGatewayConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("tenant_config: scan: %w", err)
		}
		if err := s.decryptConfig(ctx, cfg); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return configs, rows.Err()
}

func (s *TenantConfigStore) list(ctx context.Context, query string, tenantID uuid.UUID) ([]*gateway.TenantGatewayConfig, error) {
	rows, err := s.db.ReadPool().Query(ctx, query, tenantID)
	if err != nil {
		return nil, fmt.Errorf("tenant_config: list %s: %w", tenantID, err)
	}
	defer rows.Close()

	var configs []*gateway.TenantGatewayConfig
	for rows.Next() {
		cfg, err := scanTenantGatewayConfig(rows)
		if err != nil {
			return nil, fmt.Errorf("tenant_config: scan: %w", err)
		}
		if err := s.decryptConfig(ctx, cfg); err != nil {
			return nil, err
		}
		configs = append(configs, cfg)
	}
	return configs, rows.Err()
}

func (s *TenantConfigStore) Upsert(ctx context.Context, cfg *gateway.TenantGatewayConfig) (int, error) {
	if !gateway.ValidProvider(cfg.Provider) {
		return 0, fmt.Errorf("tenant_config: invalid provider %q", cfg.Provider)
	}
	if _, err := gateway.ParseProviderConfig(cfg.Provider, cfg.Config); err != nil {
		return 0, fmt.Errorf("tenant_config: validate config: %w", err)
	}

	encrypted := []byte(cfg.Config)
	if s.enc != nil {
		var err error
		encrypted, err = s.enc.Encrypt(ctx, cfg.Config, aad(cfg.TenantID, cfg.GatewayID))
		if err != nil {
			return 0, fmt.Errorf("tenant_config: encrypt: %w", err)
		}
	}

	var version int
	configVersion := cfg.ConfigVersion
	if configVersion == 0 {
		configVersion = 1
	}
	err := s.db.pool.QueryRow(ctx, `INSERT INTO tenant_gateway_configs (tenant_id, gateway_id, provider, encrypted_config, config_version, is_active, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, NOW())
ON CONFLICT (tenant_id, gateway_id) DO UPDATE SET
    provider = EXCLUDED.provider,
    encrypted_config = EXCLUDED.encrypted_config,
    config_version = tenant_gateway_configs.config_version + 1,
    is_active = EXCLUDED.is_active,
    updated_at = NOW()
RETURNING config_version`,
		cfg.TenantID, cfg.GatewayID, string(cfg.Provider),
		encrypted, configVersion, cfg.IsActive,
	).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("tenant_config: upsert %s/%s: %w", cfg.TenantID, cfg.GatewayID, err)
	}
	return version, nil
}

func (s *TenantConfigStore) SetActive(ctx context.Context, tenantID uuid.UUID, gatewayID string, active bool) error {
	_, err := s.db.pool.Exec(ctx, `UPDATE tenant_gateway_configs
SET is_active = $3, updated_at = NOW()
WHERE tenant_id = $1 AND gateway_id = $2`, tenantID, gatewayID, active)
	if err != nil {
		return fmt.Errorf("tenant_config: set active %s/%s: %w", tenantID, gatewayID, err)
	}
	return nil
}

func (s *TenantConfigStore) Delete(ctx context.Context, tenantID uuid.UUID, gatewayID string) error {
	_, err := s.db.pool.Exec(ctx, `DELETE FROM tenant_gateway_configs
WHERE tenant_id = $1 AND gateway_id = $2`, tenantID, gatewayID)
	if err != nil {
		return fmt.Errorf("tenant_config: delete %s/%s: %w", tenantID, gatewayID, err)
	}
	return nil
}

func (s *TenantConfigStore) decryptConfig(ctx context.Context, cfg *gateway.TenantGatewayConfig) error {
	if s.enc == nil || len(cfg.Config) == 0 {
		return nil
	}
	plaintext, err := s.enc.Decrypt(ctx, cfg.Config, aad(cfg.TenantID, cfg.GatewayID))
	if err != nil {
		return fmt.Errorf("tenant_config: decrypt %s/%s: %w", cfg.TenantID, cfg.GatewayID, err)
	}
	cfg.Config = json.RawMessage(plaintext)
	return nil
}

func aad(tenantID uuid.UUID, gatewayID string) []byte {
	return []byte(tenantID.String() + ":" + gatewayID)
}

func scanTenantGatewayConfig(row interface {
	Scan(dest ...any) error
}) (*gateway.TenantGatewayConfig, error) {
	var cfg gateway.TenantGatewayConfig
	var provider string
	err := row.Scan(
		&cfg.TenantID, &cfg.GatewayID, &provider,
		&cfg.Config, &cfg.ConfigVersion, &cfg.IsActive,
		&cfg.CreatedAt, &cfg.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	cfg.Provider = gateway.Provider(provider)
	return &cfg, nil
}
