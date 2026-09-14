//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/adapters/encryption"
	"github.com/crownroutes/payment-service/internal/adapters/postgres"
	"github.com/crownroutes/payment-service/internal/domain/gateway"
	"github.com/crownroutes/payment-service/internal/testsupport"
)

func TestTenantConfigStore_EncryptDecryptRoundTrip(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "tenant_gateway_configs")
	ctx := context.Background()

	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i)
	}
	km, err := encryption.NewLocalKeyManager("test-kek", masterKey)
	if err != nil {
		t.Fatalf("key manager: %v", err)
	}
	env := encryption.NewEnvelope(km, encryption.Config{})
	store := postgres.NewTenantConfigStore(pg.DB, env)

	tenantID := uuid.New()
	rawConfig := `{"api_key":"sk_live_real_key_abc123","base_url":"https://api.stripe.com","webhook_secret":"whsec_xyz"}`
	cfg := &gateway.TenantGatewayConfig{
		TenantID:  tenantID,
		GatewayID: "stripe",
		Provider:  gateway.ProviderStripe,
		Config:    json.RawMessage(rawConfig),
		IsActive:  true,
	}

	version, err := store.Upsert(ctx, cfg)
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if version != 1 {
		t.Errorf("expected version 1, got %d", version)
	}

	got, err := store.Get(ctx, tenantID, "stripe")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Config) != rawConfig {
		t.Errorf("config mismatch:\n  want: %s\n  got:  %s", rawConfig, string(got.Config))
	}
	if got.Provider != gateway.ProviderStripe {
		t.Errorf("expected provider stripe, got %s", got.Provider)
	}
	if !got.IsActive {
		t.Error("expected config to be active")
	}
	if got.ConfigVersion != 1 {
		t.Errorf("expected config_version 1, got %d", got.ConfigVersion)
	}
}

func TestTenantConfigStore_EncryptedBytesDontContainPlaintext(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "tenant_gateway_configs")
	ctx := context.Background()

	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i)
	}
	km, err := encryption.NewLocalKeyManager("test-kek", masterKey)
	if err != nil {
		t.Fatalf("key manager: %v", err)
	}
	env := encryption.NewEnvelope(km, encryption.Config{})
	store := postgres.NewTenantConfigStore(pg.DB, env)

	tenantID := uuid.New()
	rawConfig := `{"api_key":"sk_live_secret_abc"}`
	cfg := &gateway.TenantGatewayConfig{
		TenantID:  tenantID,
		GatewayID: "stripe",
		Provider:  gateway.ProviderStripe,
		Config:    json.RawMessage(rawConfig),
		IsActive:  true,
	}

	if _, err := store.Upsert(ctx, cfg); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	var stored []byte
	err = pg.DB.Pool().QueryRow(ctx, `SELECT encrypted_config FROM tenant_gateway_configs WHERE tenant_id = $1 AND gateway_id = 'stripe'`, tenantID).Scan(&stored)
	if err != nil {
		t.Fatalf("read raw encrypted_config: %v", err)
	}
	if bytes.Contains(stored, []byte("sk_live_secret_abc")) {
		t.Error("encrypted_config BYTEA contains plaintext — encryption not effective")
	}
	if len(stored) == 0 {
		t.Fatal("encrypted_config is empty")
	}
}

func TestTenantConfigStore_WrongEncryptionKeyFails(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "tenant_gateway_configs")
	ctx := context.Background()

	masterKey := make([]byte, 32)
	for i := range masterKey {
		masterKey[i] = byte(i)
	}
	km, err := encryption.NewLocalKeyManager("test-kek", masterKey)
	if err != nil {
		t.Fatalf("key manager: %v", err)
	}
	env := encryption.NewEnvelope(km, encryption.Config{})
	store := postgres.NewTenantConfigStore(pg.DB, env)

	tenantID := uuid.New()
	rawConfig := `{"api_key":"sk_live_secret_xyz"}`
	cfg := &gateway.TenantGatewayConfig{
		TenantID:  tenantID,
		GatewayID: "stripe",
		Provider:  gateway.ProviderStripe,
		Config:    json.RawMessage(rawConfig),
		IsActive:  true,
	}

	if _, err := store.Upsert(ctx, cfg); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	wrongKey := make([]byte, 32)
	for i := range wrongKey {
		wrongKey[i] = byte(255 - i)
	}
	wrongKM, err := encryption.NewLocalKeyManager("test-kek", wrongKey)
	if err != nil {
		t.Fatalf("wrong key manager: %v", err)
	}
	wrongEnv := encryption.NewEnvelope(wrongKM, encryption.Config{})
	wrongStore := postgres.NewTenantConfigStore(pg.DB, wrongEnv)

	_, err = wrongStore.Get(ctx, tenantID, "stripe")
	if err == nil {
		t.Error("expected error when decrypting with wrong key")
	}
}

func TestTenantConfigStore_NoEncryptorUsesPlaintext(t *testing.T) {
	pg := testsupport.RequirePostgres(t)
	pg.Truncate(t, "tenant_gateway_configs")
	ctx := context.Background()

	store := postgres.NewTenantConfigStore(pg.DB, nil)

	tenantID := uuid.New()
	rawConfig := `{"api_key":"sk_test_dev_key"}`
	cfg := &gateway.TenantGatewayConfig{
		TenantID:  tenantID,
		GatewayID: "stripe",
		Provider:  gateway.ProviderStripe,
		Config:    json.RawMessage(rawConfig),
		IsActive:  true,
	}

	if _, err := store.Upsert(ctx, cfg); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, err := store.Get(ctx, tenantID, "stripe")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got.Config) != rawConfig {
		t.Errorf("config mismatch:\n  want: %s\n  got:  %s", rawConfig, string(got.Config))
	}

	var stored []byte
	err = pg.DB.Pool().QueryRow(ctx, `SELECT encrypted_config FROM tenant_gateway_configs WHERE tenant_id = $1 AND gateway_id = 'stripe'`, tenantID).Scan(&stored)
	if err != nil {
		t.Fatalf("read raw encrypted_config: %v", err)
	}
	if !bytes.Contains(stored, []byte("sk_test_dev_key")) {
		t.Error("without encryptor, encrypted_config should contain plaintext")
	}
}
