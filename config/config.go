package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type AppConfig struct {
	ServiceName    string `mapstructure:"service_name"`
	Environment    string `mapstructure:"environment"`
	ServiceVersion string `mapstructure:"service_version"`
	Port           int    `mapstructure:"port"`
	MtlsStrictMode bool   `mapstructure:"mtls_strict_mode"`
}

type StartupConfig struct {
	ConnectMaxAttempts    int           `mapstructure:"connect_max_attempts"`
	ConnectAttemptTimeout time.Duration `mapstructure:"connect_attempt_timeout_sec"`
	ConnectBackoff        time.Duration `mapstructure:"connect_backoff_sec"`
}

type DatabaseConfig struct {
	PrimaryHost       string        `mapstructure:"primary_host"`
	ReplicaHost       string        `mapstructure:"replica_host"`
	Port              int           `mapstructure:"port"`
	Name              string        `mapstructure:"name"`
	User              string        `mapstructure:"user"`
	Password          string        `mapstructure:"password"`
	SSLMode           string        `mapstructure:"ssl_mode"`
	SearchPath        string        `mapstructure:"search_path"`
	MaxOpenConns      int           `mapstructure:"max_open_conns"`
	MaxIdleConns      int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime   time.Duration `mapstructure:"conn_max_lifetime_sec"`
	ConnMaxIdleTime   time.Duration `mapstructure:"conn_max_idle_time_sec"`
	HealthCheckPeriod time.Duration `mapstructure:"health_check_period_sec"`
}

func (d DatabaseConfig) ReplicaDSN() (string, bool) {
	if d.ReplicaHost == "" {
		return "", false
	}
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		quoteDSNValue(d.ReplicaHost), d.Port, quoteDSNValue(d.Name),
		quoteDSNValue(d.User), quoteDSNValue(d.Password), quoteDSNValue(d.SSLMode),
	)
	if d.SearchPath != "" {
		dsn += " search_path=" + quoteDSNValue(d.SearchPath)
	}
	return dsn, true
}

type ValkeyConfig struct {
	Addrs        []string      `mapstructure:"addrs"`
	RateLimitDB  int           `mapstructure:"rate_limit_db"`
	CacheDB      int           `mapstructure:"cache_db"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout_sec"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout_sec"`
	WriteTimeout time.Duration `mapstructure:"write_timeout_sec"`
}

type SNSConfig struct {
	PaymentEventsTopic string `mapstructure:"payment_events_topic"`
}

type AWSConfig struct {
	Region string `mapstructure:"region"`
}

type OutboxConfig struct {
	ShardCount                int   `mapstructure:"shard_count"`
	WALLagAlertThresholdMB    int64 `mapstructure:"wal_lag_alert_threshold_mb"`
	WALLagCriticalThresholdMB int64 `mapstructure:"wal_lag_critical_threshold_mb"`
	PollIntervalSec           int   `mapstructure:"poll_interval_sec"`
	MaxAttempts               int   `mapstructure:"max_attempts"`
	BatchSize                 int   `mapstructure:"batch_size"`
	ClaimTTLSec               int   `mapstructure:"claim_ttl_sec"`
	Publisher                 string `mapstructure:"publisher"`
	SNSAggregateVersionAttr   bool   `mapstructure:"sns_aggregate_version_attr"`
	WorkerIndex               int    `mapstructure:"worker_index"`
	WorkerCount               int    `mapstructure:"worker_count"`
}

type RateLimitConfig struct {
	FallbackMultiplier  float64 `mapstructure:"fallback_multiplier"`
	LocalMaxBuckets     int     `mapstructure:"local_max_buckets"`
	HealthCheckIntervalMS int   `mapstructure:"health_check_interval_ms"`
	Capacity            int64   `mapstructure:"capacity"`
	RefillPerSec        float64 `mapstructure:"refill_per_sec"`
}

type ObservabilityConfig struct {
	Backend      string `mapstructure:"backend"`
	LogLevel     string `mapstructure:"log_level"`
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`
	OTLPProtocol string `mapstructure:"otlp_protocol"`
}

type SecurityConfig struct {
	EncryptionKey          string `mapstructure:"encryption_key"`
	TLSCertFile            string `mapstructure:"tls_cert_file"`
	TLSKeyFile             string `mapstructure:"tls_key_file"`
	TLSCAFile              string `mapstructure:"tls_ca_file"`
	CertRefreshIntervalSec int    `mapstructure:"cert_refresh_interval_sec"`
	ServiceTokens          string `mapstructure:"service_tokens"`
	OpsTokens              string `mapstructure:"ops_tokens"`
}

type GatewayConfig struct {
	HTTPTimeout             time.Duration `mapstructure:"http_timeout_sec"`
	CircuitBreakerThreshold int           `mapstructure:"circuit_breaker_threshold"`
}

type SMTPConfig struct {
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	From     string `mapstructure:"from"`
}

type SMSConfig struct {
	Provider string `mapstructure:"provider"`
	APIKey   string `mapstructure:"api_key"`
	From     string `mapstructure:"from"`
}

type NotificationConfig struct {
	SMTP SMTPConfig `mapstructure:"smtp"`
	SMS  SMSConfig  `mapstructure:"sms"`
}

type JobsConfig struct {
	LeaseExpiryIntervalSec          int `mapstructure:"lease_expiry_interval_sec"`
	IdempotencyProcessingTimeoutSec int `mapstructure:"idempotency_processing_timeout_sec"`
	PartitionWeeksAhead             int `mapstructure:"partition_weeks_ahead"`
	PartitionRetentionWeeks         int `mapstructure:"partition_retention_weeks"`
	PartitionDropAfterDays          int `mapstructure:"partition_drop_after_days"`
	ReconciliationIntervalSec       int `mapstructure:"reconciliation_interval_sec"`
}

type Config struct {
	App           AppConfig           `mapstructure:"app"`
	Startup       StartupConfig       `mapstructure:"startup"`
	Database      DatabaseConfig      `mapstructure:"database"`
	Valkey        ValkeyConfig        `mapstructure:"valkey"`
	SNS           SNSConfig           `mapstructure:"sns"`
	AWS           AWSConfig           `mapstructure:"aws"`
	Outbox        OutboxConfig        `mapstructure:"outbox"`
	RateLimit     RateLimitConfig     `mapstructure:"rate_limit"`
	Gateway       GatewayConfig       `mapstructure:"gateway"`
	Notification  NotificationConfig  `mapstructure:"notification"`
	Observability ObservabilityConfig `mapstructure:"observability"`
	Security      SecurityConfig      `mapstructure:"security"`
	Jobs          JobsConfig          `mapstructure:"jobs"`
}

func LoadConfig() (*Config, error) {
	if _, err := os.Stat(".env"); err == nil {
		if err := godotenv.Load(".env"); err != nil {
			return nil, fmt.Errorf("config: load .env: %w", err)
		}
	}

	v := viper.New()

	if configPath := os.Getenv("CONFIG_PATH"); configPath != "" {
		v.SetConfigFile(configPath)
		if filepath.Ext(configPath) == "" {
			// viper cannot infer the config type from an extensionless path.
			v.SetConfigType("yaml")
		}

		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			v.SetConfigName("config")
			v.SetConfigType("yaml")
			v.AddConfigPath(".")
			if err := v.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("config: read: %w", err)
			}
		} else if err != nil {
			return nil, fmt.Errorf("config: stat %q: %w", configPath, err)
		} else {
			if err := v.ReadInConfig(); err != nil {
				return nil, fmt.Errorf("config: read: %w", err)
			}
		}
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: read: %w", err)
		}
	}

	v.SetDefault("outbox.shard_count", 64)
	v.SetDefault("app.service_name", "payment-service")

	// Defaults for fields validated by Validate(); these must be set before
	// Unmarshal so a config that omits them still passes validation.
	v.SetDefault("outbox.wal_lag_alert_threshold_mb", 100)
	v.SetDefault("outbox.wal_lag_critical_threshold_mb", 200)
	v.SetDefault("outbox.poll_interval_sec", 5)
	v.SetDefault("outbox.claim_ttl_sec", 30)
	v.SetDefault("outbox.max_attempts", 10)
	v.SetDefault("outbox.batch_size", 100)
	v.SetDefault("outbox.worker_count", 1)
	v.SetDefault("outbox.worker_index", 0)
	v.SetDefault("rate_limit.fallback_multiplier", 0.9)

	envBindings := map[string]string{
		"app.service_name":                 "SERVICE_NAME",
		"app.environment":                  "ENVIRONMENT",
		"app.service_version":              "SERVICE_VERSION",
		"app.port":                         "PORT",
		"app.mtls_strict_mode":             "MTLS_STRICT_MODE",
		"database.primary_host":            "DATABASE_PRIMARY_HOST",
		"database.replica_host":            "DATABASE_REPLICA_HOST",
		"database.port":                    "DATABASE_PORT",
		"database.name":                    "DATABASE_NAME",
		"database.user":                    "DATABASE_USER",
		"database.password":                "DATABASE_PASSWORD",
		"database.ssl_mode":                "DATABASE_SSL_MODE",
		"database.max_open_conns":          "DATABASE_MAX_OPEN_CONNS",
		"database.max_idle_conns":          "DATABASE_MAX_IDLE_CONNS",
		"database.conn_max_lifetime_sec":   "DATABASE_CONN_MAX_LIFETIME",
		"database.conn_max_idle_time_sec":  "DATABASE_CONN_MAX_IDLE_TIME",
		"database.health_check_period_sec": "DATABASE_HEALTH_CHECK_PERIOD",
		"valkey.addrs":                     "VALKEY_ADDRS",
		"valkey.rate_limit_db":             "VALKEY_RATE_LIMIT_DB",
		"valkey.cache_db":                  "VALKEY_CACHE_DB",
		"valkey.dial_timeout_sec":          "VALKEY_DIAL_TIMEOUT",
		"valkey.read_timeout_sec":          "VALKEY_READ_TIMEOUT",
		"valkey.write_timeout_sec":         "VALKEY_WRITE_TIMEOUT",
		"sns.payment_events_topic":         "SNS_PAYMENT_EVENTS_TOPIC",
		"aws.region":                       "AWS_REGION",
		"outbox.wal_lag_alert_threshold_mb":    "OUTBOX_RELAY_WAL_LAG_ALERT_THRESHOLD_MB",
		"outbox.wal_lag_critical_threshold_mb": "OUTBOX_RELAY_WAL_LAG_CRITICAL_THRESHOLD_MB",
		"outbox.poll_interval_sec":         "OUTBOX_RELAY_POLL_INTERVAL_SEC",
		"outbox.max_attempts":              "OUTBOX_RELAY_MAX_ATTEMPTS",
		"outbox.batch_size":                "OUTBOX_RELAY_BATCH_SIZE",
		"outbox.claim_ttl_sec":             "OUTBOX_RELAY_CLAIM_TTL_SEC",
		"outbox.publisher":                 "OUTBOX_PUBLISHER",
		"outbox.shard_count":                "OUTBOX_SHARD_COUNT",
		"outbox.sns_aggregate_version_attr": "OUTBOX_SNS_AGGREGATE_VERSION_ATTRIBUTE",
		"outbox.worker_index":              "RELAY_WORKER_INDEX",
		"outbox.worker_count":              "RELAY_WORKER_COUNT",
		"rate_limit.fallback_multiplier":   "RATE_LIMIT_FALLBACK_MULTIPLIER",
		"rate_limit.local_max_buckets":     "RATE_LIMIT_LOCAL_MAX_BUCKETS",
		"rate_limit.health_check_interval_ms": "RATE_LIMIT_VALKEY_HEALTH_CHECK_INTERVAL_MS",
		"rate_limit.capacity":              "RATE_LIMIT_CAPACITY",
		"rate_limit.refill_per_sec":        "RATE_LIMIT_REFILL_PER_SEC",
		"gateway.http_timeout_sec":          "GATEWAY_HTTP_TIMEOUT",
		"gateway.circuit_breaker_threshold":  "CIRCUIT_BREAKER_FAILURE_THRESHOLD",
		"notification.smtp.host":            "SMTP_HOST",
		"notification.smtp.port":            "SMTP_PORT",
		"notification.smtp.username":        "SMTP_USERNAME",
		"notification.smtp.password":        "SMTP_PASSWORD",
		"notification.smtp.from":            "SMTP_FROM",
		"notification.sms.provider":         "SMS_PROVIDER",
		"notification.sms.api_key":          "SMS_API_KEY",
		"notification.sms.from":             "SMS_FROM",

		"observability.backend":            "OBSERVABILITY_BACKEND",
		"observability.log_level":          "LOG_LEVEL",
		"observability.otlp_endpoint":      "OTLP_ENDPOINT",
		"observability.otlp_protocol":      "OTLP_PROTOCOL",
		"security.encryption_key":          "ENCRYPTION_KEY",
		"security.tls_cert_file":           "TLS_CERT_FILE",
		"security.tls_key_file":            "TLS_KEY_FILE",
		"security.tls_ca_file":             "TLS_CA_FILE",
		"security.cert_refresh_interval_sec": "TLS_CERT_REFRESH_INTERVAL_SECONDS",
		"security.service_tokens":          "SERVICE_TOKENS",
		"security.ops_tokens":              "OPS_TOKENS",
		"jobs.lease_expiry_interval_sec":   "LEASE_EXPIRY_INTERVAL_SECONDS",
		"jobs.idempotency_processing_timeout_sec": "LEASE_REAPER_IDEMPOTENCY_TIMEOUT_SEC",
		"jobs.partition_weeks_ahead":       "PARTITION_WEEKS_AHEAD",
		"jobs.partition_retention_weeks":   "PARTITION_RETENTION_WEEKS",
		"jobs.partition_drop_after_days":   "PARTITION_DROP_AFTER_DAYS",
		"jobs.reconciliation_interval_sec": "RECONCILIATION_INTERVAL_SECONDS",
		"startup.connect_max_attempts":     "STARTUP_CONNECT_MAX_ATTEMPTS",
		"startup.connect_attempt_timeout_sec": "STARTUP_CONNECT_ATTEMPT_TIMEOUT",
		"startup.connect_backoff_sec":      "STARTUP_CONNECT_BACKOFF",
	}
	for key, envVar := range envBindings {
		_ = v.BindEnv(key, envVar)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		intSecondsToDurationHook(),
		mapstructure.StringToTimeDurationHookFunc(),
	))); err != nil {
		return nil, fmt.Errorf("config: unmarshal: %w", err)
	}

	if err := Validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func intSecondsToDurationHook() mapstructure.DecodeHookFuncType {
	return func(from reflect.Type, to reflect.Type, data any) (any, error) {
		if to != reflect.TypeOf(time.Duration(0)) {
			return data, nil
		}
		switch v := data.(type) {
		case int:
			return time.Duration(v) * time.Second, nil
		case int64:
			return time.Duration(v) * time.Second, nil
		case float64:
			return time.Duration(v) * time.Second, nil
		case string:
			// Env var values arrive as strings. A bare number is interpreted as
			// seconds; anything else falls through to StringToTimeDurationHookFunc.
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return time.Duration(n) * time.Second, nil
			}
		}
		return data, nil
	}
}

func Validate(c *Config) error {
	var errs []string

	validEnvs := map[string]bool{"prod": true, "staging": true, "dev": true}
	if !validEnvs[c.App.Environment] {
		errs = append(errs, "app.environment must be one of: prod, staging, dev")
	}

	validLogLevels := map[string]bool{
		"error": true, "warn": true, "info": true, "debug": true, "trace": true,
	}
	if !validLogLevels[c.Observability.LogLevel] {
		errs = append(errs, "observability.log_level must be one of: error, warn, info, debug, trace")
	}

	validOTLPProtocols := map[string]bool{"http/protobuf": true, "grpc": true}
	if c.Observability.OTLPEndpoint != "" && !validOTLPProtocols[c.Observability.OTLPProtocol] {
		errs = append(errs, `observability.otlp_protocol must be "http/protobuf" or "grpc"`)
	}

	if c.Outbox.WALLagAlertThresholdMB >= c.Outbox.WALLagCriticalThresholdMB {
		errs = append(errs, "outbox.wal_lag_alert_threshold_mb must be < outbox.wal_lag_critical_threshold_mb")
	}

	if c.Outbox.ClaimTTLSec <= c.Outbox.PollIntervalSec {
		errs = append(errs, "outbox.claim_ttl_sec must be > outbox.poll_interval_sec")
	}

	switch c.Outbox.Publisher {
	case "", "log", "sns":
	default:
		errs = append(errs, "outbox.publisher must be 'log' or 'sns'")
	}
	if c.Outbox.Publisher == "sns" && c.SNS.PaymentEventsTopic == "" {
		errs = append(errs, "sns.payment_events_topic is required when outbox.publisher=sns")
	}

	if c.Outbox.WorkerCount < 1 {
		errs = append(errs, "outbox.worker_count must be >= 1")
	}
	if c.Outbox.WorkerIndex < 0 || c.Outbox.WorkerIndex >= c.Outbox.WorkerCount {
		errs = append(errs, "outbox.worker_index must be in [0, worker_count)")
	}

	if c.RateLimit.FallbackMultiplier <= 0 || c.RateLimit.FallbackMultiplier > 1 {
		errs = append(errs, "rate_limit.fallback_multiplier must be in (0, 1]")
	}

	if len(errs) > 0 {
		return errors.New("config validation failed:\n  - " + strings.Join(errs, "\n  - "))
	}
	return nil
}

func quoteDSNValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `'`, `\'`)
	return "'" + v + "'"
}

func (d DatabaseConfig) DSN() string {
	dsn := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		quoteDSNValue(d.PrimaryHost), d.Port, quoteDSNValue(d.Name),
		quoteDSNValue(d.User), quoteDSNValue(d.Password), quoteDSNValue(d.SSLMode),
	)
	if d.SearchPath != "" {
		dsn += " search_path=" + quoteDSNValue(d.SearchPath)
	}
	return dsn
}

func ParseKeyValues(raw string) map[string]string {
	out := map[string]string{}
	if raw == "" {
		return out
	}
	for _, pair := range strings.Split(raw, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) == 2 && kv[0] != "" {
			out[kv[0]] = kv[1]
		}
	}
	return out
}

func SplitEnv(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
