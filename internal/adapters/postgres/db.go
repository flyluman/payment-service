package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/crownroutes/payment-service/config"
)

// DB holds connection pools for primary (write) and optional replica (read).
type DB struct {
	pool *pgxpool.Pool
	read *pgxpool.Pool
}

func New(ctx context.Context, cfg config.DatabaseConfig) (*DB, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("postgres: parse config: %w", err)
	}

	poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	poolCfg.MinConns = 0
	poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime
	poolCfg.MaxConnIdleTime = cfg.ConnMaxIdleTime
	poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod

	createCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(createCtx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	db := &DB{pool: pool}

	if replicaDSN, ok := cfg.ReplicaDSN(); ok {
		replicaCfg, err := pgxpool.ParseConfig(replicaDSN)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres: parse replica config: %w", err)
		}
		replicaCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		replicaCfg.MaxConns = int32(cfg.MaxOpenConns)
		replicaCfg.MinConns = 0
		replicaCfg.MaxConnLifetime = cfg.ConnMaxLifetime
		replicaCfg.MaxConnIdleTime = cfg.ConnMaxIdleTime
		replicaCfg.HealthCheckPeriod = cfg.HealthCheckPeriod

		replicaPool, err := pgxpool.NewWithConfig(createCtx, replicaCfg)
		if err != nil {
			pool.Close()
			return nil, fmt.Errorf("postgres: create replica pool: %w", err)
		}
		if err := replicaPool.Ping(ctx); err != nil {
			replicaPool.Close()
			pool.Close()
			return nil, fmt.Errorf("postgres: ping replica: %w", err)
		}
		db.read = replicaPool
	}

	return db, nil
}

func (db *DB) Pool() *pgxpool.Pool { return db.pool }
func (db *DB) ReadPool() *pgxpool.Pool {
	if db.read != nil {
		return db.read
	}
	return db.pool
}
func (db *DB) Close() {
	if db.read != nil {
		db.read.Close()
	}
	db.pool.Close()
}
func (db *DB) Ping(ctx context.Context) error { return db.pool.Ping(ctx) }
