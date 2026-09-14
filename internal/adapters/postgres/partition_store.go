package postgres

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PartitionStore struct {
	db       *DB
	lockConn *pgxpool.Conn
}

func NewPartitionStore(db *DB) *PartitionStore {
	return &PartitionStore{db: db}
}

var partitionNamePattern = regexp.MustCompile(`^outbox_\d{4}_W\d{2}$`)

const partitionBoundLayout = "2006-01-02 15:04:05-07"

const partitionAdvisoryLockID int64 = 42

func (s *PartitionStore) AcquireLock(ctx context.Context) (bool, error) {
	conn, err := s.db.pool.Acquire(ctx)
	if err != nil {
		return false, fmt.Errorf("partition: acquire connection: %w", err)
	}

	var ok bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, partitionAdvisoryLockID).Scan(&ok); err != nil {
		conn.Release()
		return false, fmt.Errorf("partition: acquire advisory lock: %w", err)
	}
	if !ok {
		conn.Release()
		return false, nil
	}

	s.lockConn = conn
	return true, nil
}

func (s *PartitionStore) ReleaseLock(ctx context.Context) error {
	if s.lockConn == nil {
		return nil
	}
	defer func() {
		s.lockConn.Release()
		s.lockConn = nil
	}()
	_, err := s.lockConn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, partitionAdvisoryLockID)
	return err
}

func (s *PartitionStore) CreatePartition(ctx context.Context, name string, start, end time.Time) error {
	if !partitionNamePattern.MatchString(name) {
		return fmt.Errorf("partition: invalid partition name %q", name)
	}
	sql := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF outbox_events FOR VALUES FROM ('%s') TO ('%s')",
		pgx.Identifier{name}.Sanitize(),
		start.UTC().Format(partitionBoundLayout),
		end.UTC().Format(partitionBoundLayout),
	)
	if _, err := s.db.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("partition: create %s: %w", name, err)
	}
	return nil
}

func (s *PartitionStore) ListPartitions(ctx context.Context) ([]string, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT c.relname
FROM pg_inherits i
JOIN pg_class c ON c.oid = i.inhrelid
JOIN pg_class p ON p.oid = i.inhparent
WHERE p.relname = 'outbox_events'
  AND c.relname <> 'outbox_default'`)
	if err != nil {
		return nil, fmt.Errorf("partition: list: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("partition: scan name: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}

func (s *PartitionStore) CountUnpublished(ctx context.Context, partition string) (int, error) {
	if !partitionNamePattern.MatchString(partition) {
		return 0, fmt.Errorf("partition: invalid partition name %q", partition)
	}
	sql := fmt.Sprintf("SELECT count(*) FROM %s WHERE status IN ('PENDING', 'FAILED', 'PUBLISHING')", pgx.Identifier{partition}.Sanitize())
	var n int
	if err := s.db.ReadPool().QueryRow(ctx, sql).Scan(&n); err != nil {
		return 0, fmt.Errorf("partition: count unpublished in %s: %w", partition, err)
	}
	return n, nil
}

func (s *PartitionStore) DetachPartition(ctx context.Context, name string) error {
	if !partitionNamePattern.MatchString(name) {
		return fmt.Errorf("partition: invalid partition name %q", name)
	}
	sql := fmt.Sprintf("ALTER TABLE outbox_events DETACH PARTITION %s", pgx.Identifier{name}.Sanitize())
	if _, err := s.db.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("partition: detach %s: %w", name, err)
	}
	return nil
}

func (s *PartitionStore) DropPartition(ctx context.Context, name string) error {
	if !partitionNamePattern.MatchString(name) {
		return fmt.Errorf("partition: invalid partition name %q", name)
	}
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", pgx.Identifier{name}.Sanitize())
	if _, err := s.db.pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("partition: drop %s: %w", name, err)
	}
	return nil
}

func (s *PartitionStore) LogAction(ctx context.Context, name, action string) error {
	if _, err := s.db.pool.Exec(ctx, `INSERT INTO partition_management_log (partition_name, action) VALUES ($1, $2)`, name, action); err != nil {
		return fmt.Errorf("partition: log %s/%s: %w", name, action, err)
	}
	return nil
}

func (s *PartitionStore) PartitionsDetachedBefore(ctx context.Context, cutoff time.Time) ([]string, error) {
	rows, err := s.db.ReadPool().Query(ctx, `SELECT DISTINCT l.partition_name
FROM partition_management_log l
WHERE l.action = 'detach'
  AND l.executed_at < $1
  AND NOT EXISTS (
    SELECT 1 FROM partition_management_log d
    WHERE d.partition_name = l.partition_name AND d.action = 'drop'
  )`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("partition: list detached before: %w", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("partition: scan detached name: %w", err)
		}
		names = append(names, n)
	}
	return names, rows.Err()
}
