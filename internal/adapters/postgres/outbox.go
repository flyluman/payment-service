package postgres

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/crownroutes/payment-service/internal/ports"
)

const defaultOutboxClaimTTL = 60 * time.Second
const defaultShardCount = 64

type OutboxWriter struct {
	db         *DB
	claimTTL   time.Duration
	shardCount int
	log        ports.Logger
}

type TenantWebhookWriter struct {
	db *DB
}

var (
	_ ports.OutboxWriter          = (*OutboxWriter)(nil)
	_ ports.TenantWebhookWriter = (*TenantWebhookWriter)(nil)
)

type txKey struct{}

func NewOutboxWriter(db *DB) *OutboxWriter {
	return &OutboxWriter{db: db, claimTTL: defaultOutboxClaimTTL, shardCount: defaultShardCount}
}

func (w *OutboxWriter) SetShardCount(n int) {
	if n > 1 {
		w.shardCount = n
	}
}

func (w *OutboxWriter) SetLogger(log ports.Logger) {
	w.log = log
}

// ShardIndex computes the shard index for a given aggregate ID.
// Deterministic: same aggregateID + shardCount always returns same shard.
func ShardIndex(aggregateID uuid.UUID, shardCount int) int {
	h := fnv.New32a()
	h.Write(aggregateID[:])
	return int(h.Sum32() % uint32(shardCount))
}

func (w *OutboxWriter) SetClaimTTL(d time.Duration) {
	if d > 0 {
		w.claimTTL = d
	}
}

func NewTenantWebhookWriter(db *DB) *TenantWebhookWriter {
	return &TenantWebhookWriter{db: db}
}

func WithTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

func (w *OutboxWriter) Write(ctx context.Context, event ports.OutboxEvent) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	id := event.ID
	if id == uuid.Nil {
		id = uuid.Must(uuid.NewV7())
	}

	var nextAttempt any
	if event.NextAttemptAt != nil {
		nextAttempt = *event.NextAttemptAt
	}

	version := event.EventVersion
	if version == 0 {
		version = 1
	}

	shard := ShardIndex(event.AggregateID, w.shardCount)

	_, err = tx.Exec(ctx, `INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload, shard_index,
     event_version, aggregate_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', NOW(), COALESCE($9, NOW()), 0)`,
		id, event.AggregateID, event.AggregateType, event.EventType,
		string(event.Payload), shard, version, event.AggregateVersion, nextAttempt,
	)
	if err != nil {
		return fmt.Errorf("outbox: write event %s: %w", event.EventType, err)
	}
	return nil
}

func (w *OutboxWriter) MarkPublished(ctx context.Context, id uuid.UUID, createdAt time.Time) error {
	tag, err := w.db.pool.Exec(ctx, `UPDATE outbox_events
SET status = 'PUBLISHED', published_at = NOW(), locked_at = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING'`, id, createdAt)
	if err != nil {
		return fmt.Errorf("outbox: mark published %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox: event %s not found or already published", id)
	}
	return nil
}

func (w *OutboxWriter) MarkFailed(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string, nextAttempt time.Time) error {
	tag, err := w.db.pool.Exec(ctx, `UPDATE outbox_events
SET
    status          = 'PENDING',
    attempts        = attempts + 1,
    last_error      = $3,
    next_attempt_at = $4,
    locked_at       = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING'`, id, createdAt, lastErr, nextAttempt)
	if err != nil {
		return fmt.Errorf("outbox: mark failed %s: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("outbox: event %s not found or not in PUBLISHING state", id)
	}
	return nil
}

func (w *OutboxWriter) MarkExhausted(ctx context.Context, id uuid.UUID, createdAt time.Time, lastErr string) error {
	return withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var event struct {
			AggregateID      uuid.UUID
			AggregateType    string
			EventType        string
			Payload          []byte
			EventVersion     int
			AggregateVersion int
		}

		err := tx.QueryRow(ctx, `UPDATE outbox_events
SET status = 'FAILED', last_error = $3, locked_at = NULL
WHERE id = $1 AND created_at = $2 AND status = 'PUBLISHING'
RETURNING aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version`, id, createdAt, lastErr).Scan(
			&event.AggregateID, &event.AggregateType,
			&event.EventType, &event.Payload, &event.EventVersion, &event.AggregateVersion,
		)
		if err != nil {
			return fmt.Errorf("outbox: exhaust event %s: %w", id, err)
		}

		_, err = tx.Exec(ctx, `INSERT INTO outbox_dead_letters
    (id, original_event_id, aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version, failure_reason, failed_at)
VALUES
    (gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8, NOW())`,
			id, event.AggregateID, event.AggregateType,
			event.EventType, string(event.Payload), event.EventVersion, event.AggregateVersion, lastErr,
		)
		if err != nil {
			return fmt.Errorf("outbox: write dead letter for %s: %w", id, err)
		}

		if w.log != nil {
			w.log.Error(ports.LogEventOutboxDeadLetter, map[string]any{
				"event_id":      id,
				"event_type":    event.EventType,
				"aggregate_id":  event.AggregateID,
				"aggregate_type": event.AggregateType,
				"failure_reason": lastErr,
			}, nil)
		}

		return nil
	})
}

func (w *OutboxWriter) PollPending(ctx context.Context, shards []int, batchSize int) ([]ports.PendingEvent, error) {
	claimTTLSec := int64(w.claimTTL.Seconds())

	if len(shards) == 0 {
		return nil, nil
	}
	shardArg := make([]int32, len(shards))
	for i, s := range shards {
		shardArg[i] = int32(s)
	}

	rows, err := w.db.pool.Query(ctx, `UPDATE outbox_events o
SET status = 'PUBLISHING', locked_at = NOW()
FROM (
    SELECT id, created_at
    FROM outbox_events
    WHERE shard_index = ANY($1::int[])
      AND next_attempt_at <= NOW()
      AND (
            status = 'PENDING'
         OR (status = 'PUBLISHING' AND locked_at < NOW() - make_interval(secs => $3))
      )
    ORDER BY attempts ASC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $2
) AS claimed
WHERE o.id = claimed.id AND o.created_at = claimed.created_at
RETURNING o.id, o.aggregate_id, o.aggregate_type, o.event_type,
          o.payload, o.event_version, o.aggregate_version, o.attempts, o.created_at`, shardArg, batchSize, claimTTLSec)
	if err != nil {
		return nil, fmt.Errorf("outbox: poll pending: %w", err)
	}
	defer rows.Close()

	var events []ports.PendingEvent
	for rows.Next() {
		var e ports.PendingEvent
		if err := rows.Scan(
			&e.ID, &e.AggregateID, &e.AggregateType,
			&e.EventType, &e.Payload, &e.EventVersion, &e.AggregateVersion, &e.Attempts, &e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan pending event: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (w *OutboxWriter) ReplayDeadLetter(ctx context.Context, deadLetterID uuid.UUID, actor, reason string) (uuid.UUID, error) {
	newEventID := uuid.Must(uuid.NewV7())

	err := withTx(ctx, w.db.pool, func(tx pgx.Tx) error {
		var dl struct {
			AggregateID      uuid.UUID
			AggregateType    string
			EventType        string
			Payload          []byte
			EventVersion     int
			AggregateVersion int
			ResolvedAt       *time.Time
		}

		err := tx.QueryRow(ctx, `SELECT aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version, resolved_at
FROM outbox_dead_letters
WHERE id = $1`, deadLetterID).Scan(
			&dl.AggregateID, &dl.AggregateType, &dl.EventType,
			&dl.Payload, &dl.EventVersion, &dl.AggregateVersion, &dl.ResolvedAt,
		)
		if err != nil {
			return fmt.Errorf("outbox: dead letter %s not found: %w", deadLetterID, err)
		}
		if dl.ResolvedAt != nil {
			return fmt.Errorf("outbox: dead letter %s already resolved", deadLetterID)
		}

		shard := ShardIndex(dl.AggregateID, w.shardCount)
		_, err = tx.Exec(ctx, `INSERT INTO outbox_events
    (id, aggregate_id, aggregate_type, event_type, payload, shard_index,
     event_version, aggregate_version, status, created_at, next_attempt_at, attempts)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, 'PENDING', NOW(), NOW(), 0)`,
			newEventID, dl.AggregateID, dl.AggregateType,
			dl.EventType, string(dl.Payload), shard, dl.EventVersion, dl.AggregateVersion,
		)
		if err != nil {
			return fmt.Errorf("outbox: re-enqueue dead letter %s: %w", deadLetterID, err)
		}

		_, err = tx.Exec(ctx, `UPDATE outbox_dead_letters
SET resolved_at = NOW(), resolved_by = $2
WHERE id = $1`, deadLetterID, actor)
		if err != nil {
			return fmt.Errorf("outbox: resolve dead letter %s: %w", deadLetterID, err)
		}

		return nil
	})

	return newEventID, err
}

func (w *OutboxWriter) ListDeadLetters(ctx context.Context, filter ports.DeadLetterFilter) ([]ports.DeadLetter, error) {
	query := `
		SELECT id, aggregate_id, aggregate_type, event_type, payload, event_version, aggregate_version,
		       COALESCE(error_message, failure_reason), attempts, COALESCE(created_at, failed_at), resolved_at, resolved_by
		FROM outbox_dead_letters WHERE 1=1
	`
	args := []any{}
	argIdx := 1

	if filter.Resolved != nil {
		if *filter.Resolved {
			query += " AND resolved_at IS NOT NULL"
		} else {
			query += " AND resolved_at IS NULL"
		}
	}
	if filter.EventType != nil {
		query += " AND event_type = $" + strconv.Itoa(argIdx)
		args = append(args, *filter.EventType)
		argIdx++
	}
	if filter.DateFrom != nil {
		query += " AND created_at >= $" + strconv.Itoa(argIdx)
		args = append(args, *filter.DateFrom)
		argIdx++
	}
	if filter.DateTo != nil {
		query += " AND created_at <= $" + strconv.Itoa(argIdx)
		args = append(args, *filter.DateTo)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	query += " LIMIT $" + strconv.Itoa(argIdx)
	args = append(args, limit+1)

	rows, err := w.db.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var letters []ports.DeadLetter
	for rows.Next() {
		var dl ports.DeadLetter
		if err := rows.Scan(&dl.ID, &dl.AggregateID, &dl.AggregateType, &dl.EventType,
			&dl.Payload, &dl.EventVersion, &dl.AggregateVersion,
			&dl.ErrorMessage, &dl.Attempts, &dl.CreatedAt, &dl.ResolvedAt, &dl.ResolvedBy); err != nil {
			return nil, err
		}
		letters = append(letters, dl)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(letters) > limit {
		letters = letters[:limit]
	}

	return letters, nil
}

func (w *TenantWebhookWriter) WriteDelivery(ctx context.Context, d ports.TenantWebhookDelivery) error {
	tx, err := txFromContext(ctx)
	if err != nil {
		return err
	}

	id := d.ID
	if id == uuid.Nil {
		id = uuid.Must(uuid.NewV7())
	}

	_, err = tx.Exec(ctx, `INSERT INTO tenant_webhook_deliveries
    (id, tenant_id, transaction_id, event_type, payload, endpoint_url,
     status, attempts, next_attempt_at, created_at)
VALUES
    ($1, $2, $3, $4, $5, $6, 'PENDING', 0, NOW(), NOW())`,
		id, d.TenantID, d.TransactionID,
		d.EventType, string(d.Payload), d.EndpointURL,
	)
	if err != nil {
		return fmt.Errorf("merchant webhook: write delivery: %w", err)
	}
	return nil
}

func txFromContext(ctx context.Context) (pgx.Tx, error) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	if !ok || tx == nil {
		return nil, fmt.Errorf("postgres: no transaction in context — call WithTx before writing to outbox")
	}
	return tx, nil
}

type Queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func queryer(ctx context.Context, pool *pgxpool.Pool) Queryer {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return pool
}

func readQueryer(ctx context.Context, db *DB) Queryer {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok && tx != nil {
		return tx
	}
	return db.ReadPool()
}

func withTx(ctx context.Context, pool interface {
	Begin(context.Context) (pgx.Tx, error)
}, fn func(pgx.Tx) error) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
