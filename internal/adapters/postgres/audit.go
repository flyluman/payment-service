package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/crownroutes/payment-service/internal/ports"
)

type AuditLogStore struct {
	db *DB
}

func NewAuditLogStore(db *DB) *AuditLogStore {
	return &AuditLogStore{db: db}
}

func (s *AuditLogStore) WriteEntry(ctx context.Context, entry *ports.AuditEntry) error {
	var metadata []byte
	if entry.Metadata != nil {
		var err error
		metadata, err = json.Marshal(entry.Metadata)
		if err != nil {
			return fmt.Errorf("audit: marshal metadata: %w", err)
		}
	}

	_, err := s.db.Pool().Exec(ctx,
		`INSERT INTO audit_log (id, transaction_id, event_type, actor, actor_type, previous_state, new_state, reason, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
		entry.ID,
		entry.TransactionID,
		entry.EventType,
		entry.Actor,
		entry.Actor,
		entry.PreviousState,
		entry.NewState,
		entry.Reason,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("audit: insert entry: %w", err)
	}
	return nil
}

func (s *AuditLogStore) ListByTransaction(ctx context.Context, transactionID uuid.UUID, limit int) ([]ports.AuditEntry, error) {
	if limit <= 0 {
		limit = 50
	}

	rows, err := s.db.Pool().Query(ctx,
		`SELECT id, transaction_id, event_type, actor, previous_state, new_state, reason, metadata, created_at
		 FROM audit_log
		 WHERE transaction_id = $1
		 ORDER BY created_at ASC
		 LIMIT $2`,
		transactionID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("audit: list by transaction: %w", err)
	}
	defer rows.Close()

	var entries []ports.AuditEntry
	for rows.Next() {
		var e ports.AuditEntry
		var metadata []byte
		if err := rows.Scan(
			&e.ID,
			&e.TransactionID,
			&e.EventType,
			&e.Actor,
			&e.PreviousState,
			&e.NewState,
			&e.Reason,
			&metadata,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("audit: scan entry: %w", err)
		}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &e.Metadata); err != nil {
				return nil, fmt.Errorf("audit: unmarshal metadata: %w", err)
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

var _ ports.AuditLogStore = (*AuditLogStore)(nil)
