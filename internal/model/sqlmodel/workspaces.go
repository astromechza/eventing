package sqlmodel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"

	"github.com/astromechza/eventing/internal/model"
	"github.com/astromechza/eventing/internal/ulid"
)

func (s *SqlDataAccess) GetWorkspace(ctx context.Context, id string) (*model.Workspace, error) {
	var raw []byte
	err := s.pool.QueryRow(ctx, `SELECT raw FROM workspaces WHERE uid = $1`, id).Scan(&raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, model.ErrNotExist{Inner: err}
		}
		return nil, fmt.Errorf("failed to make select query: %w", err)
	}
	ws := new(model.Workspace)
	if err := proto.Unmarshal(raw, ws); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workspace: %w", err)
	}
	return ws, nil
}

func (s *SqlDataAccess) BulkGetWorkspace(ctx context.Context, ids []string) (map[string]*model.Workspace, error) {
	rows, err := s.pool.Query(ctx, `SELECT raw FROM workspaces WHERE uid = ANY($1)`, ids)
	if err != nil {
		return nil, fmt.Errorf("failed to make select query: %w", err)
	}
	defer rows.Close()
	output := make(map[string]*model.Workspace, len(ids))
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		ws := new(model.Workspace)
		if err := proto.Unmarshal(raw, ws); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workspace: %w", err)
		}
		output[ws.GetUid()] = ws
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}
	return output, nil
}

func (s *SqlDataAccess) CreateWorkspace(ctx context.Context, ws *model.Workspace) (*model.Workspace, error) {
	ws.Uid, ws.OldestRevision, ws.NewestRevision = ulid.New(), 1, 1
	raw, err := proto.Marshal(ws)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal workspace: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if _, err := tx.Exec(ctx, `INSERT INTO workspaces (uid, revision, raw) VALUES ($1, $2, $3)`, ws.Uid, ws.NewestRevision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, $4, false) RETURNING entry`, ws.Uid, ws.NewestRevision, time.Now().UTC(), raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, fmt.Sprintf("%s,%d,%v", ws.Uid, ws.NewestRevision, false)); err != nil {
		return nil, fmt.Errorf("failed to emit notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return ws, nil
}

func (s *SqlDataAccess) UpdateWorkspace(ctx context.Context, ws *model.Workspace) (*model.Workspace, error) {
	currentRevision := ws.NewestRevision
	ws.NewestRevision += 1
	raw, err := proto.Marshal(ws)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal workspace: %w", err)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if _, err := tx.Exec(ctx, `UPDATE workspaces SET revision = $3, raw = $4 WHERE uid = $1 AND revision = $2`, ws.Uid, currentRevision, ws.NewestRevision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, $4, false) RETURNING entry`, ws.Uid, ws.NewestRevision, time.Now().UTC(), raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, fmt.Sprintf("%s,%d,%v", ws.Uid, ws.NewestRevision, false)); err != nil {
		return nil, fmt.Errorf("failed to emit notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return ws, nil
}

func (s *SqlDataAccess) DeleteWorkspace(ctx context.Context, ws *model.Workspace) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if res, err := tx.Exec(ctx, `DELETE FROM workspaces WHERE uid = $1 AND revision = $2`, ws.Uid, ws.NewestRevision); err != nil {
		return fmt.Errorf("failed to delete workspace: %w", err)
	} else if res.RowsAffected() == 0 {
		return model.ErrIncorrectRevision{}
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, null, true) RETURNING entry`, ws.Uid, ws.NewestRevision+1, time.Now().UTC()).Scan(&entry); err != nil {
		return fmt.Errorf("failed to insert change entry: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, fmt.Sprintf("%s,%d,%v", ws.Uid, ws.NewestRevision+1, true)); err != nil {
		return fmt.Errorf("failed to emit notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}
