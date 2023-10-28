package sqlmodel

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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
	ws.Uid, ws.Revision = ulid.New(), 1
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

	if _, err := tx.Exec(ctx, `INSERT INTO workspaces (uid, revision, raw) VALUES ($1, $2, $3)`, ws.Uid, ws.Revision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	at := ws.CreatedAt.AsTime()
	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, $4, false) RETURNING entry`, ws.Uid, ws.Revision, at, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
	}

	packedN, err := packNotification(entry, at, ws.Uid, ws.Revision, false)
	if err != nil {
		return nil, fmt.Errorf("failed to pack notification: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, packedN); err != nil {
		return nil, fmt.Errorf("failed to emit notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit: %w", err)
	}

	return ws, nil
}

func (s *SqlDataAccess) UpdateWorkspace(ctx context.Context, ws *model.Workspace) (*model.Workspace, error) {
	currentRevision := ws.Revision
	ws.Revision += 1
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

	if _, err := tx.Exec(ctx, `UPDATE workspaces SET revision = $3, raw = $4 WHERE uid = $1 AND revision = $2`, ws.Uid, currentRevision, ws.Revision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	at := time.Now().UTC()
	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, $4, false) RETURNING entry`, ws.Uid, ws.Revision, at, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
	}

	packedN, err := packNotification(entry, at, ws.Uid, ws.Revision, false)
	if err != nil {
		return nil, fmt.Errorf("failed to pack notification: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, packedN); err != nil {
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

	if res, err := tx.Exec(ctx, `DELETE FROM workspaces WHERE uid = $1 AND revision = $2`, ws.Uid, ws.Revision); err != nil {
		return fmt.Errorf("failed to delete workspace: %w", err)
	} else if res.RowsAffected() == 0 {
		return model.ErrIncorrectRevision{}
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	at := time.Now().UTC()
	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, at, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, null, true) RETURNING entry`, ws.Uid, ws.Revision+1, at).Scan(&entry); err != nil {
		return fmt.Errorf("failed to insert change entry: %w", err)
	}

	packedN, err := packNotification(entry, at, ws.Uid, ws.Revision+1, true)
	if err != nil {
		return fmt.Errorf("failed to pack notification: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, NotificationChannel, packedN); err != nil {
		return fmt.Errorf("failed to emit notification: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

func packNotification(entry int64, at time.Time, uid string, rev int64, tombstone bool) (string, error) {
	raw, err := proto.Marshal(&model.WorkspaceChange{
		Entry: entry, At: timestamppb.New(at),
		Payload: &model.WorkspaceChange_Notification{
			Notification: &model.WorkspaceNotification{
				Uid: uid, Revision: rev, Tombstone: tombstone,
			},
		},
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
