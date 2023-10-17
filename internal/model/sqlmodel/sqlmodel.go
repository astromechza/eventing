package sqlmodel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/astromechza/eventing/internal/model"
	"github.com/astromechza/eventing/internal/ulid"
)

const AdvisoryLockWorkspaceChanges = 10001

type SqlDataAccess struct {
	db     *sql.DB
	logger *slog.Logger
}

func (s *SqlDataAccess) GetWorkspace(ctx context.Context, id string) (*model.Workspace, error) {
	var raw []byte
	err := s.db.QueryRowContext(ctx, `SELECT raw FROM workspaces WHERE uid = $1`, id).Scan(&raw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, &model.ErrNotExist{Inner: err}
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
	rows, err := s.db.QueryContext(ctx, `SELECT raw FROM workspaces WHERE uid IN $1`, &ids)
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			s.logger.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if _, err := tx.ExecContext(ctx, `INSERT INTO workspaces (uid, revision, raw) VALUES ($1, $2, $3)`, ws.Uid, ws.NewestRevision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO workspaces_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, false) RETURNING entry`, ws.Uid, ws.NewestRevision, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			s.logger.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if _, err := tx.ExecContext(ctx, `UPDATE workspaces SET revison = $3, raw = $4 WHERE uid = $1 AND revision = $2`, ws.Uid, currentRevision, ws.NewestRevision, raw); err != nil {
		return nil, fmt.Errorf("failed to insert new workspace: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return nil, fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO workspaces_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, false) RETURNING entry`, ws.Uid, ws.NewestRevision, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
	}
	return ws, nil
}

func (s *SqlDataAccess) DeleteWorkspace(ctx context.Context, ws *model.Workspace) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
			s.logger.ErrorContext(ctx, "failed to rollback transaction", "err", err)
		}
	}()

	if res, err := tx.ExecContext(ctx, `DELETE FROM workspaces WHERE uid = $1 AND revision = $2`, ws.Uid, ws.NewestRevision); err != nil {
		return fmt.Errorf("failed to delete workspace: %w", err)
	} else if rows, _ := res.RowsAffected(); rows == 0 {
		return new(model.ErrIncorrectRevision)
	}

	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRowContext(ctx, `INSERT INTO workspaces_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, null, true) RETURNING entry`, ws.Uid, ws.NewestRevision+1).Scan(&entry); err != nil {
		return fmt.Errorf("failed to insert change entry: %w", err)
	}
	return nil
}

func (s *SqlDataAccess) ListWorkspaceChanges(ctx context.Context, cursor []byte) (page *model.WorkspaceChangePage, err error) {
	//TODO implement me
	panic("implement me")
}

func (s *SqlDataAccess) StreamWorkspaceChanges(ctx context.Context, cursor []byte) (stream <-chan *model.WorkspaceChangePage, err error) {
	//TODO implement me
	panic("implement me")
}

var _ model.DataAccess = (*SqlDataAccess)(nil)
