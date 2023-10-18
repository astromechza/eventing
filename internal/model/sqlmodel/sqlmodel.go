package sqlmodel

import (
	"context"
	"database/sql"
	"embed"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/protobuf/proto"

	"github.com/astromechza/eventing/internal/model"
	"github.com/astromechza/eventing/internal/ulid"
)

const AdvisoryLockWorkspaceChanges = 10001

//go:embed migraions/*.sql
var embedMigrations embed.FS

type SqlDataAccess struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, connString string) (*SqlDataAccess, error) {
	slog.InfoContext(ctx, "Performing database migrations..")
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	defer db.Close()
	goose.SetBaseFS(embedMigrations)
	if err := goose.SetDialect("postgres"); err != nil {
		return nil, fmt.Errorf("failed to set dialect: %w", err)
	}
	if err := goose.Up(db, "migraions"); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	slog.InfoContext(ctx, "Connecting to database..")
	pool, err := pgxpool.New(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("failed to setup connection pool: %w", err)
	}

	return &SqlDataAccess{pool: pool}, nil
}

func (s *SqlDataAccess) Close() {
	slog.Info("Closing database pool")
	s.pool.Close()
}

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
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, false) RETURNING entry`, ws.Uid, ws.NewestRevision, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
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
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, $3, false) RETURNING entry`, ws.Uid, ws.NewestRevision, raw).Scan(&entry); err != nil {
		return nil, fmt.Errorf("failed to insert change entry: %w", err)
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
		return new(model.ErrIncorrectRevision)
	}

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, AdvisoryLockWorkspaceChanges); err != nil {
		return fmt.Errorf("failed to acquire changelog lock: %w", err)
	}

	var entry int64
	if err := tx.QueryRow(ctx, `INSERT INTO workspace_changes (entry, uid, revision, raw, tombstone) VALUES (nextval('workspace_change_entry'), $1, $2, null, true) RETURNING entry`, ws.Uid, ws.NewestRevision+1).Scan(&entry); err != nil {
		return fmt.Errorf("failed to insert change entry: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit: %w", err)
	}

	return nil
}

func (s *SqlDataAccess) ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int) (page *model.WorkspaceChangePage, err error) {
	baseEntry := uint64(0)
	if cursor != nil {
		if len(cursor) != 8 {
			return nil, fmt.Errorf("invalid cursor")
		}
		baseEntry = binary.BigEndian.Uint64(cursor)
	}
	if limit <= 0 {
		return nil, fmt.Errorf("list limit must be > 0")
	}
	rows, err := s.pool.Query(ctx, `SELECT entry, uid, revision, raw, tombstone FROM workspace_changes WHERE entry > $1 ORDER BY entry LIMIT $2`, baseEntry, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to select rows: %w", err)
	}
	defer rows.Close()

	page = &model.WorkspaceChangePage{}
	items := make([]*model.WorkspaceChange, 0)
	for rows.Next() {
		var entry int64
		var uid string
		var revision int64
		var raw []byte
		var tombstone bool
		if err = rows.Scan(&entry, &uid, &revision, &raw, &tombstone); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		change := new(model.WorkspaceChange)
		change.Reset()
		if tombstone {
			change.Entry = entry
			change.Payload = &model.WorkspaceChange_Tombstone{
				Tombstone: &model.WorkspaceTombstone{
					Uid:            uid,
					NewestRevision: revision,
				},
			}
			items = append(items, change)
		} else {
			var ws model.Workspace
			if err := proto.Unmarshal(raw, &ws); err != nil {
				return nil, fmt.Errorf("failed to unmarshal workspace: %w", err)
			}
			change.Entry = entry
			change.Payload = &model.WorkspaceChange_Workspace{
				Workspace: &ws,
			}
			items = append(items, change)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate rows: %w", err)
	}
	page.Changes = items

	if len(page.Changes) > 0 {
		out := make([]byte, 8)
		binary.BigEndian.PutUint64(out, uint64(page.Changes[len(page.Changes)-1].Entry))
		page.NextCursor = out
	} else {
		page.NextCursor = cursor
	}
	return page, nil
}

func (s *SqlDataAccess) StreamWorkspaceChanges(ctx context.Context, cursor []byte) (stream <-chan *model.WorkspaceChangePage, err error) {
	//TODO implement me
	panic("implement me")
}

var _ model.DataAccess = (*SqlDataAccess)(nil)
