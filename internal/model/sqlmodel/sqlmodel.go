package sqlmodel

import (
	"context"
	"database/sql"
	"embed"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/astromechza/eventing/internal/model"
	"github.com/astromechza/eventing/internal/ulid"
)

const AdvisoryLockWorkspaceChanges = 10001
const NotificationChannel = "workspace_changes"

//go:embed migrations/*.sql
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
	if err := goose.Up(db, "migrations"); err != nil {
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

func (s *SqlDataAccess) ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int, limitUids []string) (page *model.WorkspaceChangePage, err error) {
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
	args := []interface{}{baseEntry, limit}
	parts := []string{
		`SELECT entry, uid, revision, at, raw, tombstone 
		 FROM workspace_changes WHERE entry > $1 `,
		`ORDER BY entry LIMIT $2`,
	}
	if len(limitUids) > 0 {
		args = append(args, limitUids)
		parts = append(parts[:2], parts[1:]...)
		parts[1] = fmt.Sprintf(`AND uid = ANY($%d)`, len(args))
	}
	rows, err := s.pool.Query(ctx, strings.Join(parts, " "), args...)
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
		var at time.Time
		var raw []byte
		var tombstone bool
		if err = rows.Scan(&entry, &uid, &revision, &at, &raw, &tombstone); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		change := new(model.WorkspaceChange)
		change.Reset()
		change.Entry = entry
		change.At = timestamppb.New(at)
		if tombstone {
			change.Payload = &model.WorkspaceChange_Tombstone{
				Tombstone: &model.WorkspaceTombstone{
					Uid:            uid,
					NewestRevision: revision,
				},
			}
		} else {
			var ws model.Workspace
			if err := proto.Unmarshal(raw, &ws); err != nil {
				return nil, fmt.Errorf("failed to unmarshal workspace: %w", err)
			}
			change.Payload = &model.WorkspaceChange_Workspace{
				Workspace: &ws,
			}
		}
		items = append(items, change)
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

func (s *SqlDataAccess) CompactWorkspaceChanges(ctx context.Context, uid string, revision int64) (int, error) {
	if res, err := s.pool.Exec(ctx, `DELETE FROM workspace_changes WHERE uid = $1 AND revision < $2`, uid, revision); err != nil {
		return 0, fmt.Errorf("failed to delete from workspace change log: %w", err)
	} else {
		return int(res.RowsAffected()), nil
	}
}

var _ model.DataAccess = (*SqlDataAccess)(nil)
