package sqlmodel

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/astromechza/eventing/internal/model"
)

const AdvisoryLockWorkspaceChanges = 10001
const NotificationChannel = "workspace_changes"

//go:embed migrations/*.sql
var embedMigrations embed.FS

type SqlDataAccess struct {
	pool *pgxpool.Pool

	lastNotifierId    atomic.Int64
	notifierStateLock sync.RWMutex
	notifiers         []notifier
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

var _ model.DataAccess = (*SqlDataAccess)(nil)
