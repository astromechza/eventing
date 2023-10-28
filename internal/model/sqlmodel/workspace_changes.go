package sqlmodel

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/astromechza/eventing/internal/model"
)

func (s *SqlDataAccess) PeekLastWorkspaceChange(ctx context.Context) (cursor []byte, err error) {
	var lastEntry uint64
	if err := s.pool.QueryRow(ctx, `SELECT entry FROM workspace_changes ORDER BY entry DESC LIMIT 1`).Scan(&lastEntry); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, model.ErrNotExist{Inner: err}
		}
		return nil, fmt.Errorf("failed to collect last change: %w", err)
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, lastEntry)
	return out, nil
}

func (s *SqlDataAccess) ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int, limitUids []string) (page *model.WorkspaceChangePage, err error) {
	pgFilterUids := pgtype.Array[string]{Elements: limitUids, Dims: []pgtype.ArrayDimension{{int32(len(limitUids)), 1}}}

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

	rows, err := s.pool.Query(
		ctx,
		`SELECT entry, uid, revision, at, raw, tombstone 
		 FROM workspace_changes WHERE entry > $1 AND ($2 = 0 OR uid = ANY($3)) ORDER BY entry LIMIT $4`,
		baseEntry, len(limitUids), pgFilterUids, limit,
	)
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
					Uid:      uid,
					Revision: revision,
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
