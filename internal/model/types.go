package model

import (
	"context"
	"fmt"
)

//go:generate protoc --proto_path=../../spec --go_out=. --go_opt=paths=source_relative --go_opt=Mevents.proto=github.com/astromechza/eventing/internal/model ../../spec/events.proto

// DataAccess
//
// To stream _all_ the changes back over this API to a consumer in the right order, you can do the following:
// 1. Start listening to changes on the uid space
// 2. Peak the head of the stream to get a cursor if you don't have one already
// 3. Wait until a notification is received or until a max time elapses (N seconds)
// 4. Pull all changes since the cursor and merge, note the max event you see
// 5. Ignore any notifications that are lower than the max event from (4)
// 6. Go to (3)
// 7. If the connect breaks, resume from the last page and cursor you received
type DataAccess interface {
	// GetWorkspace returns the workspace with the given uid, we assume permission checks have _already_ been done on
	// this uid. This method returns the workspace, an ErrNotExist, or some other error.
	GetWorkspace(ctx context.Context, id string) (*Workspace, error)
	// BulkGetWorkspace returns the details of the given workspaces by uid in bulk. This is because there is no higher
	// level container in which to list workspaces. We assume permission checks have already been done against this
	// list of workspaces. This method returns a map from uid to workspace, or an error. It may return both if some
	// workspaces returned an ErrNotExist error.
	BulkGetWorkspace(ctx context.Context, id []string) (map[string]*Workspace, error)
	// CreateWorkspace inserts a new workspace into the data access layer.
	CreateWorkspace(ctx context.Context, ws *Workspace) (*Workspace, error)
	// UpdateWorkspace updates a workspace in place. It will return an error if the workspace doesn't already exist at
	// the given revision number.
	UpdateWorkspace(ctx context.Context, ws *Workspace) (*Workspace, error)
	// DeleteWorkspace deletes the workspace permanently. It will return an error if the workspace doesn't already exist
	// at the given revision number.
	DeleteWorkspace(ctx context.Context, ws *Workspace) error

	// PeekLastWorkspaceChange returns the last cursor available or an error
	PeekLastWorkspaceChange(ctx context.Context) (cursor []byte, err error)
	// ListWorkspaceChanges returns a list of changes for all workspaces since the last cursor.
	ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int, filterUids []string) (page *WorkspaceChangePage, err error)
	// CompactWorkspaceChanges will delete old workspace change entries for the given uid lower than revision. This is
	// a very naive compaction algorithm and relies on the caller having already acquired knowledge via ListWorkspaceChanges.
	CompactWorkspaceChanges(ctx context.Context, uid string, revision int64) (int, error)

	// RegisterForWorkspaceChanges will notify the given channel with the changed row when we know that the item
	// has received a change. The channel will be closed if the client does not keep up with the update rate or the
	// system is shutting down.
	RegisterForWorkspaceChanges(ctx context.Context, filterUids []string, output chan *WorkspaceChange) (error, func())
}

type WorkspaceChangePage struct {
	Changes    []*WorkspaceChange
	NextCursor []byte
}

type ErrNotExist struct {
	Inner error
}

func (e ErrNotExist) Error() string {
	if e.Inner == nil {
		return "not found"
	}
	return fmt.Sprintf("not found: %v", e.Inner)
}

func (e ErrNotExist) String() string {
	return e.Error()
}

func (e ErrNotExist) Unwrap() error {
	return e.Inner
}

type ErrIncorrectRevision struct {
	Inner error
}

func (e ErrIncorrectRevision) Error() string {
	if e.Inner == nil {
		return "incorrect revision"
	}
	return fmt.Sprintf("incorrect revision: %v", e.Inner)
}

func (e ErrIncorrectRevision) String() string {
	return e.Error()
}

func (e ErrIncorrectRevision) Unwrap() error {
	return e.Inner
}
