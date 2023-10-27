package model

import (
	"context"
	"fmt"
)

//go:generate protoc --go_out=. --go_opt=paths=source_relative types.proto

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
	ListWorkspaceChanges(ctx context.Context, cursor []byte, limit int, limitUids []string) (page *WorkspaceChangePage, err error)
	// ListenForWorkspaceUidChanges will notify the given channel with the changed uid when we know that the item
	// has received a change. The channel will be closed if the client does not keep up with the update rate or the
	// system is shutting down.
	ListenForWorkspaceUidChanges(ctx context.Context, filterUids []string, output chan string) (error, func())
	// CompactWorkspaceChanges will delete old workspace change entries for the given uid lower than revision. This is
	// a very naive compaction algorithm and relies on the caller having already acquired knowledge via ListWorkspaceChanges.
	CompactWorkspaceChanges(ctx context.Context, uid string, revision int64) (int, error)
}

// TODO: add created at to workspace change as well

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
